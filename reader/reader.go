package reader

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

	"golang.org/x/sync/errgroup"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/marshal"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
)

type ParquetReaderOptions struct {
	CaseInsensitive bool
}

type ParquetReader struct {
	SchemaHandler *schema.SchemaHandler
	NP            int64 // parallel number
	Footer        *parquet.FileMetaData
	PFile         source.ParquetFileReader

	ColumnBuffers map[string]*ColumnBufferType

	// One reader can only read one type objects
	ObjType        reflect.Type
	ObjPartialType reflect.Type

	// Determines whether case sensitivity is enabled
	CaseInsensitive bool
}

// Create a parquet reader: obj is a object with schema tags or a JSON schema string
func NewParquetReader(pFile source.ParquetFileReader, obj any, np int64, opts ...ParquetReaderOptions) (*ParquetReader, error) {
	var caseInsensitive bool
	if len(opts) > 0 {
		caseInsensitive = opts[0].CaseInsensitive
	}

	var err error
	res := new(ParquetReader)
	res.NP = np
	res.PFile = pFile
	res.CaseInsensitive = caseInsensitive
	if err = res.ReadFooter(); err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}
	res.ColumnBuffers = make(map[string]*ColumnBufferType)

	if obj != nil {
		if sa, ok := obj.(string); ok {
			err = res.SetSchemaHandlerFromJSON(sa)
			if err != nil {
				return res, fmt.Errorf("set schema from JSON: %w", err)
			}
			return res, nil

		} else if sa, ok := obj.([]*parquet.SchemaElement); ok {
			res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(sa)
		} else {
			if res.SchemaHandler, err = schema.NewSchemaHandlerFromStruct(obj); err != nil {
				return res, fmt.Errorf("build schema handler: %w", err)
			}

			res.ObjType = reflect.TypeOf(obj).Elem()
		}
	} else {
		res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(res.Footer.Schema)
	}

	res.RenameSchema()
	for i := range len(res.SchemaHandler.SchemaElements) {
		schema := res.SchemaHandler.SchemaElements[i]
		if schema == nil {
			continue
		}
		if schema.GetNumChildren() == 0 {
			if pathStr, exists := res.SchemaHandler.IndexMap[int32(i)]; exists {
				if res.ColumnBuffers[pathStr], err = NewColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr); err != nil {
					return res, fmt.Errorf("init column buffer for %s: %w", pathStr, err)
				}
			}
		}
	}

	return res, nil
}

func (pr *ParquetReader) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error

	if pr.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return fmt.Errorf("parse JSON schema: %w", err)
	}

	pr.RenameSchema()
	for i := range len(pr.SchemaHandler.SchemaElements) {
		schemaElement := pr.SchemaHandler.SchemaElements[i]
		if schemaElement.GetNumChildren() == 0 {
			pathStr := pr.SchemaHandler.IndexMap[int32(i)]
			if pr.ColumnBuffers[pathStr], err = NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr); err != nil {
				return fmt.Errorf("init column buffer for %s: %w", pathStr, err)
			}
		}
	}
	return nil
}

// Rename schema name to inname
func (pr *ParquetReader) RenameSchema() {
	if pr.SchemaHandler == nil || pr.SchemaHandler.Infos == nil || pr.Footer == nil || pr.Footer.Schema == nil {
		return
	}

	for i := range len(pr.SchemaHandler.Infos) {
		if i < len(pr.Footer.Schema) && pr.Footer.Schema[i] != nil && pr.SchemaHandler.Infos[i] != nil {
			pr.Footer.Schema[i].Name = pr.SchemaHandler.Infos[i].InName
		}
	}

	exPathToInPath := make(map[string]string)
	if pr.CaseInsensitive {
		for exPath, inPath := range pr.SchemaHandler.ExPathToInPath {
			lowerCaseKey := strings.ToLower(exPath)
			exPathToInPath[lowerCaseKey] = inPath
		}
	} else {
		exPathToInPath = pr.SchemaHandler.ExPathToInPath
	}

	if pr.Footer.RowGroups == nil {
		return
	}

	for _, rowGroup := range pr.Footer.RowGroups {
		if rowGroup == nil || rowGroup.Columns == nil {
			continue
		}
		for _, chunk := range rowGroup.Columns {
			if chunk == nil || chunk.MetaData == nil {
				continue
			}
			exPath := append([]string{pr.SchemaHandler.GetRootExName()}, chunk.MetaData.GetPathInSchema()...)
			exPathStr := common.PathToStr(exPath)

			if pr.CaseInsensitive {
				exPathStr = strings.ToLower(exPathStr)
			}

			if inPathStr, exists := exPathToInPath[exPathStr]; exists {
				inPath := common.StrToPath(inPathStr)[1:]
				chunk.MetaData.PathInSchema = inPath
			}
		}
	}
}

func (pr *ParquetReader) GetNumRows() int64 {
	return pr.Footer.GetNumRows()
}

// Get the footer size
func (pr *ParquetReader) GetFooterSize() (uint32, error) {
	if pr.PFile == nil {
		return 0, fmt.Errorf("PFile is nil")
	}

	var err error
	buf := make([]byte, 4)
	if _, err = pr.PFile.Seek(-8, io.SeekEnd); err != nil {
		return 0, err
	}
	if _, err = io.ReadFull(pr.PFile, buf); err != nil {
		return 0, err
	}
	size := binary.LittleEndian.Uint32(buf)
	return size, err
}

// Read footer from parquet file
func (pr *ParquetReader) ReadFooter() error {
	size, err := pr.GetFooterSize()
	if err != nil {
		return fmt.Errorf("get footer size: %w", err)
	}
	if _, err = pr.PFile.Seek(-(int64)(8+size), io.SeekEnd); err != nil {
		return fmt.Errorf("seek to footer: %w", err)
	}
	pr.Footer = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{})
	thriftReader := thrift.NewStreamTransportR(pr.PFile)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
	protocol := pf.GetProtocol(bufferReader)
	if err := pr.Footer.Read(context.TODO(), protocol); err != nil {
		return fmt.Errorf("read footer: %w", err)
	}
	return nil
}

// Skip rows of parquet file
func (pr *ParquetReader) SkipRows(num int64) error {
	var err error
	if num <= 0 {
		return nil
	}

	// Ensure column buffers exist
	for _, pathStr := range pr.SchemaHandler.ValueColumns {
		if _, ok := pr.ColumnBuffers[pathStr]; !ok {
			if pr.ColumnBuffers[pathStr], err = NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr); err != nil {
				return fmt.Errorf("failed to create column buffer for %s: %w", pathStr, err)
			}
		}
	}

	// Use errgroup with a semaphore to cap concurrency at pr.NP
	g, _ := errgroup.WithContext(context.Background())
	sem := make(chan struct{}, max(1, int(pr.NP)))
	for key := range pr.ColumnBuffers {
		pathStr := key
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			if _, err := pr.ColumnBuffers[pathStr].SkipRowsWithError(int64(num)); err != nil && err == io.EOF {
				return nil
			} else if err != nil {
				return fmt.Errorf("failed to skip rows for column %s: %w", pathStr, err)
			}
			return nil
		})
	}

	// Wait for all tasks; returns first non-nil error
	return g.Wait()
}

// Read rows of parquet file and unmarshal all to dst
func (pr *ParquetReader) Read(dstInterface any) error {
	return pr.read(dstInterface, "")
}

// Read maxReadNumber objects
func (pr *ParquetReader) ReadByNumber(maxReadNumber int) ([]any, error) {
	var err error
	if pr.ObjType == nil {
		if pr.ObjType, err = pr.SchemaHandler.GetType(pr.SchemaHandler.GetRootInName()); err != nil {
			return nil, fmt.Errorf("get type: %w", err)
		}
	}

	vs := reflect.MakeSlice(reflect.SliceOf(pr.ObjType), maxReadNumber, maxReadNumber)
	res := reflect.New(vs.Type())
	res.Elem().Set(vs)

	if err = pr.Read(res.Interface()); err != nil {
		return nil, fmt.Errorf("read by number: %w", err)
	}

	ln := res.Elem().Len()
	ret := make([]any, ln)
	for i := range ln {
		ret[i] = res.Elem().Index(i).Interface()
	}

	return ret, nil
}

// Read rows of parquet file and unmarshal all to dst
func (pr *ParquetReader) ReadPartial(dstInterface any, prefixPath string) error {
	prefixPath, err := pr.SchemaHandler.ConvertToInPathStr(prefixPath)
	if err != nil {
		return fmt.Errorf("convert path: %w", err)
	}

	if err := pr.read(dstInterface, prefixPath); err != nil {
		return fmt.Errorf("read partial: %w", err)
	}
	return nil
}

// Read maxReadNumber partial objects
func (pr *ParquetReader) ReadPartialByNumber(maxReadNumber int, prefixPath string) ([]any, error) {
	if maxReadNumber < 0 {
		return nil, fmt.Errorf("maxReadNumber cannot be negative: %d", maxReadNumber)
	}

	var err error
	if pr.ObjPartialType == nil {
		if pr.ObjPartialType, err = pr.SchemaHandler.GetType(prefixPath); err != nil {
			return nil, fmt.Errorf("get type for prefix: %w", err)
		}
	}

	vs := reflect.MakeSlice(reflect.SliceOf(pr.ObjPartialType), maxReadNumber, maxReadNumber)
	res := reflect.New(vs.Type())
	res.Elem().Set(vs)

	if err = pr.ReadPartial(res.Interface(), prefixPath); err != nil {
		return nil, fmt.Errorf("read partial by number: %w", err)
	}

	ln := res.Elem().Len()
	ret := make([]any, ln)
	for i := range ln {
		ret[i] = res.Elem().Index(i).Interface()
	}

	return ret, nil
}

// Read rows of parquet file with a prefixPath
func (pr *ParquetReader) read(dstInterface any, prefixPath string) error {
	if dstInterface == nil {
		return fmt.Errorf("dstInterface cannot be nil")
	}

	tmap := make(map[string]*layout.Table)
	var locker sync.Mutex
	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	num := reflect.ValueOf(dstInterface).Elem().Len()
	if num <= 0 {
		return nil
	}

	// Worker pool with closed task channel and first-error capture
	taskChan := make(chan string)
	var wgCols sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	worker := func() {
		defer wgCols.Done()
		for pathStr := range taskChan {
			cb := pr.ColumnBuffers[pathStr]
			table, _, rerr := cb.ReadRowsWithError(int64(num))
			if rerr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = rerr
				}
				errMu.Unlock()
				continue
			}
			// Merge into shared map
			locker.Lock()
			if _, ok := tmap[pathStr]; ok {
				tmap[pathStr].Merge(table)
			} else {
				tmap[pathStr] = layout.NewTableFromTable(table)
				tmap[pathStr].Merge(table)
			}
			locker.Unlock()
			// No direct error from ReadRows; errors surface during unmarshal below
			_ = pathStr
		}
	}

	for i := int64(0); i < pr.NP; i++ {
		wgCols.Add(1)
		go worker()
	}

	for key := range pr.ColumnBuffers {
		if strings.HasPrefix(key, prefixPath) {
			taskChan <- key
		}
	}
	close(taskChan)
	wgCols.Wait()

	dstList := make([]any, pr.NP)
	delta := (int64(num) + pr.NP - 1) / pr.NP

	var wg sync.WaitGroup
	for c := range pr.NP {
		bgn := c * delta
		end := bgn + delta
		if end > int64(num) {
			end = int64(num)
		}
		if bgn >= int64(num) {
			bgn, end = int64(num), int64(num)
		}
		wg.Add(1)
		go func(b, e, index int) {
			defer func() {
				wg.Done()
			}()

			dstList[index] = reflect.New(reflect.SliceOf(ot)).Interface()
			if err2 := marshal.Unmarshal(&tmap, b, e, dstList[index], pr.SchemaHandler, prefixPath); err2 != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err2
				}
				errMu.Unlock()
			}
		}(int(bgn), int(end), int(c))
	}

	wg.Wait()

	dstValue := reflect.ValueOf(dstInterface).Elem()
	dstValue.SetLen(0)
	for _, dst := range dstList {
		dstValue.Set(reflect.AppendSlice(dstValue, reflect.ValueOf(dst).Elem()))
	}

	if firstErr != nil {
		return firstErr
	}
	return nil
}

// Reset resets all column buffers back to the beginning of the file.
// After calling Reset(), the next Read() will start from the first row.
// This allows re-reading the parquet file without creating a new reader.
func (pr *ParquetReader) Reset() error {
	// Close all existing column buffer resources
	for _, cb := range pr.ColumnBuffers {
		if cb == nil || cb.PFile == nil {
			continue
		}

		if err := cb.PFile.Close(); err != nil {
			return fmt.Errorf("failed to close column buffer: %w", err)
		}
	}

	// Recreate all column buffers from scratch
	for pathStr := range pr.ColumnBuffers {
		newCB, err := NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr)
		if err != nil {
			return fmt.Errorf("failed to recreate column buffer for %s: %w", pathStr, err)
		}
		pr.ColumnBuffers[pathStr] = newCB
	}
	return nil
}

// Stop Read
// ReadStopWithError closes all column buffer file handles and returns any errors encountered.
// This is the error-returning version of ReadStop.
func (pr *ParquetReader) ReadStopWithError() error {
	var errs []error
	for pathStr, cb := range pr.ColumnBuffers {
		if cb != nil {
			if err := cb.PFile.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close column buffer for path %s: %w", pathStr, err))
			}
		}
	}
	return errors.Join(errs...)
}

// Deprecated: Use ReadStopWithError instead. This method ignores errors.
func (pr *ParquetReader) ReadStop() {
	_ = pr.ReadStopWithError()
}
