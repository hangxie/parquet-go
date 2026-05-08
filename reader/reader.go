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

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
)

// ParquetReader is a reader for parquet files.
type ParquetReader struct {
	SchemaHandler *schema.SchemaHandler
	np            int64 // parallel number
	Footer        *parquet.FileMetaData
	PFile         source.ParquetFileReader
	FileCrypto    *parquet.FileCryptoMetaData

	ColumnBuffers map[string]*ColumnBufferType

	// One reader can only read one type objects
	ObjType        reflect.Type
	ObjPartialType reflect.Type

	caseInsensitive   bool           // case-insensitive schema matching
	crcMode           common.CRCMode // CRC validation when reading pages
	footerKey         []byte
	resolvedFooterKey []byte
	aadPrefix         []byte
	keyRetriever      KeyRetriever
	columnKeys        map[string][]byte
}

// NewParquetReader creates a parquet reader. obj is an object with schema tags or a JSON schema string.
func NewParquetReader(pFile source.ParquetFileReader, obj any, opts ...ReaderOption) (*ParquetReader, error) {
	var err error
	res := new(ParquetReader)
	res.PFile = pFile

	if err = applyReaderDefaults(res, opts); err != nil {
		return nil, err
	}
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
				if res.ColumnBuffers[pathStr], err = NewColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr, &layout.PageReadOptions{CRCMode: res.crcMode, MaxPageSize: layout.DefaultMaxPageSize}); err != nil {
					return res, fmt.Errorf("init column buffer for %s: %w", pathStr, err)
				}
				res.ColumnBuffers[pathStr].Reader = res
				if err := res.reconfigureDecryptorForBuffer(res.ColumnBuffers[pathStr]); err != nil {
					return res, fmt.Errorf("configure page decryptor for %s: %w", pathStr, err)
				}
			}
		}
	}

	res.detectBloomFilters()
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
			if pr.ColumnBuffers[pathStr], err = NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr, &layout.PageReadOptions{CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize}); err != nil {
				return fmt.Errorf("init column buffer for %s: %w", pathStr, err)
			}
			pr.ColumnBuffers[pathStr].Reader = pr
			if err := pr.reconfigureDecryptorForBuffer(pr.ColumnBuffers[pathStr]); err != nil {
				return fmt.Errorf("configure page decryptor for %s: %w", pathStr, err)
			}
		}
	}
	pr.detectBloomFilters()
	return nil
}

func (pr *ParquetReader) newColumnBuffer(pathStr string) (*ColumnBufferType, error) {
	cb, err := NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr, &layout.PageReadOptions{CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize})
	if err != nil {
		return nil, err
	}
	cb.Reader = pr
	if err := pr.reconfigureDecryptorForBuffer(cb); err != nil {
		_ = cb.PFile.Close()
		return nil, err
	}
	return cb, nil
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
	if pr.caseInsensitive {
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

			if pr.caseInsensitive {
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

func (pr *ParquetReader) getFooterTail() (uint32, string, error) {
	if pr.PFile == nil {
		return 0, "", fmt.Errorf("PFile is nil")
	}

	buf := make([]byte, 8)
	if _, err := pr.PFile.Seek(-8, io.SeekEnd); err != nil {
		return 0, "", err
	}
	if _, err := io.ReadFull(pr.PFile, buf); err != nil {
		return 0, "", err
	}
	return binary.LittleEndian.Uint32(buf[:4]), string(buf[4:]), nil
}

// Read footer from parquet file
func (pr *ParquetReader) ReadFooter() error {
	size, magic, err := pr.getFooterTail()
	if err != nil {
		return fmt.Errorf("get footer tail: %w", err)
	}
	switch magic {
	case "PARE":
		if err := pr.readEncryptedFooter(size); err != nil {
			return err
		}
	default:
		if err := pr.readPlainFooter(size); err != nil {
			return err
		}
	}
	if err := pr.decryptEncryptedColumnMetadata(); err != nil {
		return fmt.Errorf("decrypt encrypted column metadata: %w", err)
	}
	return nil
}

func (pr *ParquetReader) readPlainFooter(size uint32) error {
	if _, err := pr.PFile.Seek(-int64(8+size), io.SeekEnd); err != nil {
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

	if pr.Footer.IsSetEncryptionAlgorithm() {
		if _, err := pr.PFile.Seek(-int64(8+size), io.SeekEnd); err != nil {
			return fmt.Errorf("seek to plaintext footer section: %w", err)
		}
		section := make([]byte, size)
		if _, err := io.ReadFull(pr.PFile, section); err != nil {
			return fmt.Errorf("read plaintext footer section: %w", err)
		}
		if err := pr.verifyPlaintextFooter(section); err != nil {
			return err
		}
	}
	return nil
}

// Read reads rows of the parquet file and unmarshals them into dst.
func (pr *ParquetReader) Read(dstInterface any) error {
	return pr.read(dstInterface, "")
}

// ReadByNumber reads up to maxReadNumber objects.
func (pr *ParquetReader) ReadByNumber(maxReadNumber int) ([]any, error) {
	if maxReadNumber < 0 {
		return nil, fmt.Errorf("negative maxReadNumber: %d", maxReadNumber)
	}

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

// ReadPartial reads rows and unmarshals only the subtree rooted at prefixPath.
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

// ReadPartialByNumber reads up to maxReadNumber partial objects rooted at prefixPath.
func (pr *ParquetReader) ReadPartialByNumber(maxReadNumber int, prefixPath string) ([]any, error) {
	if maxReadNumber < 0 {
		return nil, fmt.Errorf("negative maxReadNumber: %d", maxReadNumber)
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

func (pr *ParquetReader) read(dstInterface any, prefixPath string) error {
	if dstInterface == nil {
		return fmt.Errorf("dstInterface is nil")
	}
	tmap := make(map[string]*layout.Table)
	num := reflect.ValueOf(dstInterface).Elem().Len()
	if num <= 0 {
		return nil
	}
	if err := pr.fetchColumnData(num, prefixPath, tmap); err != nil {
		return err
	}
	return pr.unmarshalToResult(num, tmap, dstInterface, prefixPath)
}

func (pr *ParquetReader) fetchColumnData(num int, prefixPath string, tmap map[string]*layout.Table) error {
	var locker sync.Mutex
	taskChan := make(chan string)
	var wgCols sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	worker := func() {
		defer wgCols.Done()
		for pathStr := range taskChan {
			cb := pr.ColumnBuffers[pathStr]
			table, _, rerr := cb.ReadRows(int64(num))
			if rerr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = rerr
				}
				errMu.Unlock()
				continue
			}
			locker.Lock()
			if _, ok := tmap[pathStr]; ok {
				tmap[pathStr].Merge(table)
			} else {
				tmap[pathStr] = layout.NewTableFromTable(table)
				tmap[pathStr].Merge(table)
			}
			locker.Unlock()
		}
	}

	for i := int64(0); i < pr.np; i++ {
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
	return firstErr
}

func (pr *ParquetReader) unmarshalToResult(num int, tmap map[string]*layout.Table, dstInterface any, prefixPath string) error {
	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	dstList := make([]any, pr.np)
	delta := (int64(num) + pr.np - 1) / pr.np

	var firstErr error
	var errMu sync.Mutex
	var wg sync.WaitGroup
	for c := range pr.np {
		bgn := c * delta
		end := min(bgn+delta, int64(num))
		if bgn >= int64(num) {
			bgn, end = int64(num), int64(num)
		}
		wg.Add(1)
		go func(b, e, index int) {
			defer wg.Done()
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
	return firstErr
}

// Reset closes and recreates all column buffers, allowing the file to be
// re-read from the beginning without creating a new reader.
func (pr *ParquetReader) Reset() error {
	for _, cb := range pr.ColumnBuffers {
		if cb == nil || cb.PFile == nil {
			continue
		}
		if err := cb.PFile.Close(); err != nil {
			return fmt.Errorf("close column buffer: %w", err)
		}
	}
	for pathStr := range pr.ColumnBuffers {
		newCB, err := pr.newColumnBuffer(pathStr)
		if err != nil {
			return fmt.Errorf("recreate column buffer for %s: %w", pathStr, err)
		}
		pr.ColumnBuffers[pathStr] = newCB
	}
	return nil
}

// ReadStop closes all column buffer file handles.
func (pr *ParquetReader) ReadStop() error {
	var errs []error
	for pathStr, cb := range pr.ColumnBuffers {
		if cb == nil {
			continue
		}
		if err := cb.PFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close column buffer for path %s: %w", pathStr, err))
		}
	}
	return errors.Join(errs...)
}
