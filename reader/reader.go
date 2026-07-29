package reader

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
)

// ParquetReader reads parquet files.
//
// A ParquetReader must not be used by multiple goroutines concurrently.
// Callers must serialize all operations on an instance, including row reads,
// column reads, skips, inspection methods, Reset, ReadStop, and their
// context-aware variants. WithNP controls internal parallelism and does not
// make concurrent method calls safe.
type ParquetReader struct {
	SchemaHandler *schema.SchemaHandler
	// Footer is loaded once by ReadFooter and then treated as immutable for the
	// reader lifetime. Direct mutation or reassignment by callers is unsupported.
	Footer     *parquet.FileMetaData
	PFile      source.ParquetFileReader
	FileCrypto *parquet.FileCryptoMetaData

	ColumnBuffers map[string]*ColumnBufferType

	// One reader can only read one type objects
	ObjType        reflect.Type
	ObjPartialType reflect.Type

	// Reader options.
	np              int64          // parallel number
	caseInsensitive bool           // case-insensitive schema matching
	crcMode         common.CRCMode // CRC validation when reading pages

	// Encryption options.
	footerKey         []byte
	resolvedFooterKey []byte
	aadPrefix         []byte
	keyRetriever      KeyRetriever
	columnKeys        map[string][]byte

	// Lazy runtime state.
	columnKeysFullPath bool
	keyCache           sync.Map
	footerMu           sync.Mutex
	footerLoaded       bool
	defaultCtx         context.Context
	ctx                context.Context

	// encryptedPageOffsets is populated once from the immutable footer and then
	// treated as read-only. It tracks every per-column offset that belongs to an
	// encrypted column: data, dictionary, index, and bloom-filter pages. The set
	// is keyed on CryptoMetadata, which is the authoritative signal that a
	// column's payloads are encrypted. If the footer carries an encrypted column
	// whose MetaData cannot be recovered (no plaintext and no
	// EncryptedColumnMetadata), the build records encryptedPageOffsetsErr and
	// every lookup surfaces that error.
	encryptedPageOffsetOnce sync.Once
	encryptedPageOffsets    map[int64]struct{}
	encryptedPageOffsetsErr error
}

// NewParquetReader creates a parquet reader. obj is an object with schema tags or a JSON schema string.
//
// Deprecated: use NewParquetReaderWithContext.
func NewParquetReader(pFile source.ParquetFileReader, obj any, opts ...ReaderOption) (*ParquetReader, error) {
	return NewParquetReaderWithContext(context.Background(), pFile, obj, opts...)
}

// NewParquetReaderWithContext creates a parquet reader using ctx for footer,
// schema, and column-buffer initialization.
func NewParquetReaderWithContext(ctx context.Context, pFile source.ParquetFileReader, obj any, opts ...ReaderOption) (*ParquetReader, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	var err error
	res := new(ParquetReader)
	res.PFile = pFile
	res.defaultCtx = ctx
	res.ctx = ctx

	if err = applyReaderDefaults(res, opts); err != nil {
		return nil, fmt.Errorf("apply reader options: %w", err)
	}
	if err = res.ReadFooterWithContext(ctx); err != nil {
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

	if err = res.validateColumnKeyPaths(); err != nil {
		return res, err
	}
	for i := range len(res.SchemaHandler.SchemaElements) {
		schema := res.SchemaHandler.SchemaElements[i]
		if schema == nil {
			continue
		}
		if schema.GetNumChildren() == 0 {
			if pathStr, exists := res.SchemaHandler.IndexMap[int32(i)]; exists {
				if res.ColumnBuffers[pathStr], err = res.newColumnBuffer(pathStr); err != nil {
					return res, fmt.Errorf("init column buffer for %s: %w", pathStr, err)
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

	if err = pr.validateColumnKeyPaths(); err != nil {
		return err
	}

	for i := range len(pr.SchemaHandler.SchemaElements) {
		schemaElement := pr.SchemaHandler.SchemaElements[i]
		if schemaElement.GetNumChildren() == 0 {
			pathStr := pr.SchemaHandler.IndexMap[int32(i)]
			if pr.ColumnBuffers[pathStr], err = pr.newColumnBuffer(pathStr); err != nil {
				return fmt.Errorf("init column buffer for %s: %w", pathStr, err)
			}
		}
	}
	pr.detectBloomFilters()
	return nil
}

func (pr *ParquetReader) newColumnBuffer(pathStr string) (*ColumnBufferType, error) {
	cb, err := newColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr, &layout.PageReadOptions{Context: pr.context(), CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize}, pr.caseInsensitive)
	if err != nil {
		return nil, fmt.Errorf("new column buffer for %s: %w", pathStr, err)
	}
	cb.Reader = pr
	if err := pr.reconfigureOptionalDecryptorForBuffer(cb); err != nil {
		_ = cb.PFile.Close()
		return nil, fmt.Errorf("configure decryptor for %s: %w", pathStr, err)
	}
	return cb, nil
}

// Get the footer size
//
// Deprecated: use GetFooterSizeWithContext.
func (pr *ParquetReader) GetFooterSize() (uint32, error) {
	return pr.GetFooterSizeWithContext(pr.defaultContext())
}

// GetFooterSizeWithContext returns the footer size using ctx.
func (pr *ParquetReader) GetFooterSizeWithContext(ctx context.Context) (uint32, error) {
	if err := pr.setContext(ctx); err != nil {
		return 0, err
	}
	if pr.PFile == nil {
		return 0, fmt.Errorf("PFile is nil")
	}

	buf := make([]byte, 4)
	if _, err := source.SeekWithContext(pr.context(), pr.PFile, -8, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("seek to footer size: %w", err)
	}
	if _, err := source.ReadFullWithContext(pr.context(), pr.PFile, buf); err != nil {
		return 0, fmt.Errorf("read footer size: %w", err)
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (pr *ParquetReader) getFooterTail() (uint32, string, error) {
	if pr.PFile == nil {
		return 0, "", fmt.Errorf("PFile is nil")
	}

	buf := make([]byte, 8)
	if _, err := source.SeekWithContext(pr.context(), pr.PFile, -8, io.SeekEnd); err != nil {
		return 0, "", fmt.Errorf("seek to footer tail: %w", err)
	}
	if _, err := source.ReadFullWithContext(pr.context(), pr.PFile, buf); err != nil {
		return 0, "", fmt.Errorf("read footer tail: %w", err)
	}
	return binary.LittleEndian.Uint32(buf[:4]), string(buf[4:]), nil
}

// ReadFooter reads and publishes the file footer once.
//
// After a successful read, the reader treats Footer as immutable. Repeated calls
// return without reloading so internal caches remain tied to the same footer.
//
// Deprecated: use ReadFooterWithContext.
func (pr *ParquetReader) ReadFooter() error {
	return pr.ReadFooterWithContext(pr.defaultContext())
}

// ReadFooterWithContext reads and publishes the file footer once using ctx.
func (pr *ParquetReader) ReadFooterWithContext(ctx context.Context) error {
	if err := pr.setContext(ctx); err != nil {
		return err
	}
	pr.footerMu.Lock()
	defer pr.footerMu.Unlock()

	if pr.footerLoaded {
		return nil
	}

	size, magic, err := pr.getFooterTail()
	if err != nil {
		return fmt.Errorf("get footer tail: %w", err)
	}
	switch magic {
	case common.MagicBytesEncrypted:
		if err := pr.readEncryptedFooter(size); err != nil {
			return fmt.Errorf("read encrypted footer: %w", err)
		}
	default:
		if err := pr.readPlainFooter(size); err != nil {
			return fmt.Errorf("read plain footer: %w", err)
		}
	}
	if err := pr.decryptEncryptedColumnMetadata(); err != nil {
		return fmt.Errorf("decrypt encrypted column metadata: %w", err)
	}
	pr.footerLoaded = true
	return nil
}

func (pr *ParquetReader) readPlainFooter(size uint32) error {
	if _, err := source.SeekWithContext(pr.context(), pr.PFile, -int64(8+size), io.SeekEnd); err != nil {
		return fmt.Errorf("seek to footer: %w", err)
	}
	pr.Footer = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{})
	thriftReader := thrift.NewStreamTransportR(source.ReaderWithContext{Ctx: pr.context(), Reader: pr.PFile})
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
	protocol := pf.GetProtocol(bufferReader)
	if err := pr.Footer.Read(pr.context(), protocol); err != nil {
		return fmt.Errorf("read footer: %w", err)
	}

	if pr.Footer.IsSetEncryptionAlgorithm() {
		if _, err := source.SeekWithContext(pr.context(), pr.PFile, -int64(8+size), io.SeekEnd); err != nil {
			return fmt.Errorf("seek to plaintext footer section: %w", err)
		}
		section := make([]byte, size)
		if _, err := source.ReadFullWithContext(pr.context(), pr.PFile, section); err != nil {
			return fmt.Errorf("read plaintext footer section: %w", err)
		}
		if err := pr.verifyPlaintextFooter(section); err != nil {
			return fmt.Errorf("verify plaintext footer: %w", err)
		}
	}
	return nil
}

// Read reads rows of the parquet file and unmarshals them into dst.
//
// Deprecated: use ReadWithContext.
func (pr *ParquetReader) Read(dstInterface any) error {
	return pr.ReadWithContext(pr.defaultContext(), dstInterface)
}

// ReadWithContext reads rows of the parquet file using ctx.
func (pr *ParquetReader) ReadWithContext(ctx context.Context, dstInterface any) error {
	if err := pr.setContext(ctx); err != nil {
		return err
	}
	return pr.read(dstInterface, "")
}

// ReadByNumber reads up to maxReadNumber objects.
//
// Deprecated: use ReadByNumberWithContext.
func (pr *ParquetReader) ReadByNumber(maxReadNumber int) ([]any, error) {
	return pr.ReadByNumberWithContext(pr.defaultContext(), maxReadNumber)
}

// ReadByNumberWithContext reads up to maxReadNumber objects using ctx.
func (pr *ParquetReader) ReadByNumberWithContext(ctx context.Context, maxReadNumber int) ([]any, error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, err
	}
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

	if err = pr.ReadWithContext(ctx, res.Interface()); err != nil {
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
// prefixPath components must be separated by common.ParGoPathDelimiter (build it
// with common.PathToStr, e.g. common.PathToStr([]string{"parquet_go_root", "name"})).
//
// Deprecated: use ReadPartialWithContext.
func (pr *ParquetReader) ReadPartial(dstInterface any, prefixPath string) error {
	return pr.ReadPartialWithContext(pr.defaultContext(), dstInterface, prefixPath)
}

// ReadPartialWithContext reads a subtree rooted at prefixPath using ctx.
func (pr *ParquetReader) ReadPartialWithContext(ctx context.Context, dstInterface any, prefixPath string) error {
	if err := pr.setContext(ctx); err != nil {
		return err
	}
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
// prefixPath components must be separated by common.ParGoPathDelimiter (build it
// with common.PathToStr, e.g. common.PathToStr([]string{"parquet_go_root", "name"})).
//
// Deprecated: use ReadPartialByNumberWithContext.
func (pr *ParquetReader) ReadPartialByNumber(maxReadNumber int, prefixPath string) ([]any, error) {
	return pr.ReadPartialByNumberWithContext(pr.defaultContext(), maxReadNumber, prefixPath)
}

// ReadPartialByNumberWithContext reads up to maxReadNumber partial objects using ctx.
func (pr *ParquetReader) ReadPartialByNumberWithContext(ctx context.Context, maxReadNumber int, prefixPath string) ([]any, error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, err
	}
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

	if err = pr.ReadPartialWithContext(ctx, res.Interface(), prefixPath); err != nil {
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
		return fmt.Errorf("fetch column data: %w", err)
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
		if prefixPath == "" || common.IsChildPath(prefixPath, key) {
			if err := pr.context().Err(); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				break
			}
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
