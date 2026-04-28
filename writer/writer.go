package writer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/bloomfilter"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

// WriterOption configures a ParquetWriter.
type WriterOption func(*ParquetWriter)

// WithNP sets the number of goroutines for parallel processing. Default is 4.
func WithNP(np int64) WriterOption {
	return func(pw *ParquetWriter) { pw.np = np }
}

// WithPageSize sets the page size in bytes. Default is 8KB.
func WithPageSize(size int64) WriterOption {
	return func(pw *ParquetWriter) { pw.pageSize = size }
}

// WithRowGroupSize sets the row group size in bytes. Default is 128MB.
func WithRowGroupSize(size int64) WriterOption {
	return func(pw *ParquetWriter) { pw.rowGroupSize = size }
}

// WithCompressionType sets the compression codec. Default is SNAPPY.
func WithCompressionType(ct parquet.CompressionCodec) WriterOption {
	return func(pw *ParquetWriter) { pw.compressionType = ct }
}

// WithCompressionLevel sets the compression level for a specific codec.
// Not all codecs support compression levels; invalid codecs or levels are
// reported when constructing the writer.
func WithCompressionLevel(codec parquet.CompressionCodec, level int) WriterOption {
	return func(pw *ParquetWriter) {
		if pw.compressionLevels == nil {
			pw.compressionLevels = make(map[parquet.CompressionCodec]int)
		}
		pw.compressionLevels[codec] = level
	}
}

// WithDataPageVersion sets the data page version (1 or 2). Default is 1.
func WithDataPageVersion(v int32) WriterOption {
	return func(pw *ParquetWriter) { pw.dataPageVersion = v }
}

// WithWriteCRC enables or disables CRC32 page checksums. Default is false.
func WithWriteCRC(enabled bool) WriterOption {
	return func(pw *ParquetWriter) { pw.writeCRC = enabled }
}

// ParquetWriter is a writer  parquet file
type ParquetWriter struct {
	SchemaHandler *schema.SchemaHandler
	Footer        *parquet.FileMetaData
	PFile         source.ParquetFileWriter

	np                int64 // parallel number
	pageSize          int64
	rowGroupSize      int64
	compressionType   parquet.CompressionCodec
	compressionLevels map[parquet.CompressionCodec]int
	compressor        *compress.Compressor
	dataPageVersion   int32 // 1 for DATA_PAGE (default), 2 for DATA_PAGE_V2
	writeCRC          bool  // compute and write CRC32 checksums on pages (default false)
	offset            int64

	objs              []any
	objsSize          int64
	objSize           int64
	checkSizeCritical int64

	pagesMapBuf map[string][]*layout.Page
	size        int64
	numRows     int64

	// DictRecs stores dictionary recorders per column path
	// key: path string, value: *layout.DictRecType
	DictRecs sync.Map

	columnIndexes []*parquet.ColumnIndex
	offsetIndexes []*parquet.OffsetIndex

	// bloomFilters holds the active bloom filters being built for the current row group.
	// Key is the column path string (e.g. common.ParGoRootInName + "\x01Name").
	bloomFilters map[string]*bloomfilter.Filter
	// bloomFilterData holds serialized (header+bitset) per column chunk, parallel to ColumnIndexes/OffsetIndexes.
	bloomFilterData [][]byte

	marshalFunc func(src []any, sh *schema.SchemaHandler) (*map[string]*layout.Table, error)

	stopped            bool
	encodingsValidated bool // tracks if encoding/version validation has been done
}

// NewParquetWriterFromWriter creates a ParquetWriter from an io.Writer.
func NewParquetWriterFromWriter(w io.Writer, obj any, opts ...WriterOption) (*ParquetWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewParquetWriter(wf, obj, opts...)
}

// initBase sets up the ParquetWriter with defaults, applies and validates
// functional options, and writes the PAR1 magic header. Options are validated
// before any IO so that invalid options never produce partial output.
//
// Callers must still set SchemaHandler, marshalFunc, call initBloomFilters,
// and set stopped = false after successful schema init.
func (pw *ParquetWriter) initBase(pFile source.ParquetFileWriter, opts ...WriterOption) error {
	pw.np = 4                                    // default parallel number
	pw.pageSize = common.DefaultPageSize         // 8K
	pw.rowGroupSize = common.DefaultRowGroupSize // 128M
	pw.compressionType = parquet.CompressionCodec_SNAPPY
	pw.compressionLevels = nil
	pw.compressor = nil
	pw.dataPageVersion = 1 // default to DATA_PAGE (V1)
	pw.offset = 4
	pw.PFile = pFile
	pw.pagesMapBuf = make(map[string][]*layout.Page)
	// DictRecs sync.Map zero value is ready to use
	pw.Footer = parquet.NewFileMetaData()
	pw.Footer.Version = 1
	pw.columnIndexes = make([]*parquet.ColumnIndex, 0)
	pw.offsetIndexes = make([]*parquet.OffsetIndex, 0)
	// include the createdBy to avoid
	// WARN  CorruptStatistics:118 - Ignoring statistics because created_by is null or empty! See PARQUET-251 and PARQUET-297
	pw.Footer.CreatedBy = common.ToPtr("github.com/hangxie/parquet-go/v3")
	// marshalFunc must be set by the caller (NewParquetWriter, NewCSVWriter, etc.)
	// after initBase returns. Each writer type uses a different marshal function.
	// stopped starts true so that a partially-constructed writer (e.g. after
	// schema failure) rejects Write calls rather than panicking.
	pw.stopped = true

	// Apply functional options
	for _, opt := range opts {
		opt(pw)
	}

	// Validate options before any IO to avoid partial writes on invalid input.
	if pw.np <= 0 {
		return fmt.Errorf("WithNP: value must be positive, got %d", pw.np)
	}
	if pw.pageSize <= 0 {
		return fmt.Errorf("WithPageSize: value must be positive, got %d", pw.pageSize)
	}
	if pw.rowGroupSize <= 0 {
		return fmt.Errorf("WithRowGroupSize: value must be positive, got %d", pw.rowGroupSize)
	}
	if pw.dataPageVersion != 1 && pw.dataPageVersion != 2 {
		return fmt.Errorf("WithDataPageVersion: value must be 1 or 2, got %d", pw.dataPageVersion)
	}
	if len(pw.compressionLevels) > 0 {
		opts := make([]compress.CompressorOption, 0, len(pw.compressionLevels))
		for codec, level := range pw.compressionLevels {
			opts = append(opts, compress.WithCompressionLevel(codec, level))
		}
		compressor, err := compress.NewCompressor(opts...)
		if err != nil {
			return fmt.Errorf("WithCompressionLevel: %w", err)
		}
		pw.compressor = compressor
	}

	if _, err := pw.PFile.Write([]byte("PAR1")); err != nil {
		return fmt.Errorf("write magic header: %w", err)
	}
	return nil
}

// NewParquetWriter creates a parquet writer. Obj is an object with tags or a JSON schema string.
func NewParquetWriter(pFile source.ParquetFileWriter, obj any, opts ...WriterOption) (*ParquetWriter, error) {
	res := new(ParquetWriter)
	if err := res.initBase(pFile, opts...); err != nil {
		return nil, err
	}
	res.marshalFunc = marshal.Marshal

	if obj != nil {
		if sa, ok := obj.(string); ok {
			// SetSchemaHandlerFromJSON handles Footer.Schema internally
			if err := res.SetSchemaHandlerFromJSON(sa); err != nil {
				return nil, fmt.Errorf("set schema from JSON: %w", err)
			}
		} else {
			var err error
			if sa, ok := obj.(*schema.SchemaHandler); ok {
				res.SchemaHandler = schema.NewSchemaHandlerFromSchemaHandler(sa)
			} else if sa, ok := obj.([]*parquet.SchemaElement); ok {
				res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(sa)
			} else {
				if res.SchemaHandler, err = schema.NewSchemaHandlerFromStruct(obj); err != nil {
					return nil, fmt.Errorf("build schema handler: %w", err)
				}
			}
			res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
		}
	}

	if err := res.initBloomFilters(); err != nil {
		return nil, fmt.Errorf("init bloom filters: %w", err)
	}

	// Enable writing after init completed successfully
	res.stopped = false

	return res, nil
}

func (pw *ParquetWriter) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error
	if pw.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return fmt.Errorf("parse JSON schema: %w", err)
	}
	pw.Footer.Schema = pw.Footer.Schema[:0]
	pw.Footer.Schema = append(pw.Footer.Schema, pw.SchemaHandler.SchemaElements...)
	if err := pw.initBloomFilters(); err != nil {
		return fmt.Errorf("init bloom filters: %w", err)
	}
	pw.encodingsValidated = false
	return nil
}

// Rename schema name to exname in tags
func (pw *ParquetWriter) RenameSchema() {
	for i := range len(pw.Footer.Schema) {
		pw.Footer.Schema[i].Name = pw.SchemaHandler.Infos[i].ExName
	}
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			inPathStr := common.PathToStr(chunk.MetaData.PathInSchema)
			exPathStr := pw.SchemaHandler.InPathToExPath[inPathStr]
			exPath := common.StrToPath(exPathStr)[1:]
			chunk.MetaData.PathInSchema = exPath
		}
	}
}

func (pw *ParquetWriter) WriteStop() error {
	if pw.stopped {
		return nil
	}
	pw.stopped = true

	var err error
	if err = pw.Flush(true); err != nil {
		return fmt.Errorf("flush before stop: %w", err)
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	pw.RenameSchema()

	if err = pw.writeColumnIndexes(ts); err != nil {
		return err
	}

	if err = pw.writeOffsetIndexes(ts); err != nil {
		return err
	}

	if err = pw.writeBloomFilters(); err != nil {
		return err
	}

	// Set ColumnOrders so readers can correctly interpret min/max statistics.
	// One TYPE_ORDER entry per leaf column, per the Parquet spec.
	if pw.SchemaHandler != nil {
		numCols := pw.SchemaHandler.GetColumnNum()
		pw.Footer.ColumnOrders = make([]*parquet.ColumnOrder, numCols)
		for i := range numCols {
			pw.Footer.ColumnOrders[i] = &parquet.ColumnOrder{
				TYPE_ORDER: parquet.NewTypeDefinedOrder(),
			}
		}
	}

	footerBuf, err := ts.Write(context.TODO(), pw.Footer)
	if err != nil {
		return fmt.Errorf("serialize footer: %w", err)
	}

	if _, err = pw.PFile.Write(footerBuf); err != nil {
		return fmt.Errorf("write footer: %w", err)
	}
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))

	if _, err = pw.PFile.Write(footerSizeBuf); err != nil {
		return fmt.Errorf("write footer size: %w", err)
	}
	if _, err = pw.PFile.Write([]byte("PAR1")); err != nil {
		return fmt.Errorf("write magic tail: %w", err)
	}

	return nil
}

// Write writes one object to the parquet file.
func (pw *ParquetWriter) Write(src any) error {
	if pw.stopped {
		return fmt.Errorf("writer stopped")
	}
	if pw.SchemaHandler == nil {
		return fmt.Errorf("schema handler not initialized")
	}

	// Validate encodings are compatible with data page version (once per writer)
	if !pw.encodingsValidated {
		if err := pw.SchemaHandler.ValidateEncodingsForDataPageVersion(pw.dataPageVersion); err != nil {
			return fmt.Errorf("encoding validation: %w", err)
		}
		pw.encodingsValidated = true
	}

	var err error
	ln := int64(len(pw.objs))

	val := reflect.ValueOf(src)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		src = val.Interface()
	}

	if pw.checkSizeCritical <= ln {
		pw.objSize = (pw.objSize+common.SizeOf(val))/2 + 1
	}
	pw.objsSize += pw.objSize
	pw.objs = append(pw.objs, src)

	criSize := pw.np * pw.pageSize * pw.SchemaHandler.GetColumnNum()

	if pw.objsSize >= criSize {
		err = pw.Flush(false)
	} else {
		dln := (criSize - pw.objsSize + pw.objSize - 1) / pw.objSize / 2
		pw.checkSizeCritical = dln + ln
	}
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

func (pw *ParquetWriter) buildChunkMap() (map[string]*layout.Chunk, error) {
	chunkMap := make(map[string]*layout.Chunk)
	for name, pages := range pw.pagesMapBuf {
		compressionType := pw.compressionType
		if idx, ok := pw.SchemaHandler.MapIndex[name]; ok {
			if info := pw.SchemaHandler.Infos[idx]; info != nil && info.CompressionType != nil {
				compressionType = *info.CompressionType
			}
		}

		if len(pages) > 0 && (pages[0].Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY || pages[0].Info.Encoding == parquet.Encoding_RLE_DICTIONARY) {
			v, ok := pw.DictRecs.Load(name)
			if !ok {
				return nil, fmt.Errorf("missing dictionary recorder for column %s", name)
			}
			dictRec := v.(*layout.DictRecType)
			dictPage, _, err := layout.DictRecToDictPageWithOption(dictRec, layout.PageWriteOption{
				PageSize:     int32(pw.pageSize),
				CompressType: compressionType,
				WriteCRC:     pw.writeCRC,
			})
			if err != nil {
				return nil, fmt.Errorf("convert dict rec to dict page for column %s: %w", name, err)
			}
			tmp := append([]*layout.Page{dictPage}, pages...)
			var chunkErr error
			chunkMap[name], chunkErr = layout.PagesToDictChunk(tmp)
			if chunkErr != nil {
				return nil, fmt.Errorf("convert pages to dict chunk for column %s: %w", name, chunkErr)
			}
		} else {
			var err error
			chunkMap[name], err = layout.PagesToChunk(pages)
			if err != nil {
				return nil, fmt.Errorf("convert pages to chunk for column %s: %w", name, err)
			}
		}
	}
	return chunkMap, nil
}

func (pw *ParquetWriter) buildRowGroup(chunkMap map[string]*layout.Chunk) *layout.RowGroup {
	rowGroup := layout.NewRowGroup()
	rowGroup.RowGroupHeader.Columns = make([]*parquet.ColumnChunk, 0)

	for k := range len(pw.SchemaHandler.SchemaElements) {
		se := pw.SchemaHandler.SchemaElements[k]
		if se.GetNumChildren() > 0 {
			continue
		}
		chunk := chunkMap[pw.SchemaHandler.IndexMap[int32(k)]]
		if chunk == nil {
			continue
		}
		rowGroup.Chunks = append(rowGroup.Chunks, chunk)
		rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalUncompressedSize
		rowGroup.RowGroupHeader.Columns = append(rowGroup.RowGroupHeader.Columns, chunk.ChunkHeader)
	}
	rowGroup.RowGroupHeader.NumRows = pw.numRows
	pw.numRows = 0
	return rowGroup
}

// Flush the write buffer to parquet file
func (pw *ParquetWriter) Flush(flag bool) error {
	if err := pw.flushObjs(); err != nil {
		return fmt.Errorf("flush objects during flush: %w", err)
	}

	if (pw.size+pw.objsSize >= pw.rowGroupSize || flag) && len(pw.pagesMapBuf) > 0 {
		chunkMap, err := pw.buildChunkMap()
		if err != nil {
			return err
		}
		pw.DictRecs.Clear()

		rowGroup := pw.buildRowGroup(chunkMap)

		for k := range len(rowGroup.Chunks) {
			if err := pw.writeChunkPages(rowGroup.Chunks[k], k); err != nil {
				return err
			}
		}

		pw.Footer.RowGroups = append(pw.Footer.RowGroups, rowGroup.RowGroupHeader)

		if err := pw.serializeBloomFilters(); err != nil {
			return err
		}

		pw.size = 0
		pw.pagesMapBuf = make(map[string][]*layout.Page)
	}
	pw.Footer.NumRows += int64(len(pw.objs))
	pw.objs = pw.objs[:0]
	pw.objsSize = 0
	return nil
}
