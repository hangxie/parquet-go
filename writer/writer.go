package writer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/bloomfilter"
	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/compress"
	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

// WriterOption configures a ParquetWriter. Use With* functions to create options.
type WriterOption func(*writerConfig) error

// writerConfig holds resolved per-writer configuration (unexported).
type writerConfig struct {
	np                int64
	pageSize          int64
	rowGroupSize      int64
	compressionType   parquet.CompressionCodec
	dataPageVersion   int32
	writeCRC          bool
	compressionLevels map[parquet.CompressionCodec]int
}

const (
	maxPageSize     = 1<<31 - 1 // Thrift page header uses i32
	maxRowGroupSize = 1 << 35   // ~34 GB practical memory bound
)

func defaultWriterConfig() writerConfig {
	return writerConfig{
		np:              4,
		pageSize:        common.DefaultPageSize,
		rowGroupSize:    common.DefaultRowGroupSize,
		compressionType: parquet.CompressionCodec_SNAPPY,
		dataPageVersion: 1,
		writeCRC:        false,
	}
}

// WithNP sets the number of goroutines for parallel page encoding.
func WithNP(n int64) WriterOption {
	return func(c *writerConfig) error {
		if n < 1 {
			return fmt.Errorf("NP must be >= 1, got %d", n)
		}
		c.np = n
		return nil
	}
}

// WithPageSize sets the target uncompressed page size in bytes.
func WithPageSize(n int64) WriterOption {
	return func(c *writerConfig) error {
		if n <= 0 || n > maxPageSize {
			return fmt.Errorf("page size must be between 1 and %d, got %d", maxPageSize, n)
		}
		c.pageSize = n
		return nil
	}
}

// WithRowGroupSize sets the target row group size in bytes.
func WithRowGroupSize(n int64) WriterOption {
	return func(c *writerConfig) error {
		if n <= 0 || n > maxRowGroupSize {
			return fmt.Errorf("row group size must be between 1 and %d, got %d", maxRowGroupSize, n)
		}
		c.rowGroupSize = n
		return nil
	}
}

// WithCompressionType sets the compression codec for this writer.
func WithCompressionType(codec parquet.CompressionCodec) WriterOption {
	return func(c *writerConfig) error {
		if compress.DefaultCompressor(codec) == nil {
			return fmt.Errorf("unsupported compression codec %v (%d); available: UNCOMPRESSED, SNAPPY, GZIP, LZ4, LZ4_RAW, ZSTD, BROTLI", codec, int32(codec))
		}
		c.compressionType = codec
		return nil
	}
}

// WithDataPageVersion sets the data page version (1 or 2).
func WithDataPageVersion(v int32) WriterOption {
	return func(c *writerConfig) error {
		if v != 1 && v != 2 {
			return fmt.Errorf("data page version must be 1 or 2, got %d", v)
		}
		c.dataPageVersion = v
		return nil
	}
}

// WithWriteCRC enables or disables CRC32 checksum computation on pages.
func WithWriteCRC(b bool) WriterOption {
	return func(c *writerConfig) error {
		c.writeCRC = b
		return nil
	}
}

// WithCompressionLevel sets a per-codec compression level for this writer.
// Call multiple times for different codecs. Codecs not configured here fall
// back to the codec-specific built-in default level (e.g., gzip.DefaultCompression
// for GZIP, zstd.SpeedDefault for ZSTD).
//
// Compressors created via this option are concurrency-safe: each compression
// call allocates fresh state, so multiple goroutines may write pages in
// parallel without additional synchronization.
func WithCompressionLevel(codec parquet.CompressionCodec, level int) WriterOption {
	return func(c *writerConfig) error {
		if c.compressionLevels == nil {
			c.compressionLevels = make(map[parquet.CompressionCodec]int)
		}
		c.compressionLevels[codec] = level
		return nil
	}
}

// ParquetWriter is a writer for parquet files.
// Do not mutate exported fields after construction; doing so causes
// data races and undefined behavior. Use With* options at construction time.
type ParquetWriter struct {
	SchemaHandler *schema.SchemaHandler
	Footer        *parquet.FileMetaData
	PFile         source.ParquetFileWriter

	np              int64
	pageSize        int64
	rowGroupSize    int64
	compressionType parquet.CompressionCodec
	dataPageVersion int32 // 1 for DATA_PAGE (default), 2 for DATA_PAGE_V2
	writeCRC        bool  // compute and write CRC32 checksums on pages (default false)
	Offset          int64

	Objs              []any
	ObjsSize          int64
	ObjSize           int64
	CheckSizeCritical int64

	PagesMapBuf map[string][]*layout.Page
	Size        int64
	NumRows     int64

	// DictRecs stores dictionary recorders per column path
	// key: path string, value: *layout.DictRecType
	DictRecs sync.Map

	ColumnIndexes []*parquet.ColumnIndex
	OffsetIndexes []*parquet.OffsetIndex

	// BloomFilters holds the active bloom filters being built for the current row group.
	// Key is the column path string (e.g. common.ParGoRootInName + "\x01Name").
	BloomFilters map[string]*bloomfilter.Filter
	// BloomFilterData holds serialized (header+bitset) per column chunk, parallel to ColumnIndexes/OffsetIndexes.
	BloomFilterData [][]byte

	MarshalFunc func(src []any, sh *schema.SchemaHandler) (*map[string]*layout.Table, error)

	compressors map[parquet.CompressionCodec]*compress.Compressor // per-writer compressor instances

	stopped       bool
	headerWritten bool
}

// NP returns the number of parallel goroutines for page encoding.
func (pw *ParquetWriter) NP() int64 { return pw.np }

// PageSize returns the target uncompressed page size in bytes.
func (pw *ParquetWriter) PageSize() int64 { return pw.pageSize }

// RowGroupSize returns the target row group size in bytes.
func (pw *ParquetWriter) RowGroupSize() int64 { return pw.rowGroupSize }

// CompressionType returns the compression codec for this writer.
func (pw *ParquetWriter) CompressionType() parquet.CompressionCodec { return pw.compressionType }

// DataPageVersion returns the data page version (1 or 2).
func (pw *ParquetWriter) DataPageVersion() int32 { return pw.dataPageVersion }

// WriteCRC returns whether CRC32 checksum computation is enabled.
func (pw *ParquetWriter) WriteCRC() bool { return pw.writeCRC }

func createdByString() string {
	version := "v3-dev"
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	}
	return fmt.Sprintf("parquet-go version %s (build %s)", version, runtime.Version())
}

func applyWriterConfig(pw *ParquetWriter, cfg writerConfig) {
	pw.np = cfg.np
	pw.pageSize = cfg.pageSize
	pw.rowGroupSize = cfg.rowGroupSize
	pw.compressionType = cfg.compressionType
	pw.dataPageVersion = cfg.dataPageVersion
	pw.writeCRC = cfg.writeCRC

	pw.PagesMapBuf = make(map[string][]*layout.Page)
	pw.ColumnIndexes = make([]*parquet.ColumnIndex, 0)
	pw.OffsetIndexes = make([]*parquet.OffsetIndex, 0)
	pw.Offset = 4
	pw.Footer = parquet.NewFileMetaData()
	pw.Footer.Version = 2
	pw.Footer.CreatedBy = common.ToPtr(createdByString())
	pw.stopped = true
}

func (pw *ParquetWriter) createCompressors(cfg writerConfig) error {
	if cfg.compressionLevels == nil {
		return nil
	}
	pw.compressors = make(map[parquet.CompressionCodec]*compress.Compressor, len(cfg.compressionLevels))
	for codec, level := range cfg.compressionLevels {
		c, err := compress.NewCompressor(codec, level)
		if err != nil {
			return fmt.Errorf("create compressor for codec %v at level %d: %w", codec, level, err)
		}
		pw.compressors[codec] = c
	}
	return nil
}

// NewParquetWriterFromWriter creates a ParquetWriter that writes to an io.Writer.
func NewParquetWriterFromWriter(w io.Writer, obj any, opts ...WriterOption) (*ParquetWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewParquetWriter(wf, obj, opts...)
}

// NewParquetWriter creates a parquet writer. Obj is an object with tags,
// a JSON schema string, a *schema.SchemaHandler, or []*parquet.SchemaElement.
// Pass nil for obj to set the schema later via SetSchemaHandlerFromJSON.
func NewParquetWriter(pFile source.ParquetFileWriter, obj any, opts ...WriterOption) (*ParquetWriter, error) {
	cfg := defaultWriterConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, fmt.Errorf("apply writer option: %w", err)
		}
	}

	// Cross-validate
	if cfg.rowGroupSize < cfg.pageSize {
		return nil, fmt.Errorf("row group size (%d) must be >= page size (%d)", cfg.rowGroupSize, cfg.pageSize)
	}

	res := new(ParquetWriter)
	applyWriterConfig(res, cfg)
	res.PFile = pFile

	if err := res.createCompressors(cfg); err != nil {
		return nil, err
	}

	res.MarshalFunc = marshal.Marshal

	if obj != nil {
		var err error
		if sa, ok := obj.(string); ok {
			// SetSchemaHandlerFromJSON handles validation, PAR1 write, and bloom filter init
			err = res.SetSchemaHandlerFromJSON(sa)
			if err != nil {
				return nil, fmt.Errorf("set schema from JSON: %w", err)
			}
		} else {
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

			if err := res.SchemaHandler.ValidateEncodingsForDataPageVersion(res.dataPageVersion); err != nil {
				return nil, fmt.Errorf("encoding validation: %w", err)
			}

			if _, err := res.PFile.Write([]byte("PAR1")); err != nil {
				return nil, fmt.Errorf("write magic header: %w", err)
			}
			res.headerWritten = true
			res.initBloomFilters()
		}
	}

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

	if err := pw.SchemaHandler.ValidateEncodingsForDataPageVersion(pw.dataPageVersion); err != nil {
		return fmt.Errorf("encoding validation: %w", err)
	}

	// Write deferred magic header if not yet written (obj was nil at construction)
	if !pw.headerWritten {
		if _, err := pw.PFile.Write([]byte("PAR1")); err != nil {
			return fmt.Errorf("write magic header: %w", err)
		}
		pw.headerWritten = true
	}
	pw.initBloomFilters()
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

// Write the footer and stop writing
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

	// write ColumnIndex
	if len(pw.ColumnIndexes) > 0 {
		idx := 0
		for _, rowGroup := range pw.Footer.RowGroups {
			for _, columnChunk := range rowGroup.Columns {
				columnIndexBuf, err := ts.Write(context.TODO(), pw.ColumnIndexes[idx])
				if err != nil {
					return fmt.Errorf("serialize column index: %w", err)
				}
				if _, err = pw.PFile.Write(columnIndexBuf); err != nil {
					return fmt.Errorf("write column index: %w", err)
				}

				idx++

				pos := pw.Offset
				columnChunk.ColumnIndexOffset = &pos
				columnIndexBufSize := int32(len(columnIndexBuf))
				columnChunk.ColumnIndexLength = &columnIndexBufSize

				pw.Offset += int64(columnIndexBufSize)
			}
		}
	}

	// write OffsetIndex
	if len(pw.OffsetIndexes) > 0 {
		idx := 0
		for _, rowGroup := range pw.Footer.RowGroups {
			for _, columnChunk := range rowGroup.Columns {
				offsetIndexBuf, err := ts.Write(context.TODO(), pw.OffsetIndexes[idx])
				if err != nil {
					return fmt.Errorf("serialize offset index: %w", err)
				}
				if _, err = pw.PFile.Write(offsetIndexBuf); err != nil {
					return fmt.Errorf("write offset index: %w", err)
				}

				idx++

				pos := pw.Offset
				columnChunk.OffsetIndexOffset = &pos
				offsetIndexBufSize := int32(len(offsetIndexBuf))
				columnChunk.OffsetIndexLength = &offsetIndexBufSize

				pw.Offset += int64(offsetIndexBufSize)
			}
		}
	}

	// write BloomFilter
	if len(pw.BloomFilterData) > 0 {
		idx := 0
		for _, rowGroup := range pw.Footer.RowGroups {
			for _, columnChunk := range rowGroup.Columns {
				data := pw.BloomFilterData[idx]
				idx++
				if data == nil {
					continue
				}
				if _, err = pw.PFile.Write(data); err != nil {
					return fmt.Errorf("write bloom filter: %w", err)
				}
				pos := pw.Offset
				columnChunk.MetaData.BloomFilterOffset = &pos
				totalLen := int32(len(data))
				columnChunk.MetaData.BloomFilterLength = &totalLen
				pw.Offset += int64(totalLen)
			}
		}
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

// Write one object to parquet file
func (pw *ParquetWriter) Write(src any) error {
	if pw.stopped {
		return fmt.Errorf("writer stopped")
	}

	var err error
	ln := int64(len(pw.Objs))

	val := reflect.ValueOf(src)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		src = val.Interface()
	}

	if pw.CheckSizeCritical <= ln {
		pw.ObjSize = (pw.ObjSize+common.SizeOf(val))/2 + 1
	}
	pw.ObjsSize += pw.ObjSize
	pw.Objs = append(pw.Objs, src)

	criSize := pw.np * pw.pageSize * pw.SchemaHandler.GetColumnNum()

	if pw.ObjsSize >= criSize {
		err = pw.Flush(false)
	} else {
		dln := (criSize - pw.ObjsSize + pw.ObjSize - 1) / pw.ObjSize / 2
		pw.CheckSizeCritical = dln + ln
	}
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

func (pw *ParquetWriter) flushObjs() error {
	var err error
	l := int64(len(pw.Objs))
	if l <= 0 {
		return nil
	}
	pagesMapList := make([]map[string][]*layout.Page, pw.np)
	for i := range int(pw.np) {
		pagesMapList[i] = make(map[string][]*layout.Page)
	}

	delta := (l + pw.np - 1) / pw.np
	// layout.TableTo(Data|Dict)Pages appears not to be thread-safe; guard calls
	// with a conversion mutex to avoid races observed in tests.
	var convMu sync.Mutex
	// bloomMu protects bloom filter Insert calls from concurrent goroutines.
	var bloomMu sync.Mutex
	var wg sync.WaitGroup
	var errs []error = make([]error, pw.np)

	for c := range pw.np {
		bgn := c * delta
		end := bgn + delta
		if end > l {
			end = l
		}
		if bgn >= l {
			bgn, end = l, l
		}

		wg.Add(1)
		go func(b, e int, index int64) {
			defer wg.Done()

			if e <= b {
				return
			}

			tableMap, err2 := pw.MarshalFunc(pw.Objs[b:e], pw.SchemaHandler)

			if err2 == nil {
				for name, table := range *tableMap {
					// Collect values for bloom filter before dict encoding converts them to indices.
					if bf, ok := pw.BloomFilters[name]; ok && table.Schema.Type != nil {
						bloomMu.Lock()
						for _, val := range table.Values {
							if val == nil {
								continue
							}
							if h, hashErr := bloomfilter.HashValue(val, *table.Schema.Type); hashErr == nil {
								bf.Insert(h)
							}
						}
						bloomMu.Unlock()
					}

					// Use per-column compression if specified, otherwise fall back to file-level compression
					compressionType := pw.compressionType
					if table.Info != nil && table.Info.CompressionType != nil {
						compressionType = *table.Info.CompressionType
					}

					if table.Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY ||
						table.Info.Encoding == parquet.Encoding_RLE_DICTIONARY {
						// Load or create the dictionary recorder atomically
						var dictRec *layout.DictRecType
						if v, ok := pw.DictRecs.Load(name); ok {
							dictRec = v.(*layout.DictRecType)
						} else {
							newRec := layout.NewDictRec(*table.Schema.Type)
							actual, _ := pw.DictRecs.LoadOrStore(name, newRec)
							dictRec = actual.(*layout.DictRecType)
						}

						// mutiple goroutines may write to same dict page
						convMu.Lock()
						pages, _, localErr := layout.TableToDictDataPages(dictRec, table, 32, layout.PageWriteOption{
							PageSize:     int32(pw.pageSize),
							CompressType: compressionType,
							WriteCRC:     pw.writeCRC,
							Compressors:  pw.compressors,
						})
						convMu.Unlock()
						if localErr != nil {
							errs[index] = localErr
						} else {
							pagesMapList[index][name] = pages
						}
					} else {
						pages, _, localErr := layout.TableToDataPages(table, layout.PageWriteOption{
							PageSize:        int32(pw.pageSize),
							CompressType:    compressionType,
							DataPageVersion: pw.dataPageVersion,
							WriteCRC:        pw.writeCRC,
							Compressors:     pw.compressors,
						})
						if localErr != nil {
							errs[index] = localErr
						} else {
							pagesMapList[index][name] = pages
						}
					}
				}
			} else {
				errs[index] = err2
			}
		}(int(bgn), int(end), c)
	}

	wg.Wait()
	for _, err2 := range errs {
		if err2 != nil {
			err = err2
			break
		}
	}

	for _, pagesMap := range pagesMapList {
		for name, pages := range pagesMap {
			if _, ok := pw.PagesMapBuf[name]; !ok {
				pw.PagesMapBuf[name] = pages
			} else {
				pw.PagesMapBuf[name] = append(pw.PagesMapBuf[name], pages...)
			}
			for _, page := range pages {
				pw.Size += int64(len(page.RawData))
				page.DataTable = nil // release memory
			}
		}
	}

	pw.NumRows += int64(len(pw.Objs))
	if err != nil {
		return fmt.Errorf("flush objects: %w", err)
	}
	return nil
}

// Flush the write buffer to parquet file
func (pw *ParquetWriter) Flush(flag bool) error {
	var err error

	if err = pw.flushObjs(); err != nil {
		return fmt.Errorf("flush objects during flush: %w", err)
	}

	if (pw.Size+pw.ObjsSize >= pw.rowGroupSize || flag) && len(pw.PagesMapBuf) > 0 {
		// pages -> chunk
		chunkMap := make(map[string]*layout.Chunk)
		for name, pages := range pw.PagesMapBuf {
			// Determine compression type for this column
			compressionType := pw.compressionType
			if idx, ok := pw.SchemaHandler.MapIndex[name]; ok {
				if info := pw.SchemaHandler.Infos[idx]; info != nil && info.CompressionType != nil {
					compressionType = *info.CompressionType
				}
			}

			if len(pages) > 0 && (pages[0].Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY || pages[0].Info.Encoding == parquet.Encoding_RLE_DICTIONARY) {
				v, ok := pw.DictRecs.Load(name)
				if !ok {
					return fmt.Errorf("missing dictionary recorder for column %s", name)
				}
				dictRec := v.(*layout.DictRecType)
				dictPage, _, err := layout.DictRecToDictPage(dictRec, layout.PageWriteOption{
					PageSize:     int32(pw.pageSize),
					CompressType: compressionType,
					WriteCRC:     pw.writeCRC,
					Compressors:  pw.compressors,
				})
				if err != nil {
					return fmt.Errorf("convert dict rec to dict page for column %s: %w", name, err)
				}
				tmp := append([]*layout.Page{dictPage}, pages...)
				chunkMap[name], err = layout.PagesToDictChunk(tmp)
				if err != nil {
					return fmt.Errorf("convert pages to dict chunk for column %s: %w", name, err)
				}
			} else {
				chunkMap[name], err = layout.PagesToChunk(pages)
				if err != nil {
					return fmt.Errorf("convert pages to chunk for column %s: %w", name, err)
				}
			}
		}

		pw.DictRecs.Clear()

		// chunks -> rowGroup
		rowGroup := layout.NewRowGroup()
		rowGroup.RowGroupHeader.Columns = make([]*parquet.ColumnChunk, 0)

		for k := range len(pw.SchemaHandler.SchemaElements) {
			// for _, chunk := range chunkMap {
			schema := pw.SchemaHandler.SchemaElements[k]
			if schema.GetNumChildren() > 0 {
				continue
			}
			chunk := chunkMap[pw.SchemaHandler.IndexMap[int32(k)]]
			if chunk == nil {
				continue
			}
			rowGroup.Chunks = append(rowGroup.Chunks, chunk)
			// rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalCompressedSize
			rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalUncompressedSize
			rowGroup.RowGroupHeader.Columns = append(rowGroup.RowGroupHeader.Columns, chunk.ChunkHeader)
		}
		rowGroup.RowGroupHeader.NumRows = pw.NumRows
		pw.NumRows = 0

		for k := range len(rowGroup.Chunks) {
			rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = -1
			rowGroup.Chunks[k].ChunkHeader.FileOffset = pw.Offset

			pages := rowGroup.Chunks[k].Pages
			dataPageCount := 0
			for _, p := range pages {
				if p.Header.Type != parquet.PageType_DICTIONARY_PAGE {
					dataPageCount++
				}
			}

			// add ColumnIndex
			columnIndex := parquet.NewColumnIndex()
			columnIndex.NullPages = make([]bool, dataPageCount)
			columnIndex.MinValues = make([][]byte, dataPageCount)
			columnIndex.MaxValues = make([][]byte, dataPageCount)
			columnIndex.BoundaryOrder = parquet.BoundaryOrder_UNORDERED
			pw.ColumnIndexes = append(pw.ColumnIndexes, columnIndex)

			// add OffsetIndex
			offsetIndex := parquet.NewOffsetIndex()
			offsetIndex.PageLocations = make([]*parquet.PageLocation, 0)
			pw.OffsetIndexes = append(pw.OffsetIndexes, offsetIndex)

			firstRowIndex := int64(0)
			dataPageIdx := 0

			for l := range len(pages) {
				page := pages[l]
				if page.Header.Type == parquet.PageType_DICTIONARY_PAGE {
					tmp := pw.Offset
					rowGroup.Chunks[k].ChunkHeader.MetaData.DictionaryPageOffset = &tmp
				} else if rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset <= 0 {
					rowGroup.Chunks[k].ChunkHeader.MetaData.DataPageOffset = pw.Offset
				}

				// only record DataPage
				if page.Header.Type != parquet.PageType_DICTIONARY_PAGE {
					if page.Header.DataPageHeader == nil && page.Header.DataPageHeaderV2 == nil {
						return fmt.Errorf("unsupported data page: %s", page.Header.String())
					}

					var minVal []byte
					var maxVal []byte
					var nullCount *int64
					if page.Header.DataPageHeader != nil && page.Header.DataPageHeader.Statistics != nil {
						minVal = page.Header.DataPageHeader.Statistics.Min
						maxVal = page.Header.DataPageHeader.Statistics.Max
						nullCount = page.Header.DataPageHeader.Statistics.NullCount

					} else if page.Header.DataPageHeaderV2 != nil && page.Header.DataPageHeaderV2.Statistics != nil {
						minVal = page.Header.DataPageHeaderV2.Statistics.Min
						maxVal = page.Header.DataPageHeaderV2.Statistics.Max
						nullCount = page.Header.DataPageHeaderV2.Statistics.NullCount
					}

					columnIndex.MinValues[dataPageIdx] = minVal
					columnIndex.MaxValues[dataPageIdx] = maxVal
					// Statistics.NullCount is nil when statistics are omitted for the column otherwise for all column page headers it will be populated.
					if nullCount != nil {
						if columnIndex.NullCounts == nil {
							columnIndex.NullCounts = make([]int64, dataPageCount)
						}
						columnIndex.NullCounts[dataPageIdx] = *nullCount
					}

					// Append pre-computed per-page level histograms for ColumnIndex.
					if page.DefinitionLevelHistogram != nil {
						columnIndex.DefinitionLevelHistograms = append(columnIndex.DefinitionLevelHistograms, page.DefinitionLevelHistogram...)
					}
					if page.RepetitionLevelHistogram != nil {
						columnIndex.RepetitionLevelHistograms = append(columnIndex.RepetitionLevelHistograms, page.RepetitionLevelHistogram...)
					}

					pageLocation := parquet.NewPageLocation()
					pageLocation.Offset = pw.Offset
					pageLocation.FirstRowIndex = firstRowIndex
					pageLocation.CompressedPageSize = int32(len(page.RawData))

					offsetIndex.PageLocations = append(offsetIndex.PageLocations, pageLocation)

					if page.Header.DataPageHeader != nil {
						firstRowIndex += int64(page.Header.DataPageHeader.NumValues)
					} else if page.Header.DataPageHeaderV2 != nil {
						firstRowIndex += int64(page.Header.DataPageHeaderV2.NumValues)
					}
					dataPageIdx++
				}

				data := page.RawData
				if _, err = pw.PFile.Write(data); err != nil {
					return fmt.Errorf("write page data: %w", err)
				}
				pw.Offset += int64(len(data))
			}
		}

		pw.Footer.RowGroups = append(pw.Footer.RowGroups, rowGroup.RowGroupHeader)

		// Serialize bloom filter data for this row group, one entry per column chunk.
		if len(pw.BloomFilters) > 0 {
			ts := thrift.NewTSerializer()
			ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
			for k := range len(pw.SchemaHandler.SchemaElements) {
				se := pw.SchemaHandler.SchemaElements[k]
				if se.GetNumChildren() > 0 {
					continue
				}
				path := pw.SchemaHandler.IndexMap[int32(k)]
				bf, ok := pw.BloomFilters[path]
				if !ok {
					pw.BloomFilterData = append(pw.BloomFilterData, nil)
					continue
				}
				headerBuf, err := ts.Write(context.TODO(), bf.Header())
				if err != nil {
					return fmt.Errorf("serialize bloom filter header for column %s: %w", path, err)
				}
				pw.BloomFilterData = append(pw.BloomFilterData, append(headerBuf, bf.Bitset()...))
			}
			// Reset bloom filters for next row group
			pw.initBloomFilters()
		}

		pw.Size = 0
		pw.PagesMapBuf = make(map[string][]*layout.Page)
	}
	pw.Footer.NumRows += int64(len(pw.Objs))
	pw.Objs = pw.Objs[:0]
	pw.ObjsSize = 0
	return nil
}

// initBloomFilters creates bloom filters for columns that have bloomfilter=true in their tags.
func (pw *ParquetWriter) initBloomFilters() {
	if pw.SchemaHandler == nil {
		return
	}
	pw.BloomFilters = make(map[string]*bloomfilter.Filter)
	for i, info := range pw.SchemaHandler.Infos {
		if info == nil || !info.BloomFilter {
			continue
		}
		path := pw.SchemaHandler.IndexMap[int32(i)]
		numBytes := int(info.BloomFilterSize)
		if numBytes <= 0 {
			numBytes = bloomfilter.DefaultNumBytes
		}
		pw.BloomFilters[path] = bloomfilter.New(numBytes)
	}
}
