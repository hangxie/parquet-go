package writer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/bloomfilter"
	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/layout"
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

	np              int64 // parallel number
	pageSize        int64
	rowGroupSize    int64
	compressionType parquet.CompressionCodec
	dataPageVersion int32 // 1 for DATA_PAGE (default), 2 for DATA_PAGE_V2
	writeCRC        bool  // compute and write CRC32 checksums on pages (default false)
	offset          int64

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

	res.initBloomFilters()

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
	pw.initBloomFilters()
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

// Write the footer and stop writing
func (pw *ParquetWriter) writeColumnIndexes(ts *thrift.TSerializer) error {
	if len(pw.columnIndexes) == 0 {
		return nil
	}
	idx := 0
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			columnIndexBuf, err := ts.Write(context.TODO(), pw.columnIndexes[idx])
			if err != nil {
				return fmt.Errorf("serialize column index: %w", err)
			}
			if _, err = pw.PFile.Write(columnIndexBuf); err != nil {
				return fmt.Errorf("write column index: %w", err)
			}

			idx++

			pos := pw.offset
			columnChunk.ColumnIndexOffset = &pos
			columnIndexBufSize := int32(len(columnIndexBuf))
			columnChunk.ColumnIndexLength = &columnIndexBufSize

			pw.offset += int64(columnIndexBufSize)
		}
	}
	return nil
}

func (pw *ParquetWriter) writeOffsetIndexes(ts *thrift.TSerializer) error {
	if len(pw.offsetIndexes) == 0 {
		return nil
	}
	idx := 0
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			offsetIndexBuf, err := ts.Write(context.TODO(), pw.offsetIndexes[idx])
			if err != nil {
				return fmt.Errorf("serialize offset index: %w", err)
			}
			if _, err = pw.PFile.Write(offsetIndexBuf); err != nil {
				return fmt.Errorf("write offset index: %w", err)
			}

			idx++

			pos := pw.offset
			columnChunk.OffsetIndexOffset = &pos
			offsetIndexBufSize := int32(len(offsetIndexBuf))
			columnChunk.OffsetIndexLength = &offsetIndexBufSize

			pw.offset += int64(offsetIndexBufSize)
		}
	}
	return nil
}

func (pw *ParquetWriter) writeBloomFilters() error {
	if len(pw.bloomFilterData) == 0 {
		return nil
	}
	idx := 0
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			data := pw.bloomFilterData[idx]
			idx++
			if data == nil {
				continue
			}
			if _, err := pw.PFile.Write(data); err != nil {
				return fmt.Errorf("write bloom filter: %w", err)
			}
			pos := pw.offset
			columnChunk.MetaData.BloomFilterOffset = &pos
			totalLen := int32(len(data))
			columnChunk.MetaData.BloomFilterLength = &totalLen
			pw.offset += int64(totalLen)
		}
	}
	return nil
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

func (pw *ParquetWriter) insertBloomValues(name string, table *layout.Table, bloomMu *sync.Mutex) {
	bf, ok := pw.bloomFilters[name]
	if !ok || table.Schema.Type == nil {
		return
	}
	bloomMu.Lock()
	defer bloomMu.Unlock()
	for _, val := range table.Values {
		if val == nil {
			continue
		}
		if h, hashErr := bloomfilter.HashValue(val, *table.Schema.Type); hashErr == nil {
			bf.Insert(h)
		}
	}
}

func (pw *ParquetWriter) tableCompressionType(table *layout.Table) parquet.CompressionCodec {
	if table.Info != nil && table.Info.CompressionType != nil {
		return *table.Info.CompressionType
	}
	return pw.compressionType
}

func (pw *ParquetWriter) tableToDictPages(name string, table *layout.Table, compressionType parquet.CompressionCodec, convMu *sync.Mutex) ([]*layout.Page, error) {
	var dictRec *layout.DictRecType
	if v, ok := pw.DictRecs.Load(name); ok {
		dictRec = v.(*layout.DictRecType)
	} else {
		newRec := layout.NewDictRec(*table.Schema.Type)
		actual, _ := pw.DictRecs.LoadOrStore(name, newRec)
		dictRec = actual.(*layout.DictRecType)
	}

	convMu.Lock()
	pages, _, err := layout.TableToDictDataPagesWithOption(dictRec, table, 32, layout.PageWriteOption{
		PageSize:     int32(pw.pageSize),
		CompressType: compressionType,
		WriteCRC:     pw.writeCRC,
	})
	convMu.Unlock()
	return pages, err
}

func (pw *ParquetWriter) tableToPlainPages(table *layout.Table, compressionType parquet.CompressionCodec) ([]*layout.Page, error) {
	pages, _, err := layout.TableToDataPagesWithOption(table, layout.PageWriteOption{
		PageSize:        int32(pw.pageSize),
		CompressType:    compressionType,
		DataPageVersion: pw.dataPageVersion,
		WriteCRC:        pw.writeCRC,
	})
	return pages, err
}

func (pw *ParquetWriter) convertTableToPages(name string, table *layout.Table, convMu *sync.Mutex) ([]*layout.Page, error) {
	compressionType := pw.tableCompressionType(table)
	if table.Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY ||
		table.Info.Encoding == parquet.Encoding_RLE_DICTIONARY {
		return pw.tableToDictPages(name, table, compressionType, convMu)
	}
	return pw.tableToPlainPages(table, compressionType)
}

func (pw *ParquetWriter) mergePageResults(pagesMapList []map[string][]*layout.Page) {
	for _, pagesMap := range pagesMapList {
		for name, pages := range pagesMap {
			if _, ok := pw.pagesMapBuf[name]; !ok {
				pw.pagesMapBuf[name] = pages
			} else {
				pw.pagesMapBuf[name] = append(pw.pagesMapBuf[name], pages...)
			}
			for _, page := range pages {
				pw.size += int64(len(page.RawData))
				page.DataTable = nil // release memory
			}
		}
	}
}

func (pw *ParquetWriter) flushObjs() error {
	l := int64(len(pw.objs))
	if l <= 0 {
		return nil
	}
	pagesMapList := make([]map[string][]*layout.Page, pw.np)
	for i := range pw.np {
		pagesMapList[i] = make(map[string][]*layout.Page)
	}

	delta := (l + pw.np - 1) / pw.np
	var convMu sync.Mutex
	var bloomMu sync.Mutex
	var wg sync.WaitGroup
	errs := make([]error, pw.np)

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

			tableMap, err2 := pw.marshalFunc(pw.objs[b:e], pw.SchemaHandler)
			if err2 != nil {
				errs[index] = err2
				return
			}

			for name, table := range *tableMap {
				pw.insertBloomValues(name, table, &bloomMu)
				pages, localErr := pw.convertTableToPages(name, table, &convMu)
				if localErr != nil {
					errs[index] = localErr
					return
				}
				pagesMapList[index][name] = pages
			}
		}(int(bgn), int(end), c)
	}

	wg.Wait()

	var err error
	for _, err2 := range errs {
		if err2 != nil {
			err = err2
			break
		}
	}

	pw.mergePageResults(pagesMapList)
	pw.numRows += int64(len(pw.objs))
	if err != nil {
		return fmt.Errorf("flush objects: %w", err)
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

type pageStats struct {
	minVal    []byte
	maxVal    []byte
	nullCount *int64
}

func extractPageStats(page *layout.Page) pageStats {
	if page.Header.DataPageHeader != nil && page.Header.DataPageHeader.Statistics != nil {
		s := page.Header.DataPageHeader.Statistics
		return pageStats{minVal: s.Min, maxVal: s.Max, nullCount: s.NullCount}
	}
	if page.Header.DataPageHeaderV2 != nil && page.Header.DataPageHeaderV2.Statistics != nil {
		s := page.Header.DataPageHeaderV2.Statistics
		return pageStats{minVal: s.Min, maxVal: s.Max, nullCount: s.NullCount}
	}
	return pageStats{}
}

func dataPageNumValues(page *layout.Page) int64 {
	if page.Header.DataPageHeader != nil {
		return int64(page.Header.DataPageHeader.NumValues)
	}
	if page.Header.DataPageHeaderV2 != nil {
		return int64(page.Header.DataPageHeaderV2.NumValues)
	}
	return 0
}

func (pw *ParquetWriter) recordDataPage(page *layout.Page, columnIndex *parquet.ColumnIndex, offsetIndex *parquet.OffsetIndex, dataPageIdx, dataPageCount int, firstRowIndex *int64) error {
	if page.Header.DataPageHeader == nil && page.Header.DataPageHeaderV2 == nil {
		return fmt.Errorf("unsupported data page: %s", page.Header.String())
	}

	stats := extractPageStats(page)
	columnIndex.MinValues[dataPageIdx] = stats.minVal
	columnIndex.MaxValues[dataPageIdx] = stats.maxVal
	if stats.nullCount != nil {
		if columnIndex.NullCounts == nil {
			columnIndex.NullCounts = make([]int64, dataPageCount)
		}
		columnIndex.NullCounts[dataPageIdx] = *stats.nullCount
	}

	if page.DefinitionLevelHistogram != nil {
		columnIndex.DefinitionLevelHistograms = append(columnIndex.DefinitionLevelHistograms, page.DefinitionLevelHistogram...)
	}
	if page.RepetitionLevelHistogram != nil {
		columnIndex.RepetitionLevelHistograms = append(columnIndex.RepetitionLevelHistograms, page.RepetitionLevelHistogram...)
	}

	pageLocation := parquet.NewPageLocation()
	pageLocation.Offset = pw.offset
	pageLocation.FirstRowIndex = *firstRowIndex
	pageLocation.CompressedPageSize = int32(len(page.RawData))
	offsetIndex.PageLocations = append(offsetIndex.PageLocations, pageLocation)

	*firstRowIndex += dataPageNumValues(page)
	return nil
}

func (pw *ParquetWriter) writeChunkPages(chunk *layout.Chunk, _ int) error {
	chunk.ChunkHeader.MetaData.DataPageOffset = -1
	chunk.ChunkHeader.FileOffset = pw.offset

	pages := chunk.Pages
	dataPageCount := 0
	for _, p := range pages {
		if p.Header.Type != parquet.PageType_DICTIONARY_PAGE {
			dataPageCount++
		}
	}

	columnIndex := parquet.NewColumnIndex()
	columnIndex.NullPages = make([]bool, dataPageCount)
	columnIndex.MinValues = make([][]byte, dataPageCount)
	columnIndex.MaxValues = make([][]byte, dataPageCount)
	columnIndex.BoundaryOrder = parquet.BoundaryOrder_UNORDERED
	pw.columnIndexes = append(pw.columnIndexes, columnIndex)

	offsetIndex := parquet.NewOffsetIndex()
	offsetIndex.PageLocations = make([]*parquet.PageLocation, 0)
	pw.offsetIndexes = append(pw.offsetIndexes, offsetIndex)

	firstRowIndex := int64(0)
	dataPageIdx := 0

	for _, page := range pages {
		if page.Header.Type == parquet.PageType_DICTIONARY_PAGE {
			tmp := pw.offset
			chunk.ChunkHeader.MetaData.DictionaryPageOffset = &tmp
		} else if chunk.ChunkHeader.MetaData.DataPageOffset <= 0 {
			chunk.ChunkHeader.MetaData.DataPageOffset = pw.offset
		}

		if page.Header.Type != parquet.PageType_DICTIONARY_PAGE {
			if err := pw.recordDataPage(page, columnIndex, offsetIndex, dataPageIdx, dataPageCount, &firstRowIndex); err != nil {
				return err
			}
			dataPageIdx++
		}

		if _, err := pw.PFile.Write(page.RawData); err != nil {
			return fmt.Errorf("write page data: %w", err)
		}
		pw.offset += int64(len(page.RawData))
	}
	return nil
}

func (pw *ParquetWriter) serializeBloomFilters() error {
	if len(pw.bloomFilters) == 0 {
		return nil
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	for k := range len(pw.SchemaHandler.SchemaElements) {
		se := pw.SchemaHandler.SchemaElements[k]
		if se.GetNumChildren() > 0 {
			continue
		}
		path := pw.SchemaHandler.IndexMap[int32(k)]
		bf, ok := pw.bloomFilters[path]
		if !ok {
			pw.bloomFilterData = append(pw.bloomFilterData, nil)
			continue
		}
		headerBuf, err := ts.Write(context.TODO(), bf.Header())
		if err != nil {
			return fmt.Errorf("serialize bloom filter header for column %s: %w", path, err)
		}
		pw.bloomFilterData = append(pw.bloomFilterData, append(headerBuf, bf.Bitset()...))
	}
	pw.initBloomFilters()
	return nil
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

// initBloomFilters creates bloom filters for columns that have bloomfilter=true in their tags.
func (pw *ParquetWriter) initBloomFilters() {
	if pw.SchemaHandler == nil {
		return
	}
	pw.bloomFilters = make(map[string]*bloomfilter.Filter)
	for i, info := range pw.SchemaHandler.Infos {
		if info == nil || !info.BloomFilter {
			continue
		}
		path := pw.SchemaHandler.IndexMap[int32(i)]
		numBytes := int(info.BloomFilterSize)
		if numBytes <= 0 {
			numBytes = bloomfilter.DefaultNumBytes
		}
		pw.bloomFilters[path] = bloomfilter.New(numBytes)
	}
}
