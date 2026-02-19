package writer

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v2/bloomfilter"
	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/marshal"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
)

// ParquetWriter is a writer  parquet file
type ParquetWriter struct {
	SchemaHandler *schema.SchemaHandler
	NP            int64 // parallel number
	Footer        *parquet.FileMetaData
	PFile         source.ParquetFileWriter

	PageSize        int64
	RowGroupSize    int64
	CompressionType parquet.CompressionCodec
	DataPageVersion int32 // 1 for DATA_PAGE (default), 2 for DATA_PAGE_V2
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
	// Key is the column path string (e.g. "Parquet_go_root\x01Name").
	BloomFilters map[string]*bloomfilter.Filter
	// BloomFilterData holds serialized (header+bitset) per column chunk, parallel to ColumnIndexes/OffsetIndexes.
	BloomFilterData [][]byte

	MarshalFunc func(src []any, sh *schema.SchemaHandler) (*map[string]*layout.Table, error)

	stopped            bool
	encodingsValidated bool // tracks if encoding/version validation has been done
}

func NewParquetWriterFromWriter(w io.Writer, obj any, np int64) (*ParquetWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewParquetWriter(wf, obj, np)
}

// Create a parquet handler. Obj is a object with tags or JSON schema string.
func NewParquetWriter(pFile source.ParquetFileWriter, obj any, np int64) (*ParquetWriter, error) {
	var err error

	res := new(ParquetWriter)
	res.NP = np
	res.PageSize = common.DefaultPageSize         // 8K
	res.RowGroupSize = common.DefaultRowGroupSize // 128M
	res.CompressionType = parquet.CompressionCodec_SNAPPY
	res.DataPageVersion = 1 // default to DATA_PAGE (V1)
	res.ObjsSize = 0
	res.CheckSizeCritical = 0
	res.Size = 0
	res.NumRows = 0
	res.Offset = 4
	res.PFile = pFile
	res.PagesMapBuf = make(map[string][]*layout.Page)
	// DictRecs sync.Map zero value is ready to use
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 2
	res.ColumnIndexes = make([]*parquet.ColumnIndex, 0)
	res.OffsetIndexes = make([]*parquet.OffsetIndex, 0)
	// include the createdBy to avoid
	// WARN  CorruptStatistics:118 - Ignoring statistics because created_by is null or empty! See PARQUET-251 and PARQUET-297
	res.Footer.CreatedBy = common.ToPtr("github.com/hangxie/parquet-go v2 latest")
	_, err = res.PFile.Write([]byte("PAR1"))
	if err != nil {
		return nil, fmt.Errorf("write magic header: %w", err)
	}
	res.MarshalFunc = marshal.Marshal
	res.stopped = true

	if obj != nil {
		if sa, ok := obj.(string); ok {
			// SetSchemaHandlerFromJSON handles Footer.Schema internally
			err = res.SetSchemaHandlerFromJSON(sa)
			if err != nil {
				res.stopped = true
				return res, fmt.Errorf("set schema from JSON: %w", err)
			}
		} else {
			if sa, ok := obj.(*schema.SchemaHandler); ok {
				res.SchemaHandler = schema.NewSchemaHandlerFromSchemaHandler(sa)
			} else if sa, ok := obj.([]*parquet.SchemaElement); ok {
				res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(sa)
			} else {
				if res.SchemaHandler, err = schema.NewSchemaHandlerFromStruct(obj); err != nil {
					return res, fmt.Errorf("build schema handler: %w", err)
				}
			}
			res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
		}
	}

	res.initBloomFilters()

	// Enable writing after init completed successfully
	res.stopped = false

	return res, err
}

func (pw *ParquetWriter) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error
	if pw.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return fmt.Errorf("parse JSON schema: %w", err)
	}
	pw.Footer.Schema = pw.Footer.Schema[:0]
	pw.Footer.Schema = append(pw.Footer.Schema, pw.SchemaHandler.SchemaElements...)
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

	// Validate encodings are compatible with data page version (once per writer)
	if !pw.encodingsValidated {
		if err := pw.SchemaHandler.ValidateEncodingsForDataPageVersion(pw.DataPageVersion); err != nil {
			return fmt.Errorf("encoding validation: %w", err)
		}
		pw.encodingsValidated = true
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

	criSize := pw.NP * pw.PageSize * pw.SchemaHandler.GetColumnNum()

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
	pagesMapList := make([]map[string][]*layout.Page, pw.NP)
	for i := range int(pw.NP) {
		pagesMapList[i] = make(map[string][]*layout.Page)
	}

	delta := (l + pw.NP - 1) / pw.NP
	// layout.TableTo(Data|Dict)Pages appears not to be thread-safe; guard calls
	// with a conversion mutex to avoid races observed in tests.
	var convMu sync.Mutex
	// bloomMu protects bloom filter Insert calls from concurrent goroutines.
	var bloomMu sync.Mutex
	var wg sync.WaitGroup
	var errs []error = make([]error, pw.NP)

	for c := range pw.NP {
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
					compressionType := pw.CompressionType
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
						pages, _, localErr := layout.TableToDictDataPages(dictRec, table, int32(pw.PageSize), 32, compressionType)
						convMu.Unlock()
						if localErr != nil {
							errs[index] = localErr
						} else {
							pagesMapList[index][name] = pages
						}
					} else {
						pages, _, localErr := layout.TableToDataPagesWithVersion(table, int32(pw.PageSize), compressionType, pw.DataPageVersion)
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

	if (pw.Size+pw.ObjsSize >= pw.RowGroupSize || flag) && len(pw.PagesMapBuf) > 0 {
		// pages -> chunk
		chunkMap := make(map[string]*layout.Chunk)
		for name, pages := range pw.PagesMapBuf {
			// Determine compression type for this column
			compressionType := pw.CompressionType
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
				dictPage, _, err := layout.DictRecToDictPage(dictRec, int32(pw.PageSize), compressionType)
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

			pageCount := len(rowGroup.Chunks[k].Pages)

			// add ColumnIndex
			columnIndex := parquet.NewColumnIndex()
			columnIndex.NullPages = make([]bool, pageCount)
			columnIndex.MinValues = make([][]byte, pageCount)
			columnIndex.MaxValues = make([][]byte, pageCount)
			columnIndex.BoundaryOrder = parquet.BoundaryOrder_UNORDERED
			pw.ColumnIndexes = append(pw.ColumnIndexes, columnIndex)

			// add OffsetIndex
			offsetIndex := parquet.NewOffsetIndex()
			offsetIndex.PageLocations = make([]*parquet.PageLocation, 0)
			pw.OffsetIndexes = append(pw.OffsetIndexes, offsetIndex)

			firstRowIndex := int64(0)

			for l := range pageCount {
				page := rowGroup.Chunks[k].Pages[l]
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

					columnIndex.MinValues[l] = minVal
					columnIndex.MaxValues[l] = maxVal
					// Statistics.NullCount is nil when statistics are omitted for the column otherwise for all column page headers it will be populated.
					if nullCount != nil {
						if columnIndex.NullCounts == nil {
							columnIndex.NullCounts = make([]int64, pageCount)
						}
						columnIndex.NullCounts[l] = *nullCount
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
