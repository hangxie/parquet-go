package writer

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

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
	Offset          int64

	Objs              []any
	ObjsSize          int64
	ObjSize           int64
	CheckSizeCritical int64

	PagesMapBuf map[string][]*layout.Page
	Size        int64
	NumRows     int64

	DictRecs map[string]*layout.DictRecType

	ColumnIndexes []*parquet.ColumnIndex
	OffsetIndexes []*parquet.OffsetIndex

	MarshalFunc func(src []any, sh *schema.SchemaHandler) (*map[string]*layout.Table, error)

	stopped bool
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
	res.ObjsSize = 0
	res.CheckSizeCritical = 0
	res.Size = 0
	res.NumRows = 0
	res.Offset = 4
	res.PFile = pFile
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.DictRecs = make(map[string]*layout.DictRecType)
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.ColumnIndexes = make([]*parquet.ColumnIndex, 0)
	res.OffsetIndexes = make([]*parquet.OffsetIndex, 0)
	// include the createdBy to avoid
	// WARN  CorruptStatistics:118 - Ignoring statistics because created_by is null or empty! See PARQUET-251 and PARQUET-297
	createdBy := "parquet-go version latest"
	res.Footer.CreatedBy = &createdBy
	_, err = res.PFile.Write([]byte("PAR1"))
	if err != nil {
		return nil, err
	}
	res.MarshalFunc = marshal.Marshal
	res.stopped = true

	if obj != nil {
		if sa, ok := obj.(string); ok {
			err = res.SetSchemaHandlerFromJSON(sa)
			res.stopped = err != nil
			return res, err

		} else if sa, ok := obj.(*schema.SchemaHandler); ok {
			res.SchemaHandler = schema.NewSchemaHandlerFromSchemaHandler(sa)
		} else if sa, ok := obj.([]*parquet.SchemaElement); ok {
			res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(sa)
		} else {
			if res.SchemaHandler, err = schema.NewSchemaHandlerFromStruct(obj); err != nil {
				return res, err
			}
		}

		res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	}

	// Enable writing after init completed successfully
	res.stopped = false

	return res, err
}

func (pw *ParquetWriter) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error
	if pw.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return err
	}
	pw.Footer.Schema = pw.Footer.Schema[:0]
	pw.Footer.Schema = append(pw.Footer.Schema, pw.SchemaHandler.SchemaElements...)
	return nil
}

// Rename schema name to exname in tags
func (pw *ParquetWriter) RenameSchema() {
	for i := 0; i < len(pw.Footer.Schema); i++ {
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
		return err
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
					return err
				}
				if _, err = pw.PFile.Write(columnIndexBuf); err != nil {
					return err
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
					return err
				}
				if _, err = pw.PFile.Write(offsetIndexBuf); err != nil {
					return err
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

	footerBuf, err := ts.Write(context.TODO(), pw.Footer)
	if err != nil {
		return err
	}

	if _, err = pw.PFile.Write(footerBuf); err != nil {
		return err
	}
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))

	if _, err = pw.PFile.Write(footerSizeBuf); err != nil {
		return err
	}
	if _, err = pw.PFile.Write([]byte("PAR1")); err != nil {
		return err
	}

	return nil
}

// Write one object to parquet file
func (pw *ParquetWriter) Write(src any) error {
	if pw.stopped {
		return errors.New("writer is stopped")
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
	return err
}

func (pw *ParquetWriter) flushObjs() error {
	var err error
	l := int64(len(pw.Objs))
	if l <= 0 {
		return nil
	}
	pagesMapList := make([]map[string][]*layout.Page, pw.NP)
	for i := 0; i < int(pw.NP); i++ {
		pagesMapList[i] = make(map[string][]*layout.Page)
	}

	delta := (l + pw.NP - 1) / pw.NP
	lock := new(sync.Mutex)
	var wg sync.WaitGroup
	var errs []error = make([]error, pw.NP)

	for c := int64(0); c < pw.NP; c++ {
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
					if table.Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY ||
						table.Info.Encoding == parquet.Encoding_RLE_DICTIONARY {

						func() {
							if pw.NP > 1 {
								lock.Lock()
								defer lock.Unlock()
							}
							if _, ok := pw.DictRecs[name]; !ok {
								pw.DictRecs[name] = layout.NewDictRec(*table.Schema.Type)
							}
							pagesMapList[index][name], _, err = layout.TableToDictDataPages(pw.DictRecs[name], table, int32(pw.PageSize), 32, pw.CompressionType)
							if err != nil {
								errs[index] = err
							}
						}()
					} else {
						lock.Lock()
						pagesMapList[index][name], _, err = layout.TableToDataPages(table, int32(pw.PageSize), pw.CompressionType)
						if err != nil {
							errs[index] = err
						}
						lock.Unlock()
					}
				}
			} else {
				errs[index] = err2
			}
		}(int(bgn), int(end), c)
	}

	wg.Wait()

	lock.Lock()
	for _, err2 := range errs {
		if err2 != nil {
			err = err2
			break
		}
	}
	lock.Unlock()

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
	return err
}

// Flush the write buffer to parquet file
func (pw *ParquetWriter) Flush(flag bool) error {
	var err error

	if err = pw.flushObjs(); err != nil {
		return err
	}

	if (pw.Size+pw.ObjsSize >= pw.RowGroupSize || flag) && len(pw.PagesMapBuf) > 0 {
		// pages -> chunk
		chunkMap := make(map[string]*layout.Chunk)
		for name, pages := range pw.PagesMapBuf {
			if len(pages) > 0 && (pages[0].Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY || pages[0].Info.Encoding == parquet.Encoding_RLE_DICTIONARY) {
				dictPage, _, err := layout.DictRecToDictPage(pw.DictRecs[name], int32(pw.PageSize), pw.CompressionType)
				if err != nil {
					return err
				}
				tmp := append([]*layout.Page{dictPage}, pages...)
				chunkMap[name], err = layout.PagesToDictChunk(tmp)
				if err != nil {
					return err
				}
			} else {
				chunkMap[name], err = layout.PagesToChunk(pages)
				if err != nil {
					return err
				}
			}
		}

		pw.DictRecs = make(map[string]*layout.DictRecType) // clean records for next chunks

		// chunks -> rowGroup
		rowGroup := layout.NewRowGroup()
		rowGroup.RowGroupHeader.Columns = make([]*parquet.ColumnChunk, 0)

		for k := 0; k < len(pw.SchemaHandler.SchemaElements); k++ {
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

		for k := 0; k < len(rowGroup.Chunks); k++ {
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
						panic(errors.New("unsupported data page: " + page.Header.String()))
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

					firstRowIndex += int64(page.Header.DataPageHeader.NumValues)
				}

				data := page.RawData
				if _, err = pw.PFile.Write(data); err != nil {
					return err
				}
				pw.Offset += int64(len(data))
			}
		}

		pw.Footer.RowGroups = append(pw.Footer.RowGroups, rowGroup.RowGroupHeader)
		pw.Size = 0
		pw.PagesMapBuf = make(map[string][]*layout.Page)
	}
	pw.Footer.NumRows += int64(len(pw.Objs))
	pw.Objs = pw.Objs[:0]
	pw.ObjsSize = 0
	return nil
}
