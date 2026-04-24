package writer

import (
	"fmt"
	"sync"

	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

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
