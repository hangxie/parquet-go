package writer

import (
	"fmt"
	"sync"

	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func (pw *ParquetWriter) tableCompressionCodec(table *layout.Table) parquet.CompressionCodec {
	if table.Info != nil && table.Info.CompressionCodec != nil {
		return *table.Info.CompressionCodec
	}
	return pw.compressionType
}

func (pw *ParquetWriter) tableToDictPages(name string, table *layout.Table, compressionType parquet.CompressionCodec, compressor *compress.Compressor, convMu *sync.Mutex) ([]*layout.Page, error) {
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
		Compressor:   compressor,
	})
	convMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("build dict pages: %w", err)
	}
	return pages, nil
}

func (pw *ParquetWriter) tableToPlainPages(table *layout.Table, compressionType parquet.CompressionCodec, compressor *compress.Compressor) ([]*layout.Page, error) {
	pages, _, err := layout.TableToDataPagesWithOption(table, layout.PageWriteOption{
		PageSize:        int32(pw.pageSize),
		CompressType:    compressionType,
		DataPageVersion: pw.dataPageVersion,
		WriteCRC:        pw.writeCRC,
		Compressor:      compressor,
	})
	if err != nil {
		return nil, fmt.Errorf("build data pages: %w", err)
	}
	return pages, nil
}

func (pw *ParquetWriter) convertTableToPages(name string, table *layout.Table, convMu *sync.Mutex) ([]*layout.Page, error) {
	compressionType := pw.tableCompressionCodec(table)
	compressor := pw.compressorForColumn(name)
	if table.Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY ||
		table.Info.Encoding == parquet.Encoding_RLE_DICTIONARY {
		return pw.tableToDictPages(name, table, compressionType, compressor, convMu)
	}
	return pw.tableToPlainPages(table, compressionType, compressor)
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
	// Use MinValue/MaxValue: always populated, whereas the deprecated Min/Max
	// are omitted for unsigned/unknown-ordered columns (PARQUET-251).
	if page.Header.DataPageHeader != nil && page.Header.DataPageHeader.Statistics != nil {
		s := page.Header.DataPageHeader.Statistics
		return pageStats{minVal: s.MinValue, maxVal: s.MaxValue, nullCount: s.NullCount}
	}
	if page.Header.DataPageHeaderV2 != nil && page.Header.DataPageHeaderV2.Statistics != nil {
		s := page.Header.DataPageHeaderV2.Statistics
		return pageStats{minVal: s.MinValue, maxVal: s.MaxValue, nullCount: s.NullCount}
	}
	return pageStats{}
}

// pageIsAllNull reports whether a data page contains no non-null leaf values.
// The definition-level histogram's top bucket (max definition level) counts the
// fully-defined, non-null leaf values in the page; it is computed for every page
// of a nullable column (max definition level > 0). A required, non-nested column
// has no histogram (nil) and can never be all-null. Relying on the histogram
// keeps detection correct even when statistics are omitted or the column type
// (GEOMETRY/GEOGRAPHY, INTERVAL) intentionally carries no min/max.
func pageIsAllNull(page *layout.Page) bool {
	hist := page.DefinitionLevelHistogram
	if len(hist) == 0 {
		return false
	}
	return hist[len(hist)-1] == 0
}

// recordDataPage fills the ColumnIndex/OffsetIndex entries for one data page.
// It returns hasValidBounds=false when the page is a non-null page that carries
// no min/max (statistics omitted, or a type such as GEOMETRY/GEOGRAPHY/INTERVAL
// that intentionally has none). The caller must not emit a ColumnIndex whose
// non-null pages lack bounds: the Parquet spec requires min_values[i]/
// max_values[i] to be valid whenever null_pages[i] is false, so an index with
// empty bounds there would make predicate-pushdown readers skip matching rows.
func (pw *ParquetWriter) recordDataPage(page *layout.Page, columnIndex *parquet.ColumnIndex, offsetIndex *parquet.OffsetIndex, dataPageIdx, dataPageCount int, firstRowIndex *int64) (hasValidBounds bool, err error) {
	if page.Header.DataPageHeader == nil && page.Header.DataPageHeaderV2 == nil {
		return false, fmt.Errorf("unsupported data page: %s", page.Header.String())
	}

	stats := extractPageStats(page)
	hasValidBounds = true
	if pageIsAllNull(page) {
		// A page holding only null values carries no real min/max. Per the
		// Parquet spec such a page must set null_pages[i]=true, and only then
		// may min_values[i]/max_values[i] be empty byte arrays. Leaving
		// null_pages false with empty min/max would advertise a bogus bound
		// (e.g. "" for BYTE_ARRAY) that a predicate-pushdown engine trusts.
		columnIndex.NullPages[dataPageIdx] = true
		columnIndex.MinValues[dataPageIdx] = []byte{}
		columnIndex.MaxValues[dataPageIdx] = []byte{}
	} else {
		// nil (as opposed to an empty-but-non-nil slice, which is the valid
		// encoding of e.g. an empty BYTE_ARRAY value) means no statistic was
		// computed for this non-null page, so the whole ColumnIndex is invalid.
		if stats.minVal == nil || stats.maxVal == nil {
			hasValidBounds = false
		}
		columnIndex.MinValues[dataPageIdx] = stats.minVal
		columnIndex.MaxValues[dataPageIdx] = stats.maxVal
	}
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
	// first_row_index is the row-group-relative index of the first row in the
	// page. Pages are built row-aligned, so *firstRowIndex is exactly the index
	// of this page's first row.
	pageLocation.FirstRowIndex = *firstRowIndex
	pageLocation.CompressedPageSize = int32(len(page.RawData))
	offsetIndex.PageLocations = append(offsetIndex.PageLocations, pageLocation)

	// Advance by the row (record) count, not the leaf value count: per the
	// Parquet spec first_row_index counts repetition-level-0 entries, which
	// differs from NumValues for columns under repeated (LIST/MAP) fields.
	*firstRowIndex += page.NumRows
	return hasValidBounds, nil
}

func (pw *ParquetWriter) writeChunkPages(chunk *layout.Chunk, rowGroupOrdinal, columnOrdinal int16) error {
	chunk.ChunkHeader.MetaData.DataPageOffset = -1
	chunk.ChunkHeader.FileOffset = pw.offset
	columnPath := chunk.ChunkHeader.MetaData.GetPathInSchema()
	classification := pw.classifyColumn(columnPath)
	encryptPages := classification.Kind != columnEncryptionPlaintext
	if encryptPages {
		chunk.ChunkHeader.CryptoMetadata = pw.columnCryptoMetadata(columnPath, classification)
	}

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
	columnIndexSlot := len(pw.columnIndexes)
	pw.columnIndexes = append(pw.columnIndexes, columnIndex)

	offsetIndex := parquet.NewOffsetIndex()
	offsetIndex.PageLocations = make([]*parquet.PageLocation, 0)
	pw.offsetIndexes = append(pw.offsetIndexes, offsetIndex)

	firstRowIndex := int64(0)
	dataPageIdx := 0
	columnIndexValid := true

	for _, page := range pages {
		pageOrdinal := int16(dataPageIdx)
		isDataPage := page.Header.Type != parquet.PageType_DICTIONARY_PAGE
		if page.Header.Type == parquet.PageType_DICTIONARY_PAGE {
			tmp := pw.offset
			chunk.ChunkHeader.MetaData.DictionaryPageOffset = &tmp
		} else if chunk.ChunkHeader.MetaData.DataPageOffset <= 0 {
			chunk.ChunkHeader.MetaData.DataPageOffset = pw.offset
		}

		plainRawLen := len(page.RawData)
		if encryptPages {
			if err := pw.encryptPage(page, classification.Key, rowGroupOrdinal, columnOrdinal, pageOrdinal); err != nil {
				return fmt.Errorf("encrypt page row group %d column %d page %d: %w", rowGroupOrdinal, columnOrdinal, pageOrdinal, err)
			}
			chunk.ChunkHeader.MetaData.TotalCompressedSize += int64(len(page.RawData) - plainRawLen)
		}
		if isDataPage {
			hasValidBounds, err := pw.recordDataPage(page, columnIndex, offsetIndex, dataPageIdx, dataPageCount, &firstRowIndex)
			if err != nil {
				return fmt.Errorf("record data page %d: %w", dataPageIdx, err)
			}
			if !hasValidBounds {
				columnIndexValid = false
			}
			dataPageIdx++
		}
		if _, err := pw.PFile.Write(page.RawData); err != nil {
			return fmt.Errorf("write page data: %w", err)
		}
		pw.offset += int64(len(page.RawData))
	}

	// Drop a ColumnIndex whose non-null pages lack valid min/max bounds (e.g. a
	// column written with omitstats, or a type that carries no min/max). A nil
	// slot signals writeColumnIndexes to leave this chunk's ColumnIndexOffset
	// unset, which is spec-valid and keeps the per-chunk slot alignment intact.
	if !columnIndexValid {
		pw.columnIndexes[columnIndexSlot] = nil
	}
	return nil
}
