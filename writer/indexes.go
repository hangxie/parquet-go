package writer

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

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

type pageBounds struct {
	minVal any
	maxVal any
}

func columnIndexBoundaryOrder(schemaElement *parquet.SchemaElement, bounds []pageBounds) parquet.BoundaryOrder {
	if schemaElement == nil || schemaElement.Type == nil {
		return parquet.BoundaryOrder_UNORDERED
	}

	funcTable, err := common.FindFuncTable(schemaElement.Type, schemaElement.ConvertedType, schemaElement.LogicalType)
	if err != nil {
		return parquet.BoundaryOrder_UNORDERED
	}

	ascending, descending := true, true
	var previousMin, previousMax any
	seenNonNullPage := false
	for _, page := range bounds {
		if page.minVal == nil && page.maxVal == nil {
			continue
		}
		if page.minVal == nil || page.maxVal == nil {
			return parquet.BoundaryOrder_UNORDERED
		}
		if seenNonNullPage {
			ascending = ascending && !funcTable.LessThan(page.minVal, previousMin) && !funcTable.LessThan(page.maxVal, previousMax)
			descending = descending && !funcTable.LessThan(previousMin, page.minVal) && !funcTable.LessThan(previousMax, page.maxVal)
			if !ascending && !descending {
				return parquet.BoundaryOrder_UNORDERED
			}
		}
		previousMin, previousMax = page.minVal, page.maxVal
		seenNonNullPage = true
	}
	if ascending {
		return parquet.BoundaryOrder_ASCENDING
	}
	if descending {
		return parquet.BoundaryOrder_DESCENDING
	}
	return parquet.BoundaryOrder_UNORDERED
}

// recordDataPage records one data page in the column and offset indexes and
// returns its bounds for order detection. A non-null page without min/max
// invalidates the ColumnIndex because the Parquet spec requires valid bounds
// whenever null_pages[i] is false.
func (pw *ParquetWriter) recordDataPage(page *layout.Page, columnIndex *parquet.ColumnIndex, offsetIndex *parquet.OffsetIndex, dataPageIdx, dataPageCount int, firstRowIndex *int64) (bounds pageBounds, hasValidBounds bool, err error) {
	if page.Header.DataPageHeader == nil && page.Header.DataPageHeaderV2 == nil {
		return bounds, false, fmt.Errorf("unsupported data page: %s", page.Header.String())
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
		minValue, maxValue := layout.TruncateBinaryBounds(page.Schema, stats.minVal, stats.maxVal, pw.binaryMinMaxTruncateLength)
		if minValue == nil || maxValue == nil {
			hasValidBounds = false
		}
		columnIndex.MinValues[dataPageIdx] = minValue
		columnIndex.MaxValues[dataPageIdx] = maxValue
		bounds = pageBounds{minVal: page.MinVal, maxVal: page.MaxVal}
		if page.Schema != nil && page.Schema.Type != nil &&
			(*page.Schema.Type == parquet.Type_BYTE_ARRAY || *page.Schema.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY) {
			// Binary bounds may be truncated, so compare the values actually
			// serialized into the ColumnIndex rather than the exact page extrema.
			bounds = pageBounds{minVal: string(minValue), maxVal: string(maxValue)}
		}
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
	// One total per page, so a reader sizes a buffer for the page it is about to read.
	if page.UnencodedByteArrayDataBytes != nil {
		offsetIndex.UnencodedByteArrayDataBytes = append(offsetIndex.UnencodedByteArrayDataBytes, *page.UnencodedByteArrayDataBytes)
	}

	// Advance by the row (record) count, not the leaf value count: per the
	// Parquet spec first_row_index counts repetition-level-0 entries, which
	// differs from NumValues for columns under repeated (LIST/MAP) fields.
	*firstRowIndex += page.NumRows
	return bounds, hasValidBounds, nil
}

// chunkOmitsStats reports whether the chunk's column is tagged to carry no statistics.
func chunkOmitsStats(pages []*layout.Page) bool {
	// Read from a data page: a dictionary chunk leads with a dictionary page the writer
	// builds itself, which carries a default tag rather than the column's.
	for _, page := range pages {
		if page == nil || page.Info == nil || page.Header == nil ||
			page.Header.Type == parquet.PageType_DICTIONARY_PAGE {
			continue
		}
		return page.Info.OmitStats
	}
	return false
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
	// A column tagged omitstats carries no column index at all. Dropping it for want of
	// bounds covers most such columns, but one whose pages are all null has bounds the
	// spec calls valid, and its index would still publish level histograms.
	columnIndexValid := !chunkOmitsStats(pages)
	dataPageBounds := make([]pageBounds, 0, dataPageCount)
	var dataPageSchema *parquet.SchemaElement

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
			bounds, hasValidBounds, err := pw.recordDataPage(page, columnIndex, offsetIndex, dataPageIdx, dataPageCount, &firstRowIndex)
			if err != nil {
				return fmt.Errorf("record data page %d: %w", dataPageIdx, err)
			}
			if dataPageSchema == nil {
				dataPageSchema = page.Schema
			}
			dataPageBounds = append(dataPageBounds, bounds)
			if !hasValidBounds {
				columnIndexValid = false
			}
			dataPageIdx++
		}
		if _, err := pw.write(page.RawData); err != nil {
			return fmt.Errorf("write page data: %w", err)
		}
		pw.offset += int64(len(page.RawData))
	}

	// The field is per-page or absent, never partial.
	if len(offsetIndex.UnencodedByteArrayDataBytes) != dataPageCount {
		offsetIndex.UnencodedByteArrayDataBytes = nil
	}

	// Drop a ColumnIndex whose non-null pages lack valid min/max bounds (e.g. a
	// type that carries no min/max), or that omitstats suppressed above. A nil
	// slot signals writeColumnIndexes to leave this chunk's ColumnIndexOffset
	// unset, which is spec-valid and keeps the per-chunk slot alignment intact.
	if !columnIndexValid {
		pw.columnIndexes[columnIndexSlot] = nil
	} else {
		columnIndex.BoundaryOrder = columnIndexBoundaryOrder(dataPageSchema, dataPageBounds)
	}
	return nil
}
