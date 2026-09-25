package layout

import (
	"fmt"
	"slices"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func aggregatePageMetrics(pages []*Page, statsStartIdx int, funcTable common.FuncTable, omitStats bool) (numValues, totalUncompressed, totalCompressed int64, minVal, maxVal any, nullCount int64, encodings []parquet.Encoding) {
	encodingsMap := make(map[parquet.Encoding]struct{})
	for i, page := range pages {
		if page == nil || page.Header == nil {
			continue
		}

		if page.Header.DataPageHeader != nil {
			numValues += int64(page.Header.DataPageHeader.NumValues)
			encodingsMap[page.Header.DataPageHeader.Encoding] = struct{}{}
			encodingsMap[page.Header.DataPageHeader.DefinitionLevelEncoding] = struct{}{}
			encodingsMap[page.Header.DataPageHeader.RepetitionLevelEncoding] = struct{}{}
		} else if page.Header.DataPageHeaderV2 != nil {
			numValues += int64(page.Header.DataPageHeaderV2.NumValues)
			encodingsMap[page.Header.DataPageHeaderV2.Encoding] = struct{}{}
			encodingsMap[parquet.Encoding_RLE] = struct{}{}
		} else if page.Header.DictionaryPageHeader != nil {
			encodingsMap[page.Header.DictionaryPageHeader.Encoding] = struct{}{}
		}
		totalUncompressed += int64(page.Header.UncompressedPageSize) + int64(len(page.RawData)) - int64(page.Header.CompressedPageSize)
		totalCompressed += int64(len(page.RawData))
		if !omitStats && i >= statsStartIdx {
			minVal = common.Min(funcTable, minVal, page.MinVal)
			maxVal = common.Max(funcTable, maxVal, page.MaxVal)
			if page.NullCount != nil {
				nullCount += *page.NullCount
			}
		}
	}
	for encoding := range encodingsMap {
		encodings = append(encodings, encoding)
	}
	// Sort for deterministic output: map iteration order is randomized, which
	// would otherwise make ColumnMetaData.Encodings order vary between runs and
	// produce byte-different files for identical input.
	slices.Sort(encodings)
	return numValues, totalUncompressed, totalCompressed, minVal, maxVal, nullCount, encodings
}

func populateStatistics(metaData *parquet.ColumnMetaData, schema *parquet.SchemaElement, minVal, maxVal any, nullCount int64, omitStats bool) error {
	if omitStats {
		// An empty Statistics is a statistic a reader still has to look at, and says the
		// column has none of everything rather than saying nothing.
		return nil
	}
	metaData.Statistics = parquet.NewStatistics()
	pT := schema.Type
	metaData.Statistics.NullCount = &nullCount
	if maxVal == nil || minVal == nil {
		return nil
	}
	tmpBufMax, err := encoding.WritePlain([]any{maxVal}, *pT)
	if err != nil {
		return fmt.Errorf("encode chunk max statistic: %w", err)
	}
	tmpBufMin, err := encoding.WritePlain([]any{minVal}, *pT)
	if err != nil {
		return fmt.Errorf("encode chunk min statistic: %w", err)
	}
	if *pT == parquet.Type_BYTE_ARRAY {
		tmpBufMax = tmpBufMax[4:]
		tmpBufMin = tmpBufMin[4:]
	}
	metaData.Statistics.MaxValue = tmpBufMax
	metaData.Statistics.MinValue = tmpBufMin
	metaData.Statistics.IsMaxValueExact = common.ToPtr(true)
	metaData.Statistics.IsMinValueExact = common.ToPtr(true)
	// Deprecated Min/Max (PARQUET-251) only for signed sort orders.
	if common.IsSignedSortOrder(pT, schema.ConvertedType, schema.LogicalType) {
		metaData.Statistics.Max = tmpBufMax
		metaData.Statistics.Min = tmpBufMin
	}

	return nil
}

// aggregateSizeStatistics combines per-page level histograms and byte array
// sizes into a single SizeStatistics for the column chunk. Returns nil if
// there is nothing to report (all levels are 0 and not a BYTE_ARRAY column).
func aggregateSizeStatistics(pages []*Page, statsStartIdx int) *parquet.SizeStatistics {
	var defHist []int64
	var repHist []int64
	var totalByteArrayBytes *int64
	byteArrayBytesKnown := true

	for i := statsStartIdx; i < len(pages); i++ {
		p := pages[i]
		if p == nil {
			// A missing page understates the histograms below too; they are simply not
			// guarded. The byte-array total is, because a reader sizes a buffer from it.
			byteArrayBytesKnown = false
			continue
		}
		// Aggregate definition level histograms.
		if p.DefinitionLevelHistogram != nil {
			if defHist == nil {
				defHist = make([]int64, len(p.DefinitionLevelHistogram))
			}
			for k, v := range p.DefinitionLevelHistogram {
				defHist[k] += v
			}
		}
		// Aggregate repetition level histograms.
		if p.RepetitionLevelHistogram != nil {
			if repHist == nil {
				repHist = make([]int64, len(p.RepetitionLevelHistogram))
			}
			for k, v := range p.RepetitionLevelHistogram {
				repHist[k] += v
			}
		}
		// Aggregate unencoded byte array data bytes.
		if p.UnencodedByteArrayDataBytes == nil {
			byteArrayBytesKnown = false
			continue
		}
		if totalByteArrayBytes == nil {
			totalByteArrayBytes = new(int64)
		}
		*totalByteArrayBytes += *p.UnencodedByteArrayDataBytes
	}
	// A sum over the pages that could be measured is understated and reads as exact, so a
	// chunk reports this total only when every page contributed one.
	if !byteArrayBytesKnown {
		totalByteArrayBytes = nil
	}

	if defHist == nil && repHist == nil && totalByteArrayBytes == nil {
		return nil
	}

	ss := parquet.NewSizeStatistics()
	ss.DefinitionLevelHistogram = defHist
	ss.RepetitionLevelHistogram = repHist
	ss.UnencodedByteArrayDataBytes = totalByteArrayBytes
	return ss
}

// aggregateGeospatialStatistics combines geospatial statistics from multiple pages. Either
// half is withheld when a page could not read it: bounds around the pages that were read
// would leave out a value the chunk holds, and a type list from them would say the chunk
// holds only those types. The two are tracked apart, since a Z or M geometry carries a
// readable type and coordinates this library cannot place.
func aggregateGeospatialStatistics(pages []*Page) (*parquet.BoundingBox, []int32) {
	if len(pages) == 0 {
		return nil, nil
	}

	var combinedBBox *parquet.BoundingBox
	geoTypesMap := make(map[int32]bool)
	boundsUnknown, typesUnknown := false, false

	for _, page := range pages {
		if page == nil {
			continue
		}
		if page.GeospatialBoundsUnknown {
			boundsUnknown = true
		}
		if page.GeospatialTypesUnknown {
			typesUnknown = true
		}
		for _, gType := range page.GeospatialTypes {
			geoTypesMap[gType] = true
		}
		if page.GeospatialBBox == nil {
			continue
		}

		if combinedBBox == nil {
			combinedBBox = &parquet.BoundingBox{
				Xmin: page.GeospatialBBox.Xmin,
				Xmax: page.GeospatialBBox.Xmax,
				Ymin: page.GeospatialBBox.Ymin,
				Ymax: page.GeospatialBBox.Ymax,
			}
			continue
		}
		combinedBBox.Xmin = min(combinedBBox.Xmin, page.GeospatialBBox.Xmin)
		combinedBBox.Xmax = max(combinedBBox.Xmax, page.GeospatialBBox.Xmax)
		combinedBBox.Ymin = min(combinedBBox.Ymin, page.GeospatialBBox.Ymin)
		combinedBBox.Ymax = max(combinedBBox.Ymax, page.GeospatialBBox.Ymax)
	}

	if boundsUnknown {
		combinedBBox = nil
	}
	if typesUnknown {
		// The format spells an unknown type list as an empty one.
		return combinedBBox, []int32{}
	}

	var geoTypes []int32
	for gType := range geoTypesMap {
		geoTypes = append(geoTypes, gType)
	}
	// Sorted for the same reason as ColumnMetaData.Encodings: map order is randomized, so
	// identical input would otherwise produce byte-different files.
	slices.Sort(geoTypes)
	return combinedBBox, geoTypes
}
