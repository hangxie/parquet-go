package layout

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// Chunk stores the ColumnChunk in parquet file
type Chunk struct {
	Pages       []*Page
	ChunkHeader *parquet.ColumnChunk
}

// pagesToChunk is the internal implementation for converting pages to a chunk.
// It handles both regular pages and pages with a dictionary page at the beginning.
// The hasDictPage parameter indicates whether the first page is a dictionary page.
func validatePagesAndGetMetadataIdx(pages []*Page, hasDictPage bool) (int, int, error) {
	metadataPageIdx := 0
	statsStartIdx := 0
	if hasDictPage {
		if len(pages) < 2 {
			return 0, 0, nil
		}
		metadataPageIdx = 1
		statsStartIdx = 1
	} else if len(pages) == 0 {
		return 0, 0, fmt.Errorf("pages slice empty")
	}

	p := pages[metadataPageIdx]
	if p == nil {
		return 0, 0, fmt.Errorf("page #%d is nil", metadataPageIdx)
	}
	if p.Schema == nil {
		return 0, 0, fmt.Errorf("page #%d schema is nil", metadataPageIdx)
	}
	if p.Schema.Type == nil {
		return 0, 0, fmt.Errorf("page #%d schema type is nil", metadataPageIdx)
	}
	if p.Info == nil {
		return 0, 0, fmt.Errorf("page #%d info is nil", metadataPageIdx)
	}
	return metadataPageIdx, statsStartIdx, nil
}

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
	return numValues, totalUncompressed, totalCompressed, minVal, maxVal, nullCount, encodings
}

func populateStatistics(metaData *parquet.ColumnMetaData, pT *parquet.Type, minVal, maxVal any, nullCount int64, omitStats bool) error {
	metaData.Statistics = parquet.NewStatistics()
	if omitStats {
		return nil
	}
	metaData.Statistics.NullCount = &nullCount
	if maxVal == nil || minVal == nil {
		return nil
	}
	tmpBufMax, err := encoding.WritePlain([]any{maxVal}, *pT)
	if err != nil {
		return err
	}
	tmpBufMin, err := encoding.WritePlain([]any{minVal}, *pT)
	if err != nil {
		return err
	}
	if *pT == parquet.Type_BYTE_ARRAY {
		tmpBufMax = tmpBufMax[4:]
		tmpBufMin = tmpBufMin[4:]
	}
	metaData.Statistics.Max = tmpBufMax
	metaData.Statistics.Min = tmpBufMin
	metaData.Statistics.MaxValue = tmpBufMax
	metaData.Statistics.MinValue = tmpBufMin

	return nil
}

func pagesToChunk(pages []*Page, hasDictPage bool) (*Chunk, error) {
	metadataPageIdx, statsStartIdx, err := validatePagesAndGetMetadataIdx(pages, hasDictPage)
	if err != nil {
		return nil, err
	}
	if pages == nil || (hasDictPage && len(pages) < 2) {
		return nil, nil
	}

	pT, cT, logT, omitStats := pages[metadataPageIdx].Schema.Type, pages[metadataPageIdx].Schema.ConvertedType, pages[metadataPageIdx].Schema.LogicalType, pages[metadataPageIdx].Info.OmitStats
	funcTable, err := common.FindFuncTable(pT, cT, logT)
	if err != nil {
		return nil, fmt.Errorf("find func table for given types [%v, %v, %v]: %w", pT, cT, logT, err)
	}

	numValues, totalUncompressed, totalCompressed, minVal, maxVal, nullCount, encodings := aggregatePageMetrics(pages, statsStartIdx, funcTable, omitStats)

	chunk := new(Chunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = *pT
	metaData.Encodings = encodings
	metaData.Codec = pages[metadataPageIdx].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressed
	metaData.TotalUncompressedSize = totalUncompressed
	metaData.PathInSchema = pages[metadataPageIdx].Path

	if err := populateStatistics(metaData, pT, minVal, maxVal, nullCount, omitStats); err != nil {
		return nil, err
	}

	// Aggregate geospatial statistics from pages
	if logT != nil && (logT.IsSetGEOMETRY() || logT.IsSetGEOGRAPHY()) {
		bbox, geoTypes := aggregateGeospatialStatistics(pages)
		if bbox != nil {
			metaData.GeospatialStatistics = &parquet.GeospatialStatistics{
				Bbox:            bbox,
				GeospatialTypes: geoTypes,
			}
		}
	}

	// Aggregate SizeStatistics from per-page metrics.
	metaData.SizeStatistics = aggregateSizeStatistics(pages, statsStartIdx)

	chunk.ChunkHeader.MetaData = metaData
	return chunk, nil
}

// Convert several pages to one chunk
func PagesToChunk(pages []*Page) (*Chunk, error) {
	return pagesToChunk(pages, false)
}

// Convert several pages to one chunk with dict page first
func PagesToDictChunk(pages []*Page) (*Chunk, error) {
	return pagesToChunk(pages, true)
}

// Decode a dict chunk
func DecodeDictChunk(chunk *Chunk) {
	if chunk == nil || len(chunk.Pages) == 0 {
		return
	}

	dictPage := chunk.Pages[0]
	if dictPage == nil || dictPage.DataTable == nil {
		return
	}

	numPages := len(chunk.Pages)
	for i := 1; i < numPages; i++ {
		if chunk.Pages[i] == nil || chunk.Pages[i].DataTable == nil {
			continue
		}

		numValues := len(chunk.Pages[i].DataTable.Values)
		for j := range numValues {
			if chunk.Pages[i].DataTable.Values[j] == nil {
				continue
			}
			if index, ok := chunk.Pages[i].DataTable.Values[j].(int64); ok &&
				index >= 0 && index < int64(len(dictPage.DataTable.Values)) {
				chunk.Pages[i].DataTable.Values[j] = dictPage.DataTable.Values[index]
			}
		}
	}
	chunk.Pages = chunk.Pages[1:] // delete the head dict page
}

// aggregateSizeStatistics combines per-page level histograms and byte array
// sizes into a single SizeStatistics for the column chunk. Returns nil if
// there is nothing to report (all levels are 0 and not a BYTE_ARRAY column).
func aggregateSizeStatistics(pages []*Page, statsStartIdx int) *parquet.SizeStatistics {
	var defHist []int64
	var repHist []int64
	var totalByteArrayBytes *int64

	for i := statsStartIdx; i < len(pages); i++ {
		p := pages[i]
		if p == nil {
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
		if p.UnencodedByteArrayDataBytes != nil {
			if totalByteArrayBytes == nil {
				totalByteArrayBytes = new(int64)
			}
			*totalByteArrayBytes += *p.UnencodedByteArrayDataBytes
		}
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

// aggregateGeospatialStatistics combines geospatial statistics from multiple pages
func aggregateGeospatialStatistics(pages []*Page) (*parquet.BoundingBox, []int32) {
	if len(pages) == 0 {
		return nil, nil
	}

	var combinedBBox *parquet.BoundingBox
	geoTypesMap := make(map[int32]bool)

	for _, page := range pages {
		if page == nil || page.GeospatialBBox == nil {
			continue
		}

		// Combine bounding boxes
		if combinedBBox == nil {
			combinedBBox = &parquet.BoundingBox{
				Xmin: page.GeospatialBBox.Xmin,
				Xmax: page.GeospatialBBox.Xmax,
				Ymin: page.GeospatialBBox.Ymin,
				Ymax: page.GeospatialBBox.Ymax,
			}
		} else {
			combinedBBox.Xmin = min(combinedBBox.Xmin, page.GeospatialBBox.Xmin)
			combinedBBox.Xmax = max(combinedBBox.Xmax, page.GeospatialBBox.Xmax)
			combinedBBox.Ymin = min(combinedBBox.Ymin, page.GeospatialBBox.Ymin)
			combinedBBox.Ymax = max(combinedBBox.Ymax, page.GeospatialBBox.Ymax)
		}

		// Combine geometry types
		for _, gType := range page.GeospatialTypes {
			geoTypesMap[gType] = true
		}
	}

	if combinedBBox == nil {
		return nil, nil
	}

	// Convert geometry types map to slice
	var geoTypes []int32
	for gType := range geoTypesMap {
		geoTypes = append(geoTypes, gType)
	}

	return combinedBBox, geoTypes
}
