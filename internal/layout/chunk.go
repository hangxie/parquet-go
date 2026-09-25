package layout

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
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

func pagesToChunk(pages []*Page, hasDictPage bool) (*Chunk, error) {
	metadataPageIdx, statsStartIdx, err := validatePagesAndGetMetadataIdx(pages, hasDictPage)
	if err != nil {
		return nil, fmt.Errorf("validate pages: %w", err)
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

	if err := populateStatistics(metaData, pages[metadataPageIdx].Schema, minVal, maxVal, nullCount, omitStats); err != nil {
		return nil, fmt.Errorf("populate chunk statistics: %w", err)
	}
	if hasDictPage && !omitStats {
		metaData.Statistics.DistinctCount = dictionaryDistinctCount(pages, pT, cT, logT)
	}

	// Aggregate geospatial statistics from pages
	if logT != nil && (logT.IsSetGEOMETRY() || logT.IsSetGEOGRAPHY()) {
		bbox, geoTypes := aggregateGeospatialStatistics(pages)
		if bbox != nil || len(geoTypes) > 0 {
			// Both halves are optional, so a chunk whose bounds are unknown still
			// reports the geometry types it holds.
			metaData.GeospatialStatistics = &parquet.GeospatialStatistics{
				Bbox:            bbox,
				GeospatialTypes: geoTypes,
			}
		}
	}

	// Aggregate SizeStatistics from per-page metrics.
	if !omitStats {
		// SizeStatistics is a statistic like any other, and its byte-array total is what
		// the tag is reached for: it walks every value of a BYTE_ARRAY page.
		metaData.SizeStatistics = aggregateSizeStatistics(pages, statsStartIdx)
	}

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
