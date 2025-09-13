package layout

import (
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/encoding"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
)

// Chunk stores the ColumnChunk in parquet file
type Chunk struct {
	Pages       []*Page
	ChunkHeader *parquet.ColumnChunk
}

// Convert several pages to one chunk
func PagesToChunk(pages []*Page) (*Chunk, error) {
	ln := len(pages)
	if ln == 0 {
		return nil, fmt.Errorf("pages slice cannot be empty")
	}
	if pages[0] == nil {
		return nil, fmt.Errorf("first page cannot be nil")
	}
	if pages[0].Schema == nil {
		return nil, fmt.Errorf("first page schema cannot be nil")
	}
	if pages[0].Schema.Type == nil {
		return nil, fmt.Errorf("first page schema type cannot be nil")
	}
	if pages[0].Info == nil {
		return nil, fmt.Errorf("first page info cannot be nil")
	}

	var numValues int64 = 0
	var totalUncompressedSize int64 = 0
	var totalCompressedSize int64 = 0

	var maxVal any = pages[0].MaxVal
	var minVal any = pages[0].MinVal
	var nullCount int64 = 0
	pT, cT, logT, omitStats := pages[0].Schema.Type, pages[0].Schema.ConvertedType, pages[0].Schema.LogicalType, pages[0].Info.OmitStats
	funcTable, err := common.FindFuncTable(pT, cT, logT)
	if err != nil {
		return nil, fmt.Errorf("cannot find func table for given types [%v, %v, %v]: %w", pT, cT, logT, err)
	}

	for i := range ln {
		if pages[i] == nil || pages[i].Header == nil {
			continue
		}

		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else if pages[i].Header.DataPageHeaderV2 != nil {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) + int64(len(pages[i].RawData)) - int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		if !omitStats {
			minVal = common.Min(funcTable, minVal, pages[i].MinVal)
			maxVal = common.Max(funcTable, maxVal, pages[i].MaxVal)
			if pages[i].NullCount != nil {
				nullCount += *pages[i].NullCount
			}
		}
	}

	chunk := new(Chunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = *pages[0].Schema.Type
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_BIT_PACKED)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN)
	// metaData.Encodings = append(metaData.Encodings, parquet.Encoding_DELTA_BINARY_PACKED)
	metaData.Codec = pages[0].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[0].Path
	metaData.Statistics = parquet.NewStatistics()

	if !omitStats && maxVal != nil && minVal != nil {
		tmpBufMax, err := encoding.WritePlain([]any{maxVal}, *pT)
		if err != nil {
			return nil, err
		}
		tmpBufMin, err := encoding.WritePlain([]any{minVal}, *pT)
		if err != nil {
			return nil, err
		}
		if *pT == parquet.Type_BYTE_ARRAY {
			tmpBufMax = tmpBufMax[4:]
			tmpBufMin = tmpBufMin[4:]
		}
		metaData.Statistics.Max = tmpBufMax
		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.MaxValue = tmpBufMax
		metaData.Statistics.MinValue = tmpBufMin
	}

	if !omitStats {
		metaData.Statistics.NullCount = &nullCount
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

	chunk.ChunkHeader.MetaData = metaData
	return chunk, nil
}

// Convert several pages to one chunk with dict page first
func PagesToDictChunk(pages []*Page) (*Chunk, error) {
	if len(pages) < 2 {
		return nil, nil
	}
	if pages[1] == nil {
		return nil, fmt.Errorf("second page cannot be nil")
	}
	if pages[1].Schema == nil {
		return nil, fmt.Errorf("second page schema cannot be nil")
	}
	if pages[1].Schema.Type == nil {
		return nil, fmt.Errorf("second page schema type cannot be nil")
	}
	if pages[1].Info == nil {
		return nil, fmt.Errorf("second page info cannot be nil")
	}
	var numValues int64 = 0
	var totalUncompressedSize int64 = 0
	var totalCompressedSize int64 = 0

	var maxVal any = pages[1].MaxVal
	var minVal any = pages[1].MinVal
	var nullCount int64 = 0
	pT, cT, logT, omitStats := pages[1].Schema.Type, pages[1].Schema.ConvertedType, pages[1].Schema.LogicalType, pages[1].Info.OmitStats
	funcTable, err := common.FindFuncTable(pT, cT, logT)
	if err != nil {
		return nil, fmt.Errorf("cannot find func table for given types [%v, %v, %v]: %w", pT, cT, logT, err)
	}

	for i := range pages {
		if pages[i] == nil || pages[i].Header == nil {
			continue
		}

		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else if pages[i].Header.DataPageHeaderV2 != nil {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) + int64(len(pages[i].RawData)) - int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		if !omitStats && i > 0 {
			minVal = common.Min(funcTable, minVal, pages[i].MinVal)
			maxVal = common.Max(funcTable, maxVal, pages[i].MaxVal)
			if pages[i].NullCount != nil {
				nullCount += *pages[i].NullCount
			}
		}
	}

	chunk := new(Chunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = *pages[1].Schema.Type
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_BIT_PACKED)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN_DICTIONARY)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE_DICTIONARY)

	metaData.Codec = pages[1].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[1].Path
	metaData.Statistics = parquet.NewStatistics()

	if !omitStats && maxVal != nil && minVal != nil {
		tmpBufMax, err := encoding.WritePlain([]any{maxVal}, *pT)
		if err != nil {
			return nil, err
		}
		tmpBufMin, err := encoding.WritePlain([]any{minVal}, *pT)
		if err != nil {
			return nil, err
		}
		if *pT == parquet.Type_BYTE_ARRAY {
			tmpBufMax = tmpBufMax[4:]
			tmpBufMin = tmpBufMin[4:]
		}
		metaData.Statistics.Max = tmpBufMax
		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.MaxValue = tmpBufMax
		metaData.Statistics.MinValue = tmpBufMin
	}

	if !omitStats {
		metaData.Statistics.NullCount = &nullCount
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

	chunk.ChunkHeader.MetaData = metaData
	return chunk, nil
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
			if chunk.Pages[i].DataTable.Values[j] != nil {
				if index, ok := chunk.Pages[i].DataTable.Values[j].(int64); ok &&
					index >= 0 && index < int64(len(dictPage.DataTable.Values)) {
					chunk.Pages[i].DataTable.Values[j] = dictPage.DataTable.Values[index]
				}
			}
		}
	}
	chunk.Pages = chunk.Pages[1:] // delete the head dict page
}

// Read one chunk from parquet file (Deprecated)
func ReadChunk(thriftReader *thrift.TBufferedTransport, schemaHandler *schema.SchemaHandler, chunkHeader *parquet.ColumnChunk) (*Chunk, error) {
	if chunkHeader == nil {
		return nil, fmt.Errorf("chunkHeader cannot be nil")
	}
	if chunkHeader.MetaData == nil {
		return nil, fmt.Errorf("chunkHeader.MetaData cannot be nil")
	}

	chunk := new(Chunk)
	chunk.ChunkHeader = chunkHeader

	var readValues int64 = 0
	var numValues int64 = chunkHeader.MetaData.GetNumValues()
	for readValues < numValues {
		page, cnt, _, err := ReadPage(thriftReader, schemaHandler, chunkHeader.GetMetaData())
		if err != nil {
			return nil, err
		}
		chunk.Pages = append(chunk.Pages, page)
		readValues += cnt
	}

	if len(chunk.Pages) > 0 && chunk.Pages[0].Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		DecodeDictChunk(chunk)
	}
	return chunk, nil
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
