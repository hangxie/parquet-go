package layout

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/compress"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/types"
)

// PageWriteOption consolidates page-level write parameters.
// This struct is extensible for future features (e.g., encryption).
type PageWriteOption struct {
	PageSize        int32
	CompressType    parquet.CompressionCodec
	DataPageVersion int32
	WriteCRC        bool
	Compressor      *compress.Compressor
}

// serializePage applies optional CRC, serializes the page header via Thrift,
// and assembles page.RawData from the serialized header and compressed data.
// This is the shared post-compression pipeline for all page types.
func serializePage(page *Page, opt PageWriteOption, compressedData ...[]byte) error {
	if opt.WriteCRC {
		crc := int32(common.ComputePageCRC(compressedData...))
		page.Header.Crc = &crc
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	pageHeaderBuf, err := ts.Write(context.TODO(), page.Header)
	if err != nil {
		return err
	}

	for _, d := range compressedData {
		pageHeaderBuf = append(pageHeaderBuf, d...)
	}
	page.RawData = pageHeaderBuf

	return nil
}

// pageValueResult holds the results from scanning one page's worth of values.
type pageValueResult struct {
	endIdx    int
	numValues int32
	size      int32
	minVal    any
	maxVal    any
	nullCount int64
}

// scanPageValues scans table values from startIdx to build one page, collecting stats.
func scanPageValues(table *Table, startIdx int, pageSize int32, omitStats bool, funcTable common.FuncTable) (pageValueResult, error) {
	totalLn := len(table.Values)
	r := pageValueResult{
		endIdx: startIdx,
		minVal: table.Values[startIdx],
		maxVal: table.Values[startIdx],
	}

	for r.endIdx < totalLn && r.size < pageSize {
		if table.Values[r.endIdx] == nil {
			if err := checkRequiredNil(table, r.endIdx); err != nil {
				return r, err
			}
			r.nullCount++
			r.endIdx++
			continue
		}
		if table.DefinitionLevels[r.endIdx] == table.MaxDefinitionLevel {
			r.numValues++
			var elSize int32
			if omitStats {
				_, _, elSize = funcTable.MinMaxSize(nil, nil, table.Values[r.endIdx])
			} else {
				r.minVal, r.maxVal, elSize = funcTable.MinMaxSize(r.minVal, r.maxVal, table.Values[r.endIdx])
			}
			r.size += elSize
		}
		r.endIdx++
	}
	return r, nil
}

// setPageStats sets the statistics fields on a page based on the scan result and table schema.
func setPageStats(page *Page, scan pageValueResult, omitStats bool, cT *parquet.ConvertedType, logT *parquet.LogicalType) {
	if omitStats {
		return
	}
	isGeospatial := logT != nil && (logT.IsSetGEOMETRY() || logT.IsSetGEOGRAPHY())
	isInterval := cT != nil && *cT == parquet.ConvertedType_INTERVAL
	if !isGeospatial && !isInterval {
		page.MaxVal = scan.maxVal
		page.MinVal = scan.minVal
	}
	page.NullCount = &scan.nullCount

	if isGeospatial {
		bbox, geoTypes := computePageGeospatialStatistics(page.DataTable.Values, page.DataTable.DefinitionLevels, page.DataTable.MaxDefinitionLevel)
		page.GeospatialBBox = bbox
		page.GeospatialTypes = geoTypes
	}
}

// compressAndSerializePage compresses the page data and serializes it.
func compressAndSerializePage(page *Page, opt PageWriteOption) error {
	if opt.DataPageVersion == 2 {
		repLevels, defLevels, compressedValues, err := page.dataPageV2Compress(opt.CompressType, opt.Compressor)
		if err != nil {
			return err
		}
		return serializePage(page, opt, repLevels, defLevels, compressedValues)
	}
	compressedData, err := page.dataPageCompress(opt.CompressType, opt.Compressor)
	if err != nil {
		return err
	}
	return serializePage(page, opt, compressedData)
}

func TableToDataPagesWithOption(table *Table, opt PageWriteOption) ([]*Page, int64, error) {
	var totSize int64 = 0
	totalLn := len(table.Values)
	if totalLn == 0 {
		return []*Page{}, 0, nil
	}
	res := make([]*Page, 0)
	i := 0
	pT, cT, logT, omitStats := table.Schema.Type, table.Schema.ConvertedType, table.Schema.LogicalType, table.Info.OmitStats

	for i < totalLn {
		funcTable, err := common.FindFuncTable(pT, cT, logT)
		if err != nil {
			return nil, 0, fmt.Errorf("find func table for given types [%v, %v, %v]: %w", pT, cT, logT, err)
		}

		scan, err := scanPageValues(table, i, opt.PageSize, omitStats, funcTable)
		if err != nil {
			return nil, 0, err
		}

		page := NewDataPage()
		page.PageSize = opt.PageSize
		page.Header.DataPageHeader.NumValues = scan.numValues
		page.Header.Type = parquet.PageType_DATA_PAGE

		page.DataTable = new(Table)
		page.DataTable.RepetitionType = table.RepetitionType
		page.DataTable.Path = table.Path
		page.DataTable.MaxDefinitionLevel = table.MaxDefinitionLevel
		page.DataTable.MaxRepetitionLevel = table.MaxRepetitionLevel
		page.DataTable.Values = table.Values[i:scan.endIdx]
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:scan.endIdx]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:scan.endIdx]

		setPageStats(page, scan, omitStats, cT, logT)

		page.Schema = table.Schema
		page.CompressType = opt.CompressType
		page.Path = table.Path
		page.Info = table.Info

		page.computeLevelHistograms()

		if err = compressAndSerializePage(page, opt); err != nil {
			return nil, 0, err
		}

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = scan.endIdx
	}
	return res, totSize, nil
}

// computePageGeospatialStatistics calculates bounding box and geometry types for a page of geospatial data
func computePageGeospatialStatistics(values []any, definitionLevels []int32, maxDefinitionLevel int32) (*parquet.BoundingBox, []int32) {
	if len(values) == 0 {
		return nil, nil
	}

	calc := types.NewBoundingBoxCalculator()
	geoTypesMap := make(map[int32]bool)

	for i, val := range values {
		// Only process non-null values (those with the maximum definition level)
		if i < len(definitionLevels) && definitionLevels[i] != maxDefinitionLevel {
			continue
		}
		if val == nil {
			continue
		}

		// Convert value to WKB bytes
		var wkbBytes []byte
		switch v := val.(type) {
		case []byte:
			wkbBytes = v
		case string:
			wkbBytes = []byte(v)
		default:
			continue // Skip non-binary values
		}

		// Add to bounding box calculation
		if err := calc.AddWKB(wkbBytes); err != nil {
			continue // Skip invalid WKB data
		}

		// Extract geometry type from WKB
		if geoType := extractGeometryType(wkbBytes); geoType > 0 {
			geoTypesMap[geoType] = true
		}
	}

	// Get bounding box
	minX, minY, maxX, maxY, ok := calc.GetBounds()
	if !ok {
		return nil, nil
	}

	bbox := &parquet.BoundingBox{
		Xmin: minX,
		Xmax: maxX,
		Ymin: minY,
		Ymax: maxY,
	}

	// Convert geometry types map to slice
	var geoTypes []int32
	for gType := range geoTypesMap {
		geoTypes = append(geoTypes, gType)
	}

	return bbox, geoTypes
}

// extractGeometryType extracts the geometry type from WKB data
func extractGeometryType(wkb []byte) int32 {
	if len(wkb) < 5 {
		return 0
	}

	// Read byte order and geometry type
	order := wkb[0]
	var gType uint32
	if order == 0 { // big-endian
		gType = uint32(wkb[1])<<24 | uint32(wkb[2])<<16 | uint32(wkb[3])<<8 | uint32(wkb[4])
	} else { // little-endian
		gType = uint32(wkb[4])<<24 | uint32(wkb[3])<<16 | uint32(wkb[2])<<8 | uint32(wkb[1])
	}

	return int32(gType)
}
