package layout

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/types"
)

// PageWriteOption consolidates page-level write parameters.
// This struct is extensible for future features (e.g., encryption).
type PageWriteOption struct {
	Context         context.Context
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
	ctx := opt.Context
	if ctx == nil {
		ctx = context.Background()
	}
	pageHeaderBuf, err := ts.Write(ctx, page.Header)
	if err != nil {
		return fmt.Errorf("serialize page header: %w", err)
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

	for r.endIdx < totalLn {
		// A data page may only end on a row boundary — a repetition-level-0
		// entry. A repeated column contributes a run of values per row (until
		// the next rep-0 entry); splitting that run across pages would leave a
		// page starting mid-row, which the offset index cannot address (its
		// first_row_index must name a row start and increase strictly). Once
		// the size budget is met, keep scanning to the next rep-0 entry before
		// ending the page. Flat columns have a rep-0 entry at every value, so
		// this never extends a page.
		if r.endIdx > startIdx && r.size >= pageSize && table.RepetitionLevels[r.endIdx] == 0 {
			break
		}
		if table.Values[r.endIdx] == nil {
			if err := checkRequiredNil(table, r.endIdx); err != nil {
				return r, fmt.Errorf("page scan index %d: %w", r.endIdx, err)
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

// pageStats is what a page builder measured about the values it is about to write. Both
// builders hand one to setPageStats, so a column's statistics cannot depend on its encoding.
type pageStats struct {
	values      []any
	defLevels   []int32
	maxDefLevel int32
	minVal      any
	maxVal      any
	nullCount   int64
}

// setPageStats sets the statistics fields on a page from what its builder measured.
func setPageStats(page *Page, stats pageStats, omitStats bool, cT *parquet.ConvertedType, logT *parquet.LogicalType) {
	if omitStats {
		return
	}
	// Neither annotation has a sort order min/max could describe: the specification leaves
	// INTERVAL's undefined, and a geometry's bounds are GeospatialStatistics' job.
	isGeospatial := logT != nil && (logT.IsSetGEOMETRY() || logT.IsSetGEOGRAPHY())
	isInterval := cT != nil && *cT == parquet.ConvertedType_INTERVAL
	if !isGeospatial && !isInterval {
		page.MaxVal = stats.maxVal
		page.MinVal = stats.minVal
	}
	page.NullCount = &stats.nullCount

	if isGeospatial {
		geo := computePageGeospatialStatistics(stats.values, stats.defLevels, stats.maxDefLevel)
		page.GeospatialBBox = geo.BBox
		page.GeospatialTypes = geo.Types
		page.GeospatialBoundsUnknown = geo.BoundsUnknown
		page.GeospatialTypesUnknown = geo.TypesUnknown
	}
}

// compressAndSerializePage compresses the page data and serializes it.
func compressAndSerializePage(page *Page, opt PageWriteOption) error {
	if opt.DataPageVersion == 2 {
		repLevels, defLevels, compressedValues, err := page.dataPageV2Compress(opt.CompressType, opt.Compressor)
		if err != nil {
			return fmt.Errorf("compress data page v2: %w", err)
		}
		return serializePage(page, opt, repLevels, defLevels, compressedValues)
	}
	compressedData, err := page.dataPageCompress(opt.CompressType, opt.Compressor)
	if err != nil {
		return fmt.Errorf("compress data page: %w", err)
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
			return nil, 0, fmt.Errorf("scan page values at %d: %w", i, err)
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

		setPageStats(page, pageStats{
			values:      page.DataTable.Values,
			defLevels:   page.DataTable.DefinitionLevels,
			maxDefLevel: page.DataTable.MaxDefinitionLevel,
			minVal:      scan.minVal,
			maxVal:      scan.maxVal,
			nullCount:   scan.nullCount,
		}, omitStats, cT, logT)

		page.Schema = table.Schema
		page.CompressType = opt.CompressType
		page.Path = table.Path
		page.Info = table.Info

		page.computeLevelHistograms(page.DataTable.Values)

		if err = compressAndSerializePage(page, opt); err != nil {
			return nil, 0, fmt.Errorf("compress and serialize page at %d: %w", i, err)
		}

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = scan.endIdx
	}
	return res, totSize, nil
}

// pageGeospatialStats is what a page can say about the geospatial values it holds.
//
// Either half can be unknown on its own: a Z or M geometry has a readable type and
// coordinates this library cannot place, while bytes that are not WKB have neither.
type pageGeospatialStats struct {
	BBox  *parquet.BoundingBox
	Types []int32
	// BoundsUnknown marks a value whose coordinates could not be read, and TypesUnknown
	// one whose geometry type could not. Bounds around the rest would be smaller than the
	// page, and a type list from the rest would say the page holds only those types.
	BoundsUnknown bool
	TypesUnknown  bool
}

// computePageGeospatialStatistics measures a page of geospatial data.
func computePageGeospatialStatistics(values []any, definitionLevels []int32, maxDefinitionLevel int32) pageGeospatialStats {
	if len(values) == 0 {
		return pageGeospatialStats{}
	}

	calc := types.NewBoundingBoxCalculator()
	geoTypesMap := make(map[int32]bool)
	stats := pageGeospatialStats{}

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
			// A geospatial column holding something that is not bytes holds a value
			// with neither coordinates to measure nor a header to read.
			stats.BoundsUnknown = true
			stats.TypesUnknown = true
			continue
		}

		// An empty value is not an absent one: parquet spells absence with a null, which the
		// definition level above already skipped, and WKB has no zero-byte form. So this is
		// a value the column holds that nothing here can measure.
		if len(wkbBytes) == 0 {
			stats.BoundsUnknown = true
			stats.TypesUnknown = true
			continue
		}

		_ = calc.AddWKB(wkbBytes)

		// The type comes from the WKB header, which a Z or M geometry carries as plainly
		// as a 2D one even though its coordinates cannot be placed.
		if geoType, ok := extractGeometryType(wkbBytes); ok {
			geoTypesMap[geoType] = true
		} else {
			stats.TypesUnknown = true
		}
	}
	stats.BoundsUnknown = stats.BoundsUnknown || calc.BoundsUnknown()

	if !stats.TypesUnknown {
		for gType := range geoTypesMap {
			stats.Types = append(stats.Types, gType)
		}
	}

	// A page that holds a value it could not read reports no box of its own, rather than one
	// its own BoundsUnknown contradicts. The chunk withholds it either way.
	if minX, minY, maxX, maxY, ok := calc.GetBounds(); ok && !stats.BoundsUnknown {
		stats.BBox = &parquet.BoundingBox{Xmin: minX, Xmax: maxX, Ymin: minY, Ymax: maxY}
	}
	return stats
}

// extractGeometryType reads the geometry type from a WKB header, reporting false for one
// the format does not define. The readers that build the bounding box check the same
// header, so the two halves of the statistic agree about what counts as WKB.
func extractGeometryType(wkb []byte) (int32, bool) {
	return types.WKBGeometryType(wkb)
}
