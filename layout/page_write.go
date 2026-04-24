package layout

import (
	"context"
	"fmt"
	"math/bits"
	"slices"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/compress"
	"github.com/hangxie/parquet-go/v3/encoding"
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

// Encoding values
func (page *Page) EncodingValues(valuesBuf []any) ([]byte, error) {
	encodingMethod := parquet.Encoding_PLAIN
	if page.Info.Encoding != 0 {
		encodingMethod = page.Info.Encoding
	}
	switch encodingMethod {
	case parquet.Encoding_RLE:
		// RLE: BOOLEAN, INT32, INT64 only
		switch *page.Schema.Type {
		case parquet.Type_BOOLEAN, parquet.Type_INT32, parquet.Type_INT64:
			// valid
		default:
			return nil, fmt.Errorf("RLE encoding is not supported for %v", *page.Schema.Type)
		}
		bitWidth := page.Info.Length
		if bitWidth == 0 {
			switch *page.Schema.Type {
			case parquet.Type_BOOLEAN:
				bitWidth = 1
			case parquet.Type_INT32:
				bitWidth = 32
			case parquet.Type_INT64:
				bitWidth = 64
			}
		}
		return encoding.WriteRLEBitPackedHybrid(valuesBuf, bitWidth, *page.Schema.Type)
	case parquet.Encoding_DELTA_BINARY_PACKED:
		// DELTA_BINARY_PACKED: INT32, INT64 only
		if *page.Schema.Type != parquet.Type_INT32 && *page.Schema.Type != parquet.Type_INT64 {
			return nil, fmt.Errorf("DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64, not %v", *page.Schema.Type)
		}
		return encoding.WriteDelta(valuesBuf)
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		// DELTA_BYTE_ARRAY: BYTE_ARRAY only
		if *page.Schema.Type != parquet.Type_BYTE_ARRAY {
			return nil, fmt.Errorf("DELTA_BYTE_ARRAY encoding is only supported for BYTE_ARRAY, not %v", *page.Schema.Type)
		}
		return encoding.WriteDeltaByteArray(valuesBuf), nil
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		// DELTA_LENGTH_BYTE_ARRAY: BYTE_ARRAY only
		if *page.Schema.Type != parquet.Type_BYTE_ARRAY {
			return nil, fmt.Errorf("DELTA_LENGTH_BYTE_ARRAY encoding is only supported for BYTE_ARRAY, not %v", *page.Schema.Type)
		}
		return encoding.WriteDeltaLengthByteArray(valuesBuf), nil
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		// BYTE_STREAM_SPLIT: FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY only
		switch *page.Schema.Type {
		case parquet.Type_FLOAT, parquet.Type_DOUBLE, parquet.Type_INT32, parquet.Type_INT64, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			// valid
		default:
			return nil, fmt.Errorf("BYTE_STREAM_SPLIT encoding is only supported for FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY, not %v", *page.Schema.Type)
		}
		return encoding.WriteByteStreamSplit(valuesBuf), nil
	default:
		return encoding.WritePlain(valuesBuf, *page.Schema.Type)
	}
}

// computeLevelHistograms builds definition and repetition level histograms
// from the page's DataTable, and computes unencoded byte array data bytes
// for BYTE_ARRAY columns. These survive DataTable being nilled out later
// and are used when building ColumnIndex and SizeStatistics in the writer.
func (page *Page) computeLevelHistograms() {
	if page.DataTable == nil {
		return
	}
	if page.DataTable.MaxDefinitionLevel > 0 {
		page.DefinitionLevelHistogram = make([]int64, page.DataTable.MaxDefinitionLevel+1)
		for _, dl := range page.DataTable.DefinitionLevels {
			page.DefinitionLevelHistogram[dl]++
		}
	}
	if page.DataTable.MaxRepetitionLevel > 0 {
		page.RepetitionLevelHistogram = make([]int64, page.DataTable.MaxRepetitionLevel+1)
		for _, rl := range page.DataTable.RepetitionLevels {
			page.RepetitionLevelHistogram[rl]++
		}
	}
	// Compute unencoded byte array data bytes for BYTE_ARRAY columns.
	// Per the spec this is the total byte size excluding 4-byte length prefixes.
	if page.Schema != nil && page.Schema.Type != nil && *page.Schema.Type == parquet.Type_BYTE_ARRAY {
		var totalBytes int64
		for idx, v := range page.DataTable.Values {
			if v == nil || page.DataTable.DefinitionLevels[idx] != page.DataTable.MaxDefinitionLevel {
				continue
			}
			if s, ok := v.(string); ok {
				totalBytes += int64(len(s))
			} else if b, ok := v.([]byte); ok {
				totalBytes += int64(len(b))
			}
		}
		page.UnencodedByteArrayDataBytes = &totalBytes
	}
}

// setPageStatistics sets the min/max/null statistics on a Statistics object
func (page *Page) setPageStatistics(stats *parquet.Statistics) error {
	if page.MaxVal != nil {
		tmpBuf, err := encoding.WritePlain([]any{page.MaxVal}, *page.Schema.Type)
		if err != nil {
			return err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		stats.Max = tmpBuf
		stats.MaxValue = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf, err := encoding.WritePlain([]any{page.MinVal}, *page.Schema.Type)
		if err != nil {
			return err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		stats.Min = tmpBuf
		stats.MinValue = tmpBuf
	}
	stats.NullCount = page.NullCount
	return nil
}

func (page *Page) dataPageCompress(compressType parquet.CompressionCodec, c *compress.Compressor) ([]byte, error) {
	ln := len(page.DataTable.DefinitionLevels)

	// valuesBuf == nil means "up to i, every item in DefinitionLevels was
	// MaxDefinitionLevel and non-nil". This lets us avoid allocating the array for the
	// (somewhat) common case of "all values present".
	var valuesBuf []any
	for i := range ln {
		if page.DataTable.Values[i] == nil {
			// Check if this is a required field with a value that should be present
			// DefinitionLevel == MaxDefinitionLevel means we're at a leaf that should have a value
			if page.Schema.GetRepetitionType() == parquet.FieldRepetitionType_REQUIRED &&
				page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
				return nil, fmt.Errorf("nil value encountered for REQUIRED field %s at index %d", page.DataTable.Path, i)
			}
			// Null value for optional field - need to allocate valuesBuf if not already done
			if valuesBuf == nil {
				valuesBuf = make([]any, i, ln)
				copy(valuesBuf[:i], page.DataTable.Values[:i])
			}
		} else if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
			if valuesBuf != nil {
				valuesBuf = append(valuesBuf, page.DataTable.Values[i])
			}
		} else if valuesBuf == nil {
			valuesBuf = make([]any, i, ln)
			copy(valuesBuf[:i], page.DataTable.Values[:i])
		}
	}
	if valuesBuf == nil {
		valuesBuf = page.DataTable.Values
	}
	// valuesRawBuf := encoding.WritePlain(valuesBuf)
	valuesRawBuf, err := page.EncodingValues(valuesBuf)
	if err != nil {
		return nil, err
	}

	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		definitionLevelBuf, err = encoding.WriteRLEBitPackedHybridInt32(
			page.DataTable.DefinitionLevels,
			int32(bits.Len32(uint32(page.DataTable.MaxDefinitionLevel))))
		if err != nil {
			return nil, err
		}
	}

	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		repetitionLevelBuf, err = encoding.WriteRLEBitPackedHybridInt32(
			page.DataTable.RepetitionLevels,
			int32(bits.Len32(uint32(page.DataTable.MaxRepetitionLevel))))
		if err != nil {
			return nil, err
		}
	}

	dataBuf := slices.Concat(repetitionLevelBuf, definitionLevelBuf, valuesRawBuf)
	dataEncodeBuf, err := resolveCompressor(c).Compress(dataBuf, compressType)
	if err != nil {
		return nil, err
	}

	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.Header.DataPageHeader.NumValues = int32(len(page.DataTable.DefinitionLevels))
	page.Header.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.Encoding = page.Info.Encoding

	page.Header.DataPageHeader.Statistics = parquet.NewStatistics()
	if err = page.setPageStatistics(page.Header.DataPageHeader.Statistics); err != nil {
		return nil, err
	}

	return dataEncodeBuf, nil
}

// dataPageV2Compress compresses a data page v2 and populates the page header.
// Returns (repetitionLevels, definitionLevels, compressedValues, error).
func (page *Page) dataPageV2Compress(compressType parquet.CompressionCodec, c *compress.Compressor) ([]byte, []byte, []byte, error) {
	ln := len(page.DataTable.DefinitionLevels)

	valuesBuf := make([]any, 0)
	for i := range ln {
		if page.DataTable.Values[i] == nil {
			// Check if this is a required field with a value that should be present
			// DefinitionLevel == MaxDefinitionLevel means we're at a leaf that should have a value
			if page.Schema.GetRepetitionType() == parquet.FieldRepetitionType_REQUIRED &&
				page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
				return nil, nil, nil, fmt.Errorf("nil value encountered for REQUIRED field %s at index %d", page.DataTable.Path, i)
			}
			// Skip nil values for optional fields
			continue
		}
		if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
			valuesBuf = append(valuesBuf, page.DataTable.Values[i])
		}
	}
	// valuesRawBuf := encoding.WritePlain(valuesBuf)
	valuesRawBuf, err := page.EncodingValues(valuesBuf)
	if err != nil {
		return nil, nil, nil, err
	}

	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		numInterfaces := make([]any, ln)
		for i := range ln {
			numInterfaces[i] = int64(page.DataTable.DefinitionLevels[i])
		}
		definitionLevelBuf, err = encoding.WriteRLE(numInterfaces,
			int32(bits.Len32(uint32(page.DataTable.MaxDefinitionLevel))),
			parquet.Type_INT64)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	var r0Num int32
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		numInterfaces := make([]any, ln)
		for i := range ln {
			numInterfaces[i] = int64(page.DataTable.RepetitionLevels[i])
			if page.DataTable.RepetitionLevels[i] == 0 {
				r0Num++
			}
		}
		repetitionLevelBuf, err = encoding.WriteRLE(numInterfaces,
			int32(bits.Len32(uint32(page.DataTable.MaxRepetitionLevel))),
			parquet.Type_INT64)
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		// When MaxRepetitionLevel is 0, every entry is a top-level row
		r0Num = int32(ln)
	}

	dataEncodeBuf, err := resolveCompressor(c).Compress(valuesRawBuf, compressType)
	if err != nil {
		return nil, nil, nil, err
	}

	// If compression didn't reduce size, store data uncompressed and set
	// is_compressed=false. This is standard practice in Parquet writers
	// (e.g., PyArrow) to avoid wasting space and CPU on decompression.
	isCompressed := len(dataEncodeBuf) < len(valuesRawBuf)
	if !isCompressed {
		dataEncodeBuf = valuesRawBuf
	}

	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE_V2
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf) + len(definitionLevelBuf) + len(repetitionLevelBuf))
	page.Header.UncompressedPageSize = int32(len(valuesRawBuf) + len(definitionLevelBuf) + len(repetitionLevelBuf))
	page.Header.DataPageHeaderV2 = parquet.NewDataPageHeaderV2()
	page.Header.DataPageHeaderV2.NumValues = int32(len(page.DataTable.Values))
	page.Header.DataPageHeaderV2.NumNulls = page.Header.DataPageHeaderV2.NumValues - int32(len(valuesBuf))
	page.Header.DataPageHeaderV2.NumRows = r0Num
	// page.Header.DataPageHeaderV2.Encoding = parquet.Encoding_PLAIN
	page.Header.DataPageHeaderV2.Encoding = page.Info.Encoding

	page.Header.DataPageHeaderV2.DefinitionLevelsByteLength = int32(len(definitionLevelBuf))
	page.Header.DataPageHeaderV2.RepetitionLevelsByteLength = int32(len(repetitionLevelBuf))
	page.Header.DataPageHeaderV2.IsCompressed = isCompressed

	page.Header.DataPageHeaderV2.Statistics = parquet.NewStatistics()
	if err = page.setPageStatistics(page.Header.DataPageHeaderV2.Statistics); err != nil {
		return nil, nil, nil, err
	}

	return repetitionLevelBuf, definitionLevelBuf, dataEncodeBuf, nil
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
