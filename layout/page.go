package layout

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/bits"
	"slices"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/compress"
	"github.com/hangxie/parquet-go/v3/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

// PageReadOptions controls optional behavior when reading pages.
type PageReadOptions struct {
	CRCMode     common.CRCMode
	Compressor  *compress.Compressor
	MaxPageSize int64
}

// DefaultMaxPageSize is the default maximum size for page data (256 MB).
// This helps prevent memory exhaustion from maliciously crafted files with
// extremely large page sizes specified in headers.
const DefaultMaxPageSize = 256 * 1024 * 1024

// resolveCompressor returns c if non-nil, otherwise returns the DefaultCompressor.
func resolveCompressor(c *compress.Compressor) *compress.Compressor {
	if c != nil {
		return c
	}
	return compress.DefaultCompressor()
}

// Page is used to store the page data
type Page struct {
	// Header of a page
	Header *parquet.PageHeader
	// Table to store values
	DataTable *Table
	// Compressed data of the page, which is written in parquet file
	RawData []byte
	// Compress type: gzip/snappy/zstd/none
	CompressType parquet.CompressionCodec
	// Schema
	Schema *parquet.SchemaElement
	// Path in schema(include the root)
	Path []string
	// Maximum of the values
	MaxVal any
	// Minimum of the values
	MinVal any
	// NullCount
	NullCount *int64
	// Tag info
	Info *common.Tag

	PageSize int32

	// Geospatial statistics for GEOMETRY/GEOGRAPHY columns
	GeospatialBBox  *parquet.BoundingBox
	GeospatialTypes []int32

	// Level histograms for ColumnIndex (computed during page creation).
	// Each has size maxLevel+1; nil if maxLevel == 0.
	DefinitionLevelHistogram []int64
	RepetitionLevelHistogram []int64

	// UnencodedByteArrayDataBytes tracks the total byte size of BYTE_ARRAY
	// data values (excluding 4-byte length prefixes) for SizeStatistics.
	// Only set for BYTE_ARRAY physical type columns; nil otherwise.
	UnencodedByteArrayDataBytes *int64

	// compressor is set during page reading so that subsequent operations
	// (GetRLDLFromRawData, GetValueFromRawData) can decompress using
	// the same per-instance compressor. Nil means use DefaultCompressor.
	compressor *compress.Compressor
}

// Create a new page
func NewPage() *Page {
	page := new(Page)
	page.DataTable = nil
	page.Header = parquet.NewPageHeader()
	page.Info = &common.Tag{}
	page.PageSize = common.DefaultPageSize
	return page
}

// Create a new dict page
func NewDictPage() *Page {
	page := NewPage()
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	page.PageSize = common.DefaultPageSize
	return page
}

// Create a new data page
func NewDataPage() *Page {
	page := NewPage()
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.PageSize = common.DefaultPageSize
	return page
}

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

// TableToDataPagesWithOption converts a table to data pages using the provided options.
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

// Decode dict page
func (page *Page) Decode(dictPage *Page) {
	if dictPage == nil || page == nil ||
		(page.Header.DataPageHeader == nil && page.Header.DataPageHeaderV2 == nil) ||
		dictPage.DataTable == nil || page.DataTable == nil {
		return
	}

	if page.Header.DataPageHeader != nil &&
		(page.Header.DataPageHeader.Encoding != parquet.Encoding_RLE_DICTIONARY &&
			page.Header.DataPageHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY) {
		return
	}

	if page.Header.DataPageHeaderV2 != nil &&
		(page.Header.DataPageHeaderV2.Encoding != parquet.Encoding_RLE_DICTIONARY &&
			page.Header.DataPageHeaderV2.Encoding != parquet.Encoding_PLAIN_DICTIONARY) {
		return
	}

	numValues := len(page.DataTable.Values)
	for i := range numValues {
		if page.DataTable.Values[i] != nil {
			index, ok := page.DataTable.Values[i].(int64)
			if ok && index >= 0 && index < int64(len(dictPage.DataTable.Values)) {
				page.DataTable.Values[i] = dictPage.DataTable.Values[index]
			}
		}
	}
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

// Read page RawData
func ReadPageRawData(thriftReader *thrift.TBufferedTransport, schemaHandler *schema.SchemaHandler, colMetaData *parquet.ColumnMetaData, opts *PageReadOptions) (*Page, error) {
	var opt PageReadOptions
	if opts != nil {
		opt = *opts
	}
	if opt.MaxPageSize <= 0 {
		opt.MaxPageSize = DefaultMaxPageSize
	}
	var err error

	pageHeader, err := ReadPageHeader(thriftReader)
	if err != nil {
		return nil, err
	}

	var page *Page
	if pageHeader.GetType() == parquet.PageType_DATA_PAGE || pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		page = NewDataPage()
	} else if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		page = NewDictPage()
	} else {
		return page, fmt.Errorf("unsupported page type: %v", pageHeader.GetType())
	}

	compressedPageSize := pageHeader.GetCompressedPageSize()
	if compressedPageSize < 0 || int64(compressedPageSize) > opt.MaxPageSize {
		return nil, fmt.Errorf("page size %d exceeds limit %d", compressedPageSize, opt.MaxPageSize)
	}
	buf := make([]byte, compressedPageSize)
	if _, err := io.ReadFull(thriftReader, buf); err != nil {
		return nil, err
	}

	if err := common.ValidatePageCRC(pageHeader.IsSetCrc(), pageHeader.GetCrc(), opt.CRCMode, buf); err != nil {
		return nil, fmt.Errorf("CRC validation failed: %w", err)
	}

	page.Header = pageHeader
	page.CompressType = colMetaData.GetCodec()
	page.RawData = buf
	page.compressor = opt.Compressor
	page.Path = make([]string, 0)
	page.Path = append(page.Path, schemaHandler.GetRootInName())
	page.Path = append(page.Path, colMetaData.GetPathInSchema()...)
	pathIndex := schemaHandler.MapIndex[common.PathToStr(page.Path)]
	schema := schemaHandler.SchemaElements[pathIndex]
	page.Schema = schema
	return page, nil
}

// extractV2LevelBuffers extracts the level data from a V2 data page and reassembles it into
// a contiguous buffer with level-length prefixes followed by value data.
func (p *Page) extractV2LevelBuffers() ([]byte, error) {
	dll := p.Header.DataPageHeaderV2.GetDefinitionLevelsByteLength()
	rll := p.Header.DataPageHeaderV2.GetRepetitionLevelsByteLength()

	if dll < 0 || rll < 0 {
		return nil, fmt.Errorf("GetRLDLFromRawData: invalid level byte lengths (dll=%d, rll=%d)", dll, rll)
	}
	rawDataLen := int32(len(p.RawData))
	if dll+rll > rawDataLen {
		return nil, fmt.Errorf("GetRLDLFromRawData: level byte lengths exceed raw data size (dll=%d + rll=%d > %d)", dll, rll, rawDataLen)
	}

	bytesReader := bytes.NewReader(p.RawData)
	repetitionLevelsBuf, definitionLevelsBuf := make([]byte, rll), make([]byte, dll)
	dataBuf := make([]byte, len(p.RawData)-int(rll)-int(dll))
	if _, err := bytesReader.Read(repetitionLevelsBuf); err != nil {
		return nil, err
	}
	if _, err := bytesReader.Read(definitionLevelsBuf); err != nil {
		return nil, err
	}
	if _, err := bytesReader.Read(dataBuf); err != nil {
		return nil, err
	}

	buf := make([]byte, 0)
	if rll > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(rll)})
		if err != nil {
			return nil, err
		}
		buf = append(buf, tmpBuf...)
		buf = append(buf, repetitionLevelsBuf...)
	}
	if dll > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(dll)})
		if err != nil {
			return nil, err
		}
		buf = append(buf, tmpBuf...)
		buf = append(buf, definitionLevelsBuf...)
	}
	buf = append(buf, dataBuf...)
	return buf, nil
}

// readLevelValues reads either repetition or definition level values from a reader.
func readLevelValues(bytesReader *bytes.Reader, maxLevel int32, numValues uint64) ([]any, error) {
	if maxLevel > 0 {
		bitWidth := uint64(bits.Len32(uint32(maxLevel)))
		levels, err := ReadDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64, -1, numValues, bitWidth)
		if err != nil {
			return nil, err
		}
		if uint64(len(levels)) > numValues {
			levels = levels[:numValues]
		}
		return levels, nil
	}
	levels := make([]any, numValues)
	for i := range len(levels) {
		levels[i] = int64(0)
	}
	return levels, nil
}

// Get RepetitionLevels and Definitions from RawData
func (p *Page) GetRLDLFromRawData(schemaHandler *schema.SchemaHandler) (int64, int64, error) {
	var buf []byte
	var err error

	if p.Header.GetType() == parquet.PageType_DATA_PAGE_V2 {
		buf, err = p.extractV2LevelBuffers()
		if err != nil {
			return 0, 0, err
		}
	} else if p.CompressType != parquet.CompressionCodec_UNCOMPRESSED {
		buf, err = resolveCompressor(p.compressor).UncompressWithExpectedSize(p.RawData, p.CompressType, int64(p.Header.GetUncompressedPageSize()))
		if err != nil {
			return 0, 0, fmt.Errorf("uncompress data page: %w", err)
		}
	} else {
		buf = p.RawData
	}

	switch p.Header.GetType() {
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		return p.decodeDataPageLevels(buf, schemaHandler)
	case parquet.PageType_DICTIONARY_PAGE:
		table := new(Table)
		table.Path = p.Path
		p.DataTable = table
		p.RawData = buf
		return 0, 0, nil
	default:
		return 0, 0, fmt.Errorf("unsupported page type: %v", p.Header.GetType())
	}
}

func (p *Page) decodeDataPageLevels(buf []byte, schemaHandler *schema.SchemaHandler) (int64, int64, error) {
	var numValues uint64
	if p.Header.GetType() == parquet.PageType_DATA_PAGE {
		numValues = uint64(p.Header.DataPageHeader.GetNumValues())
	} else {
		numValues = uint64(p.Header.DataPageHeaderV2.GetNumValues())
	}

	maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(p.Path)
	maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(p.Path)

	bytesReader := bytes.NewReader(buf)
	repetitionLevels, err := readLevelValues(bytesReader, maxRepetitionLevel, numValues)
	if err != nil {
		return 0, 0, err
	}
	definitionLevels, err := readLevelValues(bytesReader, maxDefinitionLevel, numValues)
	if err != nil {
		return 0, 0, err
	}

	table := new(Table)
	table.Path = p.Path
	name := common.PathToStr(p.Path)
	table.RepetitionType = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
	table.MaxRepetitionLevel = maxRepetitionLevel
	table.MaxDefinitionLevel = maxDefinitionLevel
	table.Values = make([]any, len(definitionLevels))
	table.RepetitionLevels = make([]int32, len(definitionLevels))
	table.DefinitionLevels = make([]int32, len(definitionLevels))

	numRows := int64(0)
	for i := range len(definitionLevels) {
		dl, _ := definitionLevels[i].(int64)
		rl, _ := repetitionLevels[i].(int64)
		table.RepetitionLevels[i] = int32(rl)
		table.DefinitionLevels[i] = int32(dl)
		if table.RepetitionLevels[i] == 0 {
			numRows++
		}
	}
	p.DataTable = table
	p.RawData = buf[len(buf)-bytesReader.Len():]

	return int64(numValues), numRows, nil
}

// Get values from raw data
func (p *Page) GetValueFromRawData(schemaHandler *schema.SchemaHandler) error {
	switch p.Header.GetType() {
	case parquet.PageType_DICTIONARY_PAGE:
		return p.processDictionaryPage()
	case parquet.PageType_DATA_PAGE:
		return p.processDataPage(schemaHandler, p.Header.DataPageHeader.GetEncoding())
	case parquet.PageType_DATA_PAGE_V2:
		return p.processDataPageV2(schemaHandler)
	default:
		return fmt.Errorf("unsupported page type")
	}
}

// Process dictionary page
func (p *Page) processDictionaryPage() error {
	if p.Schema == nil {
		return fmt.Errorf("page schema is nil")
	}
	if p.Schema.Type == nil {
		return fmt.Errorf("page schema type is nil")
	}
	if p.Header == nil {
		return fmt.Errorf("page header is nil")
	}
	if p.Header.DictionaryPageHeader == nil {
		return fmt.Errorf("page dictionary header is nil")
	}
	if p.DataTable == nil {
		return fmt.Errorf("page data table is nil")
	}

	bytesReader := bytes.NewReader(p.RawData)
	values, err := encoding.ReadPlain(bytesReader,
		*p.Schema.Type,
		uint64(p.Header.DictionaryPageHeader.GetNumValues()),
		0)
	if err != nil {
		return fmt.Errorf("read plain values from dictionary page: %w", err)
	}
	p.DataTable.Values = values
	return nil
}

// Process data page v2
func (p *Page) processDataPageV2(schemaHandler *schema.SchemaHandler) error {
	if p.Header.DataPageHeaderV2.GetIsCompressed() {
		var err error
		// In V2, rep/def levels are always uncompressed and already stripped from RawData.
		// The expected uncompressed data size is the total minus the level byte lengths.
		dll := int64(p.Header.DataPageHeaderV2.GetDefinitionLevelsByteLength())
		rll := int64(p.Header.DataPageHeaderV2.GetRepetitionLevelsByteLength())
		expectedDataSize := int64(p.Header.GetUncompressedPageSize()) - dll - rll
		if p.RawData, err = resolveCompressor(p.compressor).UncompressWithExpectedSize(p.RawData, p.CompressType, expectedDataSize); err != nil {
			return fmt.Errorf("uncompress data page v2: %w", err)
		}
	}
	return p.processDataPage(schemaHandler, p.Header.DataPageHeaderV2.GetEncoding())
}

// Process data page (common logic for DATA_PAGE and DATA_PAGE_V2)
func (p *Page) processDataPage(schemaHandler *schema.SchemaHandler, encodingType parquet.Encoding) error {
	bytesReader := bytes.NewReader(p.RawData)

	var numNulls uint64 = 0
	for i := range len(p.DataTable.DefinitionLevels) {
		if p.DataTable.DefinitionLevels[i] != p.DataTable.MaxDefinitionLevel {
			numNulls++
		}
	}

	name := common.PathToStr(p.DataTable.Path)
	var ct parquet.ConvertedType = -1
	if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
		ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
	}

	values, err := ReadDataPageValues(bytesReader,
		encodingType,
		*p.Schema.Type,
		ct,
		uint64(len(p.DataTable.DefinitionLevels))-numNulls,
		uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))
	if err != nil {
		return fmt.Errorf("read data page values: %w", err)
	}

	j := 0
	for i := range len(p.DataTable.DefinitionLevels) {
		if p.DataTable.DefinitionLevels[i] == p.DataTable.MaxDefinitionLevel {
			p.DataTable.Values[i] = values[j]
			j++
		}
	}

	p.RawData = []byte{}
	return nil
}

// Read page header
func ReadPageHeader(thriftReader *thrift.TBufferedTransport) (*parquet.PageHeader, error) {
	protocol := thrift.NewTCompactProtocolConf(thriftReader, &thrift.TConfiguration{})
	pageHeader := parquet.NewPageHeader()
	err := pageHeader.Read(context.TODO(), protocol)
	return pageHeader, err
}

// convertRLEValuesForType converts int64 RLE values to the target data type (int32 or bool).
func convertRLEValuesForType(values []any, dataType parquet.Type) {
	switch dataType {
	case parquet.Type_INT32:
		for i := range values {
			values[i] = int32(values[i].(int64))
		}
	case parquet.Type_BOOLEAN:
		for i := range values {
			values[i] = values[i].(int64) > 0
		}
	}
}

// readByteStreamSplit reads values using the BYTE_STREAM_SPLIT encoding for the given data type.
func readByteStreamSplit(bytesReader *bytes.Reader, dataType parquet.Type, cnt, bitWidth uint64) ([]any, error) {
	switch dataType {
	case parquet.Type_FLOAT:
		return encoding.ReadByteStreamSplitFloat32(bytesReader, cnt)
	case parquet.Type_DOUBLE:
		return encoding.ReadByteStreamSplitFloat64(bytesReader, cnt)
	case parquet.Type_INT32:
		return encoding.ReadByteStreamSplitINT32(bytesReader, cnt)
	case parquet.Type_INT64:
		return encoding.ReadByteStreamSplitINT64(bytesReader, cnt)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return encoding.ReadByteStreamSplitFixedLenByteArray(bytesReader, cnt, bitWidth)
	default:
		return nil, fmt.Errorf("the encoding method BYTE_STREAM_SPLIT is only supported for FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY, got %v", dataType)
	}
}

// defaultBitWidth returns the default bitWidth for a data type when not specified.
func defaultBitWidth(dataType parquet.Type) uint64 {
	switch dataType {
	case parquet.Type_BOOLEAN:
		return 1
	case parquet.Type_INT32:
		return 32
	case parquet.Type_INT64:
		return 64
	default:
		return 0
	}
}

func readDeltaBinaryPacked(bytesReader *bytes.Reader, dataType parquet.Type) ([]any, error) {
	switch dataType {
	case parquet.Type_INT32:
		return encoding.ReadDeltaBinaryPackedINT32(bytesReader)
	case parquet.Type_INT64:
		return encoding.ReadDeltaBinaryPackedINT64(bytesReader)
	default:
		return nil, fmt.Errorf("the encoding method DELTA_BINARY_PACKED can only be used with int32 and int64 types, got %v", dataType)
	}
}

func readDeltaByteArrayValues(values []any, err error, dataType parquet.Type, cnt uint64) ([]any, error) {
	if err != nil {
		return nil, err
	}
	if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		for i := range values {
			values[i] = values[i].(string)
		}
	}
	return values[:cnt], nil
}

// Read data page values
func ReadDataPageValues(bytesReader *bytes.Reader, encodingMethod parquet.Encoding, dataType parquet.Type, convertedType parquet.ConvertedType, cnt, bitWidth uint64) ([]any, error) {
	var res []any
	if cnt <= 0 {
		return res, nil
	}

	if bitWidth == 0 {
		bitWidth = defaultBitWidth(dataType)
	}

	switch encodingMethod {
	case parquet.Encoding_PLAIN:
		return encoding.ReadPlain(bytesReader, dataType, cnt, bitWidth)
	case parquet.Encoding_PLAIN_DICTIONARY, parquet.Encoding_RLE_DICTIONARY:
		b, err := bytesReader.ReadByte()
		if err != nil {
			return res, err
		}
		bitWidth = uint64(b)

		buf, err := encoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, uint64(bytesReader.Len()))
		if err != nil {
			return res, err
		}
		if uint64(len(buf)) < cnt {
			return res, fmt.Errorf("expected %d values but got %d from RLE/bit-packed hybrid decoder", cnt, len(buf))
		}
		return buf[:cnt], err
	case parquet.Encoding_RLE:
		values, err := encoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)
		if err != nil {
			return res, err
		}
		if uint64(len(values)) < cnt {
			return res, fmt.Errorf("expected %d values but got %d from RLE/bit-packed hybrid decoder", cnt, len(values))
		}
		convertRLEValuesForType(values, dataType)
		return values[:cnt], nil
	case parquet.Encoding_BIT_PACKED:
		values, err := encoding.ReadBitPackedCount(bytesReader, cnt, bitWidth)
		if err != nil {
			return res, err
		}
		convertRLEValuesForType(values, dataType)
		return values, nil
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return readDeltaBinaryPacked(bytesReader, dataType)
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		values, err := encoding.ReadDeltaLengthByteArray(bytesReader)
		return readDeltaByteArrayValues(values, err, dataType, cnt)
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		values, err := encoding.ReadDeltaByteArray(bytesReader)
		return readDeltaByteArrayValues(values, err, dataType, cnt)
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		return readByteStreamSplit(bytesReader, dataType, cnt, bitWidth)
	default:
		return res, fmt.Errorf("unknown Encoding method: %v", encodingMethod)
	}
}

// readPageV2Data reads a DATA_PAGE_V2 from the reader, decompresses if needed, and reassembles with level prefixes.
func readPageV2Data(thriftReader *thrift.TBufferedTransport, pageHeader *parquet.PageHeader, colMetaData *parquet.ColumnMetaData, c *compress.Compressor, opt PageReadOptions) ([]byte, error) {
	dll := pageHeader.DataPageHeaderV2.GetDefinitionLevelsByteLength()
	rll := pageHeader.DataPageHeaderV2.GetRepetitionLevelsByteLength()
	compressedPageSize := pageHeader.GetCompressedPageSize()

	if dll < 0 || rll < 0 {
		return nil, fmt.Errorf("ReadPage: invalid level byte lengths (dll=%d, rll=%d)", dll, rll)
	}
	if dll+rll > compressedPageSize {
		return nil, fmt.Errorf("ReadPage: level byte lengths exceed page size (dll=%d + rll=%d > %d)", dll, rll, compressedPageSize)
	}

	repetitionLevelsBuf := make([]byte, rll)
	definitionLevelsBuf := make([]byte, dll)
	dataBuf := make([]byte, compressedPageSize-rll-dll)

	if _, err := io.ReadFull(thriftReader, repetitionLevelsBuf); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(thriftReader, definitionLevelsBuf); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(thriftReader, dataBuf); err != nil {
		return nil, err
	}

	if err := common.ValidatePageCRC(pageHeader.IsSetCrc(), pageHeader.GetCrc(), opt.CRCMode, repetitionLevelsBuf, definitionLevelsBuf, dataBuf); err != nil {
		return nil, fmt.Errorf("CRC validation failed: %w", err)
	}

	if pageHeader.DataPageHeaderV2.GetIsCompressed() && len(dataBuf) > 0 {
		expectedDataSize := int64(pageHeader.GetUncompressedPageSize()) - int64(rll) - int64(dll)
		var err error
		if dataBuf, err = resolveCompressor(c).UncompressWithExpectedSize(dataBuf, colMetaData.GetCodec(), expectedDataSize); err != nil {
			return nil, err
		}
	}

	return assembleLevelPrefixedBuf(rll, dll, repetitionLevelsBuf, definitionLevelsBuf, dataBuf)
}

// assembleLevelPrefixedBuf prefixes level buffers with their lengths and appends the data.
func assembleLevelPrefixedBuf(rll, dll int32, repBuf, defBuf, dataBuf []byte) ([]byte, error) {
	buf := make([]byte, 0)
	if rll > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(rll)})
		if err != nil {
			return nil, err
		}
		buf = append(buf, tmpBuf...)
		buf = append(buf, repBuf...)
	}
	if dll > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(dll)})
		if err != nil {
			return nil, err
		}
		buf = append(buf, tmpBuf...)
		buf = append(buf, defBuf...)
	}
	buf = append(buf, dataBuf...)
	return buf, nil
}

// readPageV1Data reads a non-V2 page from the reader, validates CRC, and decompresses.
func readPageV1Data(thriftReader *thrift.TBufferedTransport, pageHeader *parquet.PageHeader, colMetaData *parquet.ColumnMetaData, c *compress.Compressor, opt PageReadOptions) ([]byte, error) {
	buf := make([]byte, pageHeader.GetCompressedPageSize())
	if _, err := io.ReadFull(thriftReader, buf); err != nil {
		return nil, err
	}
	if err := common.ValidatePageCRC(pageHeader.IsSetCrc(), pageHeader.GetCrc(), opt.CRCMode, buf); err != nil {
		return nil, fmt.Errorf("CRC validation failed: %w", err)
	}
	return resolveCompressor(c).UncompressWithExpectedSize(buf, colMetaData.GetCodec(), int64(pageHeader.GetUncompressedPageSize()))
}

// readDictionaryPageBody reads and returns a dictionary page.
func readDictionaryPageBody(pageHeader *parquet.PageHeader, buf []byte, path []string, name string, schemaHandler *schema.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, error) {
	page := NewDictPage()
	page.Header = pageHeader
	table := new(Table)
	table.Path = path
	bitWidth, idx := 0, schemaHandler.MapIndex[name]
	if colMetaData.GetType() == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		bitWidth = int(schemaHandler.SchemaElements[idx].GetTypeLength())
	}
	var err error
	table.Values, err = encoding.ReadPlain(bytes.NewReader(buf), colMetaData.GetType(),
		uint64(pageHeader.DictionaryPageHeader.GetNumValues()), uint64(bitWidth))
	if err != nil {
		return nil, err
	}
	page.DataTable = table
	return page, nil
}

// readDataPageBody reads levels, values, and builds the table for a data page.
func readDataPageBody(pageHeader *parquet.PageHeader, buf []byte, path []string, name string, schemaHandler *schema.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, int64, int64, error) {
	maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
	maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)

	var numValues uint64
	var encodingType parquet.Encoding
	if pageHeader.GetType() == parquet.PageType_DATA_PAGE {
		numValues = uint64(pageHeader.DataPageHeader.GetNumValues())
		encodingType = pageHeader.DataPageHeader.GetEncoding()
	} else {
		numValues = uint64(pageHeader.DataPageHeaderV2.GetNumValues())
		encodingType = pageHeader.DataPageHeaderV2.GetEncoding()
	}

	bytesReader := bytes.NewReader(buf)
	repetitionLevels, err := readLevelValues(bytesReader, maxRepetitionLevel, numValues)
	if err != nil {
		return nil, 0, 0, err
	}
	definitionLevels, err := readLevelValues(bytesReader, maxDefinitionLevel, numValues)
	if err != nil {
		return nil, 0, 0, err
	}

	var numNulls uint64 = 0
	for i := range len(definitionLevels) {
		if int32(definitionLevels[i].(int64)) != maxDefinitionLevel {
			numNulls++
		}
	}

	var ct parquet.ConvertedType = -1
	se := schemaHandler.SchemaElements[schemaHandler.MapIndex[name]]
	if se.IsSetConvertedType() {
		ct = se.GetConvertedType()
	}
	values, err := ReadDataPageValues(bytesReader, encodingType, colMetaData.GetType(), ct,
		uint64(len(definitionLevels))-numNulls, uint64(se.GetTypeLength()))
	if err != nil {
		return nil, 0, 0, err
	}

	table := new(Table)
	table.Path = path
	table.RepetitionType = se.GetRepetitionType()
	table.MaxRepetitionLevel = maxRepetitionLevel
	table.MaxDefinitionLevel = maxDefinitionLevel
	table.Values = make([]any, len(definitionLevels))
	table.RepetitionLevels = make([]int32, len(definitionLevels))
	table.DefinitionLevels = make([]int32, len(definitionLevels))

	j := 0
	numRows := int64(0)
	for i := range len(definitionLevels) {
		dl, _ := definitionLevels[i].(int64)
		rl, _ := repetitionLevels[i].(int64)
		table.RepetitionLevels[i] = int32(rl)
		table.DefinitionLevels[i] = int32(dl)
		if table.DefinitionLevels[i] == maxDefinitionLevel {
			table.Values[i] = values[j]
			j++
		}
		if table.RepetitionLevels[i] == 0 {
			numRows++
		}
	}

	page := NewDataPage()
	page.Header = pageHeader
	page.DataTable = table
	return page, int64(len(definitionLevels)), numRows, nil
}

// Read page from parquet file
func ReadPage(thriftReader *thrift.TBufferedTransport, schemaHandler *schema.SchemaHandler, colMetaData *parquet.ColumnMetaData, opts *PageReadOptions) (*Page, int64, int64, error) {
	var opt PageReadOptions
	if opts != nil {
		opt = *opts
	}
	if opt.MaxPageSize <= 0 {
		opt.MaxPageSize = DefaultMaxPageSize
	}

	pageHeader, err := ReadPageHeader(thriftReader)
	if err != nil {
		return nil, 0, 0, err
	}

	compressedPageSize := pageHeader.GetCompressedPageSize()
	if compressedPageSize < 0 || int64(compressedPageSize) > opt.MaxPageSize {
		return nil, 0, 0, fmt.Errorf("page size %d exceeds limit %d", compressedPageSize, opt.MaxPageSize)
	}

	var buf []byte
	if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		buf, err = readPageV2Data(thriftReader, pageHeader, colMetaData, opt.Compressor, opt)
	} else {
		buf, err = readPageV1Data(thriftReader, pageHeader, colMetaData, opt.Compressor, opt)
	}
	if err != nil {
		return nil, 0, 0, err
	}

	path := make([]string, 0)
	path = append(path, schemaHandler.GetRootInName())
	path = append(path, colMetaData.GetPathInSchema()...)
	name := common.PathToStr(path)

	switch pageHeader.GetType() {
	case parquet.PageType_DICTIONARY_PAGE:
		page, err := readDictionaryPageBody(pageHeader, buf, path, name, schemaHandler, colMetaData)
		if err != nil {
			return nil, 0, 0, err
		}
		return page, 0, 0, nil
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		return readDataPageBody(pageHeader, buf, path, name, schemaHandler, colMetaData)
	case parquet.PageType_INDEX_PAGE:
		return nil, 0, 0, fmt.Errorf("unsupported page type: INDEX_PAGE")
	default:
		return nil, 0, 0, fmt.Errorf("error page type %v", pageHeader.GetType())
	}
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
