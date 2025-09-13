package layout

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/bits"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/compress"
	"github.com/hangxie/parquet-go/v2/encoding"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/types"
)

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
}

// Create a new page
func NewPage() *Page {
	page := new(Page)
	page.DataTable = nil
	page.Header = parquet.NewPageHeader()
	page.Info = common.NewTag()
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

// Convert a table to data pages
func TableToDataPages(table *Table, pageSize int32, compressType parquet.CompressionCodec) ([]*Page, int64, error) {
	var totSize int64 = 0
	totalLn := len(table.Values)
	res := make([]*Page, 0)
	i := 0
	pT, cT, logT, omitStats := table.Schema.Type, table.Schema.ConvertedType, table.Schema.LogicalType, table.Info.OmitStats

	for i < totalLn {
		j := i
		var size int32 = 0
		var numValues int32 = 0

		var maxVal any = table.Values[i]
		var minVal any = table.Values[i]
		nullCount := int64(0)

		funcTable, err := common.FindFuncTable(pT, cT, logT)
		if err != nil {
			return nil, 0, fmt.Errorf("cannot find func table for given types [%v, %v, %v]: %w", pT, cT, logT, err)
		}

		for j < totalLn && size < pageSize {
			if table.DefinitionLevels[j] == table.MaxDefinitionLevel {
				numValues++
				var elSize int32
				if omitStats {
					_, _, elSize = funcTable.MinMaxSize(nil, nil, table.Values[j])
				} else {
					minVal, maxVal, elSize = funcTable.MinMaxSize(minVal, maxVal, table.Values[j])
				}
				size += elSize
			}
			if table.Values[j] == nil {
				nullCount++
			}
			j++
		}

		page := NewDataPage()
		page.PageSize = pageSize
		page.Header.DataPageHeader.NumValues = numValues
		page.Header.Type = parquet.PageType_DATA_PAGE

		page.DataTable = new(Table)
		page.DataTable.RepetitionType = table.RepetitionType
		page.DataTable.Path = table.Path
		page.DataTable.MaxDefinitionLevel = table.MaxDefinitionLevel
		page.DataTable.MaxRepetitionLevel = table.MaxRepetitionLevel
		page.DataTable.Values = table.Values[i:j]
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:j]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:j]
		if !omitStats {
			page.MaxVal = maxVal
			page.MinVal = minVal
			page.NullCount = &nullCount
		}

		// Compute geospatial statistics for GEOMETRY and GEOGRAPHY columns
		if !omitStats && logT != nil && (logT.IsSetGEOMETRY() || logT.IsSetGEOGRAPHY()) {
			bbox, geoTypes := computePageGeospatialStatistics(page.DataTable.Values, page.DataTable.DefinitionLevels, table.MaxDefinitionLevel)
			page.GeospatialBBox = bbox
			page.GeospatialTypes = geoTypes
		}

		page.Schema = table.Schema
		page.CompressType = compressType
		page.Path = table.Path
		page.Info = table.Info

		_, err = page.DataPageCompress(compressType)
		if err != nil {
			return nil, 0, err
		}

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = j
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
		bitWidth := page.Info.Length
		return encoding.WriteRLEBitPackedHybrid(valuesBuf, bitWidth, *page.Schema.Type)
	case parquet.Encoding_DELTA_BINARY_PACKED:
		return encoding.WriteDelta(valuesBuf), nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		return encoding.WriteDeltaByteArray(valuesBuf), nil
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		return encoding.WriteDeltaLengthByteArray(valuesBuf), nil
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		return encoding.WriteByteStreamSplit(valuesBuf), nil
	default:
		return encoding.WritePlain(valuesBuf, *page.Schema.Type)
	}
}

// Compress the data page to parquet file
func (page *Page) DataPageCompress(compressType parquet.CompressionCodec) ([]byte, error) {
	ln := len(page.DataTable.DefinitionLevels)

	// values////////////////////////////////////////////
	// valuesBuf == nil means "up to i, every item in DefinitionLevels was
	// MaxDefinitionLevel". This lets us avoid allocating the array for the
	// (somewhat) common case of "all values present".
	var valuesBuf []any
	for i := range ln {
		if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
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

	// definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		definitionLevelBuf, err = encoding.WriteRLEBitPackedHybridInt32(
			page.DataTable.DefinitionLevels,
			int32(bits.Len32(uint32(page.DataTable.MaxDefinitionLevel))))
		if err != nil {
			return nil, err
		}
	}

	// repetitionLevel/////////////////////////////////
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		repetitionLevelBuf, err = encoding.WriteRLEBitPackedHybridInt32(
			page.DataTable.RepetitionLevels,
			int32(bits.Len32(uint32(page.DataTable.MaxRepetitionLevel))))
		if err != nil {
			return nil, err
		}
	}

	// dataBuf = repetitionBuf + definitionBuf + valuesRawBuf
	dataBuf := make([]byte, 0, len(repetitionLevelBuf)+len(definitionLevelBuf)+len(valuesRawBuf))
	dataBuf = append(dataBuf, repetitionLevelBuf...)
	dataBuf = append(dataBuf, definitionLevelBuf...)
	dataBuf = append(dataBuf, valuesRawBuf...)

	var dataEncodeBuf []byte = compress.Compress(dataBuf, compressType)

	// pageHeader/////////////////////////////////////
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
	if page.MaxVal != nil {
		tmpBuf, err := encoding.WritePlain([]any{page.MaxVal}, *page.Schema.Type)
		if err != nil {
			return nil, err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Max = tmpBuf
		page.Header.DataPageHeader.Statistics.MaxValue = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf, err := encoding.WritePlain([]any{page.MinVal}, *page.Schema.Type)
		if err != nil {
			return nil, err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Min = tmpBuf
		page.Header.DataPageHeader.Statistics.MinValue = tmpBuf
	}

	page.Header.DataPageHeader.Statistics.NullCount = page.NullCount

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(context.TODO(), page.Header)

	res := append(pageHeaderBuf, dataEncodeBuf...)
	page.RawData = res

	return res, nil
}

// Compress data page v2 to parquet file
func (page *Page) DataPageV2Compress(compressType parquet.CompressionCodec) ([]byte, error) {
	ln := len(page.DataTable.DefinitionLevels)

	// values////////////////////////////////////////////
	valuesBuf := make([]any, 0)
	for i := range ln {
		if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
			valuesBuf = append(valuesBuf, page.DataTable.Values[i])
		}
	}
	// valuesRawBuf := encoding.WritePlain(valuesBuf)
	valuesRawBuf, err := page.EncodingValues(valuesBuf)
	if err != nil {
		return nil, err
	}

	// definitionLevel//////////////////////////////////
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
			return nil, err
		}
	}

	// repetitionLevel/////////////////////////////////
	r0Num := int32(0)
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
			return nil, err
		}
	}

	var dataEncodeBuf []byte = compress.Compress(valuesRawBuf, compressType)

	// pageHeader/////////////////////////////////////
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
	page.Header.DataPageHeaderV2.IsCompressed = true

	page.Header.DataPageHeaderV2.Statistics = parquet.NewStatistics()
	if page.MaxVal != nil {
		tmpBuf, err := encoding.WritePlain([]any{page.MaxVal}, *page.Schema.Type)
		if err != nil {
			return nil, err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeaderV2.Statistics.Max = tmpBuf
		page.Header.DataPageHeaderV2.Statistics.MaxValue = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf, err := encoding.WritePlain([]any{page.MinVal}, *page.Schema.Type)
		if err != nil {
			return nil, err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeaderV2.Statistics.Min = tmpBuf
		page.Header.DataPageHeaderV2.Statistics.MinValue = tmpBuf
	}

	page.Header.DataPageHeaderV2.Statistics.NullCount = page.NullCount

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(context.TODO(), page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, repetitionLevelBuf...)
	res = append(res, definitionLevelBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res

	return res, nil
}

// This is a test function
func ReadPage2(thriftReader *thrift.TBufferedTransport, schemaHandler *schema.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, int64, int64, error) {
	var err error
	page, err := ReadPageRawData(thriftReader, schemaHandler, colMetaData)
	if err != nil {
		return nil, 0, 0, err
	}
	numValues, numRows, err := page.GetRLDLFromRawData(schemaHandler)
	if err != nil {
		return nil, 0, 0, err
	}
	if err = page.GetValueFromRawData(schemaHandler); err != nil {
		return page, 0, 0, err
	}
	return page, numValues, numRows, nil
}

// Read page RawData
func ReadPageRawData(thriftReader *thrift.TBufferedTransport, schemaHandler *schema.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, error) {
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
		return page, fmt.Errorf("unsupported page type")
	}

	compressedPageSize := pageHeader.GetCompressedPageSize()
	buf := make([]byte, compressedPageSize)
	if _, err := io.ReadFull(thriftReader, buf); err != nil {
		return nil, err
	}

	page.Header = pageHeader
	page.CompressType = colMetaData.GetCodec()
	page.RawData = buf
	page.Path = make([]string, 0)
	page.Path = append(page.Path, schemaHandler.GetRootInName())
	page.Path = append(page.Path, colMetaData.GetPathInSchema()...)
	pathIndex := schemaHandler.MapIndex[common.PathToStr(page.Path)]
	schema := schemaHandler.SchemaElements[pathIndex]
	page.Schema = schema
	return page, nil
}

// Get RepetitionLevels and Definitions from RawData
func (p *Page) GetRLDLFromRawData(schemaHandler *schema.SchemaHandler) (int64, int64, error) {
	var err error
	bytesReader := bytes.NewReader(p.RawData)
	buf := make([]byte, 0)

	if p.Header.GetType() == parquet.PageType_DATA_PAGE_V2 {
		dll := p.Header.DataPageHeaderV2.GetDefinitionLevelsByteLength()
		rll := p.Header.DataPageHeaderV2.GetRepetitionLevelsByteLength()
		repetitionLevelsBuf, definitionLevelsBuf := make([]byte, rll), make([]byte, dll)
		dataBuf := make([]byte, len(p.RawData)-int(rll)-int(dll))
		if _, err := bytesReader.Read(repetitionLevelsBuf); err != nil {
			return 0, 0, err
		}
		if _, err := bytesReader.Read(definitionLevelsBuf); err != nil {
			return 0, 0, err
		}
		if _, err := bytesReader.Read(dataBuf); err != nil {
			return 0, 0, err
		}

		tmpBuf := make([]byte, 0)
		if rll > 0 {
			tmpBuf, err = encoding.WritePlainINT32([]any{int32(rll)})
			if err != nil {
				return 0, 0, err
			}
			tmpBuf = append(tmpBuf, repetitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		if dll > 0 {
			tmpBuf, err = encoding.WritePlainINT32([]any{int32(dll)})
			if err != nil {
				return 0, 0, err
			}
			tmpBuf = append(tmpBuf, definitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		buf = append(buf, dataBuf...)

	} else {
		if buf, err = compress.Uncompress(p.RawData, p.CompressType); err != nil {
			return 0, 0, fmt.Errorf("unsupported compress method")
		}
	}

	bytesReader = bytes.NewReader(buf)
	if p.Header.GetType() == parquet.PageType_DATA_PAGE_V2 || p.Header.GetType() == parquet.PageType_DATA_PAGE {
		var numValues uint64
		if p.Header.GetType() == parquet.PageType_DATA_PAGE {
			numValues = uint64(p.Header.DataPageHeader.GetNumValues())
		} else {
			numValues = uint64(p.Header.DataPageHeaderV2.GetNumValues())
		}

		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(p.Path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(p.Path)

		var repetitionLevels, definitionLevels []any
		if maxRepetitionLevel > 0 {
			bitWidth := uint64(bits.Len32(uint32(maxRepetitionLevel)))
			if repetitionLevels, err = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth); err != nil {
				return 0, 0, err
			}
		} else {
			repetitionLevels = make([]any, numValues)
			for i := range len(repetitionLevels) {
				repetitionLevels[i] = int64(0)
			}
		}
		if len(repetitionLevels) > int(numValues) {
			repetitionLevels = repetitionLevels[:numValues]
		}

		if maxDefinitionLevel > 0 {
			bitWidth := uint64(bits.Len32(uint32(maxDefinitionLevel)))

			definitionLevels, err = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth)
			if err != nil {
				return 0, 0, err
			}

		} else {
			definitionLevels = make([]any, numValues)
			for i := range len(definitionLevels) {
				definitionLevels[i] = int64(0)
			}
		}
		if len(definitionLevels) > int(numValues) {
			definitionLevels = definitionLevels[:numValues]
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

	} else if p.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		table := new(Table)
		table.Path = p.Path
		p.DataTable = table
		p.RawData = buf
		return 0, 0, nil

	} else {
		return 0, 0, fmt.Errorf("unsupported page type")
	}
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
		return err
	}
	p.DataTable.Values = values
	return nil
}

// Process data page v2
func (p *Page) processDataPageV2(schemaHandler *schema.SchemaHandler) error {
	var err error
	if p.RawData, err = compress.Uncompress(p.RawData, p.CompressType); err != nil {
		return err
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
		return err
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

// Read data page values
func ReadDataPageValues(bytesReader *bytes.Reader, encodingMethod parquet.Encoding, dataType parquet.Type, convertedType parquet.ConvertedType, cnt, bitWidth uint64) ([]any, error) {
	var res []any
	if cnt <= 0 {
		return res, nil
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
		return buf[:cnt], err
	case parquet.Encoding_RLE:
		values, err := encoding.ReadRLEBitPackedHybrid(bytesReader, bitWidth, 0)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_INT32 {
			for i := range values {
				values[i] = int32(values[i].(int64))
			}
		}
		return values[:cnt], nil
	case parquet.Encoding_BIT_PACKED:
		// deprecated
		return res, fmt.Errorf("unsupported Encoding method BIT_PACKED")
	case parquet.Encoding_DELTA_BINARY_PACKED:
		switch dataType {
		case parquet.Type_INT32:
			return encoding.ReadDeltaBinaryPackedINT32(bytesReader)
		case parquet.Type_INT64:
			return encoding.ReadDeltaBinaryPackedINT64(bytesReader)
		default:
			return res, fmt.Errorf("the encoding method DELTA_BINARY_PACKED can only be used with int32 and int64 types")
		}
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		values, err := encoding.ReadDeltaLengthByteArray(bytesReader)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := range values {
				values[i] = values[i].(string)
			}
		}
		return values[:cnt], nil
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		values, err := encoding.ReadDeltaByteArray(bytesReader)
		if err != nil {
			return res, err
		}
		if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			for i := range values {
				values[i] = values[i].(string)
			}
		}
		return values[:cnt], nil
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		switch dataType {
		case parquet.Type_FLOAT:
			return encoding.ReadByteStreamSplitFloat32(bytesReader, cnt)
		case parquet.Type_DOUBLE:
			return encoding.ReadByteStreamSplitFloat64(bytesReader, cnt)
		default:
			return res, fmt.Errorf("the encoding method BYTE_STREAM_SPLIT can only be used with Float and double types")
		}
	default:
		return res, fmt.Errorf("unknown Encoding method")
	}
}

// Read page from parquet file
func ReadPage(thriftReader *thrift.TBufferedTransport, schemaHandler *schema.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, int64, int64, error) {
	var err error

	pageHeader, err := ReadPageHeader(thriftReader)
	if err != nil {
		return nil, 0, 0, err
	}

	buf := make([]byte, 0)

	var page *Page
	compressedPageSize := pageHeader.GetCompressedPageSize()

	if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		dll := pageHeader.DataPageHeaderV2.GetDefinitionLevelsByteLength()
		rll := pageHeader.DataPageHeaderV2.GetRepetitionLevelsByteLength()
		repetitionLevelsBuf := make([]byte, rll)
		definitionLevelsBuf := make([]byte, dll)
		dataBuf := make([]byte, compressedPageSize-rll-dll)

		if _, err = io.ReadFull(thriftReader, repetitionLevelsBuf); err != nil {
			return nil, 0, 0, err
		}
		if _, err = io.ReadFull(thriftReader, definitionLevelsBuf); err != nil {
			return nil, 0, 0, err
		}
		if _, err = io.ReadFull(thriftReader, dataBuf); err != nil {
			return nil, 0, 0, err
		}

		codec := colMetaData.GetCodec()
		if len(dataBuf) > 0 {
			if dataBuf, err = compress.Uncompress(dataBuf, codec); err != nil {
				return nil, 0, 0, err
			}
		}

		tmpBuf := make([]byte, 0)
		if rll > 0 {
			tmpBuf, err = encoding.WritePlainINT32([]any{int32(rll)})
			if err != nil {
				return nil, 0, 0, err
			}
			tmpBuf = append(tmpBuf, repetitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		if dll > 0 {
			tmpBuf, err = encoding.WritePlainINT32([]any{int32(dll)})
			if err != nil {
				return nil, 0, 0, err
			}
			tmpBuf = append(tmpBuf, definitionLevelsBuf...)
		}
		buf = append(buf, tmpBuf...)

		buf = append(buf, dataBuf...)

	} else {
		buf = make([]byte, compressedPageSize)
		if _, err = io.ReadFull(thriftReader, buf); err != nil {
			return nil, 0, 0, err
		}
		codec := colMetaData.GetCodec()
		if buf, err = compress.Uncompress(buf, codec); err != nil {
			return nil, 0, 0, err
		}
	}

	bytesReader := bytes.NewReader(buf)
	path := make([]string, 0)
	path = append(path, schemaHandler.GetRootInName())
	path = append(path, colMetaData.GetPathInSchema()...)
	name := common.PathToStr(path)

	if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		page = NewDictPage()
		page.Header = pageHeader
		table := new(Table)
		table.Path = path
		bitWidth, idx := 0, schemaHandler.MapIndex[name]
		if colMetaData.GetType() == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			bitWidth = int(schemaHandler.SchemaElements[idx].GetTypeLength())
		}

		table.Values, err = encoding.ReadPlain(bytesReader,
			colMetaData.GetType(),
			uint64(pageHeader.DictionaryPageHeader.GetNumValues()),
			uint64(bitWidth))
		if err != nil {
			return nil, 0, 0, err
		}
		page.DataTable = table

		return page, 0, 0, nil

	} else if pageHeader.GetType() == parquet.PageType_INDEX_PAGE {
		return nil, 0, 0, fmt.Errorf("unsupported page type: INDEX_PAGE")
	} else if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 ||
		pageHeader.GetType() == parquet.PageType_DATA_PAGE {

		page = NewDataPage()
		page.Header = pageHeader
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

		var repetitionLevels []any
		if maxRepetitionLevel > 0 {
			bitWidth := uint64(bits.Len32(uint32(maxRepetitionLevel)))

			repetitionLevels, err = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth)
			if err != nil {
				return nil, 0, 0, err
			}

		} else {
			repetitionLevels = make([]any, numValues)
			for i := range len(repetitionLevels) {
				repetitionLevels[i] = int64(0)
			}
		}
		if len(repetitionLevels) > int(numValues) {
			repetitionLevels = repetitionLevels[:numValues]
		}

		var definitionLevels []any
		if maxDefinitionLevel > 0 {
			bitWidth := uint64(bits.Len32(uint32(maxDefinitionLevel)))

			definitionLevels, err = ReadDataPageValues(bytesReader,
				parquet.Encoding_RLE,
				parquet.Type_INT64,
				-1,
				numValues,
				bitWidth)
			if err != nil {
				return nil, 0, 0, err
			}

		} else {
			definitionLevels = make([]any, numValues)
			for i := range len(definitionLevels) {
				definitionLevels[i] = int64(0)
			}
		}
		if len(definitionLevels) > int(numValues) {
			definitionLevels = definitionLevels[:numValues]
		}

		var numNulls uint64 = 0
		for i := range len(definitionLevels) {
			if int32(definitionLevels[i].(int64)) != maxDefinitionLevel {
				numNulls++
			}
		}

		var values []any
		var ct parquet.ConvertedType = -1
		if schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].IsSetConvertedType() {
			ct = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetConvertedType()
		}
		values, err = ReadDataPageValues(bytesReader,
			encodingType,
			colMetaData.GetType(),
			ct,
			uint64(len(definitionLevels))-numNulls,
			uint64(schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetTypeLength()))
		if err != nil {
			return nil, 0, 0, err
		}

		table := new(Table)
		table.Path = path
		table.RepetitionType = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
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
		page.DataTable = table

		return page, int64(len(definitionLevels)), numRows, nil

	} else {
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
