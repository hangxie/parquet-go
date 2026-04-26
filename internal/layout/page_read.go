package layout

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// PageReadOptions controls optional behavior when reading pages.
type PageReadOptions struct {
	CRCMode     common.CRCMode
	Compressor  *compress.Compressor
	MaxPageSize int64
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
