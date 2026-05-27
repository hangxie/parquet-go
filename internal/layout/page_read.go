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
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// PageReadOptions controls optional behavior when reading pages.
type PageReadOptions struct {
	CRCMode     common.CRCMode
	Compressor  *compress.Compressor
	MaxPageSize int64
	Decryptor   *PageDecryptor
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

	pageHeader, err := readPageHeader(thriftReader, opt)
	if err != nil {
		return nil, fmt.Errorf("read page header: %w", err)
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
	var buf []byte
	if opt.Decryptor != nil {
		buf, err = readEncryptedPageBody(thriftReader, pageHeader, opt)
		if err != nil {
			return nil, fmt.Errorf("read encrypted page body: %w", err)
		}
	} else {
		buf = make([]byte, compressedPageSize)
		if _, err := io.ReadFull(thriftReader, buf); err != nil {
			return nil, fmt.Errorf("read page body: %w", err)
		}
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
	if opt.Decryptor != nil && (pageHeader.GetType() == parquet.PageType_DATA_PAGE || pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2) {
		opt.Decryptor.PageOrdinal++
	}
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
	if err := pageHeader.Read(context.TODO(), protocol); err != nil {
		return pageHeader, fmt.Errorf("decode page header: %w", err)
	}
	return pageHeader, nil
}

func readPageHeader(thriftReader *thrift.TBufferedTransport, opt PageReadOptions) (*parquet.PageHeader, error) {
	if opt.Decryptor == nil {
		return ReadPageHeader(thriftReader)
	}
	module, err := encryption.ReadModule(thriftReader, opt.MaxPageSize)
	if err != nil {
		return nil, fmt.Errorf("read encrypted page header module: %w", err)
	}
	pageHeader, err := decryptPageHeader(module, opt.Decryptor)
	if err != nil {
		return nil, fmt.Errorf("decrypt page header: %w", err)
	}
	return pageHeader, nil
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

	pageHeader, err := readPageHeader(thriftReader, opt)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read page header: %w", err)
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
		return nil, 0, 0, fmt.Errorf("read page data: %w", err)
	}

	path := make([]string, 0)
	path = append(path, schemaHandler.GetRootInName())
	path = append(path, colMetaData.GetPathInSchema()...)
	name := common.PathToStr(path)

	switch pageHeader.GetType() {
	case parquet.PageType_DICTIONARY_PAGE:
		page, err := readDictionaryPageBody(pageHeader, buf, path, name, schemaHandler, colMetaData)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("read dictionary page body: %w", err)
		}
		return page, 0, 0, nil
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		page, numValues, numRows, err := readDataPageBody(pageHeader, buf, path, name, schemaHandler, colMetaData)
		if err == nil && opt.Decryptor != nil {
			opt.Decryptor.PageOrdinal++
		}
		if err != nil {
			return page, numValues, numRows, fmt.Errorf("read data page body: %w", err)
		}
		return page, numValues, numRows, nil
	case parquet.PageType_INDEX_PAGE:
		return nil, 0, 0, fmt.Errorf("unsupported page type: INDEX_PAGE")
	default:
		return nil, 0, 0, fmt.Errorf("error page type %v", pageHeader.GetType())
	}
}
