package layout

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// PageReadOptions controls optional behavior when reading pages.
type PageReadOptions struct {
	Context     context.Context
	CRCMode     common.CRCMode
	Compressor  *compress.Compressor
	MaxPageSize int64
	Decryptor   *PageDecryptor
}

// Decode replaces a dictionary-encoded page's indices with dictPage values, rejecting invalid indices.
func (page *Page) Decode(dictPage *Page) error {
	if page == nil || page.DataTable == nil ||
		(page.Header.DataPageHeader == nil && page.Header.DataPageHeaderV2 == nil) {
		return nil
	}

	if page.Header.DataPageHeader != nil &&
		(page.Header.DataPageHeader.Encoding != parquet.Encoding_RLE_DICTIONARY &&
			page.Header.DataPageHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY) {
		return nil
	}

	if page.Header.DataPageHeaderV2 != nil &&
		(page.Header.DataPageHeaderV2.Encoding != parquet.Encoding_RLE_DICTIONARY &&
			page.Header.DataPageHeaderV2.Encoding != parquet.Encoding_PLAIN_DICTIONARY) {
		return nil
	}

	hasDict := dictPage != nil && dictPage.DataTable != nil
	var dictSize int64
	if hasDict {
		dictSize = int64(len(dictPage.DataTable.Values))
	}
	for i, value := range page.DataTable.Values {
		if value == nil {
			continue
		}
		if !hasDict {
			return fmt.Errorf("dictionary-encoded page has no dictionary")
		}
		index, ok := value.(int64)
		if !ok {
			return fmt.Errorf("dictionary index has type %T", value)
		}
		if index < 0 || index >= dictSize {
			return fmt.Errorf("dictionary index %d out of range [0, %d)", index, dictSize)
		}
		page.DataTable.Values[i] = dictPage.DataTable.Values[index]
	}
	return nil
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
	page.chunkValues = colMetaData.GetNumValues()
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
func (p *Page) GetValueFromRawData() error {
	switch p.Header.GetType() {
	case parquet.PageType_DICTIONARY_PAGE:
		return p.processDictionaryPage()
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		return p.decodeBatchValues()
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
	values, err := readDictionaryValues(bytesReader, *p.Schema.Type, p.Header.DictionaryPageHeader.GetNumValues(), uint64(p.Schema.GetTypeLength()))
	if err != nil {
		return fmt.Errorf("read plain values from dictionary page: %w", err)
	}
	p.DataTable.Values = values
	return nil
}

// Read page header
//
// Deprecated: use ReadPageHeaderWithContext.
func ReadPageHeader(thriftReader *thrift.TBufferedTransport) (*parquet.PageHeader, error) {
	return ReadPageHeaderWithContext(context.Background(), thriftReader)
}

// ReadPageHeaderWithContext reads a page header using ctx.
func ReadPageHeaderWithContext(ctx context.Context, thriftReader *thrift.TBufferedTransport) (*parquet.PageHeader, error) {
	protocol := thrift.NewTCompactProtocolConf(thriftReader, &thrift.TConfiguration{})
	pageHeader := parquet.NewPageHeader()
	if err := pageHeader.Read(ctx, protocol); err != nil {
		return pageHeader, fmt.Errorf("decode page header: %w", err)
	}
	return pageHeader, nil
}

func readPageHeader(thriftReader *thrift.TBufferedTransport, opt PageReadOptions) (*parquet.PageHeader, error) {
	if opt.Decryptor == nil {
		return ReadPageHeaderWithContext(pageContext(opt), thriftReader)
	}
	module, err := encryption.ReadModule(thriftReader, opt.MaxPageSize)
	if err != nil {
		return nil, fmt.Errorf("read encrypted page header module: %w", err)
	}
	pageHeader, err := decryptPageHeader(pageContext(opt), module, opt.Decryptor)
	if err != nil {
		return nil, fmt.Errorf("decrypt page header: %w", err)
	}
	return pageHeader, nil
}

func pageContext(opt PageReadOptions) context.Context {
	if opt.Context == nil {
		return context.Background()
	}
	return opt.Context
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

	path := make([]string, 0)
	path = append(path, schemaHandler.GetRootInName())
	path = append(path, colMetaData.GetPathInSchema()...)
	name := common.PathToStr(path)

	var buf []byte
	if pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(path)
		buf, err = readPageV2Data(thriftReader, pageHeader, colMetaData, opt.Compressor, opt, maxRepetitionLevel, maxDefinitionLevel)
	} else {
		buf, err = readPageV1Data(thriftReader, pageHeader, colMetaData, opt.Compressor, opt)
	}
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read page data: %w", err)
	}

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
