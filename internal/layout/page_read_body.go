package layout

import (
	"bytes"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

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

	var repetitionLevelsBuf, definitionLevelsBuf, dataBuf []byte
	if opt.Decryptor != nil {
		plain, err := readEncryptedPageBody(thriftReader, pageHeader, opt)
		if err != nil {
			return nil, err
		}
		if int32(len(plain)) < rll+dll {
			return nil, fmt.Errorf("ReadPage: decrypted data page v2 body too small")
		}
		repetitionLevelsBuf = append([]byte(nil), plain[:rll]...)
		definitionLevelsBuf = append([]byte(nil), plain[rll:rll+dll]...)
		dataBuf = append([]byte(nil), plain[rll+dll:]...)
	} else {
		repetitionLevelsBuf = make([]byte, rll)
		definitionLevelsBuf = make([]byte, dll)
		dataBuf = make([]byte, compressedPageSize-rll-dll)

		if _, err := io.ReadFull(thriftReader, repetitionLevelsBuf); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(thriftReader, definitionLevelsBuf); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(thriftReader, dataBuf); err != nil {
			return nil, err
		}
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
	var buf []byte
	var err error
	if opt.Decryptor != nil {
		buf, err = readEncryptedPageBody(thriftReader, pageHeader, opt)
		if err != nil {
			return nil, err
		}
	} else {
		buf = make([]byte, pageHeader.GetCompressedPageSize())
		if _, err := io.ReadFull(thriftReader, buf); err != nil {
			return nil, err
		}
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
