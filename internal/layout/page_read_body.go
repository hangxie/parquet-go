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
func readPageV2Data(thriftReader *thrift.TBufferedTransport, pageHeader *parquet.PageHeader, colMetaData *parquet.ColumnMetaData, c *compress.Compressor, opt PageReadOptions, maxRepetitionLevel, maxDefinitionLevel int32) ([]byte, error) {
	if pageHeader.DataPageHeaderV2 == nil {
		return nil, fmt.Errorf("ReadPage: data page v2 missing DataPageHeaderV2")
	}
	dll := pageHeader.DataPageHeaderV2.GetDefinitionLevelsByteLength()
	rll := pageHeader.DataPageHeaderV2.GetRepetitionLevelsByteLength()
	compressedPageSize := pageHeader.GetCompressedPageSize()

	if dll < 0 || rll < 0 {
		return nil, fmt.Errorf("ReadPage: invalid level byte lengths (dll=%d, rll=%d)", dll, rll)
	}
	// Widened before summing: two positive int32 lengths can wrap negative, slipping
	// past this check and reaching make() below with a negative length.
	levelsLen := int64(dll) + int64(rll)
	if levelsLen > int64(compressedPageSize) {
		return nil, fmt.Errorf("ReadPage: level byte lengths exceed page size (dll=%d + rll=%d > %d)", dll, rll, compressedPageSize)
	}
	if err := validateV2LevelSections(rll, dll, maxRepetitionLevel, maxDefinitionLevel,
		pageHeader.DataPageHeaderV2.GetNumValues()); err != nil {
		return nil, err
	}

	var repetitionLevelsBuf, definitionLevelsBuf, dataBuf []byte
	if opt.Decryptor != nil {
		plain, err := readEncryptedPageBody(thriftReader, pageHeader, opt)
		if err != nil {
			return nil, fmt.Errorf("read encrypted v2 body: %w", err)
		}
		if int64(len(plain)) < levelsLen {
			return nil, fmt.Errorf("ReadPage: decrypted data page v2 body too small")
		}
		repetitionLevelsBuf = append([]byte(nil), plain[:rll]...)
		definitionLevelsBuf = append([]byte(nil), plain[rll:rll+dll]...)
		dataBuf = append([]byte(nil), plain[rll+dll:]...)
	} else {
		repetitionLevelsBuf = make([]byte, rll)
		definitionLevelsBuf = make([]byte, dll)
		dataBuf = make([]byte, int64(compressedPageSize)-levelsLen)

		if _, err := io.ReadFull(thriftReader, repetitionLevelsBuf); err != nil {
			return nil, fmt.Errorf("read v2 repetition levels: %w", err)
		}
		if _, err := io.ReadFull(thriftReader, definitionLevelsBuf); err != nil {
			return nil, fmt.Errorf("read v2 definition levels: %w", err)
		}
		if _, err := io.ReadFull(thriftReader, dataBuf); err != nil {
			return nil, fmt.Errorf("read v2 data: %w", err)
		}
	}

	if err := common.ValidatePageCRC(pageHeader.IsSetCrc(), pageHeader.GetCrc(), opt.CRCMode, repetitionLevelsBuf, definitionLevelsBuf, dataBuf); err != nil {
		return nil, fmt.Errorf("CRC validation failed: %w", err)
	}

	if pageHeader.DataPageHeaderV2.GetIsCompressed() && len(dataBuf) > 0 {
		expectedDataSize := int64(pageHeader.GetUncompressedPageSize()) - levelsLen
		var err error
		if dataBuf, err = resolveCompressor(c).UncompressWithExpectedSize(dataBuf, colMetaData.GetCodec(), expectedDataSize); err != nil {
			return nil, fmt.Errorf("decompress v2 data: %w", err)
		}
	}

	return assembleLevelPrefixedBuf(rll, dll, repetitionLevelsBuf, definitionLevelsBuf, dataBuf, maxRepetitionLevel, maxDefinitionLevel)
}

// validateV2LevelSections rejects a V2 page whose level sections contradict the schema.
func validateV2LevelSections(rll, dll, maxRepetitionLevel, maxDefinitionLevel, numValues int32) error {
	// A page holding no values has no levels to carry either way.
	if numValues == 0 {
		return nil
	}
	// Omitting required levels would have the next section read as those levels,
	// returning garbage for every value. Only a malformed page does this.
	if maxRepetitionLevel > 0 && rll == 0 {
		return fmt.Errorf("data page v2 carries no repetition levels for a column with max repetition level %d", maxRepetitionLevel)
	}
	if maxDefinitionLevel > 0 && dll == 0 {
		return fmt.Errorf("data page v2 carries no definition levels for a column with max definition level %d", maxDefinitionLevel)
	}
	return nil
}

// assembleLevelPrefixedBuf prefixes level buffers with their lengths and appends the data.
func assembleLevelPrefixedBuf(rll, dll int32, repBuf, defBuf, dataBuf []byte, maxRepetitionLevel, maxDefinitionLevel int32) ([]byte, error) {
	// Included only where the schema says those levels exist, matching what
	// readDataPageBody reads back; writers pad the byte length even at level 0.
	buf := make([]byte, 0)
	if rll > 0 && maxRepetitionLevel > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(rll)})
		if err != nil {
			return nil, fmt.Errorf("encode repetition level length: %w", err)
		}
		buf = append(buf, tmpBuf...)
		buf = append(buf, repBuf...)
	}
	if dll > 0 && maxDefinitionLevel > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(dll)})
		if err != nil {
			return nil, fmt.Errorf("encode definition level length: %w", err)
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
			return nil, fmt.Errorf("read encrypted page body: %w", err)
		}
	} else {
		buf = make([]byte, pageHeader.GetCompressedPageSize())
		if _, err := io.ReadFull(thriftReader, buf); err != nil {
			return nil, fmt.Errorf("read page body: %w", err)
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
	if pageHeader.DictionaryPageHeader == nil {
		return nil, fmt.Errorf("dictionary page missing DictionaryPageHeader")
	}
	// A dictionary cannot hold more entries than the column chunk has values;
	// reject inflated counts before they drive a huge allocation in ReadPlain.
	if chunkValues := colMetaData.GetNumValues(); chunkValues > 0 &&
		int64(pageHeader.DictionaryPageHeader.GetNumValues()) > chunkValues {
		return nil, fmt.Errorf("dictionary page value count %d exceeds column chunk total %d",
			pageHeader.DictionaryPageHeader.GetNumValues(), chunkValues)
	}
	var err error
	table.Values, err = readDictionaryValues(bytes.NewReader(buf), colMetaData.GetType(),
		pageHeader.DictionaryPageHeader.GetNumValues(), uint64(bitWidth))
	if err != nil {
		return nil, fmt.Errorf("decode dictionary values: %w", err)
	}
	page.DataTable = table
	return page, nil
}

// readDataPageBody reads levels, values, and builds the table for a data page.
func readDataPageBody(pageHeader *parquet.PageHeader, buf []byte, path []string, name string, schemaHandler *schema.SchemaHandler, colMetaData *parquet.ColumnMetaData) (*Page, int64, int64, error) {
	page := NewDataPage()
	page.Header, page.Path = pageHeader, path
	if err := page.initDecoder(buf, schemaHandler, colMetaData.GetNumValues()); err != nil {
		return nil, 0, 0, err
	}
	count, rows, err := page.ReadBatch(true)
	return page, count, rows, err
}
