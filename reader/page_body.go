package reader

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// ReadPageData reads and decompresses the data from a page at the given offset
// Returns the uncompressed page data
func ReadPageData(pFile io.ReadSeeker, offset int64, pageHeader *parquet.PageHeader, codec parquet.CompressionCodec, opts *layout.PageReadOptions) ([]byte, error) {
	var opt layout.PageReadOptions
	if opts != nil {
		opt = *opts
	}
	ctx := opt.Context
	if ctx == nil {
		ctx = context.Background()
	}
	// Re-read the header to get exact header size
	_, headerSize, err := readPageHeader(ctx, pFile, offset, nil)
	if err != nil {
		return nil, fmt.Errorf("read page header: %w", err)
	}

	// Move to the actual page data (after header)
	dataOffset := offset + headerSize
	_, err = pFile.Seek(dataOffset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek to page data: %w", err)
	}

	// Read compressed page data
	compressedData := make([]byte, pageHeader.CompressedPageSize)
	_, err = pFile.Read(compressedData)
	if err != nil {
		return nil, fmt.Errorf("read compressed page data: %w", err)
	}

	if err := common.ValidatePageCRC(pageHeader.IsSetCrc(), pageHeader.GetCrc(), opt.CRCMode, compressedData); err != nil {
		return nil, fmt.Errorf("CRC validation failed: %w", err)
	}

	// Handle DATA_PAGE_V2: rep/def levels are stored uncompressed at the
	// start of the page, only the values portion is compressed.
	if v2Header := pageHeader.DataPageHeaderV2; v2Header != nil {
		dll := v2Header.GetDefinitionLevelsByteLength()
		rll := v2Header.GetRepetitionLevelsByteLength()
		levelBytes := int(dll + rll)

		if levelBytes > len(compressedData) {
			return nil, fmt.Errorf("level byte lengths exceed page data (dll=%d + rll=%d > %d)", dll, rll, len(compressedData))
		}

		levelData := compressedData[:levelBytes]
		valuesData := compressedData[levelBytes:]

		if v2Header.GetIsCompressed() && len(valuesData) > 0 {
			expectedDataSize := int64(pageHeader.UncompressedPageSize) - int64(levelBytes)
			valuesData, err = compress.UncompressWithExpectedSize(valuesData, codec, expectedDataSize)
			if err != nil {
				return nil, fmt.Errorf("decompress page data: %w", err)
			}
		}

		result := make([]byte, 0, len(levelData)+len(valuesData))
		result = append(result, levelData...)
		result = append(result, valuesData...)
		return result, nil
	}

	// Non-V2 pages: decompress the entire buffer
	uncompressedData, err := compress.UncompressWithExpectedSize(compressedData, codec, int64(pageHeader.UncompressedPageSize))
	if err != nil {
		return nil, fmt.Errorf("decompress page data: %w", err)
	}

	return uncompressedData, nil
}

// DecodeDictionaryPage decodes the values from a dictionary page
// The data should be uncompressed page data
func DecodeDictionaryPage(data []byte, pageHeader *parquet.PageHeader, physicalType parquet.Type) ([]interface{}, error) {
	dictHeader := pageHeader.DictionaryPageHeader
	if dictHeader == nil {
		return nil, fmt.Errorf("missing dictionary page header")
	}

	// Dictionary pages typically use PLAIN or PLAIN_DICTIONARY encoding
	if dictHeader.Encoding != parquet.Encoding_PLAIN && dictHeader.Encoding != parquet.Encoding_PLAIN_DICTIONARY {
		return nil, fmt.Errorf("unsupported encoding for dictionary: %v", dictHeader.Encoding)
	}

	numValues := dictHeader.NumValues
	bytesReader := bytes.NewReader(data)
	values, err := encoding.ReadPlain(bytesReader, physicalType, uint64(numValues), 0)
	if err != nil {
		return nil, fmt.Errorf("decode dictionary values: %w", err)
	}

	return values, nil
}

// readPageBody reads the page body bytes (decrypting when decryptor is non-nil)
// from dataOffset, validates CRC, and decompresses according to codec. The
// reader is repositioned to dataOffset first.
func readPageBody(pFile io.ReadSeeker, dataOffset int64, pageHeader *parquet.PageHeader, codec parquet.CompressionCodec, crcMode common.CRCMode, decryptor *layout.PageDecryptor) ([]byte, error) {
	if _, err := pFile.Seek(dataOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to page data: %w", err)
	}

	var compressedData []byte
	if decryptor == nil {
		compressedData = make([]byte, pageHeader.CompressedPageSize)
		if _, err := io.ReadFull(pFile, compressedData); err != nil {
			return nil, fmt.Errorf("read compressed page data: %w", err)
		}
	} else {
		module, err := encryption.ReadModule(pFile, layout.DefaultMaxPageSize)
		if err != nil {
			return nil, fmt.Errorf("read encrypted page body: %w", err)
		}
		if decryptor.Algorithm == layout.PageEncryptionAESGCMCTR {
			compressedData, err = encryption.DecryptCTR(decryptor.Key, module)
		} else {
			compressedData, err = encryption.DecryptGCM(decryptor.Key, dictionaryOrDataPageAAD(decryptor, pageHeader), module)
		}
		if err != nil {
			return nil, fmt.Errorf("decrypt page body: %w", err)
		}
	}

	if err := common.ValidatePageCRC(pageHeader.IsSetCrc(), pageHeader.GetCrc(), crcMode, compressedData); err != nil {
		return nil, fmt.Errorf("CRC validation failed: %w", err)
	}

	uncompressed, err := compress.UncompressWithExpectedSize(compressedData, codec, int64(pageHeader.UncompressedPageSize))
	if err != nil {
		return nil, fmt.Errorf("decompress page data: %w", err)
	}
	return uncompressed, nil
}

func dictionaryOrDataPageAAD(decryptor *layout.PageDecryptor, pageHeader *parquet.PageHeader) []byte {
	moduleType := encryption.ModuleDataPage
	pageOrdinal := decryptor.PageOrdinal
	if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		moduleType = encryption.ModuleDictionaryPage
		pageOrdinal = 0
	}
	return encryption.AAD(decryptor.AADPrefix, decryptor.AADFileUnique, moduleType, decryptor.RowGroupOrdinal, decryptor.ColumnOrdinal, pageOrdinal)
}
