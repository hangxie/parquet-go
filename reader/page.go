package reader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// PageHeaderInfo contains metadata about a page extracted from its header
type PageHeaderInfo struct {
	Index            int
	Offset           int64
	PageType         parquet.PageType
	CompressedSize   int32
	UncompressedSize int32
	NumValues        int32
	Encoding         parquet.Encoding
	DefLevelEncoding parquet.Encoding
	RepLevelEncoding parquet.Encoding
	HasStatistics    bool
	Statistics       *parquet.Statistics
	HasCRC           bool
	CRC              int32
	// For dictionary pages
	IsSorted *bool
	// For data page v2
	NumNulls      int32
	NumRows       int32
	IsCompressed  *bool
	DefLevelBytes int32
	RepLevelBytes int32
}

// positionTracker wraps a reader and tracks read position for Thrift reading
type positionTracker struct {
	r   io.Reader
	pos int64
}

func (p *positionTracker) Read(buf []byte) (n int, err error) {
	n, err = p.r.Read(buf)
	p.pos += int64(n)
	return n, err
}

func (p *positionTracker) Write(buf []byte) (int, error) {
	return 0, fmt.Errorf("write not supported")
}

func (p *positionTracker) Close() error {
	if c, ok := p.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (p *positionTracker) Flush(ctx context.Context) error {
	return nil
}

func (p *positionTracker) RemainingBytes() uint64 {
	return ^uint64(0) // Unknown
}

func (p *positionTracker) IsOpen() bool {
	return true
}

func (p *positionTracker) Open() error {
	return nil
}

// readPageHeader reads a page header from the given offset. When decryptor is
// non-nil the page header is read as a length-prefixed encrypted module and
// decrypted in place. The returned headerSize is the number of bytes consumed
// on disk by the header (including the 4-byte length prefix for encrypted
// modules).
func readPageHeader(pFile io.ReadSeeker, offset int64, decryptor *layout.PageDecryptor) (*parquet.PageHeader, int64, error) {
	if _, err := pFile.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek to page: %w", err)
	}

	if decryptor == nil {
		trackingTransport := &positionTracker{r: pFile, pos: offset}
		proto := thrift.NewTCompactProtocolConf(trackingTransport, nil)

		pageHeader := parquet.NewPageHeader()
		if err := pageHeader.Read(context.Background(), proto); err != nil {
			return nil, 0, fmt.Errorf("decode page header: %w", err)
		}

		headerSize := trackingTransport.pos - offset
		if _, err := pFile.Seek(trackingTransport.pos, io.SeekStart); err != nil {
			return nil, 0, fmt.Errorf("seek after header: %w", err)
		}
		return pageHeader, headerSize, nil
	}

	module, err := encryption.ReadModule(pFile, layout.DefaultMaxPageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("read encrypted page header module: %w", err)
	}
	pageHeader, err := layout.DecryptPageHeader(module, decryptor)
	if err != nil {
		return nil, 0, fmt.Errorf("decrypt page header: %w", err)
	}
	headerSize := int64(4 + len(module))
	return pageHeader, headerSize, nil
}

// pageBodyDiskSize returns the on-disk size of a page body given the page
// header. For plaintext pages the size is the compressed page size; for
// encrypted pages the size is the 4-byte length prefix plus the encrypted
// module body. The reader is positioned at the start of the body and is left
// positioned immediately after the body when this returns.
func pageBodyDiskSize(pFile io.ReadSeeker, pageHeader *parquet.PageHeader, decryptor *layout.PageDecryptor) (int64, error) {
	if decryptor == nil {
		return int64(pageHeader.CompressedPageSize), nil
	}
	var lengthBuf [4]byte
	if _, err := io.ReadFull(pFile, lengthBuf[:]); err != nil {
		return 0, fmt.Errorf("read encrypted page body length: %w", err)
	}
	bodyLen := int64(binary.LittleEndian.Uint32(lengthBuf[:]))
	if bodyLen < 0 || bodyLen > layout.DefaultMaxPageSize {
		return 0, fmt.Errorf("encrypted page body length %d exceeds limit %d", bodyLen, layout.DefaultMaxPageSize)
	}
	return 4 + bodyLen, nil
}

// ExtractPageHeaderInfo converts a parquet.PageHeader to PageHeaderInfo
func ExtractPageHeaderInfo(pageHeader *parquet.PageHeader, offset int64, index int) PageHeaderInfo {
	info := PageHeaderInfo{
		Index:            index,
		Offset:           offset,
		PageType:         pageHeader.Type,
		CompressedSize:   pageHeader.CompressedPageSize,
		UncompressedSize: pageHeader.UncompressedPageSize,
		HasCRC:           pageHeader.IsSetCrc(),
	}

	if pageHeader.Crc != nil {
		info.CRC = *pageHeader.Crc
	}

	switch pageHeader.Type {
	case parquet.PageType_DATA_PAGE:
		if dph := pageHeader.DataPageHeader; dph != nil {
			info.NumValues = dph.NumValues
			info.Encoding = dph.Encoding
			info.DefLevelEncoding = dph.DefinitionLevelEncoding
			info.RepLevelEncoding = dph.RepetitionLevelEncoding
			info.HasStatistics = dph.IsSetStatistics()
			if info.HasStatistics {
				info.Statistics = dph.Statistics
			}
		}

	case parquet.PageType_DATA_PAGE_V2:
		if dph2 := pageHeader.DataPageHeaderV2; dph2 != nil {
			info.NumValues = dph2.NumValues
			info.NumNulls = dph2.NumNulls
			info.NumRows = dph2.NumRows
			info.Encoding = dph2.Encoding
			info.DefLevelBytes = dph2.DefinitionLevelsByteLength
			info.RepLevelBytes = dph2.RepetitionLevelsByteLength
			isCompressed := dph2.IsCompressed
			info.IsCompressed = &isCompressed
			info.HasStatistics = dph2.IsSetStatistics()
			if info.HasStatistics {
				info.Statistics = dph2.Statistics
			}
		}

	case parquet.PageType_DICTIONARY_PAGE:
		if dictHeader := pageHeader.DictionaryPageHeader; dictHeader != nil {
			info.NumValues = dictHeader.NumValues
			info.Encoding = dictHeader.Encoding
			info.IsSorted = dictHeader.IsSorted
		}

	case parquet.PageType_INDEX_PAGE:
		info.NumValues = 0 // Index pages don't have values
	}

	return info
}

// readAllPageHeaders reads all page headers from a column chunk. When
// decryptor is non-nil page headers are decrypted and body lengths are read
// from the encrypted module prefix. The decryptor's PageOrdinal is advanced
// after each data page so the next header's AAD matches the file layout.
func readAllPageHeaders(pFile io.ReadSeeker, columnChunk *parquet.ColumnChunk, decryptor *layout.PageDecryptor) ([]PageHeaderInfo, error) {
	meta := columnChunk.MetaData
	if meta == nil {
		return nil, fmt.Errorf("column chunk metadata is nil")
	}

	// Calculate start offset
	startOffset := meta.DataPageOffset
	if meta.DictionaryPageOffset != nil && *meta.DictionaryPageOffset < startOffset {
		startOffset = *meta.DictionaryPageOffset
	}

	var pages []PageHeaderInfo
	currentOffset := startOffset
	totalValuesRead := int64(0)
	pageIndex := 0

	// Read pages until we've read all values
	for totalValuesRead < meta.NumValues {
		pageHeader, headerSize, err := readPageHeader(pFile, currentOffset, decryptor)
		if err != nil {
			return nil, fmt.Errorf("read page header at offset %d: %w", currentOffset, err)
		}

		headerInfo := ExtractPageHeaderInfo(pageHeader, currentOffset, pageIndex)
		pages = append(pages, headerInfo)

		// Count values for data pages
		var valuesInPage int64
		if pageHeader.Type == parquet.PageType_DATA_PAGE && pageHeader.DataPageHeader != nil {
			valuesInPage = int64(pageHeader.DataPageHeader.NumValues)
		} else if pageHeader.Type == parquet.PageType_DATA_PAGE_V2 && pageHeader.DataPageHeaderV2 != nil {
			valuesInPage = int64(pageHeader.DataPageHeaderV2.NumValues)
		}
		totalValuesRead += valuesInPage

		bodySize, err := pageBodyDiskSize(pFile, pageHeader, decryptor)
		if err != nil {
			return nil, fmt.Errorf("read page body size at offset %d: %w", currentOffset, err)
		}
		currentOffset = currentOffset + headerSize + bodySize
		pageIndex++
		advancePageOrdinal(decryptor, pageHeader)
	}

	return pages, nil
}

// readFirstDataPageHeader reads page headers sequentially until finding the
// first data page header. When decryptor is non-nil pages are decrypted as
// they are walked; dictionary headers occupy ordinal 0 so the data-page
// ordinal does not need to advance before the first data page.
func readFirstDataPageHeader(pFile io.ReadSeeker, columnChunk *parquet.ColumnChunk, decryptor *layout.PageDecryptor) (*PageHeaderInfo, error) {
	meta := columnChunk.MetaData
	if meta == nil {
		return nil, fmt.Errorf("column chunk metadata is nil")
	}

	// Start reading from the DataPageOffset
	offset := meta.DataPageOffset
	if meta.DictionaryPageOffset != nil && *meta.DictionaryPageOffset < offset {
		// If there's a dictionary page before the data page, start from there
		offset = *meta.DictionaryPageOffset
	}

	// Read page headers sequentially until we find the first data page
	for {
		pageHeader, headerSize, err := readPageHeader(pFile, offset, decryptor)
		if err != nil {
			return nil, fmt.Errorf("read page header at offset %d: %w", offset, err)
		}

		// Check if this is a data page
		switch pageHeader.Type {
		case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
			// Found the first data page, extract and return its info
			headerInfo := ExtractPageHeaderInfo(pageHeader, offset, 0)
			return &headerInfo, nil
		}

		bodySize, err := pageBodyDiskSize(pFile, pageHeader, decryptor)
		if err != nil {
			return nil, fmt.Errorf("read page body size at offset %d: %w", offset, err)
		}
		offset = offset + headerSize + bodySize
	}
}

// advancePageOrdinal bumps the decryptor's per-data-page ordinal so the next
// header's AAD matches the writer's per-page sequence. Dictionary, index, and
// other non-data pages do not advance the ordinal.
func advancePageOrdinal(decryptor *layout.PageDecryptor, pageHeader *parquet.PageHeader) {
	if decryptor == nil {
		return
	}
	if pageHeader.GetType() == parquet.PageType_DATA_PAGE || pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
		decryptor.PageOrdinal++
	}
}

// ReadPageData reads and decompresses the data from a page at the given offset
// Returns the uncompressed page data
func ReadPageData(pFile io.ReadSeeker, offset int64, pageHeader *parquet.PageHeader, codec parquet.CompressionCodec, opts *layout.PageReadOptions) ([]byte, error) {
	var opt layout.PageReadOptions
	if opts != nil {
		opt = *opts
	}
	// Re-read the header to get exact header size
	_, headerSize, err := readPageHeader(pFile, offset, nil)
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

// GetAllPageHeaders returns metadata for all pages in a column chunk. When the
// column is encrypted the reader's configured keys are used to decrypt page
// headers transparently. Missing keys surface the standard "decryption key
// required for column" error.
func (pr *ParquetReader) GetAllPageHeaders(rgIndex, colIndex int) ([]PageHeaderInfo, error) {
	if rgIndex < 0 || rgIndex >= len(pr.Footer.RowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d (valid range: 0-%d)", rgIndex, len(pr.Footer.RowGroups)-1)
	}

	rg := pr.Footer.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return nil, fmt.Errorf("invalid column index: %d (valid range: 0-%d)", colIndex, len(rg.Columns)-1)
	}

	column := rg.Columns[colIndex]
	decryptor, err := pr.pageInspectionDecryptor(rg, column, int16(rgIndex), int16(colIndex))
	if err != nil {
		return nil, err
	}
	return readAllPageHeaders(pr.PFile, column, decryptor)
}

// GetFirstDataPageHeader returns metadata for the first data page in a column
// chunk. When the column is encrypted the reader's configured keys are used to
// decrypt page headers transparently.
func (pr *ParquetReader) GetFirstDataPageHeader(rgIndex, colIndex int) (*PageHeaderInfo, error) {
	if rgIndex < 0 || rgIndex >= len(pr.Footer.RowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d (valid range: 0-%d)", rgIndex, len(pr.Footer.RowGroups)-1)
	}

	rg := pr.Footer.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return nil, fmt.Errorf("invalid column index: %d (valid range: 0-%d)", colIndex, len(rg.Columns)-1)
	}

	column := rg.Columns[colIndex]
	decryptor, err := pr.pageInspectionDecryptor(rg, column, int16(rgIndex), int16(colIndex))
	if err != nil {
		return nil, err
	}
	return readFirstDataPageHeader(pr.PFile, column, decryptor)
}

// ReadDictionaryPageValues reads and decodes dictionary page values at the
// given offset. This offset-based form is plaintext-only because the offset
// cannot identify the column whose key would be required to decrypt the page;
// use ReadDictionaryPageValuesInColumn for encrypted columns.
func (pr *ParquetReader) ReadDictionaryPageValues(offset int64, codec parquet.CompressionCodec, physicalType parquet.Type) ([]interface{}, error) {
	if pr.pageOffsetEncrypted(offset) {
		return nil, fmt.Errorf("dictionary page inspection is not supported for encrypted columns at offset %d; use ReadDictionaryPageValuesInColumn", offset)
	}

	// Read page header at the offset
	pageHeader, _, err := readPageHeader(pr.PFile, offset, nil)
	if err != nil {
		return nil, fmt.Errorf("read page header: %w", err)
	}

	// Verify it's a dictionary page
	if pageHeader.Type != parquet.PageType_DICTIONARY_PAGE {
		return nil, fmt.Errorf("expected dictionary page but got %v", pageHeader.Type)
	}

	// Read and decode the page data
	data, err := ReadPageData(pr.PFile, offset, pageHeader, codec, &layout.PageReadOptions{CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize})
	if err != nil {
		return nil, fmt.Errorf("read page data: %w", err)
	}

	values, err := DecodeDictionaryPage(data, pageHeader, physicalType)
	if err != nil {
		return nil, fmt.Errorf("decode dictionary page: %w", err)
	}

	return values, nil
}

// ReadDictionaryPageValuesInColumn reads and decodes dictionary page values
// for the dictionary page of the given column chunk. Offset, codec, and
// physical type are derived from the column metadata, and encrypted columns
// are decrypted transparently when the reader has the right keys.
func (pr *ParquetReader) ReadDictionaryPageValuesInColumn(rgIndex, colIndex int) ([]interface{}, error) {
	if rgIndex < 0 || rgIndex >= len(pr.Footer.RowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d (valid range: 0-%d)", rgIndex, len(pr.Footer.RowGroups)-1)
	}
	rg := pr.Footer.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return nil, fmt.Errorf("invalid column index: %d (valid range: 0-%d)", colIndex, len(rg.Columns)-1)
	}

	column := rg.Columns[colIndex]
	if column == nil || column.MetaData == nil {
		return nil, fmt.Errorf("column %d metadata is nil", colIndex)
	}
	if !column.MetaData.IsSetDictionaryPageOffset() {
		return nil, fmt.Errorf("column %d does not have a dictionary page", colIndex)
	}

	decryptor, err := pr.pageInspectionDecryptor(rg, column, int16(rgIndex), int16(colIndex))
	if err != nil {
		return nil, err
	}

	offset := column.MetaData.GetDictionaryPageOffset()
	pageHeader, headerSize, err := readPageHeader(pr.PFile, offset, decryptor)
	if err != nil {
		return nil, fmt.Errorf("read page header: %w", err)
	}
	if pageHeader.Type != parquet.PageType_DICTIONARY_PAGE {
		return nil, fmt.Errorf("expected dictionary page but got %v", pageHeader.Type)
	}

	data, err := readPageBody(pr.PFile, offset+headerSize, pageHeader, column.MetaData.GetCodec(), pr.crcMode, decryptor)
	if err != nil {
		return nil, fmt.Errorf("read page data: %w", err)
	}

	values, err := DecodeDictionaryPage(data, pageHeader, column.MetaData.GetType())
	if err != nil {
		return nil, fmt.Errorf("decode dictionary page: %w", err)
	}
	return values, nil
}

// pageInspectionDecryptor builds a PageDecryptor when the column is encrypted
// and returns nil for plaintext columns. Missing keys surface the standard
// "decryption key required for column" error. rowGroupOrdinal is the index
// fallback used when the row group does not carry an explicit ordinal.
func (pr *ParquetReader) pageInspectionDecryptor(rg *parquet.RowGroup, column *parquet.ColumnChunk, rowGroupOrdinal, columnOrdinal int16) (*layout.PageDecryptor, error) {
	if column == nil || column.GetCryptoMetadata() == nil {
		return nil, nil
	}
	algorithm := pr.encryptionAlgorithm()
	if algorithm == nil {
		return nil, fmt.Errorf("encrypted column missing file encryption algorithm")
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(algorithm)
	if err != nil {
		return nil, fmt.Errorf("footer AAD: %w", err)
	}
	key, err := pr.resolveColumnKey(column)
	if err != nil {
		return nil, fmt.Errorf("resolve column key: %w", err)
	}
	pageAlgorithm := layout.PageEncryptionAESGCM
	if algorithm.IsSetAES_GCM_CTR_V1() {
		pageAlgorithm = layout.PageEncryptionAESGCMCTR
	}
	if rg != nil && rg.IsSetOrdinal() {
		rowGroupOrdinal = rg.GetOrdinal()
	}
	return &layout.PageDecryptor{
		Algorithm:       pageAlgorithm,
		Key:             key,
		AADPrefix:       aadPrefix,
		AADFileUnique:   aadFileUnique,
		RowGroupOrdinal: rowGroupOrdinal,
		ColumnOrdinal:   columnOrdinal,
	}, nil
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

func (pr *ParquetReader) pageOffsetEncrypted(offset int64) bool {
	if pr == nil || pr.Footer == nil {
		return false
	}
	for _, rowGroup := range pr.Footer.GetRowGroups() {
		if rowGroup == nil {
			continue
		}
		for _, column := range rowGroup.GetColumns() {
			if column == nil || column.GetCryptoMetadata() == nil || column.MetaData == nil {
				continue
			}
			if column.MetaData.GetDataPageOffset() == offset {
				return true
			}
			if column.MetaData.IsSetDictionaryPageOffset() && column.MetaData.GetDictionaryPageOffset() == offset {
				return true
			}
		}
	}
	return false
}
