package reader

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

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
func readPageHeader(ctx context.Context, pFile io.ReadSeeker, offset int64, decryptor *layout.PageDecryptor) (*parquet.PageHeader, int64, error) {
	if _, err := pFile.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek to page: %w", err)
	}

	if decryptor == nil {
		trackingTransport := &positionTracker{r: pFile, pos: offset}
		proto := thrift.NewTCompactProtocolConf(trackingTransport, nil)

		pageHeader := parquet.NewPageHeader()
		if err := pageHeader.Read(ctx, proto); err != nil {
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
	pageHeader, err := layout.DecryptPageHeaderWithContext(ctx, module, decryptor)
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
		if pageHeader.CompressedPageSize < 0 {
			return 0, fmt.Errorf("negative compressed page size %d", pageHeader.CompressedPageSize)
		}
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
func readAllPageHeaders(ctx context.Context, pFile io.ReadSeeker, columnChunk *parquet.ColumnChunk, decryptor *layout.PageDecryptor) ([]PageHeaderInfo, error) {
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
		pageHeader, headerSize, err := readPageHeader(ctx, pFile, currentOffset, decryptor)
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
		// Each page must advance the read offset; a non-positive step indicates a
		// corrupt header and would otherwise loop forever re-reading the same bytes.
		if headerSize+bodySize <= 0 {
			return nil, fmt.Errorf("non-advancing page at offset %d (header=%d, body=%d)", currentOffset, headerSize, bodySize)
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
func readFirstDataPageHeader(ctx context.Context, pFile io.ReadSeeker, columnChunk *parquet.ColumnChunk, decryptor *layout.PageDecryptor) (*PageHeaderInfo, error) {
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
		pageHeader, headerSize, err := readPageHeader(ctx, pFile, offset, decryptor)
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
		// Each page must advance the read offset; a non-positive step indicates a
		// corrupt header and would otherwise loop forever re-reading the same bytes.
		if headerSize+bodySize <= 0 {
			return nil, fmt.Errorf("non-advancing page at offset %d (header=%d, body=%d)", offset, headerSize, bodySize)
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
