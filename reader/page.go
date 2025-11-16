package reader

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v2/compress"
	"github.com/hangxie/parquet-go/v2/encoding"
	"github.com/hangxie/parquet-go/v2/parquet"
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

// readPageHeader reads a page header from the given offset
// This is an internal helper function used by ReadAllPageHeaders
func readPageHeader(pFile io.ReadSeeker, offset int64) (*parquet.PageHeader, int64, error) {
	// Seek to page header position
	_, err := pFile.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, 0, fmt.Errorf("seek to page: %w", err)
	}

	// Create a position-tracking transport
	trackingTransport := &positionTracker{r: pFile, pos: offset}
	proto := thrift.NewTCompactProtocolConf(trackingTransport, nil)

	pageHeader := parquet.NewPageHeader()
	if err := pageHeader.Read(context.Background(), proto); err != nil {
		return nil, 0, err
	}

	headerSize := trackingTransport.pos - offset

	// Seek to end of header
	_, err = pFile.Seek(trackingTransport.pos, io.SeekStart)
	if err != nil {
		return nil, 0, fmt.Errorf("seek after header: %w", err)
	}

	return pageHeader, headerSize, nil
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
			info.IsCompressed = &dph2.IsCompressed
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

// ReadAllPageHeaders reads all page headers from a column chunk
// Returns a slice of PageHeaderInfo containing metadata about each page
func ReadAllPageHeaders(pFile io.ReadSeeker, columnChunk *parquet.ColumnChunk) ([]PageHeaderInfo, error) {
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
		pageHeader, headerSize, err := readPageHeader(pFile, currentOffset)
		if err != nil {
			break // Can't read more pages, we're done
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

		// Move to next page
		currentOffset = currentOffset + headerSize + int64(pageHeader.CompressedPageSize)
		pageIndex++
	}

	return pages, nil
}

// ReadPageData reads and decompresses the data from a page at the given offset
// Returns the uncompressed page data
func ReadPageData(pFile io.ReadSeeker, offset int64, pageHeader *parquet.PageHeader, codec parquet.CompressionCodec) ([]byte, error) {
	// Re-read the header to get exact header size
	_, headerSize, err := readPageHeader(pFile, offset)
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

	// Decompress the data
	uncompressedData, err := compress.Uncompress(compressedData, codec)
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

// GetAllPageHeaders returns metadata for all pages in a column chunk
// This is useful for tools that need to inspect page structure
func (pr *ParquetReader) GetAllPageHeaders(rgIndex, colIndex int) ([]PageHeaderInfo, error) {
	if rgIndex < 0 || rgIndex >= len(pr.Footer.RowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d", rgIndex)
	}

	rg := pr.Footer.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return nil, fmt.Errorf("invalid column index: %d", colIndex)
	}

	column := rg.Columns[colIndex]
	return ReadAllPageHeaders(pr.PFile, column)
}

// ReadDictionaryPageValues reads and decodes dictionary page values at the given offset
// This is a convenience function for tools that need to inspect dictionary pages directly
func (pr *ParquetReader) ReadDictionaryPageValues(offset int64, codec parquet.CompressionCodec, physicalType parquet.Type) ([]interface{}, error) {
	// Read page header at the offset
	pageHeader, _, err := readPageHeader(pr.PFile, offset)
	if err != nil {
		return nil, fmt.Errorf("read page header: %w", err)
	}

	// Verify it's a dictionary page
	if pageHeader.Type != parquet.PageType_DICTIONARY_PAGE {
		return nil, fmt.Errorf("expected dictionary page but got %v", pageHeader.Type)
	}

	// Read and decode the page data
	data, err := ReadPageData(pr.PFile, offset, pageHeader, codec)
	if err != nil {
		return nil, fmt.Errorf("read page data: %w", err)
	}

	values, err := DecodeDictionaryPage(data, pageHeader, physicalType)
	if err != nil {
		return nil, fmt.Errorf("decode dictionary page: %w", err)
	}

	return values, nil
}
