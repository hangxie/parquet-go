package layout

import (
	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// DefaultMaxPageSize is the default maximum size for page data (256 MB).
// This helps prevent memory exhaustion from maliciously crafted files with
// extremely large page sizes specified in headers.
const DefaultMaxPageSize = 256 * 1024 * 1024

// resolveCompressor returns c if non-nil, otherwise returns the DefaultCompressor.
func resolveCompressor(c *compress.Compressor) *compress.Compressor {
	if c != nil {
		return c
	}
	return compress.DefaultCompressor()
}

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
	// GeospatialBoundsUnknown marks a page holding a value whose coordinates could not be
	// read, which is why it has no bounding box. A page with nothing to measure has none
	// either, and the chunk has to tell the two apart: bounds around the pages it could
	// read would leave out a value this one holds.
	GeospatialBoundsUnknown bool
	// GeospatialTypesUnknown marks a page holding a value whose geometry type could not
	// be read, so the types it does list are not all the types it holds.
	GeospatialTypesUnknown bool

	// Level histograms for ColumnIndex (computed during page creation).
	// Each has size maxLevel+1; nil if maxLevel == 0.
	DefinitionLevelHistogram []int64
	RepetitionLevelHistogram []int64

	// NumRows is the number of rows (records, i.e. repetition-level-0 entries)
	// contained in the page. It is the unit used for OffsetIndex
	// first_row_index and differs from the leaf value count for repeated
	// fields. Computed during page creation so it survives DataTable being
	// nilled out later. Pages are built row-aligned, so this is always >= 1
	// for a non-empty page and first_row_index values are strictly increasing.
	NumRows int64

	// UnencodedByteArrayDataBytes tracks the total byte size of BYTE_ARRAY
	// data values (excluding 4-byte length prefixes) for SizeStatistics.
	// Set only for a BYTE_ARRAY column whose values were measured; nil for
	// any other type, and for a page whose values did not reach the count,
	// which withholds the chunk's total rather than understating it.
	UnencodedByteArrayDataBytes *int64

	// dictionaryIndices are retained until row-group finalization.
	dictionaryIndices []int32

	// compressor is set during page reading so that subsequent operations
	// (GetRLDLFromRawData, GetValueFromRawData) can decompress using
	// the same per-instance compressor. Nil means use DefaultCompressor.
	compressor *compress.Compressor
}

// Create a new page
func NewPage() *Page {
	page := new(Page)
	page.DataTable = nil
	page.Header = parquet.NewPageHeader()
	page.Info = &common.Tag{}
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
