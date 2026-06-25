package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// newPageWithEncoding builds a minimal Page with a given physical type and encoding.
func newPageWithEncoding(typ parquet.Type, enc parquet.Encoding) *Page {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{Type: &typ, Name: "test_col"}
	page.Info = &common.Tag{}
	page.Info.Encoding = enc
	return page
}

func TestEncodingValues_RLE_UnsupportedType(t *testing.T) {
	page := newPageWithEncoding(parquet.Type_FLOAT, parquet.Encoding_RLE)
	_, err := page.EncodingValues([]any{float32(1.0)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "RLE encoding is not supported")
}

func TestEncodingValues_DeltaBinaryPacked_UnsupportedType(t *testing.T) {
	page := newPageWithEncoding(parquet.Type_FLOAT, parquet.Encoding_DELTA_BINARY_PACKED)
	_, err := page.EncodingValues([]any{float32(1.0)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "DELTA_BINARY_PACKED")
}

func TestEncodingValues_DeltaByteArray_UnsupportedType(t *testing.T) {
	page := newPageWithEncoding(parquet.Type_INT32, parquet.Encoding_DELTA_BYTE_ARRAY)
	_, err := page.EncodingValues([]any{int32(1)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "DELTA_BYTE_ARRAY")
}

func TestEncodingValues_DeltaLengthByteArray_UnsupportedType(t *testing.T) {
	page := newPageWithEncoding(parquet.Type_INT32, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY)
	_, err := page.EncodingValues([]any{int32(1)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "DELTA_LENGTH_BYTE_ARRAY")
}

func TestEncodingValues_ByteStreamSplit_UnsupportedType(t *testing.T) {
	page := newPageWithEncoding(parquet.Type_BOOLEAN, parquet.Encoding_BYTE_STREAM_SPLIT)
	_, err := page.EncodingValues([]any{true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "BYTE_STREAM_SPLIT")
}

func TestComputeLevelHistograms_NilDataTable(t *testing.T) {
	page := &Page{}
	page.computeLevelHistograms() // must not panic
	require.Nil(t, page.DefinitionLevelHistogram)
	require.Nil(t, page.RepetitionLevelHistogram)
}

func TestComputeLevelHistograms_DefinitionLevels(t *testing.T) {
	int32Type := parquet.Type_INT32
	page := &Page{
		Schema: &parquet.SchemaElement{Type: &int32Type},
		DataTable: &Table{
			MaxDefinitionLevel: 1,
			MaxRepetitionLevel: 0,
			DefinitionLevels:   []int32{0, 1, 1, 0, 1},
			RepetitionLevels:   []int32{0, 0, 0, 0, 0},
			Values:             []any{nil, "a", "b", nil, "c"},
		},
	}
	page.computeLevelHistograms()
	require.Equal(t, []int64{2, 3}, page.DefinitionLevelHistogram)
	require.Nil(t, page.RepetitionLevelHistogram)
}

func TestComputeLevelHistograms_RepetitionLevels(t *testing.T) {
	int32Type := parquet.Type_INT32
	page := &Page{
		Schema: &parquet.SchemaElement{Type: &int32Type},
		DataTable: &Table{
			MaxDefinitionLevel: 0,
			MaxRepetitionLevel: 1,
			DefinitionLevels:   []int32{0, 0, 0},
			RepetitionLevels:   []int32{0, 1, 0},
			Values:             []any{nil, nil, nil},
		},
	}
	page.computeLevelHistograms()
	require.Nil(t, page.DefinitionLevelHistogram)
	require.Equal(t, []int64{2, 1}, page.RepetitionLevelHistogram)
}

func TestComputeLevelHistograms_ByteArrayUnencodedBytes(t *testing.T) {
	byteArrayType := parquet.Type_BYTE_ARRAY
	page := &Page{
		Schema: &parquet.SchemaElement{Type: &byteArrayType},
		DataTable: &Table{
			MaxDefinitionLevel: 1,
			MaxRepetitionLevel: 0,
			DefinitionLevels:   []int32{1, 1, 0},
			RepetitionLevels:   []int32{0, 0, 0},
			Values:             []any{"hello", "world", nil},
		},
	}
	page.computeLevelHistograms()
	require.NotNil(t, page.UnencodedByteArrayDataBytes)
	require.Equal(t, int64(10), *page.UnencodedByteArrayDataBytes) // "hello"=5 + "world"=5
}

func TestComputeLevelHistograms_ByteArrayBytesValue(t *testing.T) {
	byteArrayType := parquet.Type_BYTE_ARRAY
	page := &Page{
		Schema: &parquet.SchemaElement{Type: &byteArrayType},
		DataTable: &Table{
			MaxDefinitionLevel: 1,
			MaxRepetitionLevel: 0,
			DefinitionLevels:   []int32{1},
			RepetitionLevels:   []int32{0},
			Values:             []any{[]byte{0xAA, 0xBB, 0xCC}},
		},
	}
	page.computeLevelHistograms()
	require.NotNil(t, page.UnencodedByteArrayDataBytes)
	require.Equal(t, int64(3), *page.UnencodedByteArrayDataBytes)
}

func TestEncodingValues_RLE_DefaultBitWidth(t *testing.T) {
	for _, tc := range []struct {
		typ  parquet.Type
		vals []any
	}{
		{parquet.Type_INT32, []any{int32(1), int32(2)}},
		{parquet.Type_INT64, []any{int64(1), int64(2)}},
		{parquet.Type_BOOLEAN, []any{true, false}},
	} {
		page := newPageWithEncoding(tc.typ, parquet.Encoding_RLE)
		page.Info.Length = 0 // force the default bit-width branch
		buf, err := page.EncodingValues(tc.vals)
		require.NoError(t, err, tc.typ)
		require.NotEmpty(t, buf, tc.typ)
	}
}

func TestSetPageStatistics_EncodeError(t *testing.T) {
	int32Type := parquet.Type_INT32
	page := &Page{
		Schema: &parquet.SchemaElement{Type: &int32Type},
		MaxVal: "not-an-int32", // type mismatch makes WritePlain fail
	}
	err := page.setPageStatistics(parquet.NewStatistics())
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode page max statistic")

	page = &Page{
		Schema: &parquet.SchemaElement{Type: &int32Type},
		MinVal: "not-an-int32",
	}
	err = page.setPageStatistics(parquet.NewStatistics())
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode page min statistic")
}

// newDataTablePage builds a simple all-present data page for compression tests.
func newDataTablePage(typ parquet.Type, enc parquet.Encoding, values []any) *Page {
	page := newPageWithEncoding(typ, enc)
	n := len(values)
	page.DataTable = &Table{
		Path:               []string{"test_col"},
		Values:             values,
		DefinitionLevels:   make([]int32, n),
		RepetitionLevels:   make([]int32, n),
		MaxDefinitionLevel: 0,
		MaxRepetitionLevel: 0,
	}
	return page
}

func TestDataPageCompress_EncodingError(t *testing.T) {
	page := newDataTablePage(parquet.Type_BYTE_ARRAY, parquet.Encoding_DELTA_BINARY_PACKED, []any{"a"})
	_, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode data page values")
}

func TestDataPageCompress_RepetitionLevels(t *testing.T) {
	page := newDataTablePage(parquet.Type_INT32, parquet.Encoding_PLAIN, []any{int32(1), int32(2)})
	page.DataTable.MaxDefinitionLevel = 1
	page.DataTable.MaxRepetitionLevel = 1
	page.DataTable.DefinitionLevels = []int32{1, 1}
	page.DataTable.RepetitionLevels = []int32{0, 1}

	buf, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, buf)
}

func TestDataPageCompress_CompressError(t *testing.T) {
	page := newDataTablePage(parquet.Type_INT32, parquet.Encoding_PLAIN, []any{int32(1)})
	_, err := page.dataPageCompress(parquet.CompressionCodec(9999), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "compress data page")
}

func TestDataPageCompress_StatisticsError(t *testing.T) {
	page := newDataTablePage(parquet.Type_INT32, parquet.Encoding_PLAIN, []any{int32(1)})
	page.MaxVal = "bad" // encoding of values succeeds, but stats encoding fails
	_, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "set page statistics")
}

func TestDataPageV2Compress_EncodingError(t *testing.T) {
	page := newDataTablePage(parquet.Type_BYTE_ARRAY, parquet.Encoding_DELTA_BINARY_PACKED, []any{"a"})
	_, _, _, err := page.dataPageV2Compress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode v2 page values")
}

func TestDataPageV2Compress_CompressError(t *testing.T) {
	page := newDataTablePage(parquet.Type_INT32, parquet.Encoding_PLAIN, []any{int32(1)})
	_, _, _, err := page.dataPageV2Compress(parquet.CompressionCodec(9999), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "compress v2 page")
}

func TestDataPageV2Compress_StatisticsError(t *testing.T) {
	page := newDataTablePage(parquet.Type_INT32, parquet.Encoding_PLAIN, []any{int32(1)})
	page.MaxVal = "bad"
	_, _, _, err := page.dataPageV2Compress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "set v2 page statistics")
}

func TestDataPageV2Compress_RepetitionLevels(t *testing.T) {
	page := newDataTablePage(parquet.Type_INT32, parquet.Encoding_PLAIN, []any{int32(1), int32(2)})
	page.DataTable.MaxDefinitionLevel = 1
	page.DataTable.MaxRepetitionLevel = 1
	page.DataTable.DefinitionLevels = []int32{1, 1}
	page.DataTable.RepetitionLevels = []int32{0, 1}

	rBuf, dBuf, vBuf, err := page.dataPageV2Compress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, rBuf)
	require.NotEmpty(t, dBuf)
	require.NotEmpty(t, vBuf)
	require.Equal(t, int32(1), page.Header.DataPageHeaderV2.NumRows) // one row at rep level 0
}

func TestSetPageStatistics_NilMinMax(t *testing.T) {
	int32Type := parquet.Type_INT32
	page := &Page{Schema: &parquet.SchemaElement{Type: &int32Type}}
	stats := parquet.NewStatistics()
	err := page.setPageStatistics(stats)
	require.NoError(t, err)
	require.Nil(t, stats.Min)
	require.Nil(t, stats.Max)
}

func TestSetPageStatistics_WithMinMax(t *testing.T) {
	int32Type := parquet.Type_INT32
	page := &Page{
		Schema: &parquet.SchemaElement{Type: &int32Type},
		MinVal: int32(1),
		MaxVal: int32(99),
	}
	stats := parquet.NewStatistics()
	err := page.setPageStatistics(stats)
	require.NoError(t, err)
	require.NotNil(t, stats.Min)
	require.NotNil(t, stats.Max)
}

func TestSetPageStatistics_ByteArrayStripsLengthPrefix(t *testing.T) {
	byteArrayType := parquet.Type_BYTE_ARRAY
	page := &Page{
		Schema: &parquet.SchemaElement{Type: &byteArrayType},
		MinVal: "abc",
		MaxVal: "xyz",
	}
	stats := parquet.NewStatistics()
	err := page.setPageStatistics(stats)
	require.NoError(t, err)
	// BYTE_ARRAY: WritePlain adds 4-byte length prefix, setPageStatistics strips it
	require.Equal(t, []byte("abc"), stats.MinValue)
	require.Equal(t, []byte("xyz"), stats.MaxValue)
}
