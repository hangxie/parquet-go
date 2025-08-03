package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Encoding_String(t *testing.T) {
	tests := []struct {
		enc      Encoding
		expected string
	}{
		{Encoding_PLAIN, "PLAIN"},
		{Encoding_PLAIN_DICTIONARY, "PLAIN_DICTIONARY"},
		{Encoding_RLE, "RLE"},
		{Encoding_BIT_PACKED, "BIT_PACKED"},
		{Encoding_DELTA_BINARY_PACKED, "DELTA_BINARY_PACKED"},
		{Encoding_DELTA_LENGTH_BYTE_ARRAY, "DELTA_LENGTH_BYTE_ARRAY"},
		{Encoding_DELTA_BYTE_ARRAY, "DELTA_BYTE_ARRAY"},
		{Encoding_RLE_DICTIONARY, "RLE_DICTIONARY"},
		{Encoding_BYTE_STREAM_SPLIT, "BYTE_STREAM_SPLIT"},
		{Encoding(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.enc.String()
			require.Equal(t, test.expected, got)
		})
	}
}

func Test_Encoding_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected Encoding
		hasError bool
	}{
		{"PLAIN", Encoding_PLAIN, false},
		{"PLAIN_DICTIONARY", Encoding_PLAIN_DICTIONARY, false},
		{"RLE", Encoding_RLE, false},
		{"BIT_PACKED", Encoding_BIT_PACKED, false},
		{"DELTA_BINARY_PACKED", Encoding_DELTA_BINARY_PACKED, false},
		{"DELTA_LENGTH_BYTE_ARRAY", Encoding_DELTA_LENGTH_BYTE_ARRAY, false},
		{"DELTA_BYTE_ARRAY", Encoding_DELTA_BYTE_ARRAY, false},
		{"RLE_DICTIONARY", Encoding_RLE_DICTIONARY, false},
		{"BYTE_STREAM_SPLIT", Encoding_BYTE_STREAM_SPLIT, false},
		{"INVALID", Encoding(0), true},
		{"", Encoding(0), true},
	}

	for _, test := range tests {
		testName := test.input
		if testName == "" {
			testName = "empty_string"
		}
		t.Run(testName, func(t *testing.T) {
			got, err := EncodingFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_Encoding_MarshalUnmarshal(t *testing.T) {
	enc := Encoding_PLAIN

	bytes, err := enc.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "PLAIN", string(bytes))

	var newEnc Encoding
	err = newEnc.UnmarshalText(bytes)
	require.NoError(t, err)
	require.Equal(t, enc, newEnc)
}

func Test_Encoding_Ptr(t *testing.T) {
	enc := Encoding_RLE
	ptr := EncodingPtr(enc)
	require.Equal(t, enc, *ptr)
}

func Test_Encoding_Scan(t *testing.T) {
	var enc Encoding
	err := enc.Scan(int64(Encoding_BIT_PACKED))
	require.NoError(t, err)
	require.Equal(t, Encoding_BIT_PACKED, enc)

	err = enc.Scan("invalid")
	require.Error(t, err)
}

func Test_Encoding_Value(t *testing.T) {
	enc := Encoding_DELTA_BINARY_PACKED
	val, err := enc.Value()
	require.NoError(t, err)
	require.Equal(t, int64(Encoding_DELTA_BINARY_PACKED), val)
}

func Test_CompressionCodec_String(t *testing.T) {
	tests := []struct {
		codec    CompressionCodec
		expected string
	}{
		{CompressionCodec_UNCOMPRESSED, "UNCOMPRESSED"},
		{CompressionCodec_SNAPPY, "SNAPPY"},
		{CompressionCodec_GZIP, "GZIP"},
		{CompressionCodec_LZO, "LZO"},
		{CompressionCodec_BROTLI, "BROTLI"},
		{CompressionCodec_LZ4, "LZ4"},
		{CompressionCodec_ZSTD, "ZSTD"},
		{CompressionCodec_LZ4_RAW, "LZ4_RAW"},
		{CompressionCodec(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.codec.String()
			require.Equal(t, test.expected, got)
		})
	}
}

func Test_CompressionCodec_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected CompressionCodec
		hasError bool
	}{
		{"UNCOMPRESSED", CompressionCodec_UNCOMPRESSED, false},
		{"SNAPPY", CompressionCodec_SNAPPY, false},
		{"GZIP", CompressionCodec_GZIP, false},
		{"LZO", CompressionCodec_LZO, false},
		{"BROTLI", CompressionCodec_BROTLI, false},
		{"LZ4", CompressionCodec_LZ4, false},
		{"ZSTD", CompressionCodec_ZSTD, false},
		{"LZ4_RAW", CompressionCodec_LZ4_RAW, false},
		{"INVALID", CompressionCodec(0), true},
		{"", CompressionCodec(0), true},
	}

	for _, test := range tests {
		testName := test.input
		if testName == "" {
			testName = "empty_string"
		}
		t.Run(testName, func(t *testing.T) {
			got, err := CompressionCodecFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_CompressionCodec_ScanValue(t *testing.T) {
	codec := CompressionCodec_SNAPPY

	val, err := codec.Value()
	require.NoError(t, err)
	require.Equal(t, int64(CompressionCodec_SNAPPY), val)

	var newCodec CompressionCodec
	err = newCodec.Scan(val)
	require.NoError(t, err)
	require.Equal(t, codec, newCodec)
}

func Test_CompressionCodec_Ptr(t *testing.T) {
	cc := CompressionCodec_GZIP
	ptr := CompressionCodecPtr(cc)
	require.Equal(t, cc, *ptr)
}

func Test_CompressionCodec_MarshalText(t *testing.T) {
	cc := CompressionCodec_ZSTD
	bytes, err := cc.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "ZSTD", string(bytes))
}

func Test_CompressionCodec_UnmarshalText(t *testing.T) {
	var cc CompressionCodec
	err := cc.UnmarshalText([]byte("BROTLI"))
	require.NoError(t, err)
	require.Equal(t, CompressionCodec_BROTLI, cc)

	err = cc.UnmarshalText([]byte("INVALID"))
	require.Error(t, err)
}

func Test_CompressionCodecScanNil(t *testing.T) {
	var cc CompressionCodec
	err := cc.Scan(nil)
	require.Error(t, err)
}

func Test_PageType_String(t *testing.T) {
	tests := []struct {
		pageType PageType
		expected string
	}{
		{PageType_DATA_PAGE, "DATA_PAGE"},
		{PageType_INDEX_PAGE, "INDEX_PAGE"},
		{PageType_DICTIONARY_PAGE, "DICTIONARY_PAGE"},
		{PageType_DATA_PAGE_V2, "DATA_PAGE_V2"},
		{PageType(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.pageType.String()
			require.Equal(t, test.expected, got)
		})
	}
}

func Test_PageType_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected PageType
		hasError bool
	}{
		{"DATA_PAGE", PageType_DATA_PAGE, false},
		{"INDEX_PAGE", PageType_INDEX_PAGE, false},
		{"DICTIONARY_PAGE", PageType_DICTIONARY_PAGE, false},
		{"DATA_PAGE_V2", PageType_DATA_PAGE_V2, false},
		{"INVALID", PageType(0), true},
	}

	for _, test := range tests {
		testName := test.input
		if testName == "" {
			testName = "empty_string"
		}
		t.Run(testName, func(t *testing.T) {
			got, err := PageTypeFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_PageType_MarshalUnmarshal(t *testing.T) {
	pageType := PageType_DATA_PAGE_V2

	bytes, err := pageType.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "DATA_PAGE_V2", string(bytes))

	var newPageType PageType
	err = newPageType.UnmarshalText(bytes)
	require.NoError(t, err)
	require.Equal(t, pageType, newPageType)
}

func Test_PageType_Ptr(t *testing.T) {
	pt := PageType_INDEX_PAGE
	ptr := PageTypePtr(pt)
	require.Equal(t, pt, *ptr)
}

func Test_PageType_Scan(t *testing.T) {
	var pt PageType
	err := pt.Scan(int64(PageType_DICTIONARY_PAGE))
	require.NoError(t, err)
	require.Equal(t, PageType_DICTIONARY_PAGE, pt)

	err = pt.Scan("invalid")
	require.Error(t, err)
}

func Test_PageType_Value(t *testing.T) {
	pt := PageType_DATA_PAGE
	val, err := pt.Value()
	require.NoError(t, err)
	require.Equal(t, int64(PageType_DATA_PAGE), val)
}

func Test_BoundaryOrder_String(t *testing.T) {
	tests := []struct {
		order    BoundaryOrder
		expected string
	}{
		{BoundaryOrder_UNORDERED, "UNORDERED"},
		{BoundaryOrder_ASCENDING, "ASCENDING"},
		{BoundaryOrder_DESCENDING, "DESCENDING"},
		{BoundaryOrder(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			require.Equal(t, test.expected, test.order.String())
		})
	}
}

func Test_BoundaryOrder_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected BoundaryOrder
		hasError bool
	}{
		{"UNORDERED", BoundaryOrder_UNORDERED, false},
		{"ASCENDING", BoundaryOrder_ASCENDING, false},
		{"DESCENDING", BoundaryOrder_DESCENDING, false},
		{"INVALID", BoundaryOrder(0), true},
	}

	for _, test := range tests {
		testName := test.input
		if testName == "" {
			testName = "empty_string"
		}
		t.Run(testName, func(t *testing.T) {
			got, err := BoundaryOrderFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_BoundaryOrder_Ptr(t *testing.T) {
	bo := BoundaryOrder_ASCENDING
	ptr := BoundaryOrderPtr(bo)
	require.Equal(t, bo, *ptr)
}

func Test_BoundaryOrder_MarshalText(t *testing.T) {
	bo := BoundaryOrder_DESCENDING
	bytes, err := bo.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "DESCENDING", string(bytes))
}

func Test_BoundaryOrder_UnmarshalText(t *testing.T) {
	var bo BoundaryOrder
	err := bo.UnmarshalText([]byte("UNORDERED"))
	require.NoError(t, err)
	require.Equal(t, BoundaryOrder_UNORDERED, bo)

	err = bo.UnmarshalText([]byte("INVALID"))
	require.Error(t, err)
}

func Test_BoundaryOrder_Scan(t *testing.T) {
	var bo BoundaryOrder
	err := bo.Scan(int64(BoundaryOrder_ASCENDING))
	require.NoError(t, err)
	require.Equal(t, BoundaryOrder_ASCENDING, bo)

	err = bo.Scan("invalid")
	require.Error(t, err)
}

func Test_BoundaryOrder_Value(t *testing.T) {
	bo := BoundaryOrder_DESCENDING
	val, err := bo.Value()
	require.NoError(t, err)
	require.Equal(t, int64(BoundaryOrder_DESCENDING), val)
}

func Test_EncodingNilPointerValue(t *testing.T) {
	tests := []struct {
		name  string
		getFn func() (interface{}, error)
	}{
		{"Encoding nil", func() (interface{}, error) { var enc *Encoding; return enc.Value() }},
		{"CompressionCodec nil", func() (interface{}, error) { var cc *CompressionCodec; return cc.Value() }},
		{"PageType nil", func() (interface{}, error) { var pt *PageType; return pt.Value() }},
		{"BoundaryOrder nil", func() (interface{}, error) { var bo *BoundaryOrder; return bo.Value() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := tt.getFn()
			require.NoError(t, err)
			require.Nil(t, val)
		})
	}
}

func Test_EncodingScanErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		getFn func() interface{ Scan(interface{}) error }
		input interface{}
	}{
		{"Encoding string", func() interface{ Scan(interface{}) error } { var e Encoding; return &e }, "invalid"},
		{"Encoding map", func() interface{ Scan(interface{}) error } { var e Encoding; return &e }, map[string]int{"invalid": 1}},
		{"PageType string", func() interface{ Scan(interface{}) error } { var p PageType; return &p }, "invalid"},
		{"PageType bool", func() interface{ Scan(interface{}) error } { var p PageType; return &p }, true},
		{"BoundaryOrder string", func() interface{ Scan(interface{}) error } { var b BoundaryOrder; return &b }, "invalid"},
		{"BoundaryOrder slice", func() interface{ Scan(interface{}) error } { var b BoundaryOrder; return &b }, []int{1, 2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := tt.getFn()
			err := target.Scan(tt.input)
			require.Error(t, err)
		})
	}
}

func Test_EncodingUnmarshalTextErrors(t *testing.T) {
	invalidInputs := [][]byte{
		[]byte(""),
		[]byte("COMPLETELY_INVALID"),
		[]byte("123"),
		[]byte("special!@#$%"),
		[]byte(" LEADING_SPACE"),
		[]byte("TRAILING_SPACE "),
		[]byte("lower_case"),
		[]byte("Mixed_Case"),
	}

	for _, input := range invalidInputs {
		t.Run("Encoding_"+string(input), func(t *testing.T) {
			var enc Encoding
			err := enc.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run("CompressionCodec_"+string(input), func(t *testing.T) {
			var cc CompressionCodec
			err := cc.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run("PageType_"+string(input), func(t *testing.T) {
			var pt PageType
			err := pt.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run("BoundaryOrder_"+string(input), func(t *testing.T) {
			var bo BoundaryOrder
			err := bo.UnmarshalText(input)
			require.Error(t, err)
		})
	}
}

func Test_CompressionCodecErrorScenarios(t *testing.T) {
	var cc CompressionCodec

	errorInputs := []interface{}{
		nil,
		"string",
		3.14,
		true,
		[]byte("bytes"),
		map[string]int{"key": 1},
		struct{}{},
	}

	for _, input := range errorInputs {
		err := cc.Scan(input)
		require.Error(t, err)
	}

	err := cc.Scan(int64(CompressionCodec_GZIP))
	require.NoError(t, err)
	require.Equal(t, CompressionCodec_GZIP, cc)
}
