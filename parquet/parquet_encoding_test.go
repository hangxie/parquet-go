package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_EncodingMethods(t *testing.T) {
	tests := []struct {
		name     string
		value    Encoding
		strValue string
	}{
		{"PLAIN", Encoding_PLAIN, "PLAIN"},
		{"PLAIN_DICTIONARY", Encoding_PLAIN_DICTIONARY, "PLAIN_DICTIONARY"},
		{"RLE", Encoding_RLE, "RLE"},
		{"BIT_PACKED", Encoding_BIT_PACKED, "BIT_PACKED"},
		{"DELTA_BINARY_PACKED", Encoding_DELTA_BINARY_PACKED, "DELTA_BINARY_PACKED"},
		{"DELTA_LENGTH_BYTE_ARRAY", Encoding_DELTA_LENGTH_BYTE_ARRAY, "DELTA_LENGTH_BYTE_ARRAY"},
		{"DELTA_BYTE_ARRAY", Encoding_DELTA_BYTE_ARRAY, "DELTA_BYTE_ARRAY"},
		{"RLE_DICTIONARY", Encoding_RLE_DICTIONARY, "RLE_DICTIONARY"},
		{"BYTE_STREAM_SPLIT", Encoding_BYTE_STREAM_SPLIT, "BYTE_STREAM_SPLIT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Test String method
			require.Equal(t, test.strValue, test.value.String())

			// Test FromString method
			got, err := EncodingFromString(test.strValue)
			require.NoError(t, err)
			require.Equal(t, test.value, got)

			// Test MarshalText method
			bytes, err := test.value.MarshalText()
			require.NoError(t, err)
			require.Equal(t, test.strValue, string(bytes))

			// Test UnmarshalText method
			var newEnc Encoding
			err = newEnc.UnmarshalText(bytes)
			require.NoError(t, err)
			require.Equal(t, test.value, newEnc)

			// Test Ptr method
			ptr := EncodingPtr(test.value)
			require.Equal(t, test.value, *ptr)

			// Test Scan method
			var scanEnc Encoding
			err = scanEnc.Scan(int64(test.value))
			require.NoError(t, err)
			require.Equal(t, test.value, scanEnc)

			// Test Value method
			val, err := test.value.Value()
			require.NoError(t, err)
			require.Equal(t, int64(test.value), val)
		})
	}

	// Test edge cases
	t.Run("invalid_string", func(t *testing.T) {
		_, err := EncodingFromString("INVALID")
		require.Error(t, err)

		_, err = EncodingFromString("")
		require.Error(t, err)
	})

	t.Run("unset_value", func(t *testing.T) {
		unsetEnc := Encoding(999)
		require.Equal(t, "<UNSET>", unsetEnc.String())
	})

	t.Run("scan_invalid", func(t *testing.T) {
		var enc Encoding
		err := enc.Scan("invalid")
		require.Error(t, err)
	})
}

func Test_CompressionCodecMethods(t *testing.T) {
	tests := []struct {
		name     string
		value    CompressionCodec
		strValue string
	}{
		{"UNCOMPRESSED", CompressionCodec_UNCOMPRESSED, "UNCOMPRESSED"},
		{"SNAPPY", CompressionCodec_SNAPPY, "SNAPPY"},
		{"GZIP", CompressionCodec_GZIP, "GZIP"},
		{"LZO", CompressionCodec_LZO, "LZO"},
		{"BROTLI", CompressionCodec_BROTLI, "BROTLI"},
		{"LZ4", CompressionCodec_LZ4, "LZ4"},
		{"ZSTD", CompressionCodec_ZSTD, "ZSTD"},
		{"LZ4_RAW", CompressionCodec_LZ4_RAW, "LZ4_RAW"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Test String method
			require.Equal(t, test.strValue, test.value.String())

			// Test FromString method
			got, err := CompressionCodecFromString(test.strValue)
			require.NoError(t, err)
			require.Equal(t, test.value, got)

			// Test MarshalText method
			bytes, err := test.value.MarshalText()
			require.NoError(t, err)
			require.Equal(t, test.strValue, string(bytes))

			// Test UnmarshalText method
			var newCodec CompressionCodec
			err = newCodec.UnmarshalText(bytes)
			require.NoError(t, err)
			require.Equal(t, test.value, newCodec)

			// Test Ptr method
			ptr := CompressionCodecPtr(test.value)
			require.Equal(t, test.value, *ptr)

			// Test Scan method
			var scanCodec CompressionCodec
			err = scanCodec.Scan(int64(test.value))
			require.NoError(t, err)
			require.Equal(t, test.value, scanCodec)

			// Test Value method
			val, err := test.value.Value()
			require.NoError(t, err)
			require.Equal(t, int64(test.value), val)
		})
	}

	// Test edge cases
	t.Run("invalid_string", func(t *testing.T) {
		_, err := CompressionCodecFromString("INVALID")
		require.Error(t, err)

		_, err = CompressionCodecFromString("")
		require.Error(t, err)
	})

	t.Run("unset_value", func(t *testing.T) {
		unsetCodec := CompressionCodec(999)
		require.Equal(t, "<UNSET>", unsetCodec.String())
	})

	t.Run("scan_nil", func(t *testing.T) {
		var cc CompressionCodec
		err := cc.Scan(nil)
		require.Error(t, err)
	})

	t.Run("unmarshal_invalid", func(t *testing.T) {
		var cc CompressionCodec
		err := cc.UnmarshalText([]byte("INVALID"))
		require.Error(t, err)
	})
}

func Test_PageTypeMethods(t *testing.T) {
	tests := []struct {
		name     string
		value    PageType
		strValue string
	}{
		{"DATA_PAGE", PageType_DATA_PAGE, "DATA_PAGE"},
		{"INDEX_PAGE", PageType_INDEX_PAGE, "INDEX_PAGE"},
		{"DICTIONARY_PAGE", PageType_DICTIONARY_PAGE, "DICTIONARY_PAGE"},
		{"DATA_PAGE_V2", PageType_DATA_PAGE_V2, "DATA_PAGE_V2"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Test String method
			require.Equal(t, test.strValue, test.value.String())

			// Test FromString method
			got, err := PageTypeFromString(test.strValue)
			require.NoError(t, err)
			require.Equal(t, test.value, got)

			// Test MarshalText method
			bytes, err := test.value.MarshalText()
			require.NoError(t, err)
			require.Equal(t, test.strValue, string(bytes))

			// Test UnmarshalText method
			var newPageType PageType
			err = newPageType.UnmarshalText(bytes)
			require.NoError(t, err)
			require.Equal(t, test.value, newPageType)

			// Test Ptr method
			ptr := PageTypePtr(test.value)
			require.Equal(t, test.value, *ptr)

			// Test Scan method
			var scanPageType PageType
			err = scanPageType.Scan(int64(test.value))
			require.NoError(t, err)
			require.Equal(t, test.value, scanPageType)

			// Test Value method
			val, err := test.value.Value()
			require.NoError(t, err)
			require.Equal(t, int64(test.value), val)
		})
	}

	// Test edge cases
	t.Run("invalid_string", func(t *testing.T) {
		_, err := PageTypeFromString("INVALID")
		require.Error(t, err)
	})

	t.Run("unset_value", func(t *testing.T) {
		unsetPageType := PageType(999)
		require.Equal(t, "<UNSET>", unsetPageType.String())
	})

	t.Run("scan_invalid", func(t *testing.T) {
		var pt PageType
		err := pt.Scan("invalid")
		require.Error(t, err)
	})
}

func Test_BoundaryOrderMethods(t *testing.T) {
	tests := []struct {
		name     string
		value    BoundaryOrder
		strValue string
	}{
		{"UNORDERED", BoundaryOrder_UNORDERED, "UNORDERED"},
		{"ASCENDING", BoundaryOrder_ASCENDING, "ASCENDING"},
		{"DESCENDING", BoundaryOrder_DESCENDING, "DESCENDING"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Test String method
			require.Equal(t, test.strValue, test.value.String())

			// Test FromString method
			got, err := BoundaryOrderFromString(test.strValue)
			require.NoError(t, err)
			require.Equal(t, test.value, got)

			// Test MarshalText method
			bytes, err := test.value.MarshalText()
			require.NoError(t, err)
			require.Equal(t, test.strValue, string(bytes))

			// Test UnmarshalText method
			var newBoundaryOrder BoundaryOrder
			err = newBoundaryOrder.UnmarshalText(bytes)
			require.NoError(t, err)
			require.Equal(t, test.value, newBoundaryOrder)

			// Test Ptr method
			ptr := BoundaryOrderPtr(test.value)
			require.Equal(t, test.value, *ptr)

			// Test Scan method
			var scanBoundaryOrder BoundaryOrder
			err = scanBoundaryOrder.Scan(int64(test.value))
			require.NoError(t, err)
			require.Equal(t, test.value, scanBoundaryOrder)

			// Test Value method
			val, err := test.value.Value()
			require.NoError(t, err)
			require.Equal(t, int64(test.value), val)
		})
	}

	// Test edge cases
	t.Run("invalid_string", func(t *testing.T) {
		_, err := BoundaryOrderFromString("INVALID")
		require.Error(t, err)
	})

	t.Run("unset_value", func(t *testing.T) {
		unsetBoundaryOrder := BoundaryOrder(999)
		require.Equal(t, "<UNSET>", unsetBoundaryOrder.String())
	})

	t.Run("scan_invalid", func(t *testing.T) {
		var bo BoundaryOrder
		err := bo.Scan("invalid")
		require.Error(t, err)
	})

	t.Run("unmarshal_invalid", func(t *testing.T) {
		var bo BoundaryOrder
		err := bo.UnmarshalText([]byte("INVALID"))
		require.Error(t, err)
	})
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
