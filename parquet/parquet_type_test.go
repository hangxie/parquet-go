package parquet

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Type_String(t *testing.T) {
	tests := []struct {
		typ      Type
		expected string
	}{
		{Type_BOOLEAN, "BOOLEAN"},
		{Type_INT32, "INT32"},
		{Type_INT64, "INT64"},
		{Type_INT96, "INT96"},
		{Type_FLOAT, "FLOAT"},
		{Type_DOUBLE, "DOUBLE"},
		{Type_BYTE_ARRAY, "BYTE_ARRAY"},
		{Type_FIXED_LEN_BYTE_ARRAY, "FIXED_LEN_BYTE_ARRAY"},
		{Type(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.typ.String()
			require.Equal(t, test.expected, got)
		})
	}
}

func Test_Type_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected Type
		hasError bool
	}{
		{"BOOLEAN", Type_BOOLEAN, false},
		{"INT32", Type_INT32, false},
		{"INT64", Type_INT64, false},
		{"INT96", Type_INT96, false},
		{"FLOAT", Type_FLOAT, false},
		{"DOUBLE", Type_DOUBLE, false},
		{"BYTE_ARRAY", Type_BYTE_ARRAY, false},
		{"FIXED_LEN_BYTE_ARRAY", Type_FIXED_LEN_BYTE_ARRAY, false},
		{"INVALID", Type(0), true},
		{"", Type(0), true},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := TypeFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_Type_Ptr(t *testing.T) {
	typ := Type_INT32
	ptr := TypePtr(typ)
	require.Equal(t, typ, *ptr)
}

func Test_Type_MarshalText(t *testing.T) {
	typ := Type_BOOLEAN
	bytes, err := typ.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "BOOLEAN", string(bytes))
}

func Test_Type_UnmarshalText(t *testing.T) {
	var typ Type
	err := typ.UnmarshalText([]byte("INT32"))
	require.NoError(t, err)
	require.Equal(t, Type_INT32, typ)

	err = typ.UnmarshalText([]byte("INVALID"))
	require.Error(t, err)
}

func Test_Type_Scan(t *testing.T) {
	var typ Type

	err := typ.Scan(int64(Type_INT64))
	require.NoError(t, err)
	require.Equal(t, Type_INT64, typ)

	err = typ.Scan(int64(Type_FLOAT))
	require.NoError(t, err)
	require.Equal(t, Type_FLOAT, typ)

	err = typ.Scan("invalid")
	require.Error(t, err)
}

func Test_Type_Value(t *testing.T) {
	typ := Type_DOUBLE
	val, err := typ.Value()
	require.NoError(t, err)
	require.Equal(t, int64(Type_DOUBLE), val)
}

func Test_ConvertedType_String(t *testing.T) {
	tests := []struct {
		typ      ConvertedType
		expected string
	}{
		{ConvertedType_UTF8, "UTF8"},
		{ConvertedType_MAP, "MAP"},
		{ConvertedType_MAP_KEY_VALUE, "MAP_KEY_VALUE"},
		{ConvertedType_LIST, "LIST"},
		{ConvertedType_ENUM, "ENUM"},
		{ConvertedType_DECIMAL, "DECIMAL"},
		{ConvertedType_DATE, "DATE"},
		{ConvertedType_TIME_MILLIS, "TIME_MILLIS"},
		{ConvertedType_TIME_MICROS, "TIME_MICROS"},
		{ConvertedType_TIMESTAMP_MILLIS, "TIMESTAMP_MILLIS"},
		{ConvertedType_TIMESTAMP_MICROS, "TIMESTAMP_MICROS"},
		{ConvertedType_UINT_8, "UINT_8"},
		{ConvertedType_UINT_16, "UINT_16"},
		{ConvertedType_UINT_32, "UINT_32"},
		{ConvertedType_UINT_64, "UINT_64"},
		{ConvertedType_INT_8, "INT_8"},
		{ConvertedType_INT_16, "INT_16"},
		{ConvertedType_INT_32, "INT_32"},
		{ConvertedType_INT_64, "INT_64"},
		{ConvertedType_JSON, "JSON"},
		{ConvertedType_BSON, "BSON"},
		{ConvertedType_INTERVAL, "INTERVAL"},
		{ConvertedType(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.typ.String()
			require.Equal(t, test.expected, got)
		})
	}
}

func Test_ConvertedType_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected ConvertedType
		hasError bool
	}{
		{"UTF8", ConvertedType_UTF8, false},
		{"MAP", ConvertedType_MAP, false},
		{"MAP_KEY_VALUE", ConvertedType_MAP_KEY_VALUE, false},
		{"LIST", ConvertedType_LIST, false},
		{"ENUM", ConvertedType_ENUM, false},
		{"DECIMAL", ConvertedType_DECIMAL, false},
		{"DATE", ConvertedType_DATE, false},
		{"TIME_MILLIS", ConvertedType_TIME_MILLIS, false},
		{"TIME_MICROS", ConvertedType_TIME_MICROS, false},
		{"TIMESTAMP_MILLIS", ConvertedType_TIMESTAMP_MILLIS, false},
		{"TIMESTAMP_MICROS", ConvertedType_TIMESTAMP_MICROS, false},
		{"UINT_8", ConvertedType_UINT_8, false},
		{"UINT_16", ConvertedType_UINT_16, false},
		{"UINT_32", ConvertedType_UINT_32, false},
		{"UINT_64", ConvertedType_UINT_64, false},
		{"INT_8", ConvertedType_INT_8, false},
		{"INT_16", ConvertedType_INT_16, false},
		{"INT_32", ConvertedType_INT_32, false},
		{"INT_64", ConvertedType_INT_64, false},
		{"JSON", ConvertedType_JSON, false},
		{"BSON", ConvertedType_BSON, false},
		{"INTERVAL", ConvertedType_INTERVAL, false},
		{"INVALID", ConvertedType(0), true},
		{"", ConvertedType(0), true},
	}

	for _, test := range tests {
		testName := test.input
		if testName == "" {
			testName = "empty_string"
		}
		t.Run(testName, func(t *testing.T) {
			got, err := ConvertedTypeFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_ConvertedType_Ptr(t *testing.T) {
	ct := ConvertedType_UTF8
	ptr := ConvertedTypePtr(ct)
	require.Equal(t, ct, *ptr)
}

func Test_ConvertedType_MarshalText(t *testing.T) {
	ct := ConvertedType_JSON
	bytes, err := ct.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "JSON", string(bytes))
}

func Test_ConvertedType_UnmarshalText(t *testing.T) {
	var ct ConvertedType
	err := ct.UnmarshalText([]byte("DECIMAL"))
	require.NoError(t, err)
	require.Equal(t, ConvertedType_DECIMAL, ct)

	err = ct.UnmarshalText([]byte("INVALID"))
	require.Error(t, err)
}

func Test_ConvertedType_Scan(t *testing.T) {
	var ct ConvertedType
	err := ct.Scan(int64(ConvertedType_ENUM))
	require.NoError(t, err)
	require.Equal(t, ConvertedType_ENUM, ct)

	err = ct.Scan("invalid")
	require.Error(t, err)
}

func Test_ConvertedType_Value(t *testing.T) {
	ct := ConvertedType_LIST
	val, err := ct.Value()
	require.NoError(t, err)
	require.Equal(t, int64(ConvertedType_LIST), val)
}

func Test_FieldRepetitionType_String(t *testing.T) {
	tests := []struct {
		typ      FieldRepetitionType
		expected string
	}{
		{FieldRepetitionType_REQUIRED, "REQUIRED"},
		{FieldRepetitionType_OPTIONAL, "OPTIONAL"},
		{FieldRepetitionType_REPEATED, "REPEATED"},
		{FieldRepetitionType(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.typ.String()
			require.Equal(t, test.expected, got)
		})
	}
}

func Test_FieldRepetitionType_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected FieldRepetitionType
		hasError bool
	}{
		{"REQUIRED", FieldRepetitionType_REQUIRED, false},
		{"OPTIONAL", FieldRepetitionType_OPTIONAL, false},
		{"REPEATED", FieldRepetitionType_REPEATED, false},
		{"INVALID", FieldRepetitionType(0), true},
	}

	for _, test := range tests {
		testName := test.input
		if testName == "" {
			testName = "empty_string"
		}
		t.Run(testName, func(t *testing.T) {
			got, err := FieldRepetitionTypeFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_FieldRepetitionType_Ptr(t *testing.T) {
	frt := FieldRepetitionType_REQUIRED
	ptr := FieldRepetitionTypePtr(frt)
	require.Equal(t, frt, *ptr)
}

func Test_FieldRepetitionType_MarshalText(t *testing.T) {
	frt := FieldRepetitionType_OPTIONAL
	bytes, err := frt.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "OPTIONAL", string(bytes))
}

func Test_FieldRepetitionType_UnmarshalText(t *testing.T) {
	var frt FieldRepetitionType
	err := frt.UnmarshalText([]byte("REPEATED"))
	require.NoError(t, err)
	require.Equal(t, FieldRepetitionType_REPEATED, frt)

	err = frt.UnmarshalText([]byte("INVALID"))
	require.Error(t, err)
}

func Test_FieldRepetitionType_Scan(t *testing.T) {
	var frt FieldRepetitionType
	err := frt.Scan(int64(FieldRepetitionType_REQUIRED))
	require.NoError(t, err)
	require.Equal(t, FieldRepetitionType_REQUIRED, frt)

	err = frt.Scan("invalid")
	require.Error(t, err)
}

func Test_FieldRepetitionType_Value(t *testing.T) {
	frt := FieldRepetitionType_OPTIONAL
	val, err := frt.Value()
	require.NoError(t, err)
	require.Equal(t, int64(FieldRepetitionType_OPTIONAL), val)
}

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

func Test_NilPointerValueMethods(t *testing.T) {
	tests := []struct {
		name  string
		getFn func() (interface{}, error)
	}{
		{"ConvertedType nil", func() (interface{}, error) { var ct *ConvertedType; return ct.Value() }},
		{"FieldRepetitionType nil", func() (interface{}, error) { var frt *FieldRepetitionType; return frt.Value() }},
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

func Test_CompressionCodecScanNil(t *testing.T) {
	var cc CompressionCodec
	err := cc.Scan(nil)
	require.Error(t, err)
}

func Test_DriverValueScanInterface(t *testing.T) {
	var _ driver.Valuer = (*Type)(nil)
	var _ driver.Valuer = (*ConvertedType)(nil)
	var _ driver.Valuer = (*FieldRepetitionType)(nil)
	var _ driver.Valuer = (*Encoding)(nil)
	var _ driver.Valuer = (*CompressionCodec)(nil)
	var _ driver.Valuer = (*PageType)(nil)
}

func Test_EdgeCases(t *testing.T) {
	var validType Type
	err := validType.UnmarshalText([]byte(""))
	require.Error(t, err)

	var nilType *Type
	val, err := nilType.Value()
	require.NoError(t, err)
	require.Nil(t, val)
}

func Test_UnmarshalTextErrors(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		getFn func() interface{ UnmarshalText([]byte) error }
	}{
		{"invalid encoding", []byte("NOT_A_VALID_ENCODING"), func() interface{ UnmarshalText([]byte) error } { var e Encoding; return &e }},
		{"invalid page type", []byte("INVALID_PAGE_TYPE"), func() interface{ UnmarshalText([]byte) error } { var p PageType; return &p }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := tt.getFn()
			err := target.UnmarshalText(tt.input)
			require.Error(t, err)
		})
	}
}

func Test_ScanEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		expectError bool
		expected    Type
	}{
		{"string input", "not_an_int", true, Type(0)},
		{"float input", 3.14, true, Type(0)},
		{"nil input", nil, true, Type(0)},
		{"valid int64", int64(Type_BOOLEAN), false, Type_BOOLEAN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var typ Type
			err := typ.Scan(tt.input)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, typ)
			}
		})
	}
}

func Test_AllScanErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		getFn func() interface{ Scan(interface{}) error }
		input interface{}
	}{
		{"ConvertedType string", func() interface{ Scan(interface{}) error } { var c ConvertedType; return &c }, "invalid"},
		{"ConvertedType float", func() interface{ Scan(interface{}) error } { var c ConvertedType; return &c }, 3.14},
		{"FieldRepetitionType string", func() interface{ Scan(interface{}) error } { var f FieldRepetitionType; return &f }, "invalid"},
		{"FieldRepetitionType bytes", func() interface{ Scan(interface{}) error } { var f FieldRepetitionType; return &f }, []byte("bytes")},
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

func Test_AllValueEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		fn   func() (interface{}, error)
	}{
		{"Type", func() (interface{}, error) { var t *Type; return t.Value() }},
		{"ConvertedType", func() (interface{}, error) { var ct *ConvertedType; return ct.Value() }},
		{"FieldRepetitionType", func() (interface{}, error) { var frt *FieldRepetitionType; return frt.Value() }},
		{"Encoding", func() (interface{}, error) { var enc *Encoding; return enc.Value() }},
		{"CompressionCodec", func() (interface{}, error) { var cc *CompressionCodec; return cc.Value() }},
		{"PageType", func() (interface{}, error) { var pt *PageType; return pt.Value() }},
		{"BoundaryOrder", func() (interface{}, error) { var bo *BoundaryOrder; return bo.Value() }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val, err := test.fn()
			require.NoError(t, err)
			require.Nil(t, val)
		})
	}
}

func Test_AllUnmarshalTextErrors(t *testing.T) {
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
		t.Run(string(input), func(t *testing.T) {
			var typ Type
			err := typ.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run(string(input), func(t *testing.T) {
			var ct ConvertedType
			err := ct.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run(string(input), func(t *testing.T) {
			var frt FieldRepetitionType
			err := frt.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run(string(input), func(t *testing.T) {
			var enc Encoding
			err := enc.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run(string(input), func(t *testing.T) {
			var cc CompressionCodec
			err := cc.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run(string(input), func(t *testing.T) {
			var pt PageType
			err := pt.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run(string(input), func(t *testing.T) {
			var bo BoundaryOrder
			err := bo.UnmarshalText(input)
			require.Error(t, err)
		})
	}
}

func Test_ComprehensiveErrorScenarios(t *testing.T) {
	var cc CompressionCodec

	errorInputs := []interface{}{
		nil,
		"string",
		3.14,
		true,
		[]byte("bytes"),
		map[string]int{"key": 1},
		struct{}{},
		make(chan int),
	}

	for _, input := range errorInputs {
		err := cc.Scan(input)
		require.Error(t, err)
	}

	err := cc.Scan(int64(CompressionCodec_GZIP))
	require.NoError(t, err)
	require.Equal(t, CompressionCodec_GZIP, cc)
}
