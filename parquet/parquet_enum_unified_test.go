package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// EnumTestCase defines a test case for enum types
type EnumTestCase struct {
	name       string
	stringVal  string
	enumVal    interface{}
	shouldFail bool
}

// TestEnumStringMethods tests String() and FromString() methods for all enum types
func Test_EnumStringMethods(t *testing.T) {
	testCases := map[string]struct {
		cases      []EnumTestCase
		fromString func(string) (interface{}, error)
		toString   func(interface{}) string
	}{
		"Type": {
			cases: []EnumTestCase{
				{"BOOLEAN", "BOOLEAN", Type_BOOLEAN, false},
				{"INT32", "INT32", Type_INT32, false},
				{"INT64", "INT64", Type_INT64, false},
				{"INT96", "INT96", Type_INT96, false},
				{"FLOAT", "FLOAT", Type_FLOAT, false},
				{"DOUBLE", "DOUBLE", Type_DOUBLE, false},
				{"BYTE_ARRAY", "BYTE_ARRAY", Type_BYTE_ARRAY, false},
				{"FIXED_LEN_BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY", Type_FIXED_LEN_BYTE_ARRAY, false},
				{"invalid", "INVALID", Type(0), true},
			},
			fromString: func(s string) (interface{}, error) { return TypeFromString(s) },
			toString:   func(v interface{}) string { return v.(Type).String() },
		},
		"ConvertedType": {
			cases: []EnumTestCase{
				{"UTF8", "UTF8", ConvertedType_UTF8, false},
				{"MAP", "MAP", ConvertedType_MAP, false},
				{"LIST", "LIST", ConvertedType_LIST, false},
				{"ENUM", "ENUM", ConvertedType_ENUM, false},
				{"DECIMAL", "DECIMAL", ConvertedType_DECIMAL, false},
				{"DATE", "DATE", ConvertedType_DATE, false},
				{"TIME_MILLIS", "TIME_MILLIS", ConvertedType_TIME_MILLIS, false},
				{"TIMESTAMP_MILLIS", "TIMESTAMP_MILLIS", ConvertedType_TIMESTAMP_MILLIS, false},
				{"JSON", "JSON", ConvertedType_JSON, false},
				{"BSON", "BSON", ConvertedType_BSON, false},
				{"invalid", "INVALID", ConvertedType(0), true},
			},
			fromString: func(s string) (interface{}, error) { return ConvertedTypeFromString(s) },
			toString:   func(v interface{}) string { return v.(ConvertedType).String() },
		},
		"FieldRepetitionType": {
			cases: []EnumTestCase{
				{"REQUIRED", "REQUIRED", FieldRepetitionType_REQUIRED, false},
				{"OPTIONAL", "OPTIONAL", FieldRepetitionType_OPTIONAL, false},
				{"REPEATED", "REPEATED", FieldRepetitionType_REPEATED, false},
				{"invalid", "INVALID", FieldRepetitionType(0), true},
			},
			fromString: func(s string) (interface{}, error) { return FieldRepetitionTypeFromString(s) },
			toString:   func(v interface{}) string { return v.(FieldRepetitionType).String() },
		},
		"Encoding": {
			cases: []EnumTestCase{
				{"PLAIN", "PLAIN", Encoding_PLAIN, false},
				{"PLAIN_DICTIONARY", "PLAIN_DICTIONARY", Encoding_PLAIN_DICTIONARY, false},
				{"RLE", "RLE", Encoding_RLE, false},
				{"BIT_PACKED", "BIT_PACKED", Encoding_BIT_PACKED, false},
				{"DELTA_BINARY_PACKED", "DELTA_BINARY_PACKED", Encoding_DELTA_BINARY_PACKED, false},
				{"RLE_DICTIONARY", "RLE_DICTIONARY", Encoding_RLE_DICTIONARY, false},
				{"invalid", "INVALID", Encoding(0), true},
			},
			fromString: func(s string) (interface{}, error) { return EncodingFromString(s) },
			toString:   func(v interface{}) string { return v.(Encoding).String() },
		},
		"CompressionCodec": {
			cases: []EnumTestCase{
				{"UNCOMPRESSED", "UNCOMPRESSED", CompressionCodec_UNCOMPRESSED, false},
				{"SNAPPY", "SNAPPY", CompressionCodec_SNAPPY, false},
				{"GZIP", "GZIP", CompressionCodec_GZIP, false},
				{"BROTLI", "BROTLI", CompressionCodec_BROTLI, false},
				{"LZ4", "LZ4", CompressionCodec_LZ4, false},
				{"ZSTD", "ZSTD", CompressionCodec_ZSTD, false},
				{"invalid", "INVALID", CompressionCodec(0), true},
			},
			fromString: func(s string) (interface{}, error) { return CompressionCodecFromString(s) },
			toString:   func(v interface{}) string { return v.(CompressionCodec).String() },
		},
		"PageType": {
			cases: []EnumTestCase{
				{"DATA_PAGE", "DATA_PAGE", PageType_DATA_PAGE, false},
				{"INDEX_PAGE", "INDEX_PAGE", PageType_INDEX_PAGE, false},
				{"DICTIONARY_PAGE", "DICTIONARY_PAGE", PageType_DICTIONARY_PAGE, false},
				{"DATA_PAGE_V2", "DATA_PAGE_V2", PageType_DATA_PAGE_V2, false},
				{"invalid", "INVALID", PageType(0), true},
			},
			fromString: func(s string) (interface{}, error) { return PageTypeFromString(s) },
			toString:   func(v interface{}) string { return v.(PageType).String() },
		},
		"BoundaryOrder": {
			cases: []EnumTestCase{
				{"UNORDERED", "UNORDERED", BoundaryOrder_UNORDERED, false},
				{"ASCENDING", "ASCENDING", BoundaryOrder_ASCENDING, false},
				{"DESCENDING", "DESCENDING", BoundaryOrder_DESCENDING, false},
				{"invalid", "INVALID", BoundaryOrder(0), true},
			},
			fromString: func(s string) (interface{}, error) { return BoundaryOrderFromString(s) },
			toString:   func(v interface{}) string { return v.(BoundaryOrder).String() },
		},
	}

	for enumName, testData := range testCases {
		t.Run(enumName, func(t *testing.T) {
			for _, tc := range testData.cases {
				t.Run(tc.name+"_FromString", func(t *testing.T) {
					result, err := testData.fromString(tc.stringVal)
					if tc.shouldFail {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						require.Equal(t, tc.enumVal, result)
					}
				})

				if !tc.shouldFail {
					t.Run(tc.name+"_ToString", func(t *testing.T) {
						result := testData.toString(tc.enumVal)
						require.Equal(t, tc.stringVal, result)
					})
				}
			}
		})
	}
}

// TestEnumPointerMethods tests *Ptr() methods for all enum types
func Test_EnumPointerMethods(t *testing.T) {
	testCases := []struct {
		name   string
		testFn func(t *testing.T)
	}{
		{"Type", func(t *testing.T) {
			val := Type_INT32
			ptr := TypePtr(val)
			require.NotNil(t, ptr)
			require.Equal(t, val, *ptr)
		}},
		{"ConvertedType", func(t *testing.T) {
			val := ConvertedType_UTF8
			ptr := ConvertedTypePtr(val)
			require.NotNil(t, ptr)
			require.Equal(t, val, *ptr)
		}},
		{"FieldRepetitionType", func(t *testing.T) {
			val := FieldRepetitionType_REQUIRED
			ptr := FieldRepetitionTypePtr(val)
			require.NotNil(t, ptr)
			require.Equal(t, val, *ptr)
		}},
		{"Encoding", func(t *testing.T) {
			val := Encoding_PLAIN
			ptr := EncodingPtr(val)
			require.NotNil(t, ptr)
			require.Equal(t, val, *ptr)
		}},
		{"CompressionCodec", func(t *testing.T) {
			val := CompressionCodec_SNAPPY
			ptr := CompressionCodecPtr(val)
			require.NotNil(t, ptr)
			require.Equal(t, val, *ptr)
		}},
		{"PageType", func(t *testing.T) {
			val := PageType_DATA_PAGE
			ptr := PageTypePtr(val)
			require.NotNil(t, ptr)
			require.Equal(t, val, *ptr)
		}},
		{"BoundaryOrder", func(t *testing.T) {
			val := BoundaryOrder_ASCENDING
			ptr := BoundaryOrderPtr(val)
			require.NotNil(t, ptr)
			require.Equal(t, val, *ptr)
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFn)
	}
}

// TestEnumMarshalUnmarshalText tests MarshalText/UnmarshalText methods for all enum types
func Test_EnumMarshalUnmarshalText(t *testing.T) {
	testCases := []struct {
		name        string
		value       interface{}
		expectedStr string
		testFn      func(t *testing.T, value interface{}, expectedStr string)
	}{
		{"Type", Type_INT32, "INT32", func(t *testing.T, value interface{}, expectedStr string) {
			v := value.(Type)
			data, err := v.MarshalText()
			require.NoError(t, err)
			require.Equal(t, expectedStr, string(data))

			var unmarshaled Type
			err = unmarshaled.UnmarshalText(data)
			require.NoError(t, err)
			require.Equal(t, v, unmarshaled)
		}},
		{"ConvertedType", ConvertedType_UTF8, "UTF8", func(t *testing.T, value interface{}, expectedStr string) {
			v := value.(ConvertedType)
			data, err := v.MarshalText()
			require.NoError(t, err)
			require.Equal(t, expectedStr, string(data))

			var unmarshaled ConvertedType
			err = unmarshaled.UnmarshalText(data)
			require.NoError(t, err)
			require.Equal(t, v, unmarshaled)
		}},
		{"FieldRepetitionType", FieldRepetitionType_REQUIRED, "REQUIRED", func(t *testing.T, value interface{}, expectedStr string) {
			v := value.(FieldRepetitionType)
			data, err := v.MarshalText()
			require.NoError(t, err)
			require.Equal(t, expectedStr, string(data))

			var unmarshaled FieldRepetitionType
			err = unmarshaled.UnmarshalText(data)
			require.NoError(t, err)
			require.Equal(t, v, unmarshaled)
		}},
		{"Encoding", Encoding_PLAIN, "PLAIN", func(t *testing.T, value interface{}, expectedStr string) {
			v := value.(Encoding)
			data, err := v.MarshalText()
			require.NoError(t, err)
			require.Equal(t, expectedStr, string(data))

			var unmarshaled Encoding
			err = unmarshaled.UnmarshalText(data)
			require.NoError(t, err)
			require.Equal(t, v, unmarshaled)
		}},
		{"CompressionCodec", CompressionCodec_SNAPPY, "SNAPPY", func(t *testing.T, value interface{}, expectedStr string) {
			v := value.(CompressionCodec)
			data, err := v.MarshalText()
			require.NoError(t, err)
			require.Equal(t, expectedStr, string(data))

			var unmarshaled CompressionCodec
			err = unmarshaled.UnmarshalText(data)
			require.NoError(t, err)
			require.Equal(t, v, unmarshaled)
		}},
		{"PageType", PageType_DATA_PAGE, "DATA_PAGE", func(t *testing.T, value interface{}, expectedStr string) {
			v := value.(PageType)
			data, err := v.MarshalText()
			require.NoError(t, err)
			require.Equal(t, expectedStr, string(data))

			var unmarshaled PageType
			err = unmarshaled.UnmarshalText(data)
			require.NoError(t, err)
			require.Equal(t, v, unmarshaled)
		}},
		{"BoundaryOrder", BoundaryOrder_ASCENDING, "ASCENDING", func(t *testing.T, value interface{}, expectedStr string) {
			v := value.(BoundaryOrder)
			data, err := v.MarshalText()
			require.NoError(t, err)
			require.Equal(t, expectedStr, string(data))

			var unmarshaled BoundaryOrder
			err = unmarshaled.UnmarshalText(data)
			require.NoError(t, err)
			require.Equal(t, v, unmarshaled)
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.testFn(t, tc.value, tc.expectedStr)
		})
	}
}

// TestEnumScanValue tests Scan/Value methods for all enum types
func Test_EnumScanValue(t *testing.T) {
	testCases := []struct {
		name   string
		testFn func(t *testing.T)
	}{
		{"Type", func(t *testing.T) {
			var typeVal Type
			err := typeVal.Scan(int64(Type_INT32))
			require.NoError(t, err)
			require.Equal(t, Type_INT32, typeVal)

			val, err := typeVal.Value()
			require.NoError(t, err)
			require.Equal(t, int64(Type_INT32), val)
		}},
		{"ConvertedType", func(t *testing.T) {
			var convertedType ConvertedType
			err := convertedType.Scan(int64(ConvertedType_UTF8))
			require.NoError(t, err)
			require.Equal(t, ConvertedType_UTF8, convertedType)

			val, err := convertedType.Value()
			require.NoError(t, err)
			require.Equal(t, int64(ConvertedType_UTF8), val)
		}},
		{"FieldRepetitionType", func(t *testing.T) {
			var repetitionType FieldRepetitionType
			err := repetitionType.Scan(int64(FieldRepetitionType_REQUIRED))
			require.NoError(t, err)
			require.Equal(t, FieldRepetitionType_REQUIRED, repetitionType)

			val, err := repetitionType.Value()
			require.NoError(t, err)
			require.Equal(t, int64(FieldRepetitionType_REQUIRED), val)
		}},
		{"Encoding", func(t *testing.T) {
			var encoding Encoding
			err := encoding.Scan(int64(Encoding_PLAIN))
			require.NoError(t, err)
			require.Equal(t, Encoding_PLAIN, encoding)

			val, err := encoding.Value()
			require.NoError(t, err)
			require.Equal(t, int64(Encoding_PLAIN), val)
		}},
		{"CompressionCodec", func(t *testing.T) {
			var codec CompressionCodec
			err := codec.Scan(int64(CompressionCodec_SNAPPY))
			require.NoError(t, err)
			require.Equal(t, CompressionCodec_SNAPPY, codec)

			val, err := codec.Value()
			require.NoError(t, err)
			require.Equal(t, int64(CompressionCodec_SNAPPY), val)
		}},
		{"PageType", func(t *testing.T) {
			var pageType PageType
			err := pageType.Scan(int64(PageType_DATA_PAGE))
			require.NoError(t, err)
			require.Equal(t, PageType_DATA_PAGE, pageType)

			val, err := pageType.Value()
			require.NoError(t, err)
			require.Equal(t, int64(PageType_DATA_PAGE), val)
		}},
		{"BoundaryOrder", func(t *testing.T) {
			var boundaryOrder BoundaryOrder
			err := boundaryOrder.Scan(int64(BoundaryOrder_ASCENDING))
			require.NoError(t, err)
			require.Equal(t, BoundaryOrder_ASCENDING, boundaryOrder)

			val, err := boundaryOrder.Value()
			require.NoError(t, err)
			require.Equal(t, int64(BoundaryOrder_ASCENDING), val)
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFn)
	}
}

func Test_TypeFromString_EdgeCases(t *testing.T) {
	// Test edge cases not covered in unified tests
	tests := []struct {
		name     string
		input    string
		hasError bool
	}{
		{"empty_string", "", true},
		{"lowercase", "int32", true},
		{"partial_match", "INT", true},
		{"with_spaces", " INT32 ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := TypeFromString(tt.input)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_ConvertedTypeFromString_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		hasError bool
	}{
		{"empty_string", "", true},
		{"lowercase", "utf8", true},
		{"partial_match", "UT", true},
		{"with_spaces", " UTF8 ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ConvertedTypeFromString(tt.input)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_EncodingFromString_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		hasError bool
	}{
		{"empty_string", "", true},
		{"lowercase", "plain", true},
		{"partial_match", "PLA", true},
		{"with_spaces", " PLAIN ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := EncodingFromString(tt.input)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_Scan_InvalidTypes(t *testing.T) {
	t.Run("Type_Scan_InvalidType", func(t *testing.T) {
		var typeVal Type
		err := typeVal.Scan("not_int64")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Scan value is not int64")
	})

	t.Run("ConvertedType_Scan_InvalidType", func(t *testing.T) {
		var convertedType ConvertedType
		err := convertedType.Scan("not_int64")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Scan value is not int64")
	})

	t.Run("Encoding_Scan_InvalidType", func(t *testing.T) {
		var encoding Encoding
		err := encoding.Scan("not_int64")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Scan value is not int64")
	})
}

func Test_UnmarshalText_InvalidData(t *testing.T) {
	t.Run("Type_UnmarshalText_Invalid", func(t *testing.T) {
		var typeVal Type
		err := typeVal.UnmarshalText([]byte("INVALID_TYPE"))
		require.Error(t, err)
	})

	t.Run("ConvertedType_UnmarshalText_Invalid", func(t *testing.T) {
		var convertedType ConvertedType
		err := convertedType.UnmarshalText([]byte("INVALID_CONVERTED_TYPE"))
		require.Error(t, err)
	})

	t.Run("Encoding_UnmarshalText_Invalid", func(t *testing.T) {
		var encoding Encoding
		err := encoding.UnmarshalText([]byte("INVALID_ENCODING"))
		require.Error(t, err)
	})
}
