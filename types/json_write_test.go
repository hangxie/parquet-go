package types

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestJSONTypeToParquetTypeWithLogical(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		cT          *parquet.ConvertedType
		lT          *parquet.LogicalType
		length      int
		scale       int
		expected    any
		expectError bool
	}{
		// Nil interface value returns nil, nil
		{
			name:        "nil_interface_value",
			value:       (*interface{})(nil),
			pT:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expectError: true,
		},
		// Decimal with LogicalType.DECIMAL - scale from lT overrides scale param
		{
			name:     "decimal_logical_float64_scale_override",
			value:    float64(123.45),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createDecimalLogicalType(9, 2),
			scale:    0, // lT scale=2 should override this
			expected: int32(12345),
		},
		// Decimal with LogicalType.DECIMAL and integer input (int64 path)
		{
			name:     "decimal_logical_int64_input",
			value:    int64(123),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createDecimalLogicalType(18, 3),
			expected: int64(123000),
		},
		// Decimal with LogicalType.DECIMAL and string input (json.Number-like strings)
		{
			name:     "decimal_logical_string_input",
			value:    "456.78",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createDecimalLogicalType(9, 2),
			expected: int32(45678),
		},
		// Non-decimal falls through to jsonValueToParquetDirect
		{
			name:     "non_decimal_bool_boolean_type",
			value:    true,
			pT:       parquet.TypePtr(parquet.Type_BOOLEAN),
			expected: true,
		},
		// Non-decimal float64 → DOUBLE direct conversion
		{
			name:     "non_decimal_float64_double_type",
			value:    float64(3.14),
			pT:       parquet.TypePtr(parquet.Type_DOUBLE),
			expected: float64(3.14),
		},
		// Non-decimal string → BYTE_ARRAY (UTF8) direct conversion
		{
			name:     "non_decimal_string_utf8",
			value:    "hello world",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			expected: "hello world",
		},
		// DATE converted type forces fallback to string-based conversion
		{
			name:     "date_ct_string_fallback",
			value:    "2024-01-15",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
			expected: int32(19737),
		},
		// TIME_MILLIS converted type forces fallback to string-based conversion
		{
			name:     "time_millis_ct_string_fallback",
			value:    "10:30:00.123",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			expected: int32(37800123),
		},
		// TIMESTAMP_MILLIS converted type forces fallback to string-based conversion
		{
			name:     "timestamp_millis_ct_string_fallback",
			value:    "2024-01-15T10:30:00Z",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
			expected: int64(1705314600000),
		},
		// Decimal with ConvertedType (no LogicalType) - float64 path
		{
			name:     "decimal_converted_float64",
			value:    float64(99.99),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(9999),
		},
		// Decimal with ConvertedType and uint input
		{
			name:     "decimal_converted_uint32",
			value:    uint32(500),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(5000),
		},
		// Decimal with int64 via ConvertedType
		{
			name:     "decimal_converted_int64",
			value:    int64(42),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int64(4200),
		},
		// Fallback to string-based conversion for complex types (map)
		{
			// A value with no byte-backed form reaches the string path as its %v
			// rendering, which a BYTE_ARRAY column now reads as base64 and rejects
			// rather than storing the text of \"map[key:val]\".
			name:        "map_fallback_to_string_based",
			value:       map[string]any{"key": "val"},
			pT:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expectError: true,
		},
		// FLOAT16 logical type with string input must use ParseFloat16String, not the raw
		// byte-array direct path (a naive direct conversion would keep the first two bytes
		// of the string itself instead of the parsed IEEE 754 half-precision value).
		{
			name:     "float16_logical_string_input",
			value:    "9.5",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			length:   2,
			expected: Float32ToFloat16(9.5),
		},
		{
			name:     "float16_logical_nan_string_input",
			value:    "NaN",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			length:   2,
			expected: Float32ToFloat16(float32(math.NaN())),
		},
		// UUID logical type with string input must use uuid.Parse, not the raw
		// byte-array direct path (which would keep the dashed string as-is).
		{
			name:   "uuid_logical_string_input",
			value:  "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 16,
			expected: string([]byte{
				0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
				0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
			}),
		},
		// Text logical types with no converted type: a value that happens to be valid
		// base64 must still be stored verbatim rather than decoded as binary.
		{
			name:     "string_logical_base64_looking",
			value:    "TEST",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			lT:       &parquet.LogicalType{STRING: parquet.NewStringType()},
			expected: "TEST",
		},
		{
			name:     "enum_logical_base64_looking",
			value:    "TEST",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			lT:       &parquet.LogicalType{ENUM: parquet.NewEnumType()},
			expected: "TEST",
		},
		{
			name:     "json_logical_base64_looking",
			value:    "null",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			lT:       &parquet.LogicalType{JSON: parquet.NewJsonType()},
			expected: "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var val reflect.Value
			if tt.value == nil {
				val = reflect.ValueOf((*interface{})(nil))
			} else {
				val = reflect.ValueOf(tt.value)
			}

			result, err := JSONTypeToParquetTypeWithLogical(val, tt.pT, tt.cT, tt.lT, tt.length, tt.scale)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expected != nil {
					require.Equal(t, tt.expected, result)
				}
			}
		})
	}
}

func TestJSONTypeToParquetType(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		cT          *parquet.ConvertedType
		length      int
		scale       int
		expected    any
		expectError bool
	}{
		{
			name:   "json_boolean_true",
			value:  true,
			pT:     parquet.TypePtr(parquet.Type_BOOLEAN),
			length: 0,
			scale:  0,
		},
		{
			name:   "json_number_int32",
			value:  float64(42), // JSON numbers are float64
			pT:     parquet.TypePtr(parquet.Type_INT32),
			length: 0,
			scale:  0,
		},
		{
			name:   "json_number_int64",
			value:  float64(42),
			pT:     parquet.TypePtr(parquet.Type_INT64),
			length: 0,
			scale:  0,
		},
		{
			name:   "json_number_float",
			value:  float64(3.14),
			pT:     parquet.TypePtr(parquet.Type_FLOAT),
			length: 0,
			scale:  0,
		},
		{
			name:   "json_number_double",
			value:  float64(3.14),
			pT:     parquet.TypePtr(parquet.Type_DOUBLE),
			length: 0,
			scale:  0,
		},
		{
			name:     "json_string",
			value:    "aGVsbG8=",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			length:   0,
			scale:    0,
			expected: "hello",
		},
		{
			name:        "invalid_json_type",
			value:       make(map[string]any), // Unsupported type
			pT:          parquet.TypePtr(parquet.Type_BOOLEAN),
			expectError: true,
		},
		// Comprehensive decimal tests with all numeric types
		{
			name:        "nil_interface_value",
			value:       (*interface{})(nil),
			pT:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expectError: true,
		},
		{
			name:     "decimal_float32",
			value:    float32(123.45),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(12345),
		},
		{
			name:     "decimal_float64",
			value:    123.456,
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    3,
			expected: int64(123456),
		},
		{
			name:     "decimal_int",
			value:    int(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(1234500),
		},
		{
			name:     "decimal_int8",
			value:    int8(123),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(1230),
		},
		{
			name:     "decimal_int16",
			value:    int16(1234),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(12340),
		},
		{
			name:     "decimal_int32",
			value:    int32(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(1234500),
		},
		{
			name:     "decimal_int64",
			value:    int64(123456789),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    3,
			expected: int64(123456789000),
		},
		{
			name:     "decimal_uint",
			value:    uint(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(1234500),
		},
		{
			name:     "decimal_uint8",
			value:    uint8(123),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(1230),
		},
		{
			name:     "decimal_uint16",
			value:    uint16(1234),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(12340),
		},
		{
			name:     "decimal_uint32",
			value:    uint32(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(1234500),
		},
		{
			name:     "decimal_uint64",
			value:    uint64(123456789),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    3,
			expected: int64(123456789000),
		},
		{
			name:     "decimal_string",
			value:    "123.45",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(12345),
		},
		{
			name:     "non_decimal_string",
			value:    "aGVsbG8gd29ybGQ=",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: "hello world",
		},
		{
			name:     "non_decimal_boolean",
			value:    true,
			pT:       parquet.TypePtr(parquet.Type_BOOLEAN),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := reflect.ValueOf(tt.value)
			if tt.value == nil {
				val = reflect.ValueOf((*interface{})(nil))
			}

			result, err := JSONTypeToParquetType(val, tt.pT, tt.cT, tt.length, tt.scale)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expected != nil {
					require.Equal(t, tt.expected, result)
				}
			}
		})
	}
}

func TestJSONValueToParquetDirect(t *testing.T) {
	tests := []struct {
		name       string
		value      any
		pT         *parquet.Type
		cT         *parquet.ConvertedType
		expected   any
		usesDirect bool // whether direct conversion should be used
	}{
		// Boolean
		{
			name:       "bool_true",
			value:      true,
			pT:         parquet.TypePtr(parquet.Type_BOOLEAN),
			expected:   true,
			usesDirect: true,
		},
		{
			name:       "bool_false",
			value:      false,
			pT:         parquet.TypePtr(parquet.Type_BOOLEAN),
			expected:   false,
			usesDirect: true,
		},
		// INT32
		{
			name:       "int32_from_float64",
			value:      float64(42),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			expected:   int32(42),
			usesDirect: true,
		},
		{
			name:       "int32_from_int",
			value:      int(100),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			expected:   int32(100),
			usesDirect: true,
		},
		// INT64
		{
			name:       "int64_from_float64",
			value:      float64(9999999999),
			pT:         parquet.TypePtr(parquet.Type_INT64),
			expected:   int64(9999999999),
			usesDirect: true,
		},
		// FLOAT
		{
			name:       "float_from_float64",
			value:      float64(3.14),
			pT:         parquet.TypePtr(parquet.Type_FLOAT),
			expected:   float32(3.14),
			usesDirect: true,
		},
		// DOUBLE
		{
			name:       "double_from_float64",
			value:      float64(3.14159265359),
			pT:         parquet.TypePtr(parquet.Type_DOUBLE),
			expected:   float64(3.14159265359),
			usesDirect: true,
		},
		// BYTE_ARRAY (string)
		{
			// Byte-backed columns are no longer handled directly: their JSON form is
			// base64, which the string path decodes.
			name:       "string_direct",
			value:      "aGVsbG8gd29ybGQ=",
			pT:         parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected:   "hello world",
			usesDirect: false,
		},
		// UTF8 converted type
		{
			name:       "utf8_string",
			value:      "unicode: 你好",
			pT:         parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			expected:   "unicode: 你好",
			usesDirect: true,
		},
		// INT_8 converted type
		{
			name:       "int8_converted",
			value:      float64(127),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			expected:   int32(127),
			usesDirect: true,
		},
		// UINT_32 converted type
		{
			name:       "uint32_converted",
			value:      float64(4294967295),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
			expected:   int32(-1), // uint32 max wraps to -1 in int32
			usesDirect: true,
		},
		// INT_16 converted type
		{
			name:       "int16_converted",
			value:      float64(32767),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
			expected:   int32(32767),
			usesDirect: true,
		},
		// INT_32 converted type
		{
			name:       "int32_converted_type",
			value:      float64(2147483647),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32),
			expected:   int32(2147483647),
			usesDirect: true,
		},
		// INT_64 converted type
		{
			name:       "int64_converted_type",
			value:      float64(999999999999),
			pT:         parquet.TypePtr(parquet.Type_INT64),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
			expected:   int64(999999999999),
			usesDirect: true,
		},
		// UINT_8 converted type
		{
			name:       "uint8_converted",
			value:      float64(255),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8),
			expected:   int32(255),
			usesDirect: true,
		},
		// UINT_16 converted type
		{
			name:       "uint16_converted",
			value:      float64(65535),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16),
			expected:   int32(65535),
			usesDirect: true,
		},
		// UINT_64 converted type
		{
			name:       "uint64_converted",
			value:      uint64(18446744073709551615),
			pT:         parquet.TypePtr(parquet.Type_INT64),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64),
			expected:   int64(-1), // uint64 max wraps to -1 in int64
			usesDirect: true,
		},
		// Test with uint input for int target
		{
			name:       "int32_from_uint",
			value:      uint(200),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			expected:   int32(200),
			usesDirect: true,
		},
		// Test float from int
		{
			name:       "float_from_int",
			value:      int(42),
			pT:         parquet.TypePtr(parquet.Type_FLOAT),
			expected:   float32(42),
			usesDirect: true,
		},
		// Test double from uint
		{
			name:       "double_from_uint",
			value:      uint64(12345),
			pT:         parquet.TypePtr(parquet.Type_DOUBLE),
			expected:   float64(12345),
			usesDirect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := reflect.ValueOf(tt.value)
			result, err := JSONTypeToParquetType(val, tt.pT, tt.cT, 0, 0)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func BenchmarkJSONTypeToParquetType_DirectConversion(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_INT32)
	val := reflect.ValueOf(float64(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkJSONTypeToParquetType_Boolean(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BOOLEAN)
	val := reflect.ValueOf(true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkJSONTypeToParquetType_String(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	val := reflect.ValueOf("aGVsbG8gd29ybGQ=") // base64, the form a byte-backed column takes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func TestJSONValueToParquetDirect_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		cT          *parquet.ConvertedType
		expected    any
		expectError bool
	}{
		// A schema element with no physical type says nothing about what the column
		// holds, so the value is refused whatever the annotation.
		{
			name:        "nil_pT_with_utf8",
			value:       "hello",
			pT:          nil,
			cT:          parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			expectError: true,
		},
		// BYTE_ARRAY with valid base64 string: jsonPhysicalTypeDirect decodes it.
		{
			name:     "byte_array_base64_direct",
			value:    "SGVsbG8=", // base64(\"Hello\")
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: "Hello",
		},
		// A BSON column takes Extended JSON, so text that is not a document is refused
		// rather than stored as the bytes it spells.
		{
			name:        "bson_ct_not_extended_json",
			value:       "somedata",
			pT:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:          parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			expectError: true,
		},
		// A BSON annotation on a column that cannot hold a document. Unreachable
		// through either writer, but a hand-built schema element skips validation.
		{
			name:        "bson_ct_on_a_non_byte_array_column",
			value:       `{"i":1}`,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			cT:          parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			expectError: true,
		},
		{
			name:     "bson_ct_relaxed_extended_json",
			value:    `{"i":1}`,
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			expected: string([]byte{0x0c, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}),
		},
		// bool value for FLOAT type: getNumericValue returns (0,false),
		// jsonPhysicalTypeDirect returns false, StrToParquetType fails to parse \"true\" as float.
		{
			name:        "bool_for_float_type",
			value:       true,
			pT:          parquet.TypePtr(parquet.Type_FLOAT),
			expectError: true,
		},
		// DATE cT: jsonValueToParquetDirect returns (nil,false), falls back to StrToParquetType.
		{
			name:     "date_ct_numeric_fallback",
			value:    int64(1000),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
			expected: int32(1000),
		},
		// INTERVAL cT: jsonValueToParquetDirect returns (nil,false) so the interval string is
		// parsed into its 12-byte little-endian form instead of being kept verbatim.
		{
			name:     "interval_ct_string_parsed",
			value:    "2 mon 3 day 4.500 sec",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
			expected: string([]byte{2, 0, 0, 0, 3, 0, 0, 0, 0x94, 0x11, 0, 0}),
		},
		{
			name:     "interval_ct_digits_fallback",
			value:    "12345",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
			expected: StrIntToBinary("12345", "LittleEndian", common.IntervalByteLen, false),
		},
		// ENUM and JSON are text on the wire: a value that happens to be valid base64 must
		// still be stored verbatim rather than decoded as binary.
		{
			name:     "enum_ct_base64_looking_string",
			value:    "TEST",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM),
			expected: "TEST",
		},
		{
			name:     "json_ct_base64_looking_string",
			value:    "null",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_JSON),
			expected: "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := reflect.ValueOf(tt.value)
			result, err := JSONTypeToParquetType(val, tt.pT, tt.cT, 0, 0)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestJSONTypeToParquetType_TimeNumber covers a TIME carried as a JSON number, which skips
// the string parser and so needs its own [0, 24h) check.
func TestJSONTypeToParquetType_TimeNumber(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		cT          *parquet.ConvertedType
		lT          *parquet.LogicalType
		expected    any
		expectError bool
		errMsg      string
	}{
		{
			name:     "millis_in_range",
			value:    float64(45296789),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "micros_in_range",
			value:    int64(86399999999),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, true, false),
			expected: int64(86399999999),
		},
		{
			name:     "nanos_in_range",
			value:    int64(45296789012345),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, false, true),
			expected: int64(45296789012345),
		},
		{
			name:        "millis_negative",
			value:       float64(-1000),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			name:        "millis_past_midnight",
			value:       float64(90000000),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			name:        "micros_full_day",
			value:       int64(86400000000),
			pT:          parquet.TypePtr(parquet.Type_INT64),
			lT:          createTimeLogicalType(false, true, false),
			expectError: true,
		},
		{
			name:        "millis_fractional",
			value:       1.9,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			// Truncating to int64 first made this a legal 0 instead of a negative TIME.
			name:        "millis_negative_fraction",
			value:       -0.5,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			name:        "millis_nan",
			value:       math.NaN(),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			name:        "millis_inf",
			value:       math.Inf(1),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			// Past what int64 can hold, where the conversion itself is undefined in Go.
			name:        "millis_astronomical",
			value:       1e30,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			name:        "millis_negative_whole_float",
			value:       -1.0,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			// Not a number at all: the clock string goes through the string parser.
			name:     "millis_clock_string",
			value:    "12:34:56.789",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "millis_integral_float",
			value:    float64(45296789),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "micros_unsigned",
			value:    uint64(45296789012),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, true, false),
			expected: int64(45296789012),
		},
		{
			// A uint64 this large is not representable as int64 at all.
			name:        "micros_unsigned_astronomical",
			value:       uint64(math.MaxUint64),
			pT:          parquet.TypePtr(parquet.Type_INT64),
			lT:          createTimeLogicalType(false, true, false),
			expectError: true,
		},
		{
			// Schema building backfills TIME_MILLIS next to the logical type, which is how
			// every real MILLIS column reaches here.
			name:     "millis_with_converted_type",
			value:    float64(45296789),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:        "millis_with_converted_type_out_of_range",
			value:       float64(90000000),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			cT:          parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			name:     "micros_converted_type_only",
			value:    float64(45296789012),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
			expected: int64(45296789012),
		},
		{
			name:        "millis_converted_type_only_fractional",
			value:       1.9,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			cT:          parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			name:     "millis_converted_type_only_clock_string",
			value:    "12:34:56.789",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			expected: int32(45296789),
		},
		{
			name:        "empty_unit_rejected",
			value:       int64(123456789),
			pT:          parquet.TypePtr(parquet.Type_INT64),
			lT:          &parquet.LogicalType{TIME: &parquet.TimeType{Unit: parquet.NewTimeUnit()}},
			expectError: true,
			errMsg:      "time unit not set",
		},
		{
			name:        "nil_unit_rejected",
			value:       int64(123456789),
			pT:          parquet.TypePtr(parquet.Type_INT64),
			lT:          &parquet.LogicalType{TIME: &parquet.TimeType{}},
			expectError: true,
			errMsg:      "time unit not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(tt.value), tt.pT, tt.cT, tt.lT, 0, 0)
			if tt.expectError {
				require.Error(t, err)
				errMsg := tt.errMsg
				if errMsg == "" {
					errMsg = "outside [0, 24h)"
				}
				require.Contains(t, err.Error(), errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestJSONTypeToParquetType_FixedLenByteArrayWidth(t *testing.T) {
	testCases := map[string]struct {
		value    string
		length   int
		expected string
		errMsg   string
	}{
		"base64-matches-width": {
			"SGVsbG8gV29ybGQ=", 11, "Hello World", "",
		},
		"decoded-width-decides": {
			"0123456789abcdef", 16, "",
			`FIXED_LEN_BYTE_ARRAY "0123456789abcdef" decodes to 12 bytes, column length is 16`,
		},
		"not-base64": {
			"abc", 5, "", `FIXED_LEN_BYTE_ARRAY "abc" is not valid base64`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			res, err := JSONTypeToParquetTypeWithLogical(
				reflect.ValueOf(tc.value),
				parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				nil,
				nil,
				tc.length,
				0,
			)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, res)
		})
	}
}

func TestJSONTypeToParquetTypeTextAllocations(t *testing.T) {
	pT := parquet.Type_BYTE_ARRAY
	cT := parquet.ConvertedType_UTF8
	value := reflect.ValueOf("text")
	for _, tc := range []struct {
		name string
		opts []ValueOption
		max  float64
	}{
		{"default", nil, 1}, {"explicit off", []ValueOption{WithEnforceUTF8(false)}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got any
			var err error
			allocations := testing.AllocsPerRun(100, func() {
				got, err = JSONTypeToParquetTypeWithLogical(value, &pT, &cT, nil, 0, 0, tc.opts...)
			})
			require.NoError(t, err)
			require.Equal(t, "text", got)
			require.LessOrEqual(t, allocations, tc.max, "disabled validation must not allocate a boxed string")
		})
	}
}
