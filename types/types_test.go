package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestInterfaceToParquetType(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		pT       *parquet.Type
		errMsg   string
		expected any
	}{
		// Direct type matches (should return as-is)
		{
			name:     "boolean_true_direct",
			value:    true,
			pT:       parquet.TypePtr(parquet.Type_BOOLEAN),
			expected: true,
		},
		{
			name:     "int32_direct",
			value:    int32(42),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: int32(42),
		},
		{
			name:     "int64_direct",
			value:    int64(42),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			expected: int64(42),
		},
		{
			name:   "float32_to_int64",
			value:  float32(42),
			pT:     parquet.TypePtr(parquet.Type_INT64),
			errMsg: "convert float32 to int64",
		},
		{
			name:     "float32_direct",
			value:    float32(3.14),
			pT:       parquet.TypePtr(parquet.Type_FLOAT),
			expected: float32(3.14),
		},
		{
			name:     "float64_direct",
			value:    float64(3.14),
			pT:       parquet.TypePtr(parquet.Type_DOUBLE),
			expected: float64(3.14),
		},
		{
			name:     "string_byte_array_direct",
			value:    "hello",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: "hello",
		},
		{
			name:     "string_int96_direct",
			value:    "hello",
			pT:       parquet.TypePtr(parquet.Type_INT96),
			expected: "hello",
		},
		{
			name:     "string_fixed_len_direct",
			value:    "hello",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			expected: "hello",
		},

		// Type conversions (via reflection)
		{
			name:     "int_to_int32",
			value:    int(42),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: int32(42),
		},
		{
			name:     "int8_to_int32",
			value:    int8(42),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: int32(42),
		},
		{
			name:     "int16_to_int32",
			value:    int16(42),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: int32(42),
		},
		{
			name:     "float64_to_float32",
			value:    float64(3.14),
			pT:       parquet.TypePtr(parquet.Type_FLOAT),
			expected: float32(3.14),
		},
		{
			name:     "float32_to_float64",
			value:    float32(3.14),
			pT:       parquet.TypePtr(parquet.Type_DOUBLE),
			expected: float64(float32(3.14)), // Note: precision conversion
		},
		{
			name:   "string_to_float64",
			value:  string("foobar"),
			pT:     parquet.TypePtr(parquet.Type_DOUBLE),
			errMsg: "convert string to float64",
		},
		{
			name:     "reflect_bool_to_bool",
			value:    reflect.ValueOf(true).Interface(),
			pT:       parquet.TypePtr(parquet.Type_BOOLEAN),
			expected: true,
		},

		// Error cases
		{
			name:   "string_to_bool_error",
			value:  "not a bool",
			pT:     parquet.TypePtr(parquet.Type_BOOLEAN),
			errMsg: "convert string to bool",
		},
		{
			name:   "string_to_int32_error",
			value:  "not an int",
			pT:     parquet.TypePtr(parquet.Type_INT32),
			errMsg: "convert string to int32",
		},
		{
			name:   "bool_to_float_error",
			value:  true,
			pT:     parquet.TypePtr(parquet.Type_FLOAT),
			errMsg: "convert bool to float32",
		},
		{
			name:   "int_to_string_error",
			value:  42,
			pT:     parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			errMsg: "convert int to string",
		},

		// Edge cases
		{
			name:     "nil_value",
			value:    nil,
			pT:       parquet.TypePtr(parquet.Type_BOOLEAN),
			expected: nil,
		},
		{
			name:     "nil_type",
			value:    "hello",
			pT:       nil,
			expected: "hello",
		},
		{
			name:     "unknown_type",
			value:    42,
			pT:       parquet.TypePtr(parquet.Type(-1)), // Invalid type
			expected: 42,                                // Should return as-is
		},
		// Additional edge cases from comprehensive test
		{
			name:   "reflect_value_bool_to_bool",
			value:  reflect.ValueOf(true),
			pT:     parquet.TypePtr(parquet.Type_BOOLEAN),
			errMsg: "convert reflect.Value to bool",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := InterfaceToParquetType(tt.value, tt.pT)

			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				if tt.value == nil {
					require.Nil(t, result)
				} else {
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
			name:   "json_string",
			value:  "hello",
			pT:     parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			length: 0,
			scale:  0,
		},
		{
			name:        "invalid_json_type",
			value:       make(map[string]any), // Unsupported type
			pT:          parquet.TypePtr(parquet.Type_BOOLEAN),
			expectError: false, // Actually doesn't error, just returns the value
		},
		// Comprehensive decimal tests with all numeric types
		{
			name:     "nil_interface_value",
			value:    (*interface{})(nil),
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: "<nil>",
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
			value:    "hello world",
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

func TestParquetTypeToGoReflectType(t *testing.T) {
	tests := []struct {
		name         string
		pT           *parquet.Type
		rT           *parquet.FieldRepetitionType
		expectedType string
	}{
		{
			name:         "boolean_required",
			pT:           parquet.TypePtr(parquet.Type_BOOLEAN),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "bool",
		},
		{
			name:         "int32_required",
			pT:           parquet.TypePtr(parquet.Type_INT32),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "int32",
		},
		{
			name:         "int64_required",
			pT:           parquet.TypePtr(parquet.Type_INT64),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "int64",
		},
		{
			name:         "int96_required",
			pT:           parquet.TypePtr(parquet.Type_INT96),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "string",
		},
		{
			name:         "float_required",
			pT:           parquet.TypePtr(parquet.Type_FLOAT),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "float32",
		},
		{
			name:         "double_required",
			pT:           parquet.TypePtr(parquet.Type_DOUBLE),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "float64",
		},
		{
			name:         "byte_array_required",
			pT:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "string",
		},
		{
			name:         "fixed_len_byte_array_required",
			pT:           parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "string",
		},
		{
			name:         "boolean_optional",
			pT:           parquet.TypePtr(parquet.Type_BOOLEAN),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
			expectedType: "*bool",
		},
		{
			name:         "int32_optional",
			pT:           parquet.TypePtr(parquet.Type_INT32),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
			expectedType: "*int32",
		},
		{
			name:         "int64_optional",
			pT:           parquet.TypePtr(parquet.Type_INT64),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
			expectedType: "*int64",
		},
		{
			name:         "int96_optional",
			pT:           parquet.TypePtr(parquet.Type_INT96),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
			expectedType: "*string",
		},
		{
			name:         "float_optional",
			pT:           parquet.TypePtr(parquet.Type_FLOAT),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
			expectedType: "*float32",
		},
		{
			name:         "double_optional",
			pT:           parquet.TypePtr(parquet.Type_DOUBLE),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
			expectedType: "*float64",
		},
		{
			name:         "byte_array_optional",
			pT:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
			expectedType: "*string",
		},
		{
			name:         "fixed_len_byte_array_optional",
			pT:           parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
			expectedType: "*string",
		},
		{
			name:         "nil_repetition_type",
			pT:           parquet.TypePtr(parquet.Type_BOOLEAN),
			rT:           nil,
			expectedType: "bool",
		},
		{
			name:         "unknown_type",
			pT:           parquet.TypePtr(parquet.Type(-1)), // Invalid type
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "<nil>",
		},
		{
			name:         "nil_type",
			pT:           nil,
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
			expectedType: "<nil>",
		},
		{
			name:         "unknown_type_optional",
			pT:           parquet.TypePtr(parquet.Type(-1)), // Invalid type
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL),
			expectedType: "<nil>",
		},
		{
			name:         "repeated_type",
			pT:           parquet.TypePtr(parquet.Type_BOOLEAN),
			rT:           parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REPEATED),
			expectedType: "bool", // Non-optional behavior
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParquetTypeToGoReflectType(tt.pT, tt.rT)
			var resultType string
			if result == nil {
				resultType = "<nil>"
			} else {
				resultType = result.String()
			}

			require.Equal(t, tt.expectedType, resultType)
		})
	}
}

func TestStrIntToBinary(t *testing.T) {
	testCases := []struct {
		name        string
		expectedNum int32
		inputNumStr string
		byteOrder   string
		length      int
		isSigned    bool
	}{
		// Little Endian tests
		{
			name:        "little-endian-zero",
			expectedNum: 0,
			inputNumStr: "0",
			byteOrder:   "LittleEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "little-endian-positive",
			expectedNum: 10,
			inputNumStr: "10",
			byteOrder:   "LittleEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "little-endian-negative",
			expectedNum: -10,
			inputNumStr: "-10",
			byteOrder:   "LittleEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "little-endian-max-int32",
			expectedNum: 2147483647,
			inputNumStr: "2147483647",
			byteOrder:   "LittleEndian",
			length:      0,
			isSigned:    true,
		},
		{
			name:        "little-endian-min-int32",
			expectedNum: -2147483648,
			inputNumStr: "-2147483648",
			byteOrder:   "LittleEndian",
			length:      0,
			isSigned:    true,
		},
		{
			name:        "little-endian-unsigned-overflow",
			expectedNum: -2147483648,
			inputNumStr: "2147483648",
			byteOrder:   "LittleEndian",
			length:      0,
			isSigned:    false,
		},

		// Big Endian tests
		{
			name:        "big-endian-zero",
			expectedNum: 0,
			inputNumStr: "0",
			byteOrder:   "BigEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "big-endian-positive",
			expectedNum: 10,
			inputNumStr: "10",
			byteOrder:   "BigEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "big-endian-negative",
			expectedNum: -10,
			inputNumStr: "-10",
			byteOrder:   "BigEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "big-endian-max-int32",
			expectedNum: 2147483647,
			inputNumStr: "2147483647",
			byteOrder:   "BigEndian",
			length:      0,
			isSigned:    true,
		},
		{
			name:        "big-endian-min-int32",
			expectedNum: -2147483648,
			inputNumStr: "-2147483648",
			byteOrder:   "BigEndian",
			length:      0,
			isSigned:    true,
		},
		{
			name:        "big-endian-unsigned-overflow",
			expectedNum: -2147483648,
			inputNumStr: "2147483648",
			byteOrder:   "BigEndian",
			length:      0,
			isSigned:    false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Generate expected binary representation
			buf := new(bytes.Buffer)
			if testCase.byteOrder == "LittleEndian" {
				_ = binary.Write(buf, binary.LittleEndian, testCase.expectedNum)
			} else {
				_ = binary.Write(buf, binary.BigEndian, testCase.expectedNum)
			}
			expectedBinary := buf.String()

			// Call function under test
			actualBinary := StrIntToBinary(
				testCase.inputNumStr,
				testCase.byteOrder,
				testCase.length,
				testCase.isSigned,
			)

			// Compare results
			require.Equal(t, expectedBinary, actualBinary)
		})
	}
}

func TestStrToParquetType(t *testing.T) {
	testCases := []struct {
		name           string
		inputStr       string
		expectedGoData any
		parquetType    *parquet.Type
		convertedType  *parquet.ConvertedType
		length         int
		scale          int
	}{
		// Basic primitive types
		{
			name:           "boolean-false",
			inputStr:       "false",
			expectedGoData: bool(false),
			parquetType:    parquet.TypePtr(parquet.Type_BOOLEAN),
		},
		{
			name:           "int32-positive",
			inputStr:       "1",
			expectedGoData: int32(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
		},
		{
			name:           "int64-zero",
			inputStr:       "0",
			expectedGoData: int64(0),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
		},
		{
			name:           "int96-little-endian",
			inputStr:       "12345",
			expectedGoData: StrIntToBinary("12345", "LittleEndian", 12, true),
			parquetType:    parquet.TypePtr(parquet.Type_INT96),
		},
		{
			name:           "float32",
			inputStr:       "0.1",
			expectedGoData: float32(0.1),
			parquetType:    parquet.TypePtr(parquet.Type_FLOAT),
		},
		{
			name:           "float64",
			inputStr:       "0.1",
			expectedGoData: float64(0.1),
			parquetType:    parquet.TypePtr(parquet.Type_DOUBLE),
		},
		{
			name:           "byte-array-string",
			inputStr:       "abc bcd",
			expectedGoData: string("abc bcd"),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
		},
		{
			name:           "byte-array-base64",
			inputStr:       "SGVsbG8gV29ybGQ=", // "Hello World" in base64
			expectedGoData: string("Hello World"),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
		},
		{
			name:           "fixed-len-byte-array-string",
			inputStr:       "abc bcd",
			expectedGoData: string("abc bcd"),
			parquetType:    parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
		},
		{
			name:           "fixed-len-byte-array-base64",
			inputStr:       "SGVsbG8gV29ybGQ=", // "Hello World" in base64
			expectedGoData: string("Hello World"),
			parquetType:    parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
		},

		// Converted types
		{
			name:           "utf8-string",
			inputStr:       "abc bcd",
			expectedGoData: string("abc bcd"),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		},
		{
			name:           "int8-converted",
			inputStr:       "1",
			expectedGoData: int32(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
		},
		{
			name:           "uint64-converted",
			inputStr:       "1",
			expectedGoData: uint64(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64),
		},
		{
			name:           "date-converted",
			inputStr:       "1",
			expectedGoData: int32(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
		},
		{
			name:           "timestamp-millis",
			inputStr:       "1",
			expectedGoData: int64(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
		},

		// Decimal types
		{
			name:           "decimal-int32",
			inputStr:       "123.45",
			expectedGoData: int32(12345),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			length:         12,
			scale:          2,
		},
		{
			name:           "decimal-fixed-len-byte-array",
			inputStr:       "123.45",
			expectedGoData: StrIntToBinary("12345", "BigEndian", 12, true),
			parquetType:    parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			length:         12,
			scale:          2,
		},
		{
			name:           "decimal-byte-array-high-precision",
			inputStr:       "373.1145",
			expectedGoData: StrIntToBinary("373114500000000000000", "BigEndian", 0, true),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			length:         16,
			scale:          18,
		},
		{
			name:           "unknown-basic-type",
			inputStr:       "test",
			expectedGoData: nil,
			parquetType:    parquet.TypePtr(parquet.Type(-1)), // Unknown type
		},
		{
			name:           "unknown-converted-type",
			inputStr:       "test",
			expectedGoData: nil,
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType(-1)), // Unknown converted type
		},
		{
			name:           "int16-converted",
			inputStr:       "32767",
			expectedGoData: int32(32767),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
		},
		{
			name:           "int32-converted",
			inputStr:       "123456",
			expectedGoData: int32(123456),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32),
		},
		{
			name:           "uint8-converted",
			inputStr:       "255",
			expectedGoData: int32(255),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8),
		},
		{
			name:           "uint16-converted",
			inputStr:       "65535",
			expectedGoData: int32(65535),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16),
		},
		{
			name:           "uint32-converted",
			inputStr:       "4294967295",
			expectedGoData: int32(-1), // Overflow behavior
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
		},
		{
			name:           "time-millis-converted",
			inputStr:       "86400000",
			expectedGoData: int32(86400000),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
		},
		{
			name:           "int64-converted",
			inputStr:       "9223372036854775807",
			expectedGoData: int64(9223372036854775807),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
		},
		{
			name:           "time-micros-converted",
			inputStr:       "86400000000",
			expectedGoData: int64(86400000000),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
		},
		{
			name:           "timestamp-micros-converted",
			inputStr:       "1640995200000000",
			expectedGoData: int64(1640995200000000),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS),
		},
		{
			name:           "interval-converted",
			inputStr:       "1234567890123",
			expectedGoData: StrIntToBinary("1234567890123", "LittleEndian", 12, false),
			parquetType:    parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
		},
		{
			name:           "decimal-int64",
			inputStr:       "123.45",
			expectedGoData: int64(12345),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:          2,
		},
		{
			name:           "decimal-byte-array-default",
			inputStr:       "999.999",
			expectedGoData: StrIntToBinary("999999", "BigEndian", 0, true),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY), // Will hit default case in decimal
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:          3,
		},
		{
			name:           "bson-converted",
			inputStr:       "bson_data_as_string",
			expectedGoData: string("bson_data_as_string"),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
		},
		{
			name:           "json-converted",
			inputStr:       `{"key":"value"}`,
			expectedGoData: string(`{"key":"value"}`),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_JSON),
		},
		{
			name:           "enum-converted",
			inputStr:       "RED",
			expectedGoData: string("RED"),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actualResult, _ := StrToParquetType(
				testCase.inputStr,
				testCase.parquetType,
				testCase.convertedType,
				testCase.length,
				testCase.scale,
			)

			actualStr := fmt.Sprintf("%v", actualResult)
			expectedStr := fmt.Sprintf("%v", testCase.expectedGoData)

			require.Equal(t, expectedStr, actualStr,
				"StrToParquetType conversion failed for input: %s with Type: %v, ConvertedType: %v\nExpected: %s\nGot: %s",
				testCase.inputStr, testCase.parquetType, testCase.convertedType, expectedStr, actualStr)
		})
	}
}

func TestParquetTypeToGoReflectType_NilChecks(t *testing.T) {
	tests := []struct {
		name       string
		pT         *parquet.Type
		rT         *parquet.FieldRepetitionType
		expectNil  bool
		expectType reflect.Type
	}{
		{
			name:       "nil_parquet_type",
			pT:         nil,
			rT:         nil,
			expectNil:  true,
			expectType: nil,
		},
		{
			name:       "nil_parquet_type_with_repetition",
			pT:         nil,
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REQUIRED}[0],
			expectNil:  true,
			expectType: nil,
		},
		{
			name:       "valid_type_nil_repetition",
			pT:         &[]parquet.Type{parquet.Type_BOOLEAN}[0],
			rT:         nil,
			expectNil:  false,
			expectType: reflect.TypeOf(true),
		},
		{
			name:       "valid_type_required_repetition",
			pT:         &[]parquet.Type{parquet.Type_INT32}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REQUIRED}[0],
			expectNil:  false,
			expectType: reflect.TypeOf(int32(0)),
		},
		{
			name:       "valid_type_optional_repetition",
			pT:         &[]parquet.Type{parquet.Type_INT32}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_OPTIONAL}[0],
			expectNil:  false,
			expectType: reflect.TypeOf((*int32)(nil)),
		},
		{
			name:       "int64_required",
			pT:         &[]parquet.Type{parquet.Type_INT64}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REQUIRED}[0],
			expectNil:  false,
			expectType: reflect.TypeOf(int64(0)),
		},
		{
			name:       "int64_optional",
			pT:         &[]parquet.Type{parquet.Type_INT64}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_OPTIONAL}[0],
			expectNil:  false,
			expectType: reflect.TypeOf((*int64)(nil)),
		},
		{
			name:       "string_required",
			pT:         &[]parquet.Type{parquet.Type_BYTE_ARRAY}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REQUIRED}[0],
			expectNil:  false,
			expectType: reflect.TypeOf(""),
		},
		{
			name:       "string_optional",
			pT:         &[]parquet.Type{parquet.Type_BYTE_ARRAY}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_OPTIONAL}[0],
			expectNil:  false,
			expectType: reflect.TypeOf((*string)(nil)),
		},
		{
			name:       "float_required",
			pT:         &[]parquet.Type{parquet.Type_FLOAT}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REQUIRED}[0],
			expectNil:  false,
			expectType: reflect.TypeOf(float32(0)),
		},
		{
			name:       "double_required",
			pT:         &[]parquet.Type{parquet.Type_DOUBLE}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REQUIRED}[0],
			expectNil:  false,
			expectType: reflect.TypeOf(float64(0)),
		},
		{
			name:       "int96_required",
			pT:         &[]parquet.Type{parquet.Type_INT96}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REQUIRED}[0],
			expectNil:  false,
			expectType: reflect.TypeOf(""),
		},
		{
			name:       "fixed_len_byte_array_required",
			pT:         &[]parquet.Type{parquet.Type_FIXED_LEN_BYTE_ARRAY}[0],
			rT:         &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REQUIRED}[0],
			expectNil:  false,
			expectType: reflect.TypeOf(""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParquetTypeToGoReflectType(tt.pT, tt.rT)

			if tt.expectNil {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Equal(t, tt.expectType, result)
			}
		})
	}
}

// Test edge cases with various combinations
func TestParquetTypeToGoReflectType_EdgeCases(t *testing.T) {
	// Test with repeated repetition type (should go to required path due to nil check)
	repeatedType := parquet.FieldRepetitionType_REPEATED
	int32Type := parquet.Type_INT32

	result := ParquetTypeToGoReflectType(&int32Type, &repeatedType)
	require.Equal(t, reflect.TypeOf(int32(0)), result)

	// Test unknown type (should return nil)
	unknownType := parquet.Type(-1)
	result = ParquetTypeToGoReflectType(&unknownType, nil)
	require.Nil(t, result)
}

// Test that all the safety checks prevent panics in various scenarios
func TestParquetTypeToGoReflectType_SafetyChecks(t *testing.T) {
	testCases := []struct {
		name string
		pT   *parquet.Type
		rT   *parquet.FieldRepetitionType
	}{
		{"nil_nil", nil, nil},
		{"nil_required", nil, &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REQUIRED}[0]},
		{"nil_optional", nil, &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_OPTIONAL}[0]},
		{"nil_repeated", nil, &[]parquet.FieldRepetitionType{parquet.FieldRepetitionType_REPEATED}[0]},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ParquetTypeToGoReflectType(tc.pT, tc.rT)
		})
	}
}

func TestStrToParquetTypeWithLogical(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		pT       *parquet.Type
		cT       *parquet.ConvertedType
		lT       *parquet.LogicalType
		length   int
		scale    int
		expected any
	}{
		// FLOAT16 tests
		{
			name: "float16_human_readable",
			s:    "3.14",
			pT:   parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:   &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			// expected is 2-byte float16 representation
		},
		{
			name: "float16_negative",
			s:    "-1.5",
			pT:   parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:   &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
		},
		// UUID tests
		{
			name:     "uuid_human_readable",
			s:        "550e8400-e29b-41d4-a716-446655440000",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			expected: "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00",
		},
		{
			name:     "uuid_already_binary",
			s:        "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			expected: "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00",
		},
		// TIMESTAMP nanos tests
		{
			name: "timestamp_nanos_human_readable",
			s:    "2024-01-15T10:30:00Z",
			pT:   parquet.TypePtr(parquet.Type_INT64),
			lT: &parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			}},
			expected: int64(1705314600000000000),
		},
		{
			name: "timestamp_nanos_raw_integer",
			s:    "1705314600000000000",
			pT:   parquet.TypePtr(parquet.Type_INT64),
			lT: &parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			}},
			expected: int64(1705314600000000000),
		},
		// TIME nanos tests
		{
			name: "time_nanos_human_readable",
			s:    "10:30:00.123456789",
			pT:   parquet.TypePtr(parquet.Type_INT64),
			lT: &parquet.LogicalType{TIME: &parquet.TimeType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			}},
			expected: int64(37800123456789),
		},
		{
			name: "time_nanos_raw_integer",
			s:    "37800123456789",
			pT:   parquet.TypePtr(parquet.Type_INT64),
			lT: &parquet.LogicalType{TIME: &parquet.TimeType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			}},
			expected: int64(37800123456789),
		},
		// DATE tests via LogicalType
		{
			name:     "date_human_readable_logical",
			s:        "2024-01-15",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected: int32(19737),
		},
		{
			name:     "date_raw_integer_logical",
			s:        "19737",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected: int32(19737),
		},
		// Fallback to ConvertedType
		{
			name:     "fallback_to_converted_type",
			s:        "2024-01-15",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
			lT:       nil,
			expected: int32(19737),
		},
		// Nil LogicalType
		{
			name:     "nil_logical_type",
			s:        "42",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32),
			lT:       nil,
			expected: int32(42),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := StrToParquetTypeWithLogical(tt.s, tt.pT, tt.cT, tt.lT, tt.length, tt.scale)
			require.NoError(t, err)
			if tt.expected != nil {
				require.Equal(t, tt.expected, result)
			} else {
				require.NotNil(t, result)
			}
		})
	}
}

func TestStrToParquetType_HumanReadable(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		pT       parquet.Type
		cT       parquet.ConvertedType
		expected any
	}{
		// DATE
		{
			name:     "date_human_readable",
			s:        "2024-01-15",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedType_DATE,
			expected: int32(19737),
		},
		{
			name:     "date_raw_integer",
			s:        "19737",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedType_DATE,
			expected: int32(19737),
		},
		// TIME_MILLIS
		{
			name:     "time_millis_human_readable",
			s:        "10:30:00.123",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedType_TIME_MILLIS,
			expected: int32(37800123),
		},
		{
			name:     "time_millis_raw_integer",
			s:        "37800123",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedType_TIME_MILLIS,
			expected: int32(37800123),
		},
		// TIME_MICROS
		{
			name:     "time_micros_human_readable",
			s:        "10:30:00.123456",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIME_MICROS,
			expected: int64(37800123456),
		},
		{
			name:     "time_micros_raw_integer",
			s:        "37800123456",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIME_MICROS,
			expected: int64(37800123456),
		},
		// TIMESTAMP_MILLIS
		{
			name:     "timestamp_millis_human_readable",
			s:        "2024-01-15T10:30:00Z",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected: int64(1705314600000),
		},
		{
			name:     "timestamp_millis_raw_integer",
			s:        "1705314600000",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected: int64(1705314600000),
		},
		// TIMESTAMP_MICROS
		{
			name:     "timestamp_micros_human_readable",
			s:        "2024-01-15T10:30:00Z",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIMESTAMP_MICROS,
			expected: int64(1705314600000000),
		},
		{
			name:     "timestamp_micros_raw_integer",
			s:        "1705314600000000",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIMESTAMP_MICROS,
			expected: int64(1705314600000000),
		},
		// INTERVAL
		{
			name: "interval_human_readable",
			s:    "1 mon 2 day 3.456 sec",
			pT:   parquet.Type_FIXED_LEN_BYTE_ARRAY,
			cT:   parquet.ConvertedType_INTERVAL,
		},
		{
			name: "interval_raw_integer",
			s:    "123456",
			pT:   parquet.Type_FIXED_LEN_BYTE_ARRAY,
			cT:   parquet.ConvertedType_INTERVAL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pT := tt.pT
			cT := tt.cT
			result, err := StrToParquetType(tt.s, &pT, &cT, 0, 0)
			require.NoError(t, err)
			if tt.expected != nil {
				require.Equal(t, tt.expected, result)
			} else {
				require.NotNil(t, result)
			}
		})
	}
}

func TestStrToParquetType_INT96(t *testing.T) {
	// INT96 human readable
	pT := parquet.Type_INT96
	result, err := StrToParquetType("2024-01-15T10:30:00Z", &pT, nil, 0, 0)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 12, len(result.(string)))

	// INT96 raw integer fallback
	result2, err := StrToParquetType("123456789", &pT, nil, 0, 0)
	require.NoError(t, err)
	require.NotNil(t, result2)
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
			name:       "string_direct",
			value:      "hello world",
			pT:         parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected:   "hello world",
			usesDirect: true,
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
	val := reflect.ValueOf("hello world")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

// stringBasedConversion simulates the old string-based approach for benchmarking
func stringBasedConversion(val reflect.Value, pT *parquet.Type) (any, error) {
	s := fmt.Sprintf("%v", val)
	return StrToParquetType(s, pT, nil, 0, 0)
}

func BenchmarkComparison_Int32_Direct(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_INT32)
	val := reflect.ValueOf(float64(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkComparison_Int32_StringBased(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_INT32)
	val := reflect.ValueOf(float64(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stringBasedConversion(val, pT)
	}
}

func BenchmarkComparison_Bool_Direct(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BOOLEAN)
	val := reflect.ValueOf(true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkComparison_Bool_StringBased(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BOOLEAN)
	val := reflect.ValueOf(true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stringBasedConversion(val, pT)
	}
}

func BenchmarkComparison_Double_Direct(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_DOUBLE)
	val := reflect.ValueOf(float64(3.14159265359))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkComparison_Double_StringBased(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_DOUBLE)
	val := reflect.ValueOf(float64(3.14159265359))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stringBasedConversion(val, pT)
	}
}

func BenchmarkComparison_String_Direct(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	val := reflect.ValueOf("hello world")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkComparison_String_StringBased(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	val := reflect.ValueOf("hello world")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stringBasedConversion(val, pT)
	}
}

func TestStrToParquetTypeWithLogical_Comprehensive(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		pT       *parquet.Type
		lT       *parquet.LogicalType
		expected any
	}{
		{
			name:     "uuid",
			s:        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			expected: string([]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}),
		},
		{
			name:     "float16",
			s:        "9.5",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			expected: []byte{0xC0, 0x48},
		},
		{
			name:     "date_string",
			s:        "2024-01-15",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected: int32(19737),
		},
		{
			name:     "date_int",
			s:        "19737",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected: int32(19737),
		},
		{
			name:     "timestamp_nanos",
			s:        "2024-01-15T14:30:45.123456789Z",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimestampLogicalType(false, false, true, true),
			expected: int64(1705329045123456789),
		},
		{
			name:     "timestamp_micros",
			s:        "2024-01-15T14:30:45.123456Z",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimestampLogicalType(false, true, false, true),
			expected: int64(1705329045123456),
		},
		{
			name:     "timestamp_millis",
			s:        "2024-01-15T14:30:45.123Z",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimestampLogicalType(true, false, false, true),
			expected: int64(1705329045123),
		},
		{
			name:     "timestamp_int",
			s:        "1705329045123456789",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimestampLogicalType(false, false, true, true),
			expected: int64(1705329045123456789),
		},
		{
			name:     "time_nanos",
			s:        "12:34:56.789012345",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, false, true),
			expected: int64(45296789012345),
		},
		{
			name:     "time_micros",
			s:        "12:34:56.789012",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, true, false),
			expected: int64(45296789012),
		},
		{
			name:     "time_millis",
			s:        "12:34:56.789",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "time_int32",
			s:        "45296789",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "time_int64",
			s:        "45296789012345",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, false, true),
			expected: int64(45296789012345),
		},
		{
			name:     "decimal_int32",
			s:        "123.45",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createDecimalLogicalType(9, 2),
			expected: int32(12345),
		},
		{
			name:     "decimal_int64",
			s:        "123.45",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createDecimalLogicalType(18, 2),
			expected: int64(12345),
		},
		{
			name:     "decimal_fixed_len",
			s:        "123.45",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       createDecimalLogicalType(9, 2),
			expected: StrIntToBinary("12345", "BigEndian", 12, true),
		},
		{
			name:     "decimal_byte_array",
			s:        "123.45",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			lT:       createDecimalLogicalType(9, 2),
			expected: StrIntToBinary("12345", "BigEndian", 0, true),
		},
		{
			name:     "integer_int8",
			s:        "123",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(8, true),
			expected: int8(123),
		},
		{
			name:     "integer_uint8",
			s:        "200",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(8, false),
			expected: uint8(200),
		},
		{
			name:     "integer_int16",
			s:        "12345",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(16, true),
			expected: int16(12345),
		},
		{
			name:     "integer_uint16",
			s:        "50000",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(16, false),
			expected: uint16(50000),
		},
		{
			name:     "integer_int32",
			s:        "1234567",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(32, true),
			expected: int32(1234567),
		},
		{
			name:     "integer_uint32",
			s:        "1234567",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(32, false),
			expected: uint32(1234567),
		},
		{
			name:     "integer_int64",
			s:        "1234567890",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createIntegerLogicalType(64, true),
			expected: int64(1234567890),
		},
		{
			name:     "integer_uint64",
			s:        "1234567890",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createIntegerLogicalType(64, false),
			expected: uint64(1234567890),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := StrToParquetTypeWithLogical(tt.s, tt.pT, nil, tt.lT, 12, 0)
			require.NoError(t, err)

			switch exp := tt.expected.(type) {
			case []byte:
				require.Equal(t, string(exp), result)
			case int8:
				require.Equal(t, exp, int8(result.(int32)))
			case uint8:
				require.Equal(t, exp, uint8(result.(int32)))
			case int16:
				require.Equal(t, exp, int16(result.(int32)))
			case uint16:
				require.Equal(t, exp, uint16(result.(int32)))
			case uint32:
				require.Equal(t, exp, uint32(result.(int32)))
			case uint64:
				require.Equal(t, exp, uint64(result.(int64)))
			default:
				require.Equal(t, tt.expected, result)
			}
		})
	}
}
