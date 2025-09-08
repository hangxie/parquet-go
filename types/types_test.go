package types

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_InterfaceToParquetType(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		expectError bool
		expected    any
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
			name:     "reflect_bool_to_bool",
			value:    reflect.ValueOf(true).Interface(),
			pT:       parquet.TypePtr(parquet.Type_BOOLEAN),
			expected: true,
		},

		// Error cases
		{
			name:        "string_to_bool_error",
			value:       "not a bool",
			pT:          parquet.TypePtr(parquet.Type_BOOLEAN),
			expectError: true,
		},
		{
			name:        "string_to_int32_error",
			value:       "not an int",
			pT:          parquet.TypePtr(parquet.Type_INT32),
			expectError: true,
		},
		{
			name:        "bool_to_float_error",
			value:       true,
			pT:          parquet.TypePtr(parquet.Type_FLOAT),
			expectError: true,
		},
		{
			name:        "int_to_string_error",
			value:       42,
			pT:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expectError: true,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := InterfaceToParquetType(tt.value, tt.pT)

			if tt.expectError {
				require.Error(t, err)
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

func Test_JSONTypeToParquetType(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		cT          *parquet.ConvertedType
		length      int
		scale       int
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := reflect.ValueOf(tt.value)
			_, err := JSONTypeToParquetType(val, tt.pT, tt.cT, tt.length, tt.scale)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_ParquetTypeToGoReflectType(t *testing.T) {
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

func Test_StrIntToBinary(t *testing.T) {
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

func Test_StrToParquetType(t *testing.T) {
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
			name:           "fixed-len-byte-array-string",
			inputStr:       "abc bcd",
			expectedGoData: string("abc bcd"),
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

func Test_ParquetTypeToGoReflectType_NilChecks(t *testing.T) {
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
func Test_ParquetTypeToGoReflectType_EdgeCases(t *testing.T) {
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
func Test_ParquetTypeToGoReflectType_SafetyChecks(t *testing.T) {
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

func Test_ParquetTypeToJSONType(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		pT        *parquet.Type
		cT        *parquet.ConvertedType
		precision int
		scale     int
		expected  any
	}{
		{
			name:      "int32_decimal_convertedtype",
			value:     int32(12345),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     2,
			expected:  float64(123.45),
		},
		{
			name:      "int64_decimal_convertedtype",
			value:     int64(98765),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     3,
			expected:  float64(98.765),
		},
		{
			name:      "string_decimal_byte_array",
			value:     StrIntToBinary("1234567890", "BigEndian", 0, true),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 12,
			scale:     4,
			expected:  float64(123456.7890),
		},
		{
			name:      "string_decimal_fixed_len_byte_array",
			value:     StrIntToBinary("9876543210", "BigEndian", 12, true),
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 12,
			scale:     2,
			expected:  float64(98765432.10),
		},
		{
			name:      "non_decimal_convertedtype",
			value:     "hello",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			precision: 0,
			scale:     0,
			expected:  "hello",
		},
		{
			name:      "no_convertedtype",
			value:     int32(42),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			precision: 0,
			scale:     0,
			expected:  int32(42),
		},
		{
			name:      "negative_decimal",
			value:     int32(-12345),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     2,
			expected:  float64(-123.45),
		},
		{
			name:      "zero_scale_decimal",
			value:     int32(123),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     0,
			expected:  float64(123),
		},
		{
			name:      "nil_value",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     2,
			expected:  nil,
		},
		{
			name:      "int96_timestamp",
			value:     "\x00\x80\xa7HJ'\x00\x00*\x89%\x00", // INT96 binary data for 2023-01-01T12:00:00Z
			pT:        parquet.TypePtr(parquet.Type_INT96),
			cT:        nil,
			precision: 0,
			scale:     0,
			expected:  "2023-01-01T12:00:00.000000000Z",
		},
		{
			name: "interval_converted_type",
			value: func() string {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 0)        // 0 months
				binary.LittleEndian.PutUint32(b[4:8], 1)        // 1 day
				binary.LittleEndian.PutUint32(b[8:12], 7200000) // 2 hours in milliseconds
				return string(b)
			}(),
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
			precision: 0,
			scale:     0,
			expected:  "1 day 7200.000 sec", // 1 day + 2 hours = 1 day + 7200 seconds
		},
		{
			name:      "timestamp_millis_converted_type",
			value:     int64(1640995200000), // 2022-01-01T00:00:00Z in milliseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "timestamp_micros_converted_type",
			value:     int64(1640995200000000), // 2022-01-01T00:00:00Z in microseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS),
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000000Z",
		},
		{
			name:      "byte_array_without_converted_type",
			value:     "Hello World!",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			precision: 0,
			scale:     0,
			expected:  base64.StdEncoding.EncodeToString([]byte("Hello World!")), // "SGVsbG8gV29ybGQh"
		},
		{
			name:      "fixed_len_byte_array_without_converted_type",
			value:     []byte{0x01, 0x02, 0x03, 0x04, 0xFF},
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			precision: 0,
			scale:     0,
			expected:  base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03, 0x04, 0xFF}), // "AQIDBP8="
		},
		{
			name:      "byte_array_with_utf8_converted_type",
			value:     "Hello World!",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			precision: 0,
			scale:     0,
			expected:  "Hello World!", // Should remain as string, not base64 encoded
		},
		{
			name:      "time_millis_converted_type",
			value:     int32(45296789), // 12:34:56.789
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789",
		},
		{
			name:      "time_micros_converted_type",
			value:     int64(45296789012), // 12:34:56.789012
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789012",
		},
		{
			name:      "time_millis_zero",
			value:     int32(0),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			precision: 0,
			scale:     0,
			expected:  "00:00:00.000",
		},
		{
			name:      "time_micros_zero",
			value:     int64(0),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
			precision: 0,
			scale:     0,
			expected:  "00:00:00.000000",
		},
		{
			name:      "time_millis_wrong_type",
			value:     "not_an_int",
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			precision: 0,
			scale:     0,
			expected:  "not_an_int", // Should return original value if type assertion fails
		},
		{
			name:      "bson_converted_type_bytes_input",
			value:     []byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			precision: 0,
			scale:     0,
			expected:  "FgAAABBpAAEAAAAA", // Base64 encoded BSON data
		},
		{
			name:      "bson_converted_type_nil_value",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			precision: 0,
			scale:     0,
			expected:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParquetTypeToJSONType(tt.value, tt.pT, tt.cT, tt.precision, tt.scale)

			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_ParquetTypeToJSONTypeWithLogical(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		pT        *parquet.Type
		cT        *parquet.ConvertedType
		lT        *parquet.LogicalType
		precision int
		scale     int
		expected  any
	}{
		{
			name:      "int32_logicaltype_decimal",
			value:     int32(44444),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createDecimalLogicalType(9, 2),
			precision: 9,
			scale:     2,
			expected:  float64(444.44),
		},
		{
			name:      "int64_logicaltype_decimal",
			value:     int64(-12345),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createDecimalLogicalType(18, 3),
			precision: 18,
			scale:     3,
			expected:  float64(-12.345),
		},
		{
			name:      "convertedtype_fallback",
			value:     int32(12345),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			lT:        nil,
			precision: 5,
			scale:     2,
			expected:  float64(123.45),
		},
		{
			name:      "both_types_prefer_logical",
			value:     int32(98765),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			lT:        createDecimalLogicalType(7, 3),
			precision: 7,
			scale:     3,
			expected:  float64(98.765),
		},
		{
			name:      "non_decimal_logical_type",
			value:     "hello",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        createStringLogicalType(),
			precision: 0,
			scale:     0,
			expected:  "hello",
		},
		{
			name:      "no_types",
			value:     int32(42),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        nil,
			precision: 0,
			scale:     0,
			expected:  int32(42),
		},
		{
			name:      "string_decimal_logicaltype",
			value:     StrIntToBinary("123456789", "BigEndian", 0, true),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        createDecimalLogicalType(15, 4),
			precision: 15,
			scale:     4,
			expected:  float64(12345.6789),
		},
		{
			name:      "int96_timestamp_with_logical",
			value:     "\x00\x80\xa7HJ'\x00\x00*\x89%\x00", // INT96 binary data for 2023-01-01T12:00:00Z
			pT:        parquet.TypePtr(parquet.Type_INT96),
			cT:        nil,
			lT:        createStringLogicalType(), // Non-relevant logical type
			precision: 0,
			scale:     0,
			expected:  "2023-01-01T12:00:00.000000000Z", // Should still convert INT96
		},
		{
			name:      "timestamp_logical_type_millis",
			value:     int64(1640995200000), // 2022-01-01T00:00:00Z in milliseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimestampLogicalType(true, true, false, false), // millis, UTC adjusted
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "timestamp_logical_type_micros",
			value:     int64(1640995200000000), // 2022-01-01T00:00:00Z in microseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimestampLogicalType(false, true, false, true), // micros, UTC adjusted
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000000Z",
		},
		{
			name:      "timestamp_logical_type_nanos",
			value:     int64(1640995200000000000), // 2022-01-01T00:00:00Z in nanoseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimestampLogicalType(false, false, true, true), // nanos, UTC adjusted
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000000000Z",
		},
		{
			name:      "timestamp_logical_type_not_utc_adjusted",
			value:     int64(1640995200000), // 2022-01-01T00:00:00Z in milliseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimestampLogicalType(true, false, false, false), // millis, not UTC adjusted
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "byte_array_without_logical_or_converted_type",
			value:     "Binary Data",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        nil,
			precision: 0,
			scale:     0,
			expected:  base64.StdEncoding.EncodeToString([]byte("Binary Data")), // "QmluYXJ5IERhdGE="
		},
		{
			name:      "fixed_len_byte_array_without_logical_or_converted_type",
			value:     []byte{0xDE, 0xAD, 0xBE, 0xEF},
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        nil,
			precision: 0,
			scale:     0,
			expected:  base64.StdEncoding.EncodeToString([]byte{0xDE, 0xAD, 0xBE, 0xEF}), // "3q2+7w=="
		},
		{
			name:      "byte_array_with_string_logical_type",
			value:     "String Data",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        createStringLogicalType(),
			precision: 0,
			scale:     0,
			expected:  "String Data", // Should remain as string, not base64 encoded
		},
		{
			name: "uuid_logical_type_bytes",
			value: []byte{
				0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
				0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
			}, // 16-byte UUID in big-endian format
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			precision: 0,
			scale:     0,
			expected:  "01234567-89ab-cdef-0123-456789abcdef", // Standard UUID string format
		},
		{
			name: "uuid_logical_type_string_input",
			value: string([]byte{
				0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
				0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
			}), // UUID as string bytes
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			precision: 0,
			scale:     0,
			expected:  "6ba7b810-9dad-11d1-80b4-00c04fd430c8", // Standard UUID string format
		},
		{
			name:      "time_logical_type_millis",
			value:     int32(45296789), // 12:34:56.789
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createTimeLogicalType(true, false, false), // millis
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789",
		},
		{
			name:      "time_logical_type_micros",
			value:     int64(45296789012), // 12:34:56.789012
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimeLogicalType(false, true, false), // micros
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789012",
		},
		{
			name:      "time_logical_type_nanos",
			value:     int64(45296789012345), // 12:34:56.789012345
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimeLogicalType(false, false, true), // nanos
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789012345",
		},
		{
			name:      "time_logical_type_zero_millis",
			value:     int32(0),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createTimeLogicalType(true, false, false), // millis
			precision: 0,
			scale:     0,
			expected:  "00:00:00.000",
		},
		{
			name:      "time_logical_type_wrong_type",
			value:     "not_an_int",
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createTimeLogicalType(true, false, false), // millis
			precision: 0,
			scale:     0,
			expected:  "not_an_int", // Should return original value if type assertion fails
		},
		{
			name:      "bson_logical_type_string_input",
			value:     string([]byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{BSON: &parquet.BsonType{}},
			precision: 0,
			scale:     0,
			expected:  "FgAAABBpAAEAAAAA", // Base64 encoded BSON data
		},
		{
			name:      "bson_logical_type_bytes_input",
			value:     []byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x02, 0x00, 0x00, 0x00, 0x00},
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{BSON: &parquet.BsonType{}},
			precision: 0,
			scale:     0,
			expected:  "FgAAABBpAAIAAAAA", // Base64 encoded BSON data
		},
		{
			name:      "bson_converted_type_string_input",
			value:     string([]byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x03, 0x00, 0x00, 0x00, 0x00}),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			lT:        nil,
			precision: 0,
			scale:     0,
			expected:  "FgAAABBpAAMAAAAA", // Base64 encoded BSON data
		},
		{
			name:      "bson_logical_type_nil_value",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{BSON: &parquet.BsonType{}},
			precision: 0,
			scale:     0,
			expected:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParquetTypeToJSONTypeWithLogical(tt.value, tt.pT, tt.cT, tt.lT, tt.precision, tt.scale)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Helper function to create a decimal logical type for testing
func createDecimalLogicalType(precision, scale int32) *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.DECIMAL = parquet.NewDecimalType()
	lt.DECIMAL.Precision = precision
	lt.DECIMAL.Scale = scale
	return lt
}

// Helper function to create a string logical type for testing
func createStringLogicalType() *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.STRING = parquet.NewStringType()
	return lt
}

// Helper function to create a timestamp logical type for testing
func createTimestampLogicalType(millis, micros, nanos, utcAdjusted bool) *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.TIMESTAMP = parquet.NewTimestampType()

	// Set UTC adjusted flag
	lt.TIMESTAMP.IsAdjustedToUTC = utcAdjusted

	// Set time unit
	lt.TIMESTAMP.Unit = parquet.NewTimeUnit()
	if millis {
		lt.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()
	} else if micros {
		lt.TIMESTAMP.Unit.MICROS = parquet.NewMicroSeconds()
	} else if nanos {
		lt.TIMESTAMP.Unit.NANOS = parquet.NewNanoSeconds()
	} else {
		// Default to millis if none specified
		lt.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()
	}

	return lt
}

// Helper function to create a time logical type for testing
func createTimeLogicalType(millis, micros, nanos bool) *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.TIME = parquet.NewTimeType()

	// Set time unit
	lt.TIME.Unit = parquet.NewTimeUnit()
	if millis {
		lt.TIME.Unit.MILLIS = parquet.NewMilliSeconds()
	} else if micros {
		lt.TIME.Unit.MICROS = parquet.NewMicroSeconds()
	} else if nanos {
		lt.TIME.Unit.NANOS = parquet.NewNanoSeconds()
	} else {
		// Default to millis if none specified
		lt.TIME.Unit.MILLIS = parquet.NewMilliSeconds()
	}

	// For TIME, the isAdjustedToUTC is not relevant (it's always local time)
	lt.TIME.IsAdjustedToUTC = false

	return lt
}

func Test_convertIntervalValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name: "valid_interval_bytes",
			val: func() []byte {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 0)        // 0 months
				binary.LittleEndian.PutUint32(b[4:8], 1)        // 1 day
				binary.LittleEndian.PutUint32(b[8:12], 3600000) // 1 hour in milliseconds
				return b
			}(),
			expected: "1 day 3600.000 sec",
		},
		{
			name: "valid_interval_string",
			val: func() string {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 0)     // 0 months
				binary.LittleEndian.PutUint32(b[4:8], 0)     // 0 days
				binary.LittleEndian.PutUint32(b[8:12], 1000) // 1 second in milliseconds
				return string(b)
			}(),
			expected: "1.000 sec",
		},
		{
			name: "interval_with_months_days_seconds",
			val: func() []byte {
				b := make([]byte, 12)
				binary.LittleEndian.PutUint32(b[0:4], 2)     // 2 months
				binary.LittleEndian.PutUint32(b[4:8], 15)    // 15 days
				binary.LittleEndian.PutUint32(b[8:12], 1500) // 1.5 seconds
				return b
			}(),
			expected: "2 mon 15 day 1.500 sec",
		},
		{
			name:     "invalid_length_string",
			val:      "short",
			expected: "short",
		},
		{
			name:     "invalid_length_bytes",
			val:      []byte{1, 2, 3},
			expected: []byte{1, 2, 3},
		},
		{
			name:     "non_interval_value",
			val:      42,
			expected: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertIntervalValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_TIMESTAMP_MILLISToISO8601(t *testing.T) {
	tests := []struct {
		name          string
		millis        int64
		adjustedToUTC bool
		expected      string
	}{
		{
			name:          "epoch_time",
			millis:        0,
			adjustedToUTC: true,
			expected:      "1970-01-01T00:00:00.000Z",
		},
		{
			name:          "new_year_2022",
			millis:        1640995200000, // 2022-01-01T00:00:00Z
			adjustedToUTC: true,
			expected:      "2022-01-01T00:00:00.000Z",
		},
		{
			name:          "with_milliseconds",
			millis:        1640995200123, // 2022-01-01T00:00:00.123Z
			adjustedToUTC: true,
			expected:      "2022-01-01T00:00:00.123Z",
		},
		{
			name:          "past_timestamp",
			millis:        946684800000, // 2000-01-01T00:00:00Z
			adjustedToUTC: true,
			expected:      "2000-01-01T00:00:00.000Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TIMESTAMP_MILLISToISO8601(tt.millis, tt.adjustedToUTC)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_TIMESTAMP_MICROSToISO8601(t *testing.T) {
	tests := []struct {
		name          string
		micros        int64
		adjustedToUTC bool
		expected      string
	}{
		{
			name:          "epoch_time",
			micros:        0,
			adjustedToUTC: true,
			expected:      "1970-01-01T00:00:00.000000Z",
		},
		{
			name:          "new_year_2022",
			micros:        1640995200000000, // 2022-01-01T00:00:00Z
			adjustedToUTC: true,
			expected:      "2022-01-01T00:00:00.000000Z",
		},
		{
			name:          "with_microseconds",
			micros:        1640995200123456, // 2022-01-01T00:00:00.123456Z
			adjustedToUTC: true,
			expected:      "2022-01-01T00:00:00.123456Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TIMESTAMP_MICROSToISO8601(tt.micros, tt.adjustedToUTC)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_ConvertTimestampValue(t *testing.T) {
	tests := []struct {
		name          string
		val           any
		convertedType parquet.ConvertedType
		expected      any
	}{
		{
			name:          "nil_value",
			val:           nil,
			convertedType: parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected:      nil,
		},
		{
			name:          "timestamp_millis_valid",
			val:           int64(1640995200000), // 2022-01-01T00:00:00Z
			convertedType: parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected:      "2022-01-01T00:00:00.000Z",
		},
		{
			name:          "timestamp_micros_valid",
			val:           int64(1640995200000000), // 2022-01-01T00:00:00Z
			convertedType: parquet.ConvertedType_TIMESTAMP_MICROS,
			expected:      "2022-01-01T00:00:00.000000Z",
		},
		{
			name:          "non_int64_value",
			val:           int32(123),
			convertedType: parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected:      int32(123), // Should return unchanged
		},
		{
			name:          "unsupported_converted_type",
			val:           int64(1640995200000),
			convertedType: parquet.ConvertedType_UTF8,
			expected:      int64(1640995200000), // Should return unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertTimestampValue(tt.val, tt.convertedType)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_convertTimestampLogicalValue(t *testing.T) {
	tests := []struct {
		name      string
		val       any
		timestamp *parquet.TimestampType
		expected  any
	}{
		{
			name:      "nil_value",
			val:       nil,
			timestamp: createTimestampLogicalType(true, false, false, true).GetTIMESTAMP(),
			expected:  nil,
		},
		{
			name:      "nil_timestamp",
			val:       int64(1640995200000),
			timestamp: nil,
			expected:  nil,
		},
		{
			name:      "timestamp_millis_utc_adjusted",
			val:       int64(1640995200000),                                                // 2022-01-01T00:00:00Z
			timestamp: createTimestampLogicalType(true, false, false, true).GetTIMESTAMP(), // millis, UTC adjusted
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "timestamp_micros_utc_adjusted",
			val:       int64(1640995200000000),                                             // 2022-01-01T00:00:00Z
			timestamp: createTimestampLogicalType(false, true, false, true).GetTIMESTAMP(), // micros, UTC adjusted
			expected:  "2022-01-01T00:00:00.000000Z",
		},
		{
			name:      "timestamp_nanos_utc_adjusted",
			val:       int64(1640995200000000000),                                          // 2022-01-01T00:00:00Z
			timestamp: createTimestampLogicalType(false, false, true, true).GetTIMESTAMP(), // nanos, UTC adjusted
			expected:  "2022-01-01T00:00:00.000000000Z",
		},
		{
			name:      "timestamp_millis_not_utc_adjusted",
			val:       int64(1640995200000),                                                 // 2022-01-01T00:00:00Z
			timestamp: createTimestampLogicalType(true, false, false, false).GetTIMESTAMP(), // millis, not UTC adjusted
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "non_int64_value",
			val:       int32(123),
			timestamp: createTimestampLogicalType(true, false, false, true).GetTIMESTAMP(),
			expected:  int32(123), // Should return unchanged
		},
		{
			name: "default_to_millis_when_no_unit",
			val:  int64(1640995200000),
			timestamp: func() *parquet.TimestampType {
				ts := parquet.NewTimestampType()
				ts.IsAdjustedToUTC = true
				// Don't set Unit to test default behavior
				return ts
			}(),
			expected: "2022-01-01T00:00:00.000Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertTimestampLogicalValue(tt.val, tt.timestamp)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_convertBinaryValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name:     "string_input",
			val:      "Hello, World!",
			expected: base64.StdEncoding.EncodeToString([]byte("Hello, World!")), // "SGVsbG8sIFdvcmxkIQ=="
		},
		{
			name:     "byte_slice_input",
			val:      []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F},                                    // "Hello" in bytes
			expected: base64.StdEncoding.EncodeToString([]byte{0x48, 0x65, 0x6C, 0x6C, 0x6F}), // "SGVsbG8="
		},
		{
			name:     "binary_data",
			val:      []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD},
			expected: base64.StdEncoding.EncodeToString([]byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD}), // "AAECAz/+/Q=="
		},
		{
			name:     "empty_string",
			val:      "",
			expected: base64.StdEncoding.EncodeToString([]byte("")), // ""
		},
		{
			name:     "empty_byte_slice",
			val:      []byte{},
			expected: base64.StdEncoding.EncodeToString([]byte{}), // ""
		},
		{
			name:     "non_binary_value",
			val:      42,
			expected: 42, // Should return unchanged
		},
		{
			name:     "float_value",
			val:      3.14,
			expected: 3.14, // Should return unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertBinaryValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_ConvertUUIDValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name: "valid_uuid_bytes",
			val: []byte{
				0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
				0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
			},
			expected: "01234567-89ab-cdef-0123-456789abcdef",
		},
		{
			name: "valid_uuid_string",
			val: string([]byte{
				0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
				0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
			}),
			expected: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		},
		{
			name: "zero_uuid",
			val: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			expected: "00000000-0000-0000-0000-000000000000",
		},
		{
			name: "max_uuid",
			val: []byte{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			},
			expected: "ffffffff-ffff-ffff-ffff-ffffffffffff",
		},
		{
			name:     "invalid_length_short",
			val:      []byte{0x01, 0x02, 0x03},
			expected: []byte{0x01, 0x02, 0x03}, // Should return unchanged
		},
		{
			name: "invalid_length_long",
			val: []byte{
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
				0x11, // 17 bytes - too long
			},
			expected: []byte{
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
				0x11,
			}, // Should return unchanged
		},
		{
			name:     "non_binary_value",
			val:      42,
			expected: 42, // Should return unchanged
		},
		{
			name:     "empty_bytes",
			val:      []byte{},
			expected: []byte{}, // Should return unchanged (wrong length)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertUUIDValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_ConvertDecimalValue(t *testing.T) {
	pT := parquet.TypePtr(parquet.Type_INT32)

	// int32 - now returns float64 instead of string
	res := ConvertDecimalValue(int32(12345), pT, 10, 2)
	require.Equal(t, float64(123.45), res)

	// int64 - now returns float64 instead of string
	pT = parquet.TypePtr(parquet.Type_INT64)
	res = ConvertDecimalValue(int64(12345), pT, 10, 2)
	require.Equal(t, float64(123.45), res)

	// string - now returns float64 instead of string
	pT = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	val := StrIntToBinary("12345", "BigEndian", 0, true)
	res = ConvertDecimalValue(val, pT, 10, 2)
	require.Equal(t, float64(123.45), res)

	// default
	res = ConvertDecimalValue(float32(123.45), pT, 10, 2)
	require.Equal(t, float32(123.45), res)
}

func Test_convertINT96Value(t *testing.T) {
	// nil
	res := convertINT96Value(nil)
	require.Nil(t, res)

	// string
	timeStr := "2023-01-01T12:00:00.000000000Z"
	ts, _ := time.Parse(time.RFC3339Nano, timeStr)
	int96 := TimeToINT96(ts)
	res = convertINT96Value(int96)
	require.Equal(t, timeStr, res)

	// default
	res = convertINT96Value(123)
	require.Equal(t, 123, res)
}

func Test_ConvertDateLogicalValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name:     "epoch_day_zero",
			val:      int32(0), // 1970-01-01
			expected: "1970-01-01",
		},
		{
			name:     "positive_days",
			val:      int32(19358), // 2023-01-01
			expected: "2023-01-01",
		},
		{
			name:     "negative_days",
			val:      int32(-1), // 1969-12-31
			expected: "1969-12-31",
		},
		{
			name:     "leap_year_date",
			val:      int32(19417), // 2023-02-29 would be invalid, using 2023-03-01
			expected: "2023-03-01",
		},
		{
			name:     "non_int32_value",
			val:      "not_an_int32",
			expected: "not_an_int32", // Should return unchanged
		},
		{
			name:     "int64_value",
			val:      int64(19358),
			expected: int64(19358), // Should return unchanged as it's not int32
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertDateLogicalValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_ConvertGeometryAndGeographyLogicalValue(t *testing.T) {
	// sample WKB: little-endian, Point(1,2)
	sample := []byte{1, 1, 0, 0, 0, /* type=1 point */
		0, 0, 0, 0, 0, 0, 240, 63, /* x=1.0 */
		0, 0, 0, 0, 0, 0, 0, 64 /* y=2.0 */}

	// Geometry (GeoJSON mode returns GeoJSON Feature)
	geom := parquet.NewGeometryType()
	crs := "EPSG:3857"
	geom.CRS = &crs
	// Force GeoJSON mode for geometry
	SetGeometryJSONMode(GeospatialModeGeoJSON)
	gRes := ConvertGeometryLogicalValue(sample, geom)
	feat, ok := gRes.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Feature", feat["type"])
	ggeom := feat["geometry"].(map[string]any)
	require.Equal(t, "Point", ggeom["type"])
	require.Equal(t, []float64{1, 2}, ggeom["coordinates"])
	gprops := feat["properties"].(map[string]any)
	require.Equal(t, crs, gprops["crs"]) // properties carries crs

	// Geography with algorithm
	geog := parquet.NewGeographyType()
	crs2 := "OGC:CRS84"
	geog.CRS = &crs2
	algo := parquet.EdgeInterpolationAlgorithm_VINCENTY
	geog.Algorithm = &algo
	// Default geography mode is GeoJSON (return Feature)
	SetGeographyJSONMode(GeospatialModeGeoJSON)
	gaRes := ConvertGeographyLogicalValue(sample, geog)
	feat2, ok := gaRes.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Feature", feat2["type"])
	g2 := feat2["geometry"].(map[string]any)
	require.Equal(t, "Point", g2["type"])
	require.Equal(t, []float64{1, 2}, g2["coordinates"])
	props2 := feat2["properties"].(map[string]any)
	require.Equal(t, "OGC:CRS84", props2["crs"])
	require.Equal(t, "VINCENTY", props2["algorithm"])

	// nil/empty safety
	require.Nil(t, ConvertGeometryLogicalValue(nil, geom))
	require.Nil(t, ConvertGeographyLogicalValue(nil, geog))

	// Reprojection hook test: fake reprojection that adds +1 to coords
	SetGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
		if crs == "EPSG:3857" && gj["type"] == "Point" {
			coords := gj["coordinates"].([]float64)
			return map[string]any{"type": "Point", "coordinates": []float64{coords[0] + 1, coords[1] + 1}}, true
		}
		return nil, false
	})
	// Geometry in EPSG:3857 should be adjusted by reprojection when in GeoJSON mode
	gRes2 := ConvertGeometryLogicalValue(sample, geom).(map[string]any)
	ggeom2 := gRes2["geometry"].(map[string]any)
	require.Equal(t, []float64{2, 3}, ggeom2["coordinates"])
	// Geography with CRS84 should not invoke reprojection
	gaRes2 := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	g3 := gaRes2["geometry"].(map[string]any)
	require.Equal(t, []float64{1, 2}, g3["coordinates"])

	// Reset reprojector to avoid side effects
	SetGeospatialReprojector(nil)

	// LineString and Polygon parsing
	// LineString with two points: (0,0)->(1,1)
	ls := buildWKBLineString([][]float64{{0, 0}, {1, 1}})
	SetGeometryJSONMode(GeospatialModeGeoJSON)
	gLS := ConvertGeometryLogicalValue(ls, geom).(map[string]any)
	ggLS := gLS["geometry"].(map[string]any)
	require.Equal(t, "LineString", ggLS["type"])
	require.Equal(t, [][]float64{{0, 0}, {1, 1}}, ggLS["coordinates"])

	// Polygon with one ring (closed): (0,0),(1,0),(1,1),(0,1),(0,0)
	poly := buildWKBPolygon([][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}})
	gPoly := ConvertGeometryLogicalValue(poly, geom).(map[string]any)
	gPolyGeo := gPoly["geometry"].(map[string]any)
	require.Equal(t, "Polygon", gPolyGeo["type"])
	require.Equal(t, [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}}, gPolyGeo["coordinates"])

	// Hybrid raw selection: base64 vs hex
	SetGeographyJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(true)
	gaHybrid := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	_, hasHex := gaHybrid["wkb_hex"]
	b64, hasB64 := gaHybrid["wkb_b64"].(string)
	require.False(t, hasHex)
	require.True(t, hasB64)
	require.NotEmpty(t, b64)
	// switch back to hex
	SetGeospatialHybridRawBase64(false)
}

func Test_GeometryAndGeography_AdditionalBranches(t *testing.T) {
	// Invalid WKB to trigger fallback
	invalid := []byte{1, 99, 0, 0, 0} // little-endian, unknown type 99

	// Geometry: GeoJSON mode fallback to hex
	geom := parquet.NewGeometryType()
	crs := "EPSG:4326"
	geom.CRS = &crs
	SetGeometryJSONMode(GeospatialModeGeoJSON)
	g := ConvertGeometryLogicalValue(invalid, geom).(map[string]any)
	require.Equal(t, crs, g["crs"])
	require.NotEmpty(t, g["wkb_hex"].(string))

	// Geometry: Base64 mode
	SetGeometryJSONMode(GeospatialModeBase64)
	// Provide string input path
	g2 := ConvertGeometryLogicalValue(string(invalid), geom).(map[string]any)
	require.Equal(t, base64.StdEncoding.EncodeToString(invalid), g2["wkb_b64"])
	require.Equal(t, crs, g2["crs"])

	// Geometry: Hybrid with parse failure -> hex fallback
	SetGeometryJSONMode(GeospatialModeHybrid)
	g3 := ConvertGeometryLogicalValue(invalid, geom).(map[string]any)
	require.Equal(t, crs, g3["crs"])
	require.NotEmpty(t, g3["wkb_hex"]) // hex even if Hybrid and parse fails

	// Geography: Base64 mode with default algorithm when nil
	geog := parquet.NewGeographyType()
	crs2 := "OGC:CRS84"
	geog.CRS = &crs2
	geog.Algorithm = nil // ensure default
	SetGeographyJSONMode(GeospatialModeBase64)
	ga := ConvertGeographyLogicalValue(invalid, geog).(map[string]any)
	require.Equal(t, "SPHERICAL", ga["algorithm"]) // default
	require.Equal(t, crs2, ga["crs"])
	require.Equal(t, base64.StdEncoding.EncodeToString(invalid), ga["wkb_b64"])

	// Geography: Hybrid + reprojection when CRS != CRS84
	crs3 := "EPSG:3857"
	geog2 := parquet.NewGeographyType()
	geog2.CRS = &crs3
	algo := parquet.EdgeInterpolationAlgorithm_VINCENTY
	geog2.Algorithm = &algo
	SetGeographyJSONMode(GeospatialModeHybrid)
	// Simple valid point WKB
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	// reprojection hook: shift coords by +0.5
	SetGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
		if crs == crs3 {
			coords := gj["coordinates"].([]float64)
			return map[string]any{"type": "Point", "coordinates": []float64{coords[0] + 0.5, coords[1] + 0.5}}, true
		}
		return nil, false
	})
	defer SetGeospatialReprojector(nil)
	out := ConvertGeographyLogicalValue(sample, geog2).(map[string]any)
	require.Equal(t, crs3, out["crs"])
	require.Equal(t, "VINCENTY", out["algorithm"])
	// ensure hybrid includes geojson and raw hex/base64 per global flag; leave default (hex)
	gj := out["geojson"].(map[string]any)
	require.Equal(t, "Point", gj["type"])
	require.Equal(t, []float64{1.5, 2.5}, gj["coordinates"]) // shifted by reprojector
}

func Test_GeometryAndGeography_MoreModes(t *testing.T) {
	// Valid sample point WKB
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}

	// Geometry Hex mode
	SetGeometryJSONMode(GeospatialModeHex)
	geom := parquet.NewGeometryType()
	crs := "OGC:CRS84"
	geom.CRS = &crs
	g := ConvertGeometryLogicalValue(sample, geom).(map[string]any)
	require.Equal(t, crs, g["crs"])
	require.NotEmpty(t, g["wkb_hex"]) // raw hex

	// Geometry Hybrid with base64 raw
	SetGeometryJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(true)
	gh := ConvertGeometryLogicalValue(sample, geom).(map[string]any)
	require.Equal(t, crs, gh["crs"])
	require.NotNil(t, gh["geojson"]) // includes parsed geojson
	require.NotEmpty(t, gh["wkb_b64"])
	require.NotContains(t, gh, "wkb_hex")
	SetGeospatialHybridRawBase64(false)

	// Geography GeoJSON fallback to hex when parse fails
	SetGeographyJSONMode(GeospatialModeGeoJSON)
	invalid := []byte{1, 99, 0, 0, 0}
	geog := parquet.NewGeographyType()
	crs2 := "OGC:CRS84"
	geog.CRS = &crs2
	out := ConvertGeographyLogicalValue(invalid, geog).(map[string]any)
	require.Equal(t, crs2, out["crs"])
	require.Equal(t, "SPHERICAL", out["algorithm"]) // default
	require.NotEmpty(t, out["wkb_hex"])             // fallback hex

	// Geography Hex mode
	SetGeographyJSONMode(GeospatialModeHex)
	out2 := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	require.Equal(t, crs2, out2["crs"])
	require.Equal(t, "SPHERICAL", out2["algorithm"]) // still default
	require.NotEmpty(t, out2["wkb_hex"])             // hex

	// Geography Hybrid with base64 raw
	SetGeographyJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(true)
	out3 := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	require.Equal(t, crs2, out3["crs"])
	require.Equal(t, "SPHERICAL", out3["algorithm"]) // default
	require.NotNil(t, out3["geojson"])
	require.NotEmpty(t, out3["wkb_b64"]) // base64 chosen
	require.NotContains(t, out3, "wkb_hex")
	// reset
	SetGeospatialHybridRawBase64(false)
}

// helpers to construct little-endian WKB for tests
func buildWKBLineString(coords [][]float64) []byte {
	buf := []byte{1, 2, 0, 0, 0}
	n := uint32(len(coords))
	buf = append(buf, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
	for _, c := range coords {
		buf = append(buf, f64le(c[0])...)
		buf = append(buf, f64le(c[1])...)
	}
	return buf
}

func buildWKBPolygon(rings [][][]float64) []byte {
	buf := []byte{1, 3, 0, 0, 0}
	rn := uint32(len(rings))
	buf = append(buf, byte(rn), byte(rn>>8), byte(rn>>16), byte(rn>>24))
	for _, ring := range rings {
		n := uint32(len(ring))
		buf = append(buf, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
		for _, c := range ring {
			buf = append(buf, f64le(c[0])...)
			buf = append(buf, f64le(c[1])...)
		}
	}
	return buf
}

func f64le(v float64) []byte {
	u := math.Float64bits(v)
	return []byte{byte(u), byte(u >> 8), byte(u >> 16), byte(u >> 24), byte(u >> 32), byte(u >> 40), byte(u >> 48), byte(u >> 56)}
}

func Test_ConvertFloat16LogicalValue(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"one", string([]byte{0x3c, 0x00}), float32(1.0)},
		{"half", string([]byte{0x38, 0x00}), float32(0.5)},
		{"neg_two", string([]byte{0xc0, 0x00}), float32(-2.0)},
		{"wrong_len", string([]byte{0x00}), string([]byte{0x00})},
		{"nil", nil, nil},
		// NaN should return the raw input unchanged
		{"nan_raw_return", string([]byte{0x7e, 0x00}), string([]byte{0x7e, 0x00})},
		// []byte input path
		{"bytes_input", []byte{0x3c, 0x00}, float32(1.0)},
		// unsupported type returns unchanged
		{"unsupported_type", 123, 123},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ConvertFloat16LogicalValue(tc.in)
			require.Equal(t, tc.want, got)
		})
	}

	// Additional epsilon checks for subnormals and infinities
	t.Run("subnormal_pos", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x00, 0x01})).(float32)
		expected := float32(1.0/1024.0) / float32(1<<14)
		require.InEpsilon(t, expected, got, 1e-6)
	})
	t.Run("subnormal_neg", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x80, 0x01})).(float32)
		expected := -float32(1.0/1024.0) / float32(1<<14)
		require.InEpsilon(t, expected, got, 1e-6)
	})
	t.Run("pos_inf", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x7c, 0x00})).(float32)
		require.True(t, math.IsInf(float64(got), 1))
	})
	t.Run("neg_inf", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0xfc, 0x00})).(float32)
		require.True(t, math.IsInf(float64(got), -1))
	})
	t.Run("zero", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x00, 0x00})).(float32)
		require.Equal(t, float32(0.0), got)
	})
	t.Run("wrong_len_bytes", func(t *testing.T) {
		in := []byte{0x00}
		got := ConvertFloat16LogicalValue(in)
		require.Equal(t, in, got)
	})
	t.Run("nan_bytes_raw", func(t *testing.T) {
		in := []byte{0x7e, 0x00}
		got := ConvertFloat16LogicalValue(in)
		require.Equal(t, in, got)
	})
}

func Test_ConvertIntegerLogicalValue(t *testing.T) {
	pT32 := parquet.TypePtr(parquet.Type_INT32)
	pT64 := parquet.TypePtr(parquet.Type_INT64)
	mkInt := func(width int8, signed bool) *parquet.IntType {
		it := parquet.NewIntType()
		it.BitWidth = width
		it.IsSigned = signed
		return it
	}

	tests := []struct {
		name string
		val  any
		pT   *parquet.Type
		it   *parquet.IntType
		want any
	}{
		{"int8_from_int32", int32(-5), pT32, mkInt(8, true), int8(-5)},
		{"uint8_from_int32", int32(250), pT32, mkInt(8, false), uint8(250)},
		{"int16_from_int32", int32(-200), pT32, mkInt(16, true), int16(-200)},
		{"uint16_from_int32", int32(50000), pT32, mkInt(16, false), uint16(50000)},
		{"int32_from_int32", int32(-100000), pT32, mkInt(32, true), int32(-100000)},
		// int64 paths
		{"int64_signed_64", int64(-7), pT64, mkInt(64, true), int64(-7)},
		{"int64_unsigned_64", int64(7), pT64, mkInt(64, false), uint64(7)},
		// downcast 64->32 branches
		{"int64_signed_32", int64(-8), pT64, mkInt(32, true), int32(-8)},
		{"int64_unsigned_32", int64(8), pT64, mkInt(32, false), uint32(8)},
		// nil/unsupported handling
		{"nil_val", nil, pT32, mkInt(8, true), nil},
		{"nil_it", int32(1), pT32, nil, int32(1)},
		{"unsupported_val_type", "x", pT32, mkInt(8, true), "x"},
		// cast32 default branches (non 8/16/32 widths)
		{"int32_signed_default_width", int32(5), pT32, mkInt(24, true), int32(5)},
		{"int32_unsigned_default_width", int32(5), pT32, mkInt(24, false), uint32(5)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ConvertIntegerLogicalValue(tc.val, tc.pT, tc.it)
			require.Equal(t, tc.want, got)
		})
	}
}

func Test_Geography_HybridFallbackAndStringInput(t *testing.T) {
	// invalid WKB should fallback to hex under Hybrid mode
	invalid := []byte{1, 99, 0, 0, 0}
	geog := parquet.NewGeographyType()
	crs := "OGC:CRS84"
	geog.CRS = &crs
	SetGeographyJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(true)
	out := ConvertGeographyLogicalValue(invalid, geog).(map[string]any)
	require.Equal(t, crs, out["crs"])
	require.NotEmpty(t, out["wkb_hex"]) // fallback is hex regardless of base64 flag
	SetGeospatialHybridRawBase64(false)

	// Hex mode with string input
	SetGeographyJSONMode(GeospatialModeHex)
	// Valid point(1,2) sample
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	out2 := ConvertGeographyLogicalValue(string(sample), geog).(map[string]any)
	require.Equal(t, crs, out2["crs"])
	require.NotEmpty(t, out2["wkb_hex"]) // hex

	// Hybrid mode with hex raw when base64 flag is false
	SetGeographyJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(false)
	out3 := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	require.Equal(t, crs, out3["crs"])
	require.NotNil(t, out3["geojson"])   // include geojson
	require.NotEmpty(t, out3["wkb_hex"]) // hex raw selected
	require.NotContains(t, out3, "wkb_b64")
}

func Test_ConvertGeography_ReprojectorNoOp(t *testing.T) {
	// Valid point(1,2)
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	geog := parquet.NewGeographyType()
	crs := "EPSG:3857" // non-CRS84 to trigger reprojection check
	geog.CRS = &crs
	// Set reprojector that returns false (no reprojection)
	SetGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
		return nil, false
	})
	defer SetGeospatialReprojector(nil)
	SetGeographyJSONMode(GeospatialModeGeoJSON)
	out := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	g := out["geometry"].(map[string]any)
	require.Equal(t, []float64{1, 2}, g["coordinates"]) // unchanged because reprojector declined
}

func Test_ConvertGeography_GeoJSON_NoReproject(t *testing.T) {
	// Valid point(1,2)
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	geog := parquet.NewGeographyType()
	crs := "EPSG:4326" // non-CRS84
	geog.CRS = &crs
	// Ensure reprojector is nil
	SetGeospatialReprojector(nil)
	SetGeographyJSONMode(GeospatialModeGeoJSON)
	out := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	g := out["geometry"].(map[string]any)
	require.Equal(t, []float64{1, 2}, g["coordinates"]) // unchanged geometry only
}

func Test_ConvertGeography_Defaults_NilGeoPointer(t *testing.T) {
	// Use Base64 mode to avoid wkbToGeoJSON path; check defaults when geo=nil
	invalid := []byte{1, 99, 0, 0, 0}
	SetGeographyJSONMode(GeospatialModeBase64)
	out := ConvertGeographyLogicalValue(invalid, nil).(map[string]any)
	require.Equal(t, "OGC:CRS84", out["crs"])       // default CRS
	require.Equal(t, "SPHERICAL", out["algorithm"]) // default algorithm
	require.Equal(t, base64.StdEncoding.EncodeToString(invalid), out["wkb_b64"])
}

func Test_ConvertBSONLogicalValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name:     "byte_slice_input",
			val:      []byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
			expected: "FgAAABBpAAEAAAAA", // Base64 encoded BSON data
		},
		{
			name:     "string_input",
			val:      string([]byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x02, 0x00, 0x00, 0x00, 0x00}),
			expected: "FgAAABBpAAIAAAAA", // Base64 encoded BSON data
		},
		{
			name:     "empty_byte_slice",
			val:      []byte{},
			expected: "",
		},
		{
			name:     "empty_string",
			val:      "",
			expected: "",
		},
		{
			name:     "non_binary_value",
			val:      123,
			expected: 123, // Should return as-is for non-binary values
		},
		{
			name:     "complex_bson_document",
			val:      []byte{0x27, 0x00, 0x00, 0x00, 0x02, 'k', 'e', 'y', 0x00, 0x06, 0x00, 0x00, 0x00, 'v', 'a', 'l', 'u', 'e', 0x00, 0x10, 'n', 'u', 'm', 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00},
			expected: "JwAAAAJrZXkABgAAAHZhbHVlABBudW0AKgAAAAA=", // Base64 encoded BSON document
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertBSONLogicalValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}
