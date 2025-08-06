package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_InterfaceToParquetType(t *testing.T) {
	tests := []struct {
		name  string
		value any
		pT    *parquet.Type
	}{
		{
			name:  "boolean_true",
			value: true,
			pT:    parquet.TypePtr(parquet.Type_BOOLEAN),
		},
		{
			name:  "int32_value",
			value: int32(42),
			pT:    parquet.TypePtr(parquet.Type_INT32),
		},
		{
			name:  "int64_value",
			value: int64(42),
			pT:    parquet.TypePtr(parquet.Type_INT64),
		},
		{
			name:  "float32_value",
			value: float32(3.14),
			pT:    parquet.TypePtr(parquet.Type_FLOAT),
		},
		{
			name:  "float64_value",
			value: float64(3.14),
			pT:    parquet.TypePtr(parquet.Type_DOUBLE),
		},
		{
			name:  "string_value",
			value: "hello",
			pT:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
		},
		{
			name:  "nil_value",
			value: nil,
			pT:    parquet.TypePtr(parquet.Type_BOOLEAN),
		},
		{
			name:  "invalid_type",
			value: 42,
			pT:    parquet.TypePtr(parquet.Type(-1)), // Invalid type
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := InterfaceToParquetType(tt.value, tt.pT)
			require.NoError(t, err)
			if tt.value == nil {
				require.Nil(t, result)
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
