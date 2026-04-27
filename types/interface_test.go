package types

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestInterfaceToParquetType(t *testing.T) {
	type myBool bool
	type myStr string

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
		// Named bool type: exercises convertToBool rv.Bool() path
		{
			name:     "named_bool_to_bool",
			value:    myBool(true),
			pT:       parquet.TypePtr(parquet.Type_BOOLEAN),
			expected: true,
		},
		// int to int64: exercises convertToInt64 rv.Int() path
		{
			name:     "int_to_int64",
			value:    int(42),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			expected: int64(42),
		},
		// unsigned int types to int32
		{
			name:     "uint_to_int32",
			value:    uint(42),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: int32(42),
		},
		{
			name:     "uint8_to_int32",
			value:    uint8(255),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: int32(255),
		},
		{
			name:     "uint16_to_int32",
			value:    uint16(1000),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: int32(1000),
		},
		{
			name:     "uint32_to_int32",
			value:    uint32(42),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: int32(42),
		},
		// unsigned int types to int64
		{
			name:     "uint_to_int64",
			value:    uint(42),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			expected: int64(42),
		},
		{
			name:     "uint32_to_int64",
			value:    uint32(42),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			expected: int64(42),
		},
		{
			name:     "uint64_to_int64",
			value:    uint64(42),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			expected: int64(42),
		},
		// []byte to string: exercises convertToString []byte path
		{
			name:     "bytes_to_string",
			value:    []byte("hello"),
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: "hello",
		},
		// Named string type: exercises convertToString rv.String() path
		{
			name:     "named_string_to_string",
			value:    myStr("hello"),
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: "hello",
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
