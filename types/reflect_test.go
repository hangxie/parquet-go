package types

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

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
