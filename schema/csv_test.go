package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func TestNewSchemaHandlerFromMetadata(t *testing.T) {
	tests := []struct {
		name              string
		metadata          []string
		expectError       bool
		expectedError     string
		expectedNumFields *int // nil means don't check
		validateSchema    func(t *testing.T, schema *SchemaHandler)
	}{
		// Success cases
		{
			name: "single_field",
			metadata: []string{
				"name=id, type=INT64",
			},
			expectError:       false,
			expectedNumFields: func() *int { n := 2; return &n }(), // root + 1 field
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
				require.Equal(t, "Id", schema.SchemaElements[1].Name)
				require.Equal(t, parquet.Type_INT64, *schema.SchemaElements[1].Type)
			},
		},
		{
			name: "multiple_fields",
			metadata: []string{
				"name=id, type=INT64",
				"name=name, type=BYTE_ARRAY, convertedtype=UTF8",
				"name=age, type=INT32",
			},
			expectError:       false,
			expectedNumFields: func() *int { n := 4; return &n }(), // root + 3 fields
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
				require.Equal(t, "Id", schema.SchemaElements[1].Name)
				require.Equal(t, "Name", schema.SchemaElements[2].Name)
				require.Equal(t, "Age", schema.SchemaElements[3].Name)
			},
		},
		{
			name: "complex_structure",
			metadata: []string{
				"name=id, type=INT64",
				"name=name, type=BYTE_ARRAY, convertedtype=UTF8",
				"name=email, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL",
				"name=age, type=INT32",
				"name=salary, type=FLOAT",
				"name=is_active, type=BOOLEAN",
				"name=tags, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED",
				"name=score, type=DOUBLE, scale=2, precision=10",
			},
			expectError:       false,
			expectedNumFields: func() *int { n := 9; return &n }(), // root + 8 fields
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				// Verify root
				root := schema.SchemaElements[0]
				require.Equal(t, int32(8), *root.NumChildren)

				// Test specific fields
				idField := schema.SchemaElements[1]
				require.Equal(t, "Id", idField.Name)
				require.Equal(t, parquet.Type_INT64, *idField.Type)

				emailField := schema.SchemaElements[3]
				require.Equal(t, "Email", emailField.Name)
				require.Equal(t, parquet.FieldRepetitionType_OPTIONAL, *emailField.RepetitionType)

				tagsField := schema.SchemaElements[7]
				require.Equal(t, "Tags", tagsField.Name)
				require.Equal(t, parquet.FieldRepetitionType_REPEATED, *tagsField.RepetitionType)

				scoreField := schema.SchemaElements[8]
				require.Equal(t, "Score", scoreField.Name)
				require.Equal(t, int32(2), *scoreField.Scale)
				require.Equal(t, int32(10), *scoreField.Precision)

				// Verify basic operations
				require.NotEmpty(t, schema.GetRootExName())
				require.NotEmpty(t, schema.InPathToExPath)
				require.NotEmpty(t, schema.ExPathToInPath)
			},
		},
		// Converted Types
		{
			name: "utf8_converted_type",
			metadata: []string{
				"name=string_field, type=BYTE_ARRAY, convertedtype=UTF8",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.ConvertedType_UTF8, *field.ConvertedType)
			},
		},
		{
			name: "decimal_converted_type",
			metadata: []string{
				"name=decimal_field, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.ConvertedType_DECIMAL, *field.ConvertedType)
				require.Equal(t, int32(2), *field.Scale)
				require.Equal(t, int32(10), *field.Precision)
			},
		},
		{
			name: "timestamp_millis",
			metadata: []string{
				"name=timestamp_field, type=INT64, convertedtype=TIMESTAMP_MILLIS",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.ConvertedType_TIMESTAMP_MILLIS, *field.ConvertedType)
			},
		},
		// Field Types
		{
			name: "boolean_type",
			metadata: []string{
				"name=bool_field, type=BOOLEAN",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.Type_BOOLEAN, *field.Type)
			},
		},
		{
			name: "int32_type",
			metadata: []string{
				"name=int32_field, type=INT32",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.Type_INT32, *field.Type)
			},
		},
		{
			name: "float_type",
			metadata: []string{
				"name=float_field, type=FLOAT",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.Type_FLOAT, *field.Type)
			},
		},
		{
			name: "double_type",
			metadata: []string{
				"name=double_field, type=DOUBLE",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.Type_DOUBLE, *field.Type)
			},
		},
		{
			name: "byte_array_type",
			metadata: []string{
				"name=byte_array_field, type=BYTE_ARRAY",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.Type_BYTE_ARRAY, *field.Type)
			},
		},
		// Repetition Types
		{
			name: "required_repetition",
			metadata: []string{
				"name=required_field, type=INT32, repetitiontype=REQUIRED",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.FieldRepetitionType_REQUIRED, *field.RepetitionType)
			},
		},
		{
			name: "optional_repetition",
			metadata: []string{
				"name=optional_field, type=INT32, repetitiontype=OPTIONAL",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.FieldRepetitionType_OPTIONAL, *field.RepetitionType)
			},
		},
		{
			name: "repeated_repetition",
			metadata: []string{
				"name=repeated_field, type=INT32, repetitiontype=REPEATED",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, parquet.FieldRepetitionType_REPEATED, *field.RepetitionType)
			},
		},
		// Edge Cases
		{
			name: "field_with_all_attributes",
			metadata: []string{
				"name=complex_field, type=BYTE_ARRAY, convertedtype=DECIMAL, repetitiontype=OPTIONAL, scale=2, precision=10, length=16",
			},
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				field := schema.SchemaElements[1]
				require.Equal(t, "Complex_field", field.Name)
				require.Equal(t, parquet.Type_BYTE_ARRAY, *field.Type)
				require.Equal(t, parquet.ConvertedType_DECIMAL, *field.ConvertedType)
				require.Equal(t, parquet.FieldRepetitionType_OPTIONAL, *field.RepetitionType)
				require.Equal(t, int32(2), *field.Scale)
				require.Equal(t, int32(10), *field.Precision)
				require.Equal(t, int32(16), *field.TypeLength)
			},
		},
		{
			name:              "nil_metadata",
			metadata:          nil,
			expectError:       false,
			expectedNumFields: func() *int { n := 1; return &n }(), // just root
		},
		{
			name:              "empty_metadata",
			metadata:          []string{},
			expectError:       false,
			expectedNumFields: func() *int { n := 1; return &n }(), // just root
		},
		// Error Cases - Schema Element Errors
		{
			name: "invalid_scale_value",
			metadata: []string{
				"name=field, type=INT64, scale=invalid",
			},
			expectError:   true,
			expectedError: "parse metadata",
		},
		{
			name: "invalid_precision_value",
			metadata: []string{
				"name=field, type=INT64, precision=invalid",
			},
			expectError:   true,
			expectedError: "parse metadata",
		},
		{
			name: "invalid_length_value",
			metadata: []string{
				"name=field, type=BYTE_ARRAY, length=invalid",
			},
			expectError:   true,
			expectedError: "parse metadata",
		},
		// Error Cases - StringToTag Errors
		{
			name: "completely_invalid_tag",
			metadata: []string{
				"this is not a valid tag format at all",
			},
			expectError:   true,
			expectedError: "parse metadata",
		},
		{
			name: "invalid_type_value",
			metadata: []string{
				"name=field, type=INVALID_TYPE",
			},
			expectError:   true,
			expectedError: "not a valid Type string",
		},
		{
			name: "invalid_repetition_type_value",
			metadata: []string{
				"name=field, type=INT32, repetitiontype=INVALID_REP_TYPE",
			},
			expectError:   true,
			expectedError: "not a valid FieldRepetitionType string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewSchemaHandlerFromMetadata(tt.metadata)

			if tt.expectError {
				require.Error(t, err)
				if tt.expectedError != "" {
					require.Contains(t, err.Error(), tt.expectedError)
				}
				require.Nil(t, result)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)

			if tt.expectedNumFields != nil {
				require.Equal(t, *tt.expectedNumFields, len(result.SchemaElements))
			}

			if tt.validateSchema != nil {
				tt.validateSchema(t, result)
			}
		})
	}
}
