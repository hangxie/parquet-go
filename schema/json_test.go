package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSchemaHandlerFromJSON(t *testing.T) {
	tests := []struct {
		name          string
		jsonSchema    string
		errorContains string
		expectedElems *int // nil means don't check elements count
	}{
		{
			name: "valid_schema",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{"Tag": "name=name, inname=Name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
				{"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"}
			  ]
			}
			`,
			expectedElems: func() *int { e := 3; return &e }(), // goroot + 2 fields
		},
		{
			name: "improper_json_syntax",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{"Tag": "name=name, inname=Name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
				{"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"}
				,,
			  ]
			}
			`,
			errorContains: "unmarshal json schema",
		},
		{
			name: "list_needs_exact_one_field",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{
				  "Tag": "name=name, inname=Name, type=LIST, repetitiontype=REQUIRED",
				  "Fields": []
				}
			  ]
			}
			`,
			errorContains: "LIST needs exactly 1 field",
		},
		{
			name: "map_needs_exact_two_fields",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{
				  "Tag": "name=name, inname=Name, type=MAP, repetitiontype=REQUIRED",
				  "Fields": []
				}
			  ]
			}
			`,
			errorContains: "MAP needs exactly 2 fields",
		},
		{
			name: "group_invalid_convertedtype",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{
				  "Tag": "name=nested, inname=Nested, convertedtype=INVALID_CONVERTED_TYPE, repetitiontype=REQUIRED",
				  "Fields": [
					{"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"}
				  ]
				}
			  ]
			}
			`,
			errorContains: "with convertedtype [INVALID_CONVERTED_TYPE]",
		},
		{
			name: "list_invalid_logicaltype",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{
				  "Tag": "name=items, inname=Items, type=LIST, logicaltype=DECIMAL, logicaltype.precision=bad, repetitiontype=REQUIRED",
				  "Fields": [
					{"Tag": "name=element, inname=Element, type=INT32, repetitiontype=REQUIRED"}
				  ]
				}
			  ]
			}
			`,
			errorContains: "parse logicaltype.precision",
		},
		{
			name: "variant_type",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{"Tag": "name=data, inname=Data, type=VARIANT, logicaltype=VARIANT, repetitiontype=REQUIRED"}
			  ]
			}
			`,
			expectedElems: func() *int { e := 4; return &e }(), // root + variant group + metadata + value
		},
		{
			name: "variant_invalid_convertedtype",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{"Tag": "name=data, inname=Data, type=VARIANT, logicaltype=VARIANT, convertedtype=INVALID_CONVERTED_TYPE, repetitiontype=REQUIRED"}
			  ]
			}
			`,
			errorContains: "with convertedtype [INVALID_CONVERTED_TYPE]",
		},
		{
			name: "variant_with_specification_version",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{"Tag": "name=data, inname=Data, type=VARIANT, logicaltype=VARIANT, logicaltype.specification_version=1, repetitiontype=OPTIONAL"}
			  ]
			}
			`,
			expectedElems: func() *int { e := 4; return &e }(),
		},
		{
			name: "variant_with_encoding_and_compression",
			jsonSchema: `
			{
			  "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
			  "Fields": [
				{"Tag": "name=data, type=VARIANT, encoding=PLAIN, compression=GZIP, repetitiontype=REQUIRED"}
			  ]
			}
			`,
			expectedElems: func() *int { e := 4; return &e }(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := NewSchemaHandlerFromJSON(tt.jsonSchema)

			if tt.errorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorContains)
				return
			}

			require.NoError(t, err)

			if tt.expectedElems != nil {
				require.Equal(t, *tt.expectedElems, len(handler.SchemaElements))
			}

			if tt.name == "variant_with_encoding_and_compression" {
				require.Equal(t, 4, len(handler.SchemaElements))
				// Metadata child
				require.Equal(t, "Metadata", handler.SchemaElements[2].Name)
				require.Equal(t, "PLAIN", handler.Infos[2].Encoding.String())
				require.NotNil(t, handler.Infos[2].CompressionType)
				require.Equal(t, "GZIP", handler.Infos[2].CompressionType.String())

				// Value child
				require.Equal(t, "Value", handler.SchemaElements[3].Name)
				require.Equal(t, "PLAIN", handler.Infos[3].Encoding.String())
				require.NotNil(t, handler.Infos[3].CompressionType)
				require.Equal(t, "GZIP", handler.Infos[3].CompressionType.String())
			}
		})
	}
}
