package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NewSchemaHandlerFromJSON(t *testing.T) {
	tests := []struct {
		name          string
		jsonSchema    string
		expectError   bool
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
			expectError:   false,
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
			expectError: true,
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
			expectError:   true,
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
			expectError:   true,
			errorContains: "MAP needs exactly 2 fields",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := NewSchemaHandlerFromJSON(tt.jsonSchema)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)

			if tt.expectedElems != nil {
				require.Equal(t, *tt.expectedElems, len(handler.SchemaElements))
			}
		})
	}
}
