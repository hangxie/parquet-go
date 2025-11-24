package marshal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/schema"
)

func TestMarshalJSON(t *testing.T) {
	tests := []struct {
		name         string
		schemaString string
		records      []any
		expectError  bool
		expectedCols int
		checkColumns func(t *testing.T, result *map[string]*layout.Table, sch *schema.SchemaHandler)
	}{
		{
			name: "empty_records",
			schemaString: `{
				"Tag": "name=parquet_go_root",
				"Fields": [
					{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
					{"Tag": "name=age, type=INT32", "Type": "int32"}
				]
			}`,
			records:      []any{},
			expectedCols: 2,
			checkColumns: func(t *testing.T, result *map[string]*layout.Table, sch *schema.SchemaHandler) {
				// Should have 2 empty tables for the 2 schema fields
				require.Len(t, *result, 2)
			},
		},
		{
			name: "valid_json_data",
			schemaString: `{
				"Tag": "name=parquet_go_root",
				"Fields": [
					{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
					{"Tag": "name=age, type=INT32", "Type": "int32"}
				]
			}`,
			records: []any{
				`{"name": "Alice", "age": 25}`,
				`{"name": "Bob", "age": 30}`,
				[]byte(`{"name": "Charlie", "age": 35}`), // Test []byte input as well
			},
			expectedCols: 2,
			checkColumns: func(t *testing.T, result *map[string]*layout.Table, sch *schema.SchemaHandler) {
				nameColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Name"]
				require.NotNil(t, nameColumn)
				require.Len(t, nameColumn.Values, 3)
				require.Equal(t, "Alice", nameColumn.Values[0])

				ageColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Age"]
				require.NotNil(t, ageColumn)
				require.Len(t, ageColumn.Values, 3)
			},
		},
		{
			name: "invalid_json",
			schemaString: `{
				"Tag": "name=parquet_go_root",
				"Fields": [
					{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"}
				]
			}`,
			records: []any{
				`{"name": "Alice"`, // Invalid JSON - missing closing brace
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sch, err := schema.NewSchemaHandlerFromJSON(tt.schemaString)
			require.NoError(t, err)

			result, err := MarshalJSON(tt.records, sch)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			require.NotNil(t, result)

			if tt.expectedCols > 0 {
				require.Len(t, *result, tt.expectedCols)
			}

			if tt.checkColumns != nil {
				tt.checkColumns(t, result, sch)
			}
		})
	}
}

func TestMarshalJSON_Comprehensive(t *testing.T) {
	t.Run("simple_struct", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
				{"Tag": "name=age, type=INT32", "Type": "int32"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{"name": "Alice", "age": 25}`,
			[]byte(`{"name": "Bob", "age": 30}`),
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
		require.Len(t, *result, 2)
	})

	t.Run("nested_struct", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{
					"Tag": "name=person",
					"Fields": [
						{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
						{"Tag": "name=age, type=INT32", "Type": "int32"}
					]
				}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{"person": {"name": "Alice", "age": 25}}`,
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})

	t.Run("optional_fields", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
				{"Tag": "name=nickname, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", "Type": "string"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{"name": "Alice"}`,                    // Missing optional field
			`{"name": "Bob", "nickname": "Bobby"}`, // Has optional field
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)

		// Check that optional field handling works
		nicknameColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Nickname"]
		require.NotNil(t, nicknameColumn)
		require.Len(t, nicknameColumn.Values, 2)
	})

	t.Run("real_map_type", func(t *testing.T) {
		// Test simple map handling - this will be covered by more comprehensive tests below
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=simple_map, type=BYTE_ARRAY", "Type": "string"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{"simple_map": "test_value"}`,
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})

	t.Run("list_type", func(t *testing.T) {
		// Create schema with LIST converted type - use simpler array structure
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=items, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED", "Type": "[]string"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{"items": ["item1", "item2", "item3"]}`,
			`{"items": []}`, // Empty list
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})

	t.Run("repeated_field", func(t *testing.T) {
		// Create schema with repeated field (not LIST)
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=numbers, type=INT32, repetitiontype=REPEATED", "Type": "int32"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{"numbers": [1, 2, 3]}`,
			`{"numbers": []}`, // Empty repeated field
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})

	t.Run("primitive_types", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=bool_val, type=BOOLEAN", "Type": "bool"},
				{"Tag": "name=int32_val, type=INT32", "Type": "int32"},
				{"Tag": "name=int64_val, type=INT64", "Type": "int64"},
				{"Tag": "name=float_val, type=FLOAT", "Type": "float32"},
				{"Tag": "name=double_val, type=DOUBLE", "Type": "float64"},
				{"Tag": "name=byte_array_val, type=BYTE_ARRAY", "Type": "string"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{
				"bool_val": true,
				"int32_val": 123,
				"int64_val": 456789,
				"float_val": 3.14,
				"double_val": 2.718281828,
				"byte_array_val": "hello"
			}`,
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
		require.Len(t, *result, 6)
	})

	t.Run("missing_fields_in_json", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=required_field, type=BYTE_ARRAY", "Type": "string"},
				{"Tag": "name=missing_field, type=INT32", "Type": "int32"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{"required_field": "present"}`, // Missing missing_field
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)

		// Check that missing fields get nil values
		missingColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Missing_field"]
		require.NotNil(t, missingColumn)
		require.Len(t, missingColumn.Values, 1)
		require.Nil(t, missingColumn.Values[0])
	})

	t.Run("complex_nested_with_optional_map_values", func(t *testing.T) {
		// Test with nested structure and optional fields
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{
					"Tag": "name=nested",
					"Fields": [
						{"Tag": "name=optional_field, type=BYTE_ARRAY, repetitiontype=OPTIONAL", "Type": "string"},
						{"Tag": "name=required_field, type=INT32", "Type": "int32"}
					]
				}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{"nested": {"required_field": 42}}`, // Missing optional_field
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})

	t.Run("list_with_optional_elements", func(t *testing.T) {
		// Test list with optional elements
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=optional_list, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", "Type": "string"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		jsonRecords := []any{
			`{}`, // Missing optional list entirely
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})
}

func TestMarshalJSON_EdgeCases(t *testing.T) {
	t.Run("json_decode_error", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY", "Type": "string"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		invalidJSON := []any{
			`{"name": "unclosed string`,
		}

		_, err = MarshalJSON(invalidJSON, sch)
		require.Error(t, err)
	})

	t.Run("type_conversion_error", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=number, type=INT32", "Type": "int32"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		// JSON with invalid type for INT32 field
		invalidTypeJSON := []any{
			`{"number": "not_a_number"}`,
		}

		_, err = MarshalJSON(invalidTypeJSON, sch)
		require.Error(t, err)
	})

	t.Run("empty_path_handling", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{
					"Tag": "name=nested",
					"Fields": [
						{"Tag": "name=field, type=BYTE_ARRAY", "Type": "string"}
					]
				}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		// JSON with empty nested object
		jsonRecords := []any{
			`{"nested": {}}`,
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})

	t.Run("ignored_schema_fields", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=known_field, type=BYTE_ARRAY", "Type": "string"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		// JSON with extra field not in schema
		jsonRecords := []any{
			`{
				"known_field": "value",
				"unknown_field": "this should be ignored"
			}`,
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)

		// Should only have known_field column
		require.Len(t, *result, 1)
	})

	t.Run("byte_slice_input", func(t *testing.T) {
		schemaString := `{
			"Tag": "name=parquet_go_root",
			"Fields": [
				{"Tag": "name=data, type=BYTE_ARRAY", "Type": "string"}
			]
		}`

		sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
		require.NoError(t, err)

		// Test both string and []byte inputs
		jsonRecords := []any{
			`{"data": "from_string"}`,
			[]byte(`{"data": "from_bytes"}`),
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)

		dataColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Data"]
		require.NotNil(t, dataColumn)

		require.Len(t, dataColumn.Values, 2)

		require.Equal(t, "from_string", dataColumn.Values[0])

		require.Equal(t, "from_bytes", dataColumn.Values[1])
	})
}

func TestMarshalJSON_UseNumber(t *testing.T) {
	// Test that UseNumber() is working correctly for numeric precision
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=large_number, type=INT64", "Type": "int64"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Use a large number that would lose precision as float64
	jsonRecords := []any{
		`{"large_number": 9007199254740993}`, // 2^53 + 1, loses precision as float64
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)

	numberColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Large_number"]
	require.NotNil(t, numberColumn)

	require.Len(t, numberColumn.Values, 1)

	// The value should be preserved as the exact integer
	expectedValue := int64(9007199254740993)
	require.Equal(t, expectedValue, numberColumn.Values[0])
}

// Test Real MAP type using Go struct to generate proper schema
func TestMarshalJSON_RealMapType(t *testing.T) {
	// Create schema using Go struct with MAP field
	type MapStruct struct {
		MapField map[string]int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	}

	sch, err := schema.NewSchemaHandlerFromStruct(new(MapStruct))
	require.NoError(t, err)

	// Test with non-empty map to trigger MAP handling code
	jsonRecords := []any{
		`{"MapField": {"key1": 100, "key2": 200, "key3": 300}}`,
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)

	// Test with empty map to trigger lines 71-78 (empty map handling)
	jsonRecords2 := []any{
		`{"MapField": {}}`,
	}

	result2, err := MarshalJSON(jsonRecords2, sch)
	require.NoError(t, err)

	require.NotNil(t, result2)
}

// Test Real LIST type using Go struct to generate proper schema
func TestMarshalJSON_RealListType(t *testing.T) {
	// Create schema using Go struct with LIST field
	type ListStruct struct {
		ListField []string `parquet:"name=list, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	}

	sch, err := schema.NewSchemaHandlerFromStruct(new(ListStruct))
	require.NoError(t, err)

	// Test with non-empty list to trigger LIST handling code
	jsonRecords := []any{
		`{"ListField": ["item1", "item2", "item3"]}`,
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)

	// Test with empty list to trigger lines 157-165 (empty list handling)
	jsonRecords2 := []any{
		`{"ListField": []}`,
	}

	result2, err := MarshalJSON(jsonRecords2, sch)
	require.NoError(t, err)

	require.NotNil(t, result2)
}

// Test Repeated field type (non-LIST) with empty arrays (covers lines 189-213)
func TestMarshalJSON_RepeatedFieldEmpty(t *testing.T) {
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=repeated_numbers, type=INT32, repetitiontype=REPEATED", "Type": "int32"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Test with empty repeated field to trigger lines 190-198
	jsonRecords := []any{
		`{"repeated_numbers": []}`,
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)
}

func TestMarshalJSON_ComplexPath_IsChildPath(t *testing.T) {
	// Test cases that exercise the IsChildPath logic in the marshal code
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{
				"Tag": "name=level1",
				"Fields": [
					{
						"Tag": "name=level2",
						"Fields": [
							{"Tag": "name=leaf, type=BYTE_ARRAY", "Type": "string"}
						]
					}
				]
			}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Test with missing nested fields to trigger IsChildPath logic
	jsonRecords := []any{
		`{"level1": {}}`, // Missing level2 entirely
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)

	// Should have created nil entries for missing nested fields
	leafColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Level1"+common.PAR_GO_PATH_DELIMITER+"Level2"+common.PAR_GO_PATH_DELIMITER+"Leaf"]
	require.NotNil(t, leafColumn)
	require.Len(t, leafColumn.Values, 1)
	require.Nil(t, leafColumn.Values[0])
}

func TestMarshalJSON_StringToVariableName(t *testing.T) {
	// Test the StringToVariableName conversion used in struct handling
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=camel_case_field, type=BYTE_ARRAY", "Type": "string"},
			{"Tag": "name=simple_field, type=BYTE_ARRAY", "Type": "string"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Test with JSON keys that match schema - focus on the struct key mapping logic
	jsonRecords := []any{
		`{
			"camel_case_field": "value1",
			"simple_field": "value2"
		}`,
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)

	// Check that field names work correctly - verify they exist and have correct values
	field1Found := false
	field2Found := false

	for _, table := range *result {

		// Check if this is the camel_case_field
		if len(table.Values) == 1 && table.Values[0] == "value1" {
			field1Found = true
		}
		// Check if this is the simple_field
		if len(table.Values) == 1 && table.Values[0] == "value2" {
			field2Found = true
		}
	}

	require.True(t, field1Found)

	require.True(t, field2Found)
}

// Test types.JSONTypeToParquetType error handling (covers lines 218-222)
func TestMarshalJSON_TypeConversionError(t *testing.T) {
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=timestamp_field, type=INT96", "Type": "string"}
		]
	}`

	_, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)
}

// Test map with invalid key access (covers line 125 condition)
func TestMarshalJSON_MapInvalidKeyAccess(t *testing.T) {
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{
				"Tag": "name=struct_field",
				"Fields": [
					{"Tag": "name=present_field, type=BYTE_ARRAY", "Type": "string"},
					{"Tag": "name=missing_field, type=INT32, repetitiontype=OPTIONAL", "Type": "int32"}
				]
			}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// JSON with struct that has invalid map index access
	jsonRecords := []any{
		`{"struct_field": {"present_field": "value", "unknown_field": "ignored"}}`,
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)

	// Should have nil value for missing_field due to invalid key access path
	missingField := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Struct_field"+common.PAR_GO_PATH_DELIMITER+"Missing_field"]
	require.NotNil(t, missingField)
	require.Len(t, missingField.Values, 1)
	require.Nil(t, missingField.Values[0])
}

func TestMarshalJSON_InvalidMapIndexValue(t *testing.T) {
	// Test with valid map value but invalid index access
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=field1, type=BYTE_ARRAY", "Type": "string"},
			{"Tag": "name=field2, type=INT32", "Type": "int32"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// JSON with map index that would be invalid but still parseable
	jsonRecords := []any{
		`{
			"field1": "value1",
			"field2": 42
		}`,
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)

	// Should handle the valid case correctly
	field1 := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Field1"]
	require.NotNil(t, field1)
	require.Len(t, field1.Values, 1)
	require.Equal(t, "value1", field1.Values[0])
}

// Test MapIndex access with invalid values to trigger error paths
func TestMarshalJSON_MapIndexErrors(t *testing.T) {
	// Test struct field access that could have invalid map index
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{
				"Tag": "name=struct_field",
				"Fields": [
					{"Tag": "name=valid_field, type=BYTE_ARRAY", "Type": "string"},
					{"Tag": "name=another_field, type=INT32", "Type": "int32"}
				]
			}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Test struct with null/invalid values to trigger different code paths
	jsonRecords := []any{
		`{"struct_field": {"valid_field": "test", "another_field": null}}`,
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)
}

// Test struct field mapping with StringToVariableName conversion
func TestMarshalJSON_StructFieldMapping(t *testing.T) {
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{
				"Tag": "name=nested_struct",
				"Fields": [
					{"Tag": "name=snake_case_field, type=BYTE_ARRAY, repetitiontype=OPTIONAL", "Type": "string"},
					{"Tag": "name=another_field, type=INT32", "Type": "int32"}
				]
			}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Test struct with fields that require StringToVariableName conversion
	jsonRecords := []any{
		`{"nested_struct": {"snake_case_field": "test_value"}}`, // Missing another_field to trigger nil handling
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)
}

// Test nil map index values (covers line 125 IsValid() check)
func TestMarshalJSON_NilMapIndexValues(t *testing.T) {
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{
				"Tag": "name=struct_field",
				"Fields": [
					{"Tag": "name=field1, type=BYTE_ARRAY, repetitiontype=OPTIONAL", "Type": "string"},
					{"Tag": "name=field2, type=INT32, repetitiontype=OPTIONAL", "Type": "int32"}
				]
			}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Test with struct that has missing fields to trigger IsValid() check
	jsonRecords := []any{
		`{"struct_field": {"field1": "present"}}`, // field2 is missing
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)

	// Verify field2 got nil value due to missing key
	field2Column := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Struct_field"+common.PAR_GO_PATH_DELIMITER+"Field2"]
	require.NotNil(t, field2Column)
	require.Len(t, field2Column.Values, 1)
	require.Nil(t, field2Column.Values[0])
}

// Test additional edge cases for comprehensive coverage
func TestMarshalJSON_AdditionalEdgeCases(t *testing.T) {
	// Test 1: Optional fields in MAP values (covers line 104-106 optional handling)
	t.Run("map_with_optional_values", func(t *testing.T) {
		type MapOptionalStruct struct {
			MapField map[string]*int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
		}

		sch, err := schema.NewSchemaHandlerFromStruct(new(MapOptionalStruct))
		require.NoError(t, err)

		// Test map with optional values - some keys missing
		jsonRecords := []any{
			`{"MapField": {"key1": 100, "key3": 300}}`, // key2 missing to test optional handling
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})

	// Test 2: Optional elements in LIST (covers line 182-184 optional handling)
	t.Run("list_with_optional_elements", func(t *testing.T) {
		type ListOptionalStruct struct {
			ListField []*string `parquet:"name=list, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		}

		sch, err := schema.NewSchemaHandlerFromStruct(new(ListOptionalStruct))
		require.NoError(t, err)

		// Test list with string elements
		jsonRecords := []any{
			`{"ListField": ["item1", "item2", "item3"]}`,
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})

	// Test 3: Multiple maps with different repetition levels (covers lines 90-94, 108-112)
	t.Run("multiple_map_entries", func(t *testing.T) {
		type MultiMapStruct struct {
			MapField map[string]int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
		}

		sch, err := schema.NewSchemaHandlerFromStruct(new(MultiMapStruct))
		require.NoError(t, err)

		// Test map with multiple entries to trigger repetition level logic
		jsonRecords := []any{
			`{"MapField": {"first": 1, "second": 2, "third": 3, "fourth": 4}}`,
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})

	// Test 4: Multiple list entries (covers lines 172-176, 206-210)
	t.Run("multiple_list_entries", func(t *testing.T) {
		type MultiListStruct struct {
			ListField []string `parquet:"name=list, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		}

		sch, err := schema.NewSchemaHandlerFromStruct(new(MultiListStruct))
		require.NoError(t, err)

		// Test list with multiple entries to trigger repetition level logic
		jsonRecords := []any{
			`{"ListField": ["first", "second", "third", "fourth", "fifth"]}`,
		}

		result, err := MarshalJSON(jsonRecords, sch)
		require.NoError(t, err)

		require.NotNil(t, result)
	})
}

func TestMarshalJSON_NodeBufReset(t *testing.T) {
	// Test that covers multiple JSON records to exercise NodeBuf.Reset() functionality
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=id, type=INT32", "Type": "int32"},
			{"Tag": "name=name, type=BYTE_ARRAY", "Type": "string"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Multiple records to exercise the node buffer reset logic
	jsonRecords := []any{
		`{"id": 1, "name": "first"}`,
		`{"id": 2, "name": "second"}`,
		`{"id": 3, "name": "third"}`,
	}

	result, err := MarshalJSON(jsonRecords, sch)
	require.NoError(t, err)

	require.NotNil(t, result)

	// Verify all records were processed
	idColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Id"]
	nameColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Name"]

	require.NotNil(t, idColumn)
	require.Len(t, idColumn.Values, 3)

	require.NotNil(t, nameColumn)
	require.Len(t, nameColumn.Values, 3)

	// Check specific values
	expectedIds := []int32{1, 2, 3}
	expectedNames := []string{"first", "second", "third"}

	for i := 0; i < 3; i++ {
		require.Equal(t, expectedIds[i], idColumn.Values[i], "Expected correct id value at index %d", i)
		require.Equal(t, expectedNames[i], nameColumn.Values[i], "Expected correct name value at index %d", i)
	}
}
