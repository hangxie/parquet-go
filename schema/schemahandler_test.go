package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestSchemaHandler_GetRepetitionLevelIndex(t *testing.T) {
	// Create a schema with repeated and nested fields
	schema, err := NewSchemaHandlerFromStruct(new(struct {
		SimpleField   string   `parquet:"name=simple_field, type=BYTE_ARRAY, convertedtype=UTF8"`
		RepeatedField []string `parquet:"name=repeated_field, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED"`
		NestedList    []struct {
			InnerField string   `parquet:"name=inner_field, type=BYTE_ARRAY, convertedtype=UTF8"`
			InnerList  []string `parquet:"name=inner_list, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED"`
		} `parquet:"name=nested_list, repetitiontype=REPEATED"`
	}))
	require.Nil(t, err)

	// Test simple field with repetition level 0 - should succeed since res starts at 0
	index, err := schema.GetRepetitionLevelIndex([]string{common.ParGoRootInName, "SimpleField"}, 0)
	require.Nil(t, err)               // Should succeed since res==0 before any REPEATED fields are found
	require.Equal(t, int32(1), index) // Returns i-1 where i is the position where rl is found (i=2 for path[2])

	// Test repeated field with repetition level 1
	index, err = schema.GetRepetitionLevelIndex([]string{common.ParGoRootInName, "RepeatedField"}, 1)
	require.Nil(t, err)
	// Function returns i-1 where i is the position where rl is found
	// For RepeatedField at path[2], i=2, so result should be 1
	require.Equal(t, int32(1), index)

	// Test nested repeated field with repetition level 1
	index, err = schema.GetRepetitionLevelIndex([]string{common.ParGoRootInName, "NestedList", "InnerField"}, 1)
	require.Nil(t, err)
	// For NestedList at path[2], i=2, so result should be 1
	require.Equal(t, int32(1), index)

	// Test doubly nested repeated field with repetition level 2
	index, err = schema.GetRepetitionLevelIndex([]string{common.ParGoRootInName, "NestedList", "InnerList"}, 2)
	require.Nil(t, err)
	// For InnerList at path[3], i=3, so result should be 2
	require.Equal(t, int32(2), index)

	// Test with invalid repetition level
	_, err = schema.GetRepetitionLevelIndex([]string{common.ParGoRootInName, "RepeatedField"}, 2)
	require.NotNil(t, err)

	// Test with empty path
	_, err = schema.GetRepetitionLevelIndex([]string{}, 0)
	require.NotNil(t, err)
}

func TestSchemaHandler_GetRootExName(t *testing.T) {
	// Create a schema with various field types
	schema, err := NewSchemaHandlerFromStruct(new(struct {
		SimpleField  string `parquet:"name=simple_field, type=BYTE_ARRAY, convertedtype=UTF8"`
		IntField     int32  `parquet:"name=int_field, type=INT32"`
		NestedStruct struct {
			InnerField string `parquet:"name=inner_field, type=BYTE_ARRAY, convertedtype=UTF8"`
		} `parquet:"name=nested_struct"`
	}))
	require.Nil(t, err)

	// Test getting root external name (this function takes no parameters)
	rootName := schema.GetRootExName()
	require.NotEmpty(t, rootName)

	// The root ExName should be the external name of the root element
	// For struct schemas, this is typically the root name (case-sensitive)
	require.Equal(t, common.ParGoRootExName, rootName)

	// Test with empty schema
	emptySchema := &SchemaHandler{
		SchemaElements: []*parquet.SchemaElement{},
		Infos:          []*common.Tag{},
	}
	emptyRootName := emptySchema.GetRootExName()
	require.Equal(t, "", emptyRootName)
}

func TestSchemaHandler_MaxDefinitionLevel(t *testing.T) {
	// Create a schema with optional and required fields
	schema, err := NewSchemaHandlerFromStruct(new(struct {
		RequiredField string   `parquet:"name=required_field, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"`
		OptionalField *string  `parquet:"name=optional_field, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
		RepeatedField []string `parquet:"name=repeated_field, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED"`
		NestedStruct  *struct {
			InnerOptional *int32 `parquet:"name=inner_optional, type=INT32, repetitiontype=OPTIONAL"`
			InnerRequired int32  `parquet:"name=inner_required, type=INT32, repetitiontype=REQUIRED"`
		} `parquet:"name=nested_struct, repetitiontype=OPTIONAL"`
	}))
	require.Nil(t, err)

	// Test root path
	level, err := schema.MaxDefinitionLevel([]string{common.ParGoRootInName})
	require.Nil(t, err)
	require.Equal(t, int32(0), level)

	// Test required field - should have definition level 0
	level, err = schema.MaxDefinitionLevel([]string{common.ParGoRootInName, "RequiredField"})
	require.Nil(t, err)
	require.Equal(t, int32(0), level)

	// Test optional field - should have definition level 1
	level, err = schema.MaxDefinitionLevel([]string{common.ParGoRootInName, "OptionalField"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test repeated field - should have definition level 1
	level, err = schema.MaxDefinitionLevel([]string{common.ParGoRootInName, "RepeatedField"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test nested required field inside optional struct - should have definition level 1
	level, err = schema.MaxDefinitionLevel([]string{common.ParGoRootInName, "NestedStruct", "InnerRequired"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test nested optional field inside optional struct - should have definition level 2
	level, err = schema.MaxDefinitionLevel([]string{common.ParGoRootInName, "NestedStruct", "InnerOptional"})
	require.Nil(t, err)
	require.Equal(t, int32(2), level)

	// Test invalid path
	_, err = schema.MaxDefinitionLevel([]string{common.ParGoRootInName, "NonExistentField"})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "name not in schema")

	// Test empty path - this actually succeeds and returns 0
	level, err = schema.MaxDefinitionLevel([]string{})
	require.Nil(t, err)
	require.Equal(t, int32(0), level)
}

func TestSchemaHandler_MaxRepetitionLevel(t *testing.T) {
	// Create a schema with multiple levels of repetition
	schema, err := NewSchemaHandlerFromStruct(new(struct {
		SimpleField   string   `parquet:"name=simple_field, type=BYTE_ARRAY, convertedtype=UTF8"`
		RepeatedField []string `parquet:"name=repeated_field, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED"`
		NestedList    []struct {
			InnerField string   `parquet:"name=inner_field, type=BYTE_ARRAY, convertedtype=UTF8"`
			InnerList  []string `parquet:"name=inner_list, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED"`
		} `parquet:"name=nested_list, repetitiontype=REPEATED"`
	}))
	require.Nil(t, err)

	// Test simple field - should have repetition level 0
	level, err := schema.MaxRepetitionLevel([]string{common.ParGoRootInName, "SimpleField"})
	require.Nil(t, err)
	require.Equal(t, int32(0), level)

	// Test repeated field - should have repetition level 1
	level, err = schema.MaxRepetitionLevel([]string{common.ParGoRootInName, "RepeatedField"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test nested field inside repeated struct - should have repetition level 1
	level, err = schema.MaxRepetitionLevel([]string{common.ParGoRootInName, "NestedList", "InnerField"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test doubly nested repeated field - should have repetition level 2
	level, err = schema.MaxRepetitionLevel([]string{common.ParGoRootInName, "NestedList", "InnerList"})
	require.Nil(t, err)
	require.Equal(t, int32(2), level)

	// Test invalid path
	_, err = schema.MaxRepetitionLevel([]string{common.ParGoRootInName, "NonExistentField"})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "name not in schema")

	// Test empty path - this actually succeeds and returns 0
	level, err = schema.MaxRepetitionLevel([]string{})
	require.Nil(t, err)
	require.Equal(t, int32(0), level)
}

func TestSchemaHandler_SetValueColumns_BoundsChecking(t *testing.T) {
	tests := []struct {
		name     string
		handler  *SchemaHandler
		expected []string
	}{
		{
			name: "index_not_in_index_map",
			handler: &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{Name: "root", NumChildren: &[]int32{1}[0]},
					{Name: "leaf", NumChildren: &[]int32{0}[0]},
				},
				IndexMap: map[int32]string{
					// Missing index 1, even though SchemaElements[1] exists
					0: "root",
					// 1 is missing - should be handled gracefully
				},
			},
			expected: []string{}, // Should be empty since leaf node index is not in map
		},
		{
			name: "nil_schema_element",
			handler: &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{Name: "root", NumChildren: &[]int32{1}[0]},
					nil, // Nil schema element should be skipped
				},
				IndexMap: map[int32]string{
					0: "root",
					1: "leaf", // This won't be used because SchemaElements[1] is nil
				},
			},
			expected: []string{}, // Should be empty since nil element is skipped
		},
		{
			name: "valid_leaf_nodes",
			handler: &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{Name: "root", NumChildren: &[]int32{2}[0]},
					{Name: "leaf1", NumChildren: &[]int32{0}[0]},
					{Name: "leaf2", NumChildren: &[]int32{0}[0]},
				},
				IndexMap: map[int32]string{
					0: "root",
					1: "path.to.leaf1",
					2: "path.to.leaf2",
				},
			},
			expected: []string{"path.to.leaf1", "path.to.leaf2"},
		},
		{
			name: "mixed_valid_invalid_scenarios",
			handler: &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{Name: "root", NumChildren: &[]int32{3}[0]},
					nil, // Nil element - should be skipped
					{Name: "leaf1", NumChildren: &[]int32{0}[0]},
					{Name: "branch", NumChildren: &[]int32{1}[0]}, // Non-leaf
				},
				IndexMap: map[int32]string{
					0: "root",
					1: "would.be.skipped", // Element is nil, so this is ignored
					2: "path.to.leaf1",
					3: "path.to.branch",
					// Note: index 3 points to non-leaf, so won't be added
				},
			},
			expected: []string{"path.to.leaf1"},
		},
		{
			name: "empty_schema_elements",
			handler: &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{},
				IndexMap:       map[int32]string{},
			},
			expected: []string{},
		},
		{
			name: "nil_index_map",
			handler: &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{Name: "root", NumChildren: &[]int32{1}[0]},
					{Name: "leaf", NumChildren: &[]int32{0}[0]},
				},
				IndexMap: nil, // Nil map should be handled
			},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset ValueColumns to ensure clean state
			tt.handler.ValueColumns = []string{}

			tt.handler.setValueColumns()

			// Verify the results match expectations
			require.Equal(t, tt.expected, tt.handler.ValueColumns)
		})
	}
}

func TestSchemaHandler_BoundsEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		handler *SchemaHandler
	}{
		{
			name: "large_index_values",
			handler: &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{Name: "leaf", NumChildren: &[]int32{0}[0]},
				},
				IndexMap: map[int32]string{
					0:       "valid.path",
					1000000: "invalid.path", // Large index that doesn't exist
				},
			},
		},
		{
			name: "negative_index_values",
			handler: &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{Name: "leaf", NumChildren: &[]int32{0}[0]},
				},
				IndexMap: map[int32]string{
					0:  "valid.path",
					-1: "negative.path", // Negative index
				},
			},
		},
		{
			name: "index_boundary_conditions",
			handler: &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{Name: "leaf1", NumChildren: &[]int32{0}[0]},
					{Name: "leaf2", NumChildren: &[]int32{0}[0]},
				},
				IndexMap: map[int32]string{
					0: "path.leaf1",
					1: "path.leaf2",
					2: "out.of.bounds", // Index 2 is out of bounds for array of length 2
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.handler.ValueColumns = []string{}

			tt.handler.setValueColumns()
		})
	}
}

func TestGetRepetitionType(t *testing.T) {
	t.Run("valid path", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			Field1 int32   `parquet:"name=field1, type=INT32"`
			Field2 *string `parquet:"name=field2, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
		}))
		require.NoError(t, err)

		rt, err := sh.GetRepetitionType([]string{common.ParGoRootInName, "Field1"})
		require.NoError(t, err)
		require.Equal(t, parquet.FieldRepetitionType_REQUIRED, rt)

		rt, err = sh.GetRepetitionType([]string{common.ParGoRootInName, "Field2"})
		require.NoError(t, err)
		require.Equal(t, parquet.FieldRepetitionType_OPTIONAL, rt)
	})

	t.Run("invalid path", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			Field1 int32 `parquet:"name=field1, type=INT32"`
		}))
		require.NoError(t, err)

		_, err = sh.GetRepetitionType([]string{common.ParGoRootInName, "NonExistent"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "name not in schema")
	})

	t.Run("nil schema element", func(t *testing.T) {
		// Create a schema handler with a nil element in SchemaElements
		// Use the correct path delimiter (common.ParGoPathDelimiter = "\x01")
		pathKey := common.PathToStr([]string{"test", "path"})
		sh := &SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{nil},
			MapIndex: map[string]int32{
				pathKey: 0,
			},
		}

		_, err := sh.GetRepetitionType([]string{"test", "path"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema element at index 0 is nil")
	})
}

func TestSchemaHandler_GetVariantSchemaInfo(t *testing.T) {
	t.Run("returns nil when VariantSchemas is nil", func(t *testing.T) {
		sh := &SchemaHandler{
			VariantSchemas: nil,
		}

		info := sh.GetVariantSchemaInfo("some.path")
		require.Nil(t, info)
	})

	t.Run("returns nil for non-existent path", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			V any `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
		}))
		require.NoError(t, err)

		info := sh.GetVariantSchemaInfo("NonExistent" + common.ParGoPathDelimiter + "Path")
		require.Nil(t, info)
	})

	t.Run("returns info for existing variant path", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			V any `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
		}))
		require.NoError(t, err)

		// Find the actual path from the map
		var actualPath string
		for path := range sh.VariantSchemas {
			actualPath = path
			break
		}
		require.NotEmpty(t, actualPath)

		info := sh.GetVariantSchemaInfo(actualPath)
		require.NotNil(t, info)
		require.True(t, info.MetadataIdx >= 0)
		require.True(t, info.ValueIdx >= 0)
	})

	t.Run("returns correct info for multiple variants", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			V1 any `parquet:"name=variant1, type=VARIANT, logicaltype=VARIANT"`
			V2 any `parquet:"name=variant2, type=VARIANT, logicaltype=VARIANT"`
		}))
		require.NoError(t, err)

		// Both variants should be accessible
		for path, expectedInfo := range sh.VariantSchemas {
			actualInfo := sh.GetVariantSchemaInfo(path)
			require.NotNil(t, actualInfo)
			require.Equal(t, expectedInfo, actualInfo)
		}
	})
}

func TestSchemaHandler_ValidateEncodingsForDataPageVersion(t *testing.T) {
	t.Run("plain_dictionary_v1_valid", func(t *testing.T) {
		type PlainDictStruct struct {
			Field string `parquet:"name=field, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`
		}
		sh, err := NewSchemaHandlerFromStruct(new(PlainDictStruct))
		require.NoError(t, err)
		require.NoError(t, sh.ValidateEncodingsForDataPageVersion(1))
	})

	t.Run("plain_dictionary_v2_invalid", func(t *testing.T) {
		type PlainDictStruct struct {
			Field string `parquet:"name=field, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`
		}
		sh, err := NewSchemaHandlerFromStruct(new(PlainDictStruct))
		require.NoError(t, err)
		err = sh.ValidateEncodingsForDataPageVersion(2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "PLAIN_DICTIONARY encoding is deprecated")
	})

	t.Run("delta_encoding_v2_valid", func(t *testing.T) {
		type DeltaStruct struct {
			Field string `parquet:"name=field, type=BYTE_ARRAY, encoding=DELTA_BYTE_ARRAY"`
		}
		sh, err := NewSchemaHandlerFromStruct(new(DeltaStruct))
		require.NoError(t, err)
		require.NoError(t, sh.ValidateEncodingsForDataPageVersion(2))
	})

	t.Run("delta_encoding_v1_invalid", func(t *testing.T) {
		type DeltaStruct struct {
			Field string `parquet:"name=field, type=BYTE_ARRAY, encoding=DELTA_BYTE_ARRAY"`
		}
		sh, err := NewSchemaHandlerFromStruct(new(DeltaStruct))
		require.NoError(t, err)
		err = sh.ValidateEncodingsForDataPageVersion(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "only supported for data page v2")
	})

	t.Run("rle_dictionary_both_versions_valid", func(t *testing.T) {
		type RleDictStruct struct {
			Field string `parquet:"name=field, type=BYTE_ARRAY, encoding=RLE_DICTIONARY"`
		}
		sh, err := NewSchemaHandlerFromStruct(new(RleDictStruct))
		require.NoError(t, err)
		require.NoError(t, sh.ValidateEncodingsForDataPageVersion(1))
		require.NoError(t, sh.ValidateEncodingsForDataPageVersion(2))
	})

	t.Run("plain_both_versions_valid", func(t *testing.T) {
		type PlainStruct struct {
			Field string `parquet:"name=field, type=BYTE_ARRAY, encoding=PLAIN"`
		}
		sh, err := NewSchemaHandlerFromStruct(new(PlainStruct))
		require.NoError(t, err)
		require.NoError(t, sh.ValidateEncodingsForDataPageVersion(1))
		require.NoError(t, sh.ValidateEncodingsForDataPageVersion(2))
	})

	t.Run("multiple_fields_one_invalid", func(t *testing.T) {
		type MixedStruct struct {
			Field1 string `parquet:"name=field1, type=BYTE_ARRAY, encoding=PLAIN"`
			Field2 string `parquet:"name=field2, type=BYTE_ARRAY, encoding=DELTA_BYTE_ARRAY"`
		}
		sh, err := NewSchemaHandlerFromStruct(new(MixedStruct))
		require.NoError(t, err)
		// v2 should be fine
		require.NoError(t, sh.ValidateEncodingsForDataPageVersion(2))
		// v1 should fail due to DELTA_BYTE_ARRAY
		err = sh.ValidateEncodingsForDataPageVersion(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Field2")
	})
}
