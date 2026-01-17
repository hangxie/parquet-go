package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/types"
)

// Tests for schema handler functionality

func TestNewSchemaHandlerFromSchemaHandler(t *testing.T) {
	// Create an original schema using NewSchemaHandlerFromStruct for simplicity
	originalSchema, err := NewSchemaHandlerFromStruct(new(struct {
		Field1 int32  `parquet:"name=field1, type=INT32"`
		Field2 string `parquet:"name=field2, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	}))
	require.NoError(t, err)

	// Test NewSchemaHandlerFromSchemaHandler
	schema2 := NewSchemaHandlerFromSchemaHandler(originalSchema)

	// Verify that the schema was copied correctly
	require.Equal(t, len(originalSchema.SchemaElements), len(schema2.SchemaElements))
	require.Equal(t, "Parquet_go_root", schema2.SchemaElements[0].Name)
	require.Equal(t, "Field1", schema2.SchemaElements[1].Name)
	require.Equal(t, "Field2", schema2.SchemaElements[2].Name)

	// Verify the schema elements were deep copied
	require.Equal(t, "REQUIRED", schema2.SchemaElements[1].RepetitionType.String())
	require.Equal(t, "INT32", schema2.SchemaElements[1].Type.String())

	require.Equal(t, "OPTIONAL", schema2.SchemaElements[2].RepetitionType.String())
	require.Equal(t, "BYTE_ARRAY", schema2.SchemaElements[2].Type.String())
	require.Equal(t, "UTF8", schema2.SchemaElements[2].ConvertedType.String())

	// Verify it's a separate copy, not the same reference
	require.NotSame(t, originalSchema, schema2)
	require.NotSame(t, &originalSchema.SchemaElements, &schema2.SchemaElements)
}

func TestNewSchemaHandlerFromSchemaList(t *testing.T) {
	// Create a simple schema element list directly
	requiredRep := parquet.FieldRepetitionType_REQUIRED
	optionalRep := parquet.FieldRepetitionType_OPTIONAL
	int32Type := parquet.Type_INT32
	byteArrayType := parquet.Type_BYTE_ARRAY
	utf8Conv := parquet.ConvertedType_UTF8
	numChildren := int32(2)

	schemaElements := []*parquet.SchemaElement{
		{Name: "Parquet_go_root", RepetitionType: &requiredRep, NumChildren: &numChildren},
		{Name: "Field1", Type: &int32Type, RepetitionType: &requiredRep},
		{Name: "Field2", Type: &byteArrayType, ConvertedType: &utf8Conv, RepetitionType: &optionalRep},
	}

	schema2 := NewSchemaHandlerFromSchemaList(schemaElements)

	// Verify that the schema was created correctly from the list
	require.Equal(t, len(schemaElements), len(schema2.SchemaElements))
	require.Equal(t, "Parquet_go_root", schema2.SchemaElements[0].Name)
	require.Equal(t, "Field1", schema2.SchemaElements[1].Name)
	require.Equal(t, "Field2", schema2.SchemaElements[2].Name)

	// Verify the schema elements properties
	require.Equal(t, "REQUIRED", schema2.SchemaElements[1].RepetitionType.String())
	require.Equal(t, "INT32", schema2.SchemaElements[1].Type.String())

	require.Equal(t, "OPTIONAL", schema2.SchemaElements[2].RepetitionType.String())
	require.Equal(t, "BYTE_ARRAY", schema2.SchemaElements[2].Type.String())
	require.Equal(t, "UTF8", schema2.SchemaElements[2].ConvertedType.String())

	// Verify that maps were created properly
	require.NotNil(t, schema2.MapIndex)
	require.NotNil(t, schema2.PathMap)
}

func TestNewSchemaHandlerFromStruct(t *testing.T) {
	tests := []struct {
		name              string
		structDef         interface{}
		expectError       bool
		expectedErrorText string
		validateSchema    func(t *testing.T, schema *SchemaHandler)
	}{
		{
			name: "invalid_tag",
			structDef: new(struct {
				Id int32 `parquet:"foo=bar, type=INT32"`
			}),
			expectError:       true,
			expectedErrorText: "unrecognized tag 'foo'",
		},
		{
			name: "invalid_type",
			structDef: new(struct {
				Name string `parquet:"name=name, type=UTF8"`
			}),
			expectError:       true,
			expectedErrorText: "field [Name] with type [UTF8]: not a valid Type strin",
		},
		{
			name: "ignore_field_without_tag",
			structDef: new(struct {
				Id   int32 `parquet:"name=id, type=INT32, ConvertedType=INT_32"`
				Name string
			}),
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 2, len(schema.SchemaElements))
				require.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
				require.Equal(t, "Id", schema.SchemaElements[1].Name)
				require.Equal(t, "INT32", schema.SchemaElements[1].Type.String())
				require.Equal(t, "REQUIRED", schema.SchemaElements[1].RepetitionType.String())
				require.True(t, schema.SchemaElements[1].LogicalType.IsSetINTEGER())
				require.Equal(t, "INT_32", schema.SchemaElements[1].ConvertedType.String())
			},
		},
		{
			name: "pointer_field",
			structDef: new(struct {
				Name *string `parquet:"name=name, type=BYTE_ARRAY, ConvertedType=UTF8"`
			}),
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 2, len(schema.SchemaElements))
				require.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
				require.Equal(t, "Name", schema.SchemaElements[1].Name)
				require.Equal(t, "BYTE_ARRAY", schema.SchemaElements[1].Type.String())
				require.Equal(t, "OPTIONAL", schema.SchemaElements[1].RepetitionType.String())
				require.True(t, schema.SchemaElements[1].LogicalType.IsSetSTRING())
				require.Equal(t, "UTF8", schema.SchemaElements[1].ConvertedType.String())
			},
		},
		{
			name: "map_missing_value_type",
			structDef: new(struct {
				MapField map[string]*int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
			}),
			expectError:       true,
			expectedErrorText: "field [Value] with type []: not a valid Type string",
		},
		{
			name: "map_missing_key_type",
			structDef: new(struct {
				MapField map[string]*int32 `parquet:"name=map, type=MAP, convertedtype=MAP, valuetype=INT32"`
			}),
			expectError:       true,
			expectedErrorText: "field [Key] with type []: not a valid Type string",
		},
		{
			name: "map_good",
			structDef: new(struct {
				MapField1 map[string]*int32  `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
				MapField2 *map[*string]int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
			}),
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 9, len(schema.SchemaElements))
				require.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
				require.Equal(t, "MapField1", schema.SchemaElements[1].Name)
				require.Equal(t, "Key_value", schema.SchemaElements[2].Name)
				require.Equal(t, "Key", schema.SchemaElements[3].Name)
				require.Equal(t, "Value", schema.SchemaElements[4].Name)
				require.Equal(t, "MapField2", schema.SchemaElements[5].Name)
				require.Equal(t, "Key_value", schema.SchemaElements[6].Name)
				require.Equal(t, "Key", schema.SchemaElements[7].Name)
				require.Equal(t, "Value", schema.SchemaElements[8].Name)

				require.Equal(t, "REQUIRED", schema.SchemaElements[1].RepetitionType.String())
				require.Equal(t, int32(1), schema.SchemaElements[1].GetNumChildren())
				require.Nil(t, schema.SchemaElements[1].Type)
				require.Equal(t, "MAP", schema.SchemaElements[1].ConvertedType.String())

				require.Equal(t, "REPEATED", schema.SchemaElements[2].RepetitionType.String())
				require.Equal(t, int32(2), schema.SchemaElements[2].GetNumChildren())
				require.Nil(t, schema.SchemaElements[2].Type)
				require.Equal(t, "MAP_KEY_VALUE", schema.SchemaElements[2].ConvertedType.String())

				require.Equal(t, "REQUIRED", schema.SchemaElements[3].RepetitionType.String())
				require.Equal(t, "BYTE_ARRAY", schema.SchemaElements[3].Type.String())
				require.Equal(t, "UTF8", schema.SchemaElements[3].ConvertedType.String())

				require.Equal(t, "OPTIONAL", schema.SchemaElements[4].RepetitionType.String())
				require.Equal(t, "INT32", schema.SchemaElements[4].Type.String())
				require.Nil(t, schema.SchemaElements[4].ConvertedType)

				require.Equal(t, "OPTIONAL", schema.SchemaElements[5].RepetitionType.String())
				require.Equal(t, int32(1), schema.SchemaElements[5].GetNumChildren())
				require.Nil(t, schema.SchemaElements[5].Type)
				require.Equal(t, "MAP", schema.SchemaElements[5].ConvertedType.String())

				require.Equal(t, "REPEATED", schema.SchemaElements[6].RepetitionType.String())
				require.Equal(t, int32(2), schema.SchemaElements[6].GetNumChildren())
				require.Nil(t, schema.SchemaElements[6].Type)
				require.Equal(t, "MAP_KEY_VALUE", schema.SchemaElements[6].ConvertedType.String())

				require.Equal(t, "REQUIRED", schema.SchemaElements[7].RepetitionType.String())
				require.Equal(t, "BYTE_ARRAY", schema.SchemaElements[7].Type.String())
				require.Equal(t, "UTF8", schema.SchemaElements[7].ConvertedType.String())

				require.Equal(t, "REQUIRED", schema.SchemaElements[8].RepetitionType.String())
				require.Equal(t, "INT32", schema.SchemaElements[8].Type.String())
				require.Nil(t, schema.SchemaElements[8].ConvertedType)
			},
		},
		{
			name: "list_missing_element_type",
			structDef: new(struct {
				ListField *[]string `parquet:"name=list, type=LIST, convertedtype=LIST"`
			}),
			expectError:       true,
			expectedErrorText: "field [Element] with type []: not a valid Type string",
		},
		{
			name: "list_good",
			structDef: new(struct {
				ListField1 *[]string `parquet:"name=list, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
				ListField2 []*string `parquet:"name=list, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
			}),
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 7, len(schema.SchemaElements))
				require.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
				require.Equal(t, "ListField1", schema.SchemaElements[1].Name)
				require.Equal(t, "List", schema.SchemaElements[2].Name)
				require.Equal(t, "Element", schema.SchemaElements[3].Name)
				require.Equal(t, "ListField2", schema.SchemaElements[4].Name)
				require.Equal(t, "List", schema.SchemaElements[5].Name)
				require.Equal(t, "Element", schema.SchemaElements[6].Name)

				require.Equal(t, "OPTIONAL", schema.SchemaElements[1].RepetitionType.String())
				require.Equal(t, int32(1), schema.SchemaElements[1].GetNumChildren())
				require.Nil(t, schema.SchemaElements[1].Type)
				require.Equal(t, "LIST", schema.SchemaElements[1].ConvertedType.String())

				require.Equal(t, "REPEATED", schema.SchemaElements[2].RepetitionType.String())
				require.Equal(t, int32(1), schema.SchemaElements[2].GetNumChildren())
				require.Nil(t, schema.SchemaElements[2].Type)
				require.Nil(t, schema.SchemaElements[2].ConvertedType)

				require.Equal(t, "REQUIRED", schema.SchemaElements[3].RepetitionType.String())
				require.Equal(t, "BYTE_ARRAY", schema.SchemaElements[3].Type.String())
				require.Equal(t, "UTF8", schema.SchemaElements[3].ConvertedType.String())

				require.Equal(t, "REQUIRED", schema.SchemaElements[4].RepetitionType.String())
				require.Equal(t, int32(1), schema.SchemaElements[4].GetNumChildren())
				require.Nil(t, schema.SchemaElements[4].Type)
				require.Equal(t, "LIST", schema.SchemaElements[4].ConvertedType.String())

				require.Equal(t, "REPEATED", schema.SchemaElements[5].RepetitionType.String())
				require.Equal(t, int32(1), schema.SchemaElements[5].GetNumChildren())
				require.Nil(t, schema.SchemaElements[5].Type)
				require.Nil(t, schema.SchemaElements[5].ConvertedType)

				require.Equal(t, "OPTIONAL", schema.SchemaElements[6].RepetitionType.String())
				require.Equal(t, "BYTE_ARRAY", schema.SchemaElements[6].Type.String())
				require.Equal(t, "UTF8", schema.SchemaElements[6].ConvertedType.String())
			},
		},
		{
			name: "list_repeated",
			structDef: new(struct {
				ListField []int32 `parquet:"name=repeated, type=INT32, repetitiontype=REPEATED"`
			}),
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 2, len(schema.SchemaElements))
				require.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
				require.Equal(t, "ListField", schema.SchemaElements[1].Name)

				require.Equal(t, "REPEATED", schema.SchemaElements[1].RepetitionType.String())
				require.Equal(t, "INT32", schema.SchemaElements[1].Type.String())
				require.Nil(t, schema.SchemaElements[1].ConvertedType)
			},
		},
		{
			name: "variant_type",
			structDef: new(struct {
				VariantField types.Variant `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
			}),
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				// VARIANT should create a GROUP with 2 children: Metadata and Value
				require.Equal(t, 4, len(schema.SchemaElements))
				require.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
				require.Equal(t, "VariantField", schema.SchemaElements[1].Name)
				require.Equal(t, "Metadata", schema.SchemaElements[2].Name)
				require.Equal(t, "Value", schema.SchemaElements[3].Name)

				// Verify VARIANT group structure
				require.Equal(t, "REQUIRED", schema.SchemaElements[1].RepetitionType.String())
				require.Equal(t, int32(2), schema.SchemaElements[1].GetNumChildren())
				require.Nil(t, schema.SchemaElements[1].Type) // GROUP has no type
				require.True(t, schema.SchemaElements[1].LogicalType.IsSetVARIANT())

				// Verify Metadata child
				require.Equal(t, "REQUIRED", schema.SchemaElements[2].RepetitionType.String())
				require.Equal(t, "BYTE_ARRAY", schema.SchemaElements[2].Type.String())

				// Verify Value child
				require.Equal(t, "REQUIRED", schema.SchemaElements[3].RepetitionType.String())
				require.Equal(t, "BYTE_ARRAY", schema.SchemaElements[3].Type.String())
			},
		},
		{
			name: "map_fields_comprehensive",
			structDef: new(struct {
				MapField1 map[string]*int32  `parquet:"name=map1, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
				MapField2 *map[*string]int32 `parquet:"name=map2, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
			}),
			expectError: false,
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 9, len(schema.SchemaElements))
				require.Equal(t, "Parquet_go_root", schema.SchemaElements[0].Name)
				require.Equal(t, "MapField1", schema.SchemaElements[1].Name)
				require.Equal(t, "Key_value", schema.SchemaElements[2].Name)
				require.Equal(t, "Key", schema.SchemaElements[3].Name)
				require.Equal(t, "Value", schema.SchemaElements[4].Name)
				require.Equal(t, "MapField2", schema.SchemaElements[5].Name)
				require.Equal(t, "Key_value", schema.SchemaElements[6].Name)
				require.Equal(t, "Key", schema.SchemaElements[7].Name)
				require.Equal(t, "Value", schema.SchemaElements[8].Name)

				// Verify MapField1 structure
				require.Equal(t, "REQUIRED", schema.SchemaElements[1].RepetitionType.String())
				require.Equal(t, int32(1), schema.SchemaElements[1].GetNumChildren())
				require.Nil(t, schema.SchemaElements[1].Type)
				require.Equal(t, "MAP", schema.SchemaElements[1].ConvertedType.String())

				// Verify MapField2 structure (pointer to map)
				require.Equal(t, "OPTIONAL", schema.SchemaElements[5].RepetitionType.String())
				require.Equal(t, int32(1), schema.SchemaElements[5].GetNumChildren())
				require.Nil(t, schema.SchemaElements[5].Type)
				require.Equal(t, "MAP", schema.SchemaElements[5].ConvertedType.String())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := NewSchemaHandlerFromStruct(tt.structDef)

			if tt.expectError {
				require.NotNil(t, err)
				if tt.expectedErrorText != "" {
					require.Contains(t, err.Error(), tt.expectedErrorText)
				}
				require.Nil(t, schema)
			} else {
				require.Nil(t, err)
				require.NotNil(t, schema)
				if tt.validateSchema != nil {
					tt.validateSchema(t, schema)
				}
			}
		})
	}
}

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
	index, err := schema.GetRepetitionLevelIndex([]string{"Parquet_go_root", "SimpleField"}, 0)
	require.Nil(t, err)               // Should succeed since res==0 before any REPEATED fields are found
	require.Equal(t, int32(1), index) // Returns i-1 where i is the position where rl is found (i=2 for path[2])

	// Test repeated field with repetition level 1
	index, err = schema.GetRepetitionLevelIndex([]string{"Parquet_go_root", "RepeatedField"}, 1)
	require.Nil(t, err)
	// Function returns i-1 where i is the position where rl is found
	// For RepeatedField at path[2], i=2, so result should be 1
	require.Equal(t, int32(1), index)

	// Test nested repeated field with repetition level 1
	index, err = schema.GetRepetitionLevelIndex([]string{"Parquet_go_root", "NestedList", "InnerField"}, 1)
	require.Nil(t, err)
	// For NestedList at path[2], i=2, so result should be 1
	require.Equal(t, int32(1), index)

	// Test doubly nested repeated field with repetition level 2
	index, err = schema.GetRepetitionLevelIndex([]string{"Parquet_go_root", "NestedList", "InnerList"}, 2)
	require.Nil(t, err)
	// For InnerList at path[3], i=3, so result should be 2
	require.Equal(t, int32(2), index)

	// Test with invalid repetition level
	_, err = schema.GetRepetitionLevelIndex([]string{"Parquet_go_root", "RepeatedField"}, 2)
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
	require.Equal(t, "parquet_go_root", rootName)

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
	level, err := schema.MaxDefinitionLevel([]string{"Parquet_go_root"})
	require.Nil(t, err)
	require.Equal(t, int32(0), level)

	// Test required field - should have definition level 0
	level, err = schema.MaxDefinitionLevel([]string{"Parquet_go_root", "RequiredField"})
	require.Nil(t, err)
	require.Equal(t, int32(0), level)

	// Test optional field - should have definition level 1
	level, err = schema.MaxDefinitionLevel([]string{"Parquet_go_root", "OptionalField"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test repeated field - should have definition level 1
	level, err = schema.MaxDefinitionLevel([]string{"Parquet_go_root", "RepeatedField"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test nested required field inside optional struct - should have definition level 1
	level, err = schema.MaxDefinitionLevel([]string{"Parquet_go_root", "NestedStruct", "InnerRequired"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test nested optional field inside optional struct - should have definition level 2
	level, err = schema.MaxDefinitionLevel([]string{"Parquet_go_root", "NestedStruct", "InnerOptional"})
	require.Nil(t, err)
	require.Equal(t, int32(2), level)

	// Test invalid path
	_, err = schema.MaxDefinitionLevel([]string{"Parquet_go_root", "NonExistentField"})
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
	level, err := schema.MaxRepetitionLevel([]string{"Parquet_go_root", "SimpleField"})
	require.Nil(t, err)
	require.Equal(t, int32(0), level)

	// Test repeated field - should have repetition level 1
	level, err = schema.MaxRepetitionLevel([]string{"Parquet_go_root", "RepeatedField"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test nested field inside repeated struct - should have repetition level 1
	level, err = schema.MaxRepetitionLevel([]string{"Parquet_go_root", "NestedList", "InnerField"})
	require.Nil(t, err)
	require.Equal(t, int32(1), level)

	// Test doubly nested repeated field - should have repetition level 2
	level, err = schema.MaxRepetitionLevel([]string{"Parquet_go_root", "NestedList", "InnerList"})
	require.Nil(t, err)
	require.Equal(t, int32(2), level)

	// Test invalid path
	_, err = schema.MaxRepetitionLevel([]string{"Parquet_go_root", "NonExistentField"})
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

		rt, err := sh.GetRepetitionType([]string{"Parquet_go_root", "Field1"})
		require.NoError(t, err)
		require.Equal(t, parquet.FieldRepetitionType_REQUIRED, rt)

		rt, err = sh.GetRepetitionType([]string{"Parquet_go_root", "Field2"})
		require.NoError(t, err)
		require.Equal(t, parquet.FieldRepetitionType_OPTIONAL, rt)
	})

	t.Run("invalid path", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			Field1 int32 `parquet:"name=field1, type=INT32"`
		}))
		require.NoError(t, err)

		_, err = sh.GetRepetitionType([]string{"Parquet_go_root", "NonExistent"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "name not in schema")
	})

	t.Run("nil schema element", func(t *testing.T) {
		// Create a schema handler with a nil element in SchemaElements
		// Use the correct path delimiter (common.PAR_GO_PATH_DELIMITER = "\x01")
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

func TestIsValidVariantSchema(t *testing.T) {
	// Helper to create a valid VARIANT schema handler
	createVariantSchema := func() (*SchemaHandler, int32, []int32) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			VariantField types.Variant `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
		}))
		if err != nil {
			t.Fatalf("failed to create variant schema: %v", err)
		}
		// Build children map
		sh.childrenMap = sh.buildChildrenMap()
		// VariantField is at index 1, children are Metadata (2) and Value (3)
		return sh, 1, sh.childrenMap[1]
	}

	t.Run("valid variant schema", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		require.True(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("missing VARIANT LogicalType", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Remove the VARIANT LogicalType
		sh.SchemaElements[idx].LogicalType = nil
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("wrong LogicalType (not VARIANT)", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Set a different LogicalType
		sh.SchemaElements[idx].LogicalType = parquet.NewLogicalType()
		sh.SchemaElements[idx].LogicalType.STRING = parquet.NewStringType()
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("wrong number of children (too few)", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Pass only one child
		require.False(t, sh.isValidVariantSchema(idx, children[:1]))
	})

	t.Run("extra children are allowed (for shredding)", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Pass three children (add a fake one that exists in schema list if possible, or just one that is skipped)
		require.True(t, sh.isValidVariantSchema(idx, append(children, 0)))
	})

	t.Run("wrong Metadata child name", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Change the Metadata child name
		sh.Infos[children[0]].InName = "WrongName"
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("wrong Value child name", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Change the Value child name
		sh.Infos[children[1]].InName = "WrongName"
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("wrong Metadata child type", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Change Metadata type from BYTE_ARRAY to INT32
		int32Type := parquet.Type_INT32
		sh.SchemaElements[children[0]].Type = &int32Type
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("wrong Value child type", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Change Value type from BYTE_ARRAY to INT64
		int64Type := parquet.Type_INT64
		sh.SchemaElements[children[1]].Type = &int64Type
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("optional Metadata child (should be required)", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Change Metadata repetition type to OPTIONAL
		optionalRep := parquet.FieldRepetitionType_OPTIONAL
		sh.SchemaElements[children[0]].RepetitionType = &optionalRep
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("optional Value child (valid for shredded variants)", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		// Change Value repetition type to OPTIONAL - this is valid for shredded variants
		optionalRep := parquet.FieldRepetitionType_OPTIONAL
		sh.SchemaElements[children[1]].RepetitionType = &optionalRep
		require.True(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("nil Metadata type", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		sh.SchemaElements[children[0]].Type = nil
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("nil Value type", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		sh.SchemaElements[children[1]].Type = nil
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("nil Metadata repetition type", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		sh.SchemaElements[children[0]].RepetitionType = nil
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("nil Value repetition type", func(t *testing.T) {
		sh, idx, children := createVariantSchema()
		sh.SchemaElements[children[1]].RepetitionType = nil
		require.False(t, sh.isValidVariantSchema(idx, children))
	})

	t.Run("child index out of bounds", func(t *testing.T) {
		sh, idx, _ := createVariantSchema()
		// Pass children indices that are out of bounds
		outOfBoundsChildren := []int32{100, 101}
		require.False(t, sh.isValidVariantSchema(idx, outOfBoundsChildren))
	})
}

func TestShreddedVariantSchema(t *testing.T) {
	// Test that shredded variant schemas (with typed_value) are recognized

	t.Run("valid shredded variant schema with typed_value", func(t *testing.T) {
		// Create a manual schema with metadata + value (optional) + typed_value
		requiredRep := parquet.FieldRepetitionType_REQUIRED
		optionalRep := parquet.FieldRepetitionType_OPTIONAL
		byteArrayType := parquet.Type_BYTE_ARRAY
		int32Type := parquet.Type_INT32
		variantLogicalType := parquet.NewLogicalType()
		variantLogicalType.VARIANT = parquet.NewVariantType()

		schemas := []*parquet.SchemaElement{
			{Name: "Root", NumChildren: ptr(int32(1))},
			{Name: "V", NumChildren: ptr(int32(3)), RepetitionType: &requiredRep, LogicalType: variantLogicalType},
			{Name: "Metadata", Type: &byteArrayType, RepetitionType: &requiredRep},
			{Name: "Value", Type: &byteArrayType, RepetitionType: &optionalRep},
			{Name: "Typed_value", Type: &int32Type, RepetitionType: &optionalRep},
		}

		sh := NewSchemaHandlerFromSchemaList(schemas)
		sh.childrenMap = sh.buildChildrenMap()

		// Variant is at index 1
		idx := int32(1)
		children := sh.childrenMap[1] // Get children from map

		// Should be valid
		require.True(t, sh.isValidVariantSchema(idx, children), "Shredded variant schema should be valid")

		// Get schema info
		info := sh.getVariantSchemaInfo(idx, children)
		require.NotNil(t, info, "Should get variant schema info")
		require.True(t, info.IsShredded, "Should be marked as shredded")
		require.Equal(t, int32(2), info.MetadataIdx, "Metadata index should be 2")
		require.Equal(t, int32(3), info.ValueIdx, "Value index should be 3")
		require.Len(t, info.TypedValueIdxs, 1, "Should have one typed_value")
		require.Equal(t, int32(4), info.TypedValueIdxs[0], "Typed value index should be 4")
	})

	t.Run("valid shredded variant without value column", func(t *testing.T) {
		// Create a schema with only metadata + typed_value (no value column)
		requiredRep := parquet.FieldRepetitionType_REQUIRED
		optionalRep := parquet.FieldRepetitionType_OPTIONAL
		byteArrayType := parquet.Type_BYTE_ARRAY
		int32Type := parquet.Type_INT32
		variantLogicalType := parquet.NewLogicalType()
		variantLogicalType.VARIANT = parquet.NewVariantType()

		schemas := []*parquet.SchemaElement{
			{Name: "Root", NumChildren: ptr(int32(1))},
			{Name: "V", NumChildren: ptr(int32(2)), RepetitionType: &requiredRep, LogicalType: variantLogicalType},
			{Name: "Metadata", Type: &byteArrayType, RepetitionType: &requiredRep},
			{Name: "Typed_value", Type: &int32Type, RepetitionType: &optionalRep},
		}

		sh := NewSchemaHandlerFromSchemaList(schemas)
		sh.childrenMap = sh.buildChildrenMap()

		idx := int32(1)
		children := sh.childrenMap[1]

		require.True(t, sh.isValidVariantSchema(idx, children), "Variant with only metadata+typed_value should be valid")

		info := sh.getVariantSchemaInfo(idx, children)
		require.NotNil(t, info, "Should get variant schema info")
		require.True(t, info.IsShredded, "Should be marked as shredded")
		require.Equal(t, int32(-1), info.ValueIdx, "Value index should be -1 when absent")
		require.Len(t, info.TypedValueIdxs, 1, "Should have one typed_value")
	})

	t.Run("typed_value must be OPTIONAL", func(t *testing.T) {
		// typed_value with REQUIRED repetition should be invalid
		requiredRep := parquet.FieldRepetitionType_REQUIRED
		optionalRep := parquet.FieldRepetitionType_OPTIONAL
		byteArrayType := parquet.Type_BYTE_ARRAY
		int32Type := parquet.Type_INT32
		variantLogicalType := parquet.NewLogicalType()
		variantLogicalType.VARIANT = parquet.NewVariantType()

		schemas := []*parquet.SchemaElement{
			{Name: "Root", NumChildren: ptr(int32(1))},
			{Name: "V", NumChildren: ptr(int32(3)), RepetitionType: &requiredRep, LogicalType: variantLogicalType},
			{Name: "Metadata", Type: &byteArrayType, RepetitionType: &requiredRep},
			{Name: "Value", Type: &byteArrayType, RepetitionType: &optionalRep},
			{Name: "Typed_value", Type: &int32Type, RepetitionType: &requiredRep}, // REQUIRED - invalid
		}

		sh := NewSchemaHandlerFromSchemaList(schemas)
		sh.childrenMap = sh.buildChildrenMap()

		idx := int32(1)
		children := sh.childrenMap[1]

		// Still valid because it has value column (typed_value with wrong repetition is just ignored)
		require.True(t, sh.isValidVariantSchema(idx, children))

		// But typed_value should not be in the info
		info := sh.getVariantSchemaInfo(idx, children)
		require.NotNil(t, info)
		require.Len(t, info.TypedValueIdxs, 0, "REQUIRED typed_value should not be included")
	})
}

func TestGetVariantSchemaInfo(t *testing.T) {
	t.Run("unshredded variant info via index", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			V types.Variant `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
		}))
		require.NoError(t, err)

		// Build children map
		sh.childrenMap = sh.buildChildrenMap()

		// Get info for the variant at index 1
		idx := int32(1)
		children := sh.childrenMap[1]
		info := sh.getVariantSchemaInfo(idx, children)

		require.NotNil(t, info, "Should find variant schema info")
		require.False(t, info.IsShredded, "Unshredded variant should not be marked as shredded")
		require.Empty(t, info.TypedValueIdxs, "Should have no typed_value columns")
		require.True(t, info.ValueIdx >= 0, "Should have a value index")
	})

	t.Run("invalid schema returns nil", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}))
		require.NoError(t, err)
		sh.childrenMap = sh.buildChildrenMap()

		// Index 1 is Name, not a variant
		info := sh.getVariantSchemaInfo(1, sh.childrenMap[1])
		require.Nil(t, info, "Should return nil for non-variant")
	})
}

func TestVariantSchemasMap(t *testing.T) {
	t.Run("VariantSchemas map is populated", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			V types.Variant `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
		}))
		require.NoError(t, err)

		require.NotNil(t, sh.VariantSchemas, "VariantSchemas map should be initialized")
		require.Len(t, sh.VariantSchemas, 1, "Should have one variant")

		// Get the first key in the map
		var found bool
		for path := range sh.VariantSchemas {
			t.Logf("Found variant at path: %s", path)
			found = true
		}
		require.True(t, found, "Variant should be in the map")
	})

	t.Run("multiple variants are all indexed", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			V1 types.Variant `parquet:"name=variant1, type=VARIANT, logicaltype=VARIANT"`
			V2 types.Variant `parquet:"name=variant2, type=VARIANT, logicaltype=VARIANT"`
		}))
		require.NoError(t, err)

		require.Len(t, sh.VariantSchemas, 2, "Should have two variants")
	})
}

// Helper function to get pointer to int32
func ptr(v int32) *int32 {
	return &v
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
			V types.Variant `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
		}))
		require.NoError(t, err)

		info := sh.GetVariantSchemaInfo("NonExistent" + common.PAR_GO_PATH_DELIMITER + "Path")
		require.Nil(t, info)
	})

	t.Run("returns info for existing variant path", func(t *testing.T) {
		sh, err := NewSchemaHandlerFromStruct(new(struct {
			V types.Variant `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
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
			V1 types.Variant `parquet:"name=variant1, type=VARIANT, logicaltype=VARIANT"`
			V2 types.Variant `parquet:"name=variant2, type=VARIANT, logicaltype=VARIANT"`
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
