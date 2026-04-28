package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

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
	require.Equal(t, common.ParGoRootInName, schema2.SchemaElements[0].Name)
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
		{Name: common.ParGoRootInName, RepetitionType: &requiredRep, NumChildren: &numChildren},
		{Name: "Field1", Type: &int32Type, RepetitionType: &requiredRep},
		{Name: "Field2", Type: &byteArrayType, ConvertedType: &utf8Conv, RepetitionType: &optionalRep},
	}

	schema2 := NewSchemaHandlerFromSchemaList(schemaElements)

	// Verify that the schema was created correctly from the list
	require.Equal(t, len(schemaElements), len(schema2.SchemaElements))
	require.Equal(t, common.ParGoRootInName, schema2.SchemaElements[0].Name)
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
		structDef         any
		expectedErrorText string
		validateSchema    func(t *testing.T, schema *SchemaHandler)
	}{
		{
			name: "invalid_tag",
			structDef: new(struct {
				Id int32 `parquet:"foo=bar, type=INT32"`
			}),
			expectedErrorText: "unrecognized tag 'foo'",
		},
		{
			name: "invalid_type",
			structDef: new(struct {
				Name string `parquet:"name=name, type=UTF8"`
			}),
			expectedErrorText: "field [Name] with type [UTF8]: not a valid Type strin",
		},
		{
			name: "ignore_field_without_tag",
			structDef: new(struct {
				Id   int32 `parquet:"name=id, type=INT32, ConvertedType=INT_32"`
				Name string
			}),
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 2, len(schema.SchemaElements))
				require.Equal(t, common.ParGoRootInName, schema.SchemaElements[0].Name)
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
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 2, len(schema.SchemaElements))
				require.Equal(t, common.ParGoRootInName, schema.SchemaElements[0].Name)
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
			expectedErrorText: "field [Value] with type []: not a valid Type string",
		},
		{
			name: "map_missing_key_type",
			structDef: new(struct {
				MapField map[string]*int32 `parquet:"name=map, type=MAP, convertedtype=MAP, valuetype=INT32"`
			}),
			expectedErrorText: "field [Key] with type []: not a valid Type string",
		},
		{
			name: "nested_struct_invalid_convertedtype",
			structDef: new(struct {
				Nested struct {
					Value int32 `parquet:"name=value, type=INT32"`
				} `parquet:"name=nested, convertedtype=INVALID_CONVERTED_TYPE"`
			}),
			expectedErrorText: "with convertedtype [INVALID_CONVERTED_TYPE]",
		},
		{
			name: "list_invalid_logicaltype",
			structDef: new(struct {
				Items []int32 `parquet:"name=items, type=LIST, logicaltype=DECIMAL, logicaltype.precision=bad, valuetype=INT32"`
			}),
			expectedErrorText: "parse logicaltype.precision",
		},
		{
			name: "variant_invalid_convertedtype",
			structDef: new(struct {
				Data any `parquet:"name=data, type=VARIANT, logicaltype=VARIANT, convertedtype=INVALID_CONVERTED_TYPE"`
			}),
			expectedErrorText: "with convertedtype [INVALID_CONVERTED_TYPE]",
		},
		{
			name: "map_good",
			structDef: new(struct {
				MapField1 map[string]*int32  `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
				MapField2 *map[*string]int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
			}),
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 9, len(schema.SchemaElements))
				require.Equal(t, common.ParGoRootInName, schema.SchemaElements[0].Name)
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
			expectedErrorText: "field [Element] with type []: not a valid Type string",
		},
		{
			name: "list_good",
			structDef: new(struct {
				ListField1 *[]string `parquet:"name=list, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
				ListField2 []*string `parquet:"name=list, type=LIST, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
			}),
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 7, len(schema.SchemaElements))
				require.Equal(t, common.ParGoRootInName, schema.SchemaElements[0].Name)
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
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 2, len(schema.SchemaElements))
				require.Equal(t, common.ParGoRootInName, schema.SchemaElements[0].Name)
				require.Equal(t, "ListField", schema.SchemaElements[1].Name)

				require.Equal(t, "REPEATED", schema.SchemaElements[1].RepetitionType.String())
				require.Equal(t, "INT32", schema.SchemaElements[1].Type.String())
				require.Nil(t, schema.SchemaElements[1].ConvertedType)
			},
		},
		{
			name: "variant_type",
			structDef: new(struct {
				VariantField any `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
			}),
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				// VARIANT should create a GROUP with 2 children: Metadata and Value
				require.Equal(t, 4, len(schema.SchemaElements))
				require.Equal(t, common.ParGoRootInName, schema.SchemaElements[0].Name)
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
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 9, len(schema.SchemaElements))
				require.Equal(t, common.ParGoRootInName, schema.SchemaElements[0].Name)
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
		{
			name: "variant_with_encoding_and_compression",
			structDef: new(struct {
				VariantField any `parquet:"name=variant, type=VARIANT, encoding=PLAIN, compression=GZIP"`
			}),
			validateSchema: func(t *testing.T, schema *SchemaHandler) {
				require.Equal(t, 4, len(schema.SchemaElements))

				// Verify Metadata child has encoding and compression
				require.Equal(t, "Metadata", schema.SchemaElements[2].Name)
				require.Equal(t, parquet.Encoding_PLAIN, schema.Infos[2].Encoding)
				require.NotNil(t, schema.Infos[2].CompressionCodec)
				require.Equal(t, parquet.CompressionCodec_GZIP, *schema.Infos[2].CompressionCodec)

				// Verify Value child has encoding and compression
				require.Equal(t, "Value", schema.SchemaElements[3].Name)
				require.Equal(t, parquet.Encoding_PLAIN, schema.Infos[3].Encoding)
				require.NotNil(t, schema.Infos[3].CompressionCodec)
				require.Equal(t, parquet.CompressionCodec_GZIP, *schema.Infos[3].CompressionCodec)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := NewSchemaHandlerFromStruct(tt.structDef)

			if tt.expectedErrorText != "" {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tt.expectedErrorText)
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

func TestNewSchemaHandlerFromStruct_VariantInfo(t *testing.T) {
	type VariantType struct {
		Metadata []byte `parquet:"name=metadata, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY, compression=ZSTD"`
		Value    []byte `parquet:"name=value, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY, compression=SNAPPY"`
	}

	type AllTypes struct {
		Variant VariantType `parquet:"name=Variant, type=VARIANT, logicaltype=VARIANT, logicaltype.specification_version=1"`
	}

	sh, err := NewSchemaHandlerFromStruct(new(AllTypes))
	require.NoError(t, err)

	// Parquet_go_root (0)
	//   Variant (1)
	//     metadata (2)
	//     value (3)

	require.Equal(t, int32(2), sh.SchemaElements[1].GetNumChildren())
	require.NotNil(t, sh.SchemaElements[1].LogicalType)
	require.NotNil(t, sh.SchemaElements[1].LogicalType.VARIANT)

	// Check metadata field (index 2)
	require.Equal(t, "Metadata", sh.SchemaElements[2].GetName())
	require.Equal(t, "metadata", sh.Infos[2].ExName)
	require.Equal(t, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, sh.Infos[2].Encoding)
	require.NotNil(t, sh.Infos[2].CompressionCodec)
	require.Equal(t, parquet.CompressionCodec_ZSTD, *sh.Infos[2].CompressionCodec)

	// Check value field (index 3)
	require.Equal(t, "Value", sh.SchemaElements[3].GetName())
	require.Equal(t, "value", sh.Infos[3].ExName)
	require.Equal(t, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, sh.Infos[3].Encoding)
	require.NotNil(t, sh.Infos[3].CompressionCodec)
	require.Equal(t, parquet.CompressionCodec_SNAPPY, *sh.Infos[3].CompressionCodec)
}

func TestNewSchemaHandlerFromStruct_ByteArraySlice(t *testing.T) {
	type MyStruct struct {
		Data []byte `parquet:"name=data, type=BYTE_ARRAY"`
	}

	sh, err := NewSchemaHandlerFromStruct(new(MyStruct))
	require.NoError(t, err)

	// Root (0)
	//   Data (1)
	require.Equal(t, 2, len(sh.SchemaElements))
	require.Equal(t, int32(0), sh.SchemaElements[1].GetNumChildren())
	require.Equal(t, parquet.Type_BYTE_ARRAY, *sh.SchemaElements[1].Type)
}
