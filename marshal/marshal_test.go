package marshal

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func TestMarshalVariant(t *testing.T) {
	type MyStruct struct {
		Var any `parquet:"name=var, type=VARIANT, repetitiontype=OPTIONAL"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(MyStruct))
	require.NoError(t, err)

	data := []any{
		MyStruct{
			Var: &types.Variant{
				Metadata: types.EncodeVariantMetadata([]string{"a"}),
				Value:    types.EncodeVariantInt8(123),
			},
		},
		MyStruct{
			Var: &types.Variant{
				Metadata: types.EncodeVariantMetadata([]string{"b"}),
				Value:    types.EncodeVariantString("hello"),
			},
		},
		MyStruct{Var: nil}, // nil variant
	}

	res, err := Marshal(data, sh)
	require.NoError(t, err)
	require.NotNil(t, res)

	// Check if all tables are present
	require.Contains(t, *res, common.ParGoRootInName+common.ParGoPathDelimiter+"Var"+common.ParGoPathDelimiter+"Metadata")
	require.Contains(t, *res, common.ParGoRootInName+common.ParGoPathDelimiter+"Var"+common.ParGoPathDelimiter+"Value")

	metadataTable := (*res)[common.ParGoRootInName+common.ParGoPathDelimiter+"Var"+common.ParGoPathDelimiter+"Metadata"]
	valueTable := (*res)[common.ParGoRootInName+common.ParGoPathDelimiter+"Var"+common.ParGoPathDelimiter+"Value"]

	require.Equal(t, 3, len(metadataTable.Values))
	require.Equal(t, 3, len(valueTable.Values))

	// Third row should be nil
	require.Nil(t, metadataTable.Values[2])
	require.Nil(t, valueTable.Values[2])
}

func TestMarshalVariant_Error(t *testing.T) {
	t.Run("unsupported_type", func(t *testing.T) {
		type MyStruct struct {
			Var any `parquet:"name=var, type=VARIANT"`
		}

		sh, err := schema.NewSchemaHandlerFromStruct(new(MyStruct))
		require.NoError(t, err)

		// complex128 is not supported by AnyToVariant
		data := []any{MyStruct{Var: complex(1, 2)}}
		_, err = Marshal(data, sh)
		require.Error(t, err)
		require.Contains(t, err.Error(), "convert to variant")
	})

	t.Run("missing_pathmap_children", func(t *testing.T) {
		type MyStruct struct {
			Var any `parquet:"name=var, type=VARIANT"`
		}

		sh, err := schema.NewSchemaHandlerFromStruct(new(MyStruct))
		require.NoError(t, err)

		// Manually break PathMap for the variant field
		pathMap := sh.PathMap.Children["Var"]
		require.NotNil(t, pathMap)

		// Save original children and delete one
		origChildren := pathMap.Children
		pathMap.Children = make(map[string]*schema.PathMapType)
		for k, v := range origChildren {
			if k != "Value" { // missing "Value"
				pathMap.Children[k] = v
			}
		}

		data := []any{MyStruct{Var: types.Variant{
			Metadata: types.EncodeVariantMetadata([]string{"a"}),
			Value:    types.EncodeVariantInt8(123),
		}}}

		// Since we want to test HandleVariant directly or through Marshal
		// Marshal uses mapStruct.Marshal internally
		_, err = Marshal(data, sh)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing required children")
	})
}

type marshalCases struct {
	nullPtr    *int //lint:ignore U1000 this is a placeholder for testing
	integerPtr *int
}

func TestParquetMapStructMarshal(t *testing.T) {
	// Create a simple schema to test map struct marshaling
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Create a ParquetMapStruct instance
	mapStruct := &ParquetMapStruct{schemaHandler: sch}

	// Create a map value to marshal
	mapValue := map[string]any{
		"name": "Alice",
	}

	// Create a node with the map value
	node := &Node{
		Val:     reflect.ValueOf(mapValue),
		PathMap: sch.PathMap,
		RL:      0,
		DL:      0,
	}

	nodeBuf := NewNodeBuf(10)
	stack := []*Node{}

	// Test marshaling
	result, err := mapStruct.Marshal(node, nodeBuf, stack)
	require.NoError(t, err)
	require.NotEmpty(t, result)

	// Test with empty map
	emptyMapValue := map[string]any{}
	emptyNode := &Node{
		Val:     reflect.ValueOf(emptyMapValue),
		PathMap: sch.PathMap,
		RL:      0,
		DL:      0,
	}

	emptyResult, err := mapStruct.Marshal(emptyNode, nodeBuf, stack)
	require.NoError(t, err)
	require.Empty(t, emptyResult)
}

func TestMarshalNilPathMapReturnsError(t *testing.T) {
	// Create a simple schema
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Manually create a SchemaHandler with nil PathMap to simulate the bug condition
	// This tests the defensive nil check added to Marshal
	schNilPathMap := &schema.SchemaHandler{
		SchemaElements: sch.SchemaElements,
		MapIndex:       sch.MapIndex,
		IndexMap:       sch.IndexMap,
		Infos:          sch.Infos,
		PathMap:        nil, // Intentionally nil to test the defensive check
	}

	// Try to marshal with nil PathMap - should return error, not panic
	data := []any{map[string]any{"name": "test"}}
	_, err = Marshal(data, schNilPathMap)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil PathMap")
}

func TestParquetMapStructMarshal_MissingKeys(t *testing.T) {
	// Schema expects two fields, but the map only provides one
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
			{"Tag": "name=age, type=INT32", "Type": "int32"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	mapStruct := &ParquetMapStruct{schemaHandler: sch}

	// Only provide "name", missing "age"
	mapValue := map[string]any{
		"Name": "Alice",
	}

	node := &Node{
		Val:     reflect.ValueOf(mapValue),
		PathMap: sch.PathMap,
		RL:      0,
		DL:      0,
	}

	nodeBuf := NewNodeBuf(10)
	stack := []*Node{}

	result, err := mapStruct.Marshal(node, nodeBuf, stack)
	require.NoError(t, err)
	// Should have nodes for the present key plus nil nodes for missing keys
	require.NotEmpty(t, result)

	// Find the node for the missing key — it should have an invalid reflect.Value
	var foundMissing bool
	for _, n := range result {
		if !n.Val.IsValid() {
			foundMissing = true
			break
		}
	}
	require.True(t, foundMissing)
}

func TestMarshalByteArrayField(t *testing.T) {
	type ByteStruct struct {
		Data []byte `parquet:"name=data, type=BYTE_ARRAY"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(ByteStruct))
	require.NoError(t, err)

	data := []any{
		ByteStruct{Data: []byte{0x01, 0x02, 0x03}},
		ByteStruct{Data: []byte{0xAA, 0xBB}},
	}

	res, err := Marshal(data, sh)
	require.NoError(t, err)
	require.NotNil(t, res)

	table := (*res)[common.ParGoRootInName+common.ParGoPathDelimiter+"Data"]
	require.NotNil(t, table)
	require.Equal(t, 2, len(table.Values))
	// Values should be non-nil strings (BYTE_ARRAY stored as string in parquet)
	require.NotNil(t, table.Values[0])
	require.NotNil(t, table.Values[1])
}

func TestMarshalEmptyContainer(t *testing.T) {
	type SliceStruct struct {
		Items []string `parquet:"name=items, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8, repetitiontype=OPTIONAL"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(SliceStruct))
	require.NoError(t, err)

	data := []any{
		SliceStruct{Items: []string{}}, // empty slice
	}

	res, err := Marshal(data, sh)
	require.NoError(t, err)
	require.NotNil(t, res)

	// With an empty slice, child tables should get nil appended
	for key, table := range *res {
		if key != "" {
			require.Equal(t, 1, len(table.Values))
			require.Nil(t, table.Values[0])
		}
	}
}

func TestParquetPtrMarshal(t *testing.T) {
	integer := 10
	testData := &marshalCases{
		integerPtr: &integer,
	}

	testCases := []struct {
		name              string
		fieldName         string
		expectedNodeCount int
		expectedDL        int32
	}{
		{
			name:              "null-pointer-field",
			fieldName:         "nullPtr",
			expectedNodeCount: 0,
			expectedDL:        0, // Not used for null case
		},
		{
			name:              "valid-integer-pointer-field",
			fieldName:         "integerPtr",
			expectedNodeCount: 1,
			expectedDL:        4,
		},
	}

	ptrMarshal := &ParquetPtr{}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			node := &Node{
				Val:     reflect.ValueOf(testData).Elem().FieldByName(testCase.fieldName),
				PathMap: nil,
				RL:      2,
				DL:      3,
			}

			stack := []*Node{}
			result, err := ptrMarshal.Marshal(node, nil, stack)
			require.NoError(t, err)

			// Verify node count
			require.Len(t, result, testCase.expectedNodeCount)

			// Verify DL value for non-empty results
			if testCase.expectedNodeCount > 0 && len(result) > 0 {
				require.Equal(t, testCase.expectedDL, result[0].DL)
			}
		})
	}
}
