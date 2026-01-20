package marshal

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/types"
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
	require.Contains(t, *res, "Parquet_go_root\x01Var\x01Metadata")
	require.Contains(t, *res, "Parquet_go_root\x01Var\x01Value")

	metadataTable := (*res)["Parquet_go_root\x01Var\x01Metadata"]
	valueTable := (*res)["Parquet_go_root\x01Var\x01Value"]

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
	result := mapStruct.Marshal(node, nodeBuf, stack)
	require.NotEmpty(t, result)

	// Test with empty map
	emptyMapValue := map[string]any{}
	emptyNode := &Node{
		Val:     reflect.ValueOf(emptyMapValue),
		PathMap: sch.PathMap,
		RL:      0,
		DL:      0,
	}

	emptyResult := mapStruct.Marshal(emptyNode, nodeBuf, stack)
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

func TestParquetPtrMarshal(t *testing.T) {
	var integer int = 10
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
			result := ptrMarshal.Marshal(node, nil, stack)

			// Verify node count
			require.Len(t, result, testCase.expectedNodeCount)

			// Verify DL value for non-empty results
			if testCase.expectedNodeCount > 0 && len(result) > 0 {
				require.Equal(t, testCase.expectedDL, result[0].DL)
			}
		})
	}
}
