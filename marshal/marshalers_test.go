package marshal

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/schema"
)

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
