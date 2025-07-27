package marshal

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/schema"
)

type marshalCases struct {
	nullPtr    *int //lint:ignore U1000 this is a placeholder for testing
	integerPtr *int
}

func Test_ParquetMapStructMarshal(t *testing.T) {
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

func Test_ParquetPtrMarshal(t *testing.T) {
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
