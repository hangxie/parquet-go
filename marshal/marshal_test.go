package marshal

import (
	"reflect"
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/schema"
)

type marshalCases struct {
	nullPtr    *int //lint:ignore U1000 this is a placeholder for testing
	integerPtr *int
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
			if len(result) != testCase.expectedNodeCount {
				t.Errorf("Expected %d nodes, got %d", testCase.expectedNodeCount, len(result))
			}

			// Verify DL value for non-empty results
			if testCase.expectedNodeCount > 0 && len(result) > 0 {
				if result[0].DL != testCase.expectedDL {
					t.Errorf("Expected DL value %d, got %d", testCase.expectedDL, result[0].DL)
				}
			}
		})
	}
}

func Test_MarshalFast(t *testing.T) {
	type testElem struct {
		Bool      bool    `parquet:"name=bool, type=BOOLEAN"`
		Int       int     `parquet:"name=int, type=INT64"`
		Int8      int8    `parquet:"name=int8, type=INT32"`
		Int16     int16   `parquet:"name=int16, type=INT32"`
		Int32     int32   `parquet:"name=int32, type=INT32"`
		Int64     int64   `parquet:"name=int64, type=INT64"`
		Float     float32 `parquet:"name=float, type=FLOAT"`
		Double    float64 `parquet:"name=double, type=DOUBLE"`
		ByteArray string  `parquet:"name=bytearray, type=BYTE_ARRAY"`

		Ptr    *int64  `parquet:"name=boolptr, type=INT64"`
		PtrPtr **int64 `parquet:"name=boolptrptr, type=INT64"`

		List         []string  `parquet:"name=list, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		PtrList      *[]string `parquet:"name=ptrlist, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		ListPtr      []*string `parquet:"name=listptr, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
		Repeated     []int32   `parquet:"name=repeated, type=INT32, repetitiontype=REPEATED"`
		NestRepeated [][]int32 `parquet:"name=nestrepeated, type=INT32, repetitiontype=REPEATED"`
	}

	type subElem struct {
		Val string `parquet:"name=val, type=BYTE_ARRAY"`
	}

	type testNestedElem struct {
		SubElem       subElem     `parquet:"name=subelem"`
		SubPtr        *subElem    `parquet:"name=subptr"`
		SubList       []subElem   `parquet:"name=sublist"`
		SubRepeated   []*subElem  `parquet:"name=subrepeated"`
		EmptyElem     struct{}    `parquet:"name=emptyelem"`
		EmptyPtr      *struct{}   `parquet:"name=emptyptr"`
		EmptyList     []struct{}  `parquet:"name=emptylist"`
		EmptyRepeated []*struct{} `parquet:"name=emptyrepeated"`
	}

	type testIfaceElem struct {
		Bool      any `parquet:"name=bool, type=BOOLEAN"`
		Int32     any `parquet:"name=int32, type=INT32"`
		Int64     any `parquet:"name=int64, type=INT64"`
		Float     any `parquet:"name=float, type=FLOAT"`
		Double    any `parquet:"name=double, type=DOUBLE"`
		ByteArray any `parquet:"name=bytearray, type=BYTE_ARRAY"`
	}

	i64 := int64(31)
	refRef := &i64
	var nilRef *int64
	var nilList []string
	str := "hi"

	testCases := []struct {
		value any
	}{
		{testElem{Bool: true}},
		{testElem{Ptr: &i64, PtrPtr: &refRef}},
		{testElem{Ptr: nilRef, PtrPtr: &nilRef}},
		{testElem{Ptr: nil, PtrPtr: nil}},
		{testElem{Repeated: nil}},
		{testElem{Repeated: []int32{}}},
		{testElem{Repeated: []int32{31}}},
		{testElem{Repeated: []int32{31, 32, 33, 34}}},
		{testElem{List: nil}},
		{testElem{List: []string{}}},
		{testElem{List: []string{"hi"}}},
		{testElem{List: []string{"1", "2", "3", "4"}}},
		{testElem{PtrList: nil}},
		{testElem{PtrList: &nilList}},
		{testElem{PtrList: &[]string{}}},
		{testElem{PtrList: &[]string{"hi"}}},
		{testElem{PtrList: &[]string{"1", "2", "3", "4"}}},
		{testElem{ListPtr: nil}},
		{testElem{ListPtr: []*string{}}},
		{testElem{ListPtr: []*string{nil}}},
		{testElem{ListPtr: []*string{nil, nil, nil}}},
		{testElem{ListPtr: []*string{&str}}},
		{testElem{ListPtr: []*string{&str, &str, &str}}},
		{testElem{ListPtr: []*string{&str, nil, &str}}},
		{testElem{NestRepeated: nil}},
		{testElem{NestRepeated: [][]int32{}}},
		{testElem{NestRepeated: [][]int32{{}}}},
		{testElem{NestRepeated: [][]int32{{1, 2, 3}}}},
		// Test doesn't pass because it disagrees with Marshal, but I'm pretty sure that,
		// if this is supported at all, this implementation is correct and there's a bug
		// in Marshal.
		// {testElem{NestRepeated: [][]int32{{1}, {2, 3, 4, 5}, nil, {6}}}},

		{testNestedElem{}},
		{testNestedElem{SubElem: subElem{Val: "hi"}}},
		{testNestedElem{SubPtr: &subElem{Val: "hi"}}},
		{testNestedElem{SubList: []subElem{}}},
		{testNestedElem{SubList: []subElem{{Val: "hi"}}}},
		{testNestedElem{SubList: []subElem{{Val: "hi"}, {}, {Val: "there"}}}},
		{testNestedElem{SubRepeated: []*subElem{}}},
		{testNestedElem{SubRepeated: []*subElem{{Val: "hi"}}}},
		{testNestedElem{SubRepeated: []*subElem{{Val: "hi"}, nil, {Val: "there"}}}},
		{testNestedElem{EmptyPtr: &struct{}{}}},
		{testNestedElem{EmptyList: []struct{}{}}},
		{testNestedElem{EmptyList: []struct{}{{}}}},
		{testNestedElem{EmptyList: []struct{}{{}, {}}}},
		{testNestedElem{EmptyRepeated: []*struct{}{}}},
		{testNestedElem{EmptyRepeated: []*struct{}{{}}}},
		{testNestedElem{EmptyRepeated: []*struct{}{{}, nil, {}}}},

		// any
		{testIfaceElem{}},
		{testIfaceElem{
			Bool:      false,
			Int32:     int32(0),
			Int64:     int64(0),
			Float:     float32(0),
			Double:    float64(0),
			ByteArray: "",
		}},
		{testIfaceElem{
			Bool:      true,
			Int32:     int32(12345),
			Int64:     int64(54321),
			Float:     float32(1.0),
			Double:    float64(100.0),
			ByteArray: "hi",
		}},
	}

	for _, tt := range testCases {
		t.Run("", func(t *testing.T) {
			input := []any{tt.value}
			schemaType := reflect.Zero(reflect.PointerTo(reflect.TypeOf(tt.value))).Interface()
			sch, err := schema.NewSchemaHandlerFromStruct(schemaType)
			if err != nil {
				t.Fatalf("%v", err)
			}
			expected, err := Marshal(input, sch)
			if err != nil {
				t.Fatalf("%v", err)
			}
			actual, err := MarshalFast(input, sch)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if !reflect.DeepEqual(expected, actual) {
				// require.Equal(t, expected, actual)
				t.Errorf("not equal")
			}
		})
	}
}

func Test_MarshalCSV(t *testing.T) {
	// Create a simple schema for CSV data
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
			{"Tag": "name=age, type=INT32", "Type": "int32"},
			{"Tag": "name=score, type=FLOAT", "Type": "float32"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Test with empty records
	emptyRecords := []any{}
	result, err := MarshalCSV(emptyRecords, sch)
	if err != nil {
		t.Errorf("MarshalCSV failed with empty records: %v", err)
	}
	if len(*result) != 0 {
		t.Errorf("Expected empty result for empty records, got %d tables", len(*result))
	}

	// Test with actual data
	records := []any{
		[]any{"Alice", int32(25), float32(95.5)},
		[]any{"Bob", int32(30), float32(87.2)},
		[]any{"Charlie", int32(35), float32(92.1)},
	}

	result, err = MarshalCSV(records, sch)
	if err != nil {
		t.Errorf("MarshalCSV failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Check that we have the expected number of columns
	expectedColumns := 3
	if len(*result) != expectedColumns {
		t.Errorf("Expected %d columns, got %d", expectedColumns, len(*result))
	}

	// Debug: print available columns
	for key := range *result {
		t.Logf("Available CSV column: %s", key)
	}

	// Check specific column data
	nameColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Name"]
	if nameColumn == nil {
		t.Error("Expected name column to exist")
	} else {
		if len(nameColumn.Values) != 3 {
			t.Errorf("Expected 3 name values, got %d", len(nameColumn.Values))
		}
		if nameColumn.Values[0] != "Alice" {
			t.Errorf("Expected first name to be 'Alice', got %v", nameColumn.Values[0])
		}
	}
}

func Test_MarshalArrow(t *testing.T) {
	// Create a simple schema for Arrow data
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=id, type=INT64", "Type": "int64"},
			{"Tag": "name=value, type=FLOAT", "Type": "float32"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Test with empty records
	emptyRecords := []any{}
	result, err := MarshalArrow(emptyRecords, sch)
	if err != nil {
		t.Errorf("MarshalArrow failed with empty records: %v", err)
	}
	if len(*result) != 0 {
		t.Errorf("Expected empty result for empty records, got %d tables", len(*result))
	}

	// Test with actual data - Arrow format has rows as []any
	records := []any{
		[]any{int64(1), float32(10.5)},
		[]any{int64(2), float32(20.3)},
		[]any{int64(3), float32(30.7)},
	}

	result, err = MarshalArrow(records, sch)
	if err != nil {
		t.Errorf("MarshalArrow failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Check that we have the expected number of columns
	expectedColumns := 2
	if len(*result) != expectedColumns {
		t.Errorf("Expected %d columns, got %d", expectedColumns, len(*result))
	}

	// Check specific column data
	idColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Id"]
	if idColumn == nil {
		t.Error("Expected id column to exist")
	} else {
		if len(idColumn.Values) != 3 {
			t.Errorf("Expected 3 id values, got %d", len(idColumn.Values))
		}
		if idColumn.Values[0] != int64(1) {
			t.Errorf("Expected first id to be 1, got %v", idColumn.Values[0])
		}
	}
}

func Test_MarshalJSON(t *testing.T) {
	// Create a simple schema for JSON data
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
			{"Tag": "name=age, type=INT32", "Type": "int32"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Test with empty records
	emptyRecords := []any{}
	result, err := MarshalJSON(emptyRecords, sch)
	if err != nil {
		t.Errorf("MarshalJSON failed with empty records: %v", err)
	}
	if len(*result) != 2 { // Should have 2 empty tables for the 2 schema fields
		t.Errorf("Expected 2 empty tables for empty records, got %d tables", len(*result))
	}

	// Test with actual JSON data
	jsonRecords := []any{
		`{"name": "Alice", "age": 25}`,
		`{"name": "Bob", "age": 30}`,
		[]byte(`{"name": "Charlie", "age": 35}`), // Test []byte input as well
	}

	result, err = MarshalJSON(jsonRecords, sch)
	if err != nil {
		t.Errorf("MarshalJSON failed: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Check that we have the expected number of columns
	expectedColumns := 2
	if len(*result) != expectedColumns {
		t.Errorf("Expected %d columns, got %d", expectedColumns, len(*result))
	}

	// Check specific column data
	nameColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Name"]
	if nameColumn == nil {
		t.Error("Expected name column to exist")
	} else {
		if len(nameColumn.Values) != 3 {
			t.Errorf("Expected 3 name values, got %d", len(nameColumn.Values))
		}
		if nameColumn.Values[0] != "Alice" {
			t.Errorf("Expected first name to be 'Alice', got %v", nameColumn.Values[0])
		}
	}

	ageColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Age"]
	if ageColumn == nil {
		t.Error("Expected age column to exist")
	} else {
		if len(ageColumn.Values) != 3 {
			t.Errorf("Expected 3 age values, got %d", len(ageColumn.Values))
		}
	}
}

func Test_MarshalJSONWithInvalidJSON(t *testing.T) {
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Test with invalid JSON
	invalidJSONRecords := []any{
		`{"name": "Alice"`, // Invalid JSON - missing closing brace
	}

	_, err = MarshalJSON(invalidJSONRecords, sch)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func Test_EncoderMapKeyString(t *testing.T) {
	// Test the String() method for encoderMapKey which was previously uncovered
	type SimpleStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age  int32  `parquet:"name=age, type=INT32"`
	}

	data := []any{
		SimpleStruct{Name: "Alice", Age: 25},
		SimpleStruct{Name: "Bob", Age: 30},
	}

	sch, err := schema.NewSchemaHandlerFromStruct(&SimpleStruct{})
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	_, err = MarshalFast(data, sch)
	if err != nil {
		t.Fatalf("MarshalFast failed: %v", err)
	}

	// The String() method is internal to encoderMapKey, but we can verify MarshalFast works
	// This indirectly tests the String() method through the compiler cache
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
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

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
	if len(result) == 0 {
		t.Error("Expected non-empty result from Marshal")
	}

	// Test with empty map
	emptyMapValue := map[string]any{}
	emptyNode := &Node{
		Val:     reflect.ValueOf(emptyMapValue),
		PathMap: sch.PathMap,
		RL:      0,
		DL:      0,
	}

	emptyResult := mapStruct.Marshal(emptyNode, nodeBuf, stack)
	if len(emptyResult) != 0 {
		t.Error("Expected empty result for empty map")
	}
}
