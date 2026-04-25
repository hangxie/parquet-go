package marshal

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

type Student struct {
	Name    string               `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age     int32                `parquet:"name=age, type=INT32"`
	Weight  *int32               `parquet:"name=weight, type=INT32"`
	Classes *map[string][]*Class `parquet:"name=classes, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
}

type Class struct {
	Name     string   `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	ID       *int64   `parquet:"name=id, type=INT64"`
	Required []string `parquet:"name=required, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

func (c Class) String() string {
	id := "nil"
	if c.ID != nil {
		id = fmt.Sprintf("%d", *c.ID)
	}
	res := fmt.Sprintf("{Name:%s, ID:%v, Required:%s}", c.Name, id, fmt.Sprint(c.Required))
	return res
}

func (s Student) String() string {
	weight := "nil"
	if s.Weight != nil {
		weight = fmt.Sprintf("%d", *s.Weight)
	}

	cs := "{"
	for key, classes := range *s.Classes {
		s := string(key) + ":["
		for _, class := range classes {
			s += (*class).String() + ","
		}
		s += "]"
		cs += s
	}
	cs += "}"
	res := fmt.Sprintf("{Name:%s, Age:%d, Weight:%s, Classes:%s}", s.Name, s.Age, weight, cs)
	return res
}

func TestMarshalUnmarshal(t *testing.T) {
	schemaHandler, _ := schema.NewSchemaHandlerFromStruct(new(Student))

	math01ID := int64(1)
	math01 := Class{
		Name:     "Math1",
		ID:       &math01ID,
		Required: make([]string, 0),
	}

	math02ID := int64(2)
	math02 := Class{
		Name:     "Math2",
		ID:       &math02ID,
		Required: make([]string, 0),
	}
	math02.Required = append(math02.Required, "Math01")

	physics := Class{
		Name:     "Physics",
		ID:       nil,
		Required: make([]string, 0),
	}
	physics.Required = append(physics.Required, "Math01", "Math02")

	weight01 := int32(60)
	stu01Class := make(map[string][]*Class)
	stu01Class["Science"] = make([]*Class, 0)
	stu01Class["Science"] = append(stu01Class["Science"], &math01, &math02)
	stu01 := Student{
		Name:    "zxt",
		Age:     18,
		Weight:  &weight01,
		Classes: &stu01Class,
	}

	stu02Class := make(map[string][]*Class)
	stu02Class["Science"] = make([]*Class, 0)
	stu02Class["Science"] = append(stu02Class["Science"], &physics)
	stu02 := Student{
		Name:    "tong",
		Age:     29,
		Weight:  nil,
		Classes: &stu02Class,
	}

	stus := make([]any, 0)
	stus = append(stus, stu01, stu02)

	src, _ := Marshal(stus, schemaHandler)

	for range *src {
		// Iteration over marshaled data for testing
	}

	dst := make([]Student, 0)
	// return error till we can change function signature
	_ = Unmarshal(src, 0, len(stus), &dst, schemaHandler, "")

	s0 := fmt.Sprint(stus)
	s1 := fmt.Sprint(dst)
	require.Equal(t, s0, s1)
}

func TestUnmarshal_PanicZeroValue(t *testing.T) {
	type TestStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(TestStruct))
	require.NoError(t, err)

	tableMap := map[string]*layout.Table{
		"name": {
			Path:             []string{"name"}, // Start with valid field, but we'll simulate corruption
			Values:           []interface{}{"test_value"},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{0},
		},
	}
	dst := (*any)(nil)
	err = Unmarshal(&tableMap, 0, 1, dst, schemaHandler, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "dstInterface must be a non-nil pointer")
}

func TestUnmarshalStringToByteArray(t *testing.T) {
	// Marshal stores string as BYTE_ARRAY, verify Unmarshal round-trip is consistent.
	type ByteStruct struct {
		Data string `parquet:"name=data, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(ByteStruct))
	require.NoError(t, err)

	original := []any{
		ByteStruct{Data: "hello"},
		ByteStruct{Data: "world"},
	}

	tableMap, err := Marshal(original, sh)
	require.NoError(t, err)

	dst := make([]ByteStruct, 0)
	err = Unmarshal(tableMap, 0, 2, &dst, sh, "")
	require.NoError(t, err)

	require.Len(t, dst, 2)
	require.Equal(t, "hello", dst[0].Data)
	require.Equal(t, "world", dst[1].Data)
}

func TestUnmarshal_ErrorHandling_ReflectionSafety(t *testing.T) {
	// This test verifies that the new error handling and safety checks work properly
	// by testing the unmarshal function with edge cases that could cause panics

	type SimpleTestStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(SimpleTestStruct))
	require.NoError(t, err)

	t.Run("test_graceful_error_handling", func(t *testing.T) {
		// Test that the function doesn't panic on various edge cases
		// The key improvement is that panics should be converted to errors

		// First, test with a properly structured simple case
		src := []SimpleTestStruct{{Name: "test"}}
		marshaled, err := Marshal([]any{src[0]}, schemaHandler)
		require.NoError(t, err)

		dst := make([]SimpleTestStruct, 0)
		err = Unmarshal(marshaled, 0, 1, &dst, schemaHandler, "")
		require.NoError(t, err)
		require.Len(t, dst, 1)
		require.Equal(t, "test", dst[0].Name)
	})

	t.Run("test_nil_destination_handling", func(t *testing.T) {
		// Test nil destination - this should return a clear error, not panic
		src := []SimpleTestStruct{{Name: "test"}}
		marshaled, err := Marshal([]any{src[0]}, schemaHandler)
		require.NoError(t, err)

		err = Unmarshal(marshaled, 0, 1, nil, schemaHandler, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "dstInterface must be a non-nil pointer")
	})
}

func TestUnmarshal_EdgeCases_InvalidValues(t *testing.T) {
	// This test ensures the improved error handling works with edge cases
	type SimpleStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(SimpleStruct))
	require.NoError(t, err)

	t.Run("empty_values_handling", func(t *testing.T) {
		// Test with empty data - this should not panic with the new safety checks
		marshaledData := make([]any, 0)

		marshaled, err := Marshal(marshaledData, schemaHandler)
		require.NoError(t, err)

		dst := make([]SimpleStruct, 0)
		err = Unmarshal(marshaled, 0, 0, &dst, schemaHandler, "") // Note: end=0 for empty data
		require.NoError(t, err)
		require.Len(t, dst, 0)
	})

	t.Run("safety_improvements_verification", func(t *testing.T) {
		// The main improvement is that the code now has extensive error handling
		// and safety checks to prevent panics. This test verifies basic functionality.
		src := []SimpleStruct{{Name: "test"}}
		marshaled, err := Marshal([]any{src[0]}, schemaHandler)
		require.NoError(t, err)

		dst := make([]SimpleStruct, 0)
		err = Unmarshal(marshaled, 0, 1, &dst, schemaHandler, "")
		require.NoError(t, err)
		require.Len(t, dst, 1)
		require.Equal(t, "test", dst[0].Name)
	})
}

func TestUnmarshal_ReflectionSafety_TypeConversion(t *testing.T) {
	// This test focuses on the new safety features added to handle type conversion and reflection errors
	type ConversionTestStruct struct {
		IntField    int32  `parquet:"name=int_field, type=INT32"`
		StringField string `parquet:"name=string_field, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(ConversionTestStruct))
	require.NoError(t, err)

	// Test that the function properly handles various scenarios without panicking
	t.Run("valid_type_conversion", func(t *testing.T) {
		// Test with valid data that should work
		src := []ConversionTestStruct{{IntField: 42, StringField: "test"}}
		marshaled, err := Marshal([]any{src[0]}, schemaHandler)
		require.NoError(t, err)

		dst := make([]ConversionTestStruct, 0)
		err = Unmarshal(marshaled, 0, 1, &dst, schemaHandler, "")
		require.NoError(t, err)
		require.Len(t, dst, 1)
		require.Equal(t, int32(42), dst[0].IntField)
		require.Equal(t, "test", dst[0].StringField)
	})

	t.Run("test_reflection_safety_improvements", func(t *testing.T) {
		// The key improvement in the new code is that it adds extensive error handling
		// and safety checks to prevent panics in reflection operations.
		// This test verifies that the basic functionality still works correctly.

		src := []ConversionTestStruct{
			{IntField: 123, StringField: "hello"},
			{IntField: 456, StringField: "world"},
		}

		// Marshal the data
		marshaledData := make([]any, len(src))
		for i, s := range src {
			marshaledData[i] = s
		}

		marshaled, err := Marshal(marshaledData, schemaHandler)
		require.NoError(t, err)

		// Unmarshal the data back
		dst := make([]ConversionTestStruct, 0)
		err = Unmarshal(marshaled, 0, len(src), &dst, schemaHandler, "")
		require.NoError(t, err)
		require.Len(t, dst, len(src))

		for i, expected := range src {
			require.Equal(t, expected.IntField, dst[i].IntField)
			require.Equal(t, expected.StringField, dst[i].StringField)
		}
	})
}

// Additional coverage for marshal/unmarshal.go focusing on edge branches
func TestUnmarshal_TypeConversionAndNilHandling(t *testing.T) {
	type Conv struct {
		A int64 `parquet:"name=a, type=INT64"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(Conv))
	require.NoError(t, err)

	// Start with a valid marshal
	src := Conv{A: 123}
	tmap, err := Marshal([]any{src}, sh)
	require.NoError(t, err)

	// Force a type conversion path: replace the value with int32 to trigger reflect.Value.Convert
	// Locate the table whose last path segment is the field name "A"
	var keyForA string
	for k, tbl := range *tmap {
		if len(tbl.Path) > 0 && tbl.Path[len(tbl.Path)-1] == "A" {
			keyForA = k
			break
		}
	}
	require.NotEmpty(t, keyForA)

	tblA := (*tmap)[keyForA]
	require.Len(t, tblA.Values, 1)
	tblA.Values[0] = int32(123) // different type than destination (int64)

	dst := make([]Conv, 0)
	require.NoError(t, Unmarshal(tmap, 0, 1, &dst, sh, ""))
	require.Len(t, dst, 1)
	require.Equal(t, int64(123), dst[0].A)

	// Now exercise the val == nil path in default case; value should be skipped gracefully
	tblA.Values[0] = nil
	dst = make([]Conv, 0)
	require.NoError(t, Unmarshal(tmap, 0, 1, &dst, sh, ""))
	require.Len(t, dst, 1)
	require.Equal(t, int64(0), dst[0].A) // remained zero value
}

func TestUnmarshal_FieldNotFound(t *testing.T) {
	type Pair struct {
		A int32 `parquet:"name=a, type=INT32"`
		B int32 `parquet:"name=b, type=INT32"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(Pair))
	require.NoError(t, err)

	src := Pair{A: 10, B: 20}
	tmap, err := Marshal([]any{src}, sh)
	require.NoError(t, err)

	// Field not found: mutate a table path to a non-existent field name to hit the error branch
	// Find table for A and set its last path segment to a non-existing field
	var keyForA string
	for k, tbl := range *tmap {
		if len(tbl.Path) > 0 && tbl.Path[len(tbl.Path)-1] == "A" {
			keyForA = k
			break
		}
	}
	require.NotEmpty(t, keyForA)

	tblA := (*tmap)[keyForA]
	if len(tblA.Path) > 0 {
		tblA.Path[len(tblA.Path)-1] = "NotExist"
	}

	dst := make([]Pair, 0)
	err = Unmarshal(tmap, 0, 1, &dst, sh, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "name not in schema")
}

func TestUnmarshal_MapAndList_MultipleEntries(t *testing.T) {
	type Combo struct {
		M map[string]int32 `parquet:"name=m, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
		L []int32          `parquet:"name=l, type=LIST, valuetype=INT32"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(Combo))
	require.NoError(t, err)

	src := []Combo{
		{M: map[string]int32{"a": 1, "b": 2}, L: []int32{7, 8}},
		{M: map[string]int32{"x": 9}, L: []int32{10}},
	}
	items := make([]any, len(src))
	for i := range src {
		items[i] = src[i]
	}

	tmap, err := Marshal(items, sh)
	require.NoError(t, err)

	// Unmarshal back and ensure data integrity; this also exercises slice/map stacks flushing
	dst := make([]Combo, 0)
	require.NoError(t, Unmarshal(tmap, 0, len(src), &dst, sh, ""))
	require.Equal(t, src, dst)
}

// Test helper types and functions for old list format testing
type TestTarget [][]int32

type OldListTestStruct struct {
	Array []int32 `json:"array"`
}

// createOldListSchema creates the standard old list schema pattern used across tests
func createOldListSchema() *schema.SchemaHandler {
	schemaElements := []*parquet.SchemaElement{
		{Name: common.ParGoRootInName, RepetitionType: nil, NumChildren: common.ToPtr(int32(1))},
		{Name: "array", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED), ConvertedType: common.ToPtr(parquet.ConvertedType_LIST), NumChildren: common.ToPtr(int32(1))},
		{Name: "array", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED), Type: common.ToPtr(parquet.Type_INT32)},
	}

	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)
	// Set up the critical MapIndex entries for old list detection
	schemaHandler.MapIndex[common.ParGoRootInName] = 1
	schemaHandler.MapIndex[common.ParGoRootInName+common.ParGoPathDelimiter+"array"] = 1
	schemaHandler.MapIndex[common.ParGoRootInName+common.ParGoPathDelimiter+"array"+common.ParGoPathDelimiter+"array"] = 2

	return schemaHandler
}

// createOldListTable creates a table map for old list testing
func createOldListTable(path []string, values []any, repetitionLevels, definitionLevels []int32) *map[string]*layout.Table {
	pathKey := common.ParGoRootInName + common.ParGoPathDelimiter + strings.Join(path, common.ParGoPathDelimiter)
	return &map[string]*layout.Table{
		pathKey: {
			Path:             path,
			Values:           values,
			RepetitionLevels: repetitionLevels,
			DefinitionLevels: definitionLevels,
		},
	}
}

// createConversionContext creates a standard conversion context for JSON conversion tests
func createConversionContext() *jsonConverter {
	return &jsonConverter{
		fieldCache: sync.Map{},
	}
}

func TestUnmarshal_MapPathMissingKeyValueComponent(t *testing.T) {
	// Test that a malformed path missing key/value component after map returns an error
	// instead of panicking with index out of bounds
	type MapStruct struct {
		M map[string]int32 `parquet:"name=m, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	}

	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(MapStruct))
	require.NoError(t, err)

	// Create a table with a malformed path that ends at the map without key/value
	// This simulates corrupted data where the path is truncated
	tableMap := &map[string]*layout.Table{
		common.ParGoRootInName + ".M.Key_value": {
			Path:             []string{common.ParGoRootInName, "M", "Key_value"}, // Missing "Key" or "Value" after Key_value
			Values:           []any{"test_key"},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{3},
		},
	}

	dst := make([]MapStruct, 0)
	err = Unmarshal(tableMap, 0, 1, &dst, schemaHandler, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing key/value component after map")
}

// Test_OldStyleList targets test old style list
func TestOldStyleList(t *testing.T) {
	t.Run("unmarshal_old_list_multi_inner_segments", func(t *testing.T) {
		schemaHandler := createOldListSchema()

		values := []any{int32(1), int32(2), int32(3), int32(4), int32(5), int32(6), int32(7), int32(8)}
		repetitionLevels := []int32{0, 2, 1, 2, 0, 2, 1, 2, 0, 2, 1, 2}
		definitionLevels := []int32{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
		tableMap := createOldListTable([]string{common.ParGoRootInName, "array", "array"}, values, repetitionLevels, definitionLevels)

		result := make([]TestTarget, 0)
		require.NoError(t, Unmarshal(tableMap, 0, 3, &result, schemaHandler, ""))
		require.Len(t, result, 3)
	})

	t.Run("convertStructToJSONFriendly_old_list_format", func(t *testing.T) {
		val := reflect.ValueOf(OldListTestStruct{Array: []int32{1, 2, 3}})
		schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(OldListTestStruct))
		require.NoError(t, err)

		ctx := createConversionContext()

		result, err := convertStructToJSONFriendly(val, schemaHandler, "", ctx)
		require.NoError(t, err)

		// Should return the slice content directly, not wrapped in a map
		sliceResult, ok := result.([]interface{})
		require.True(t, ok)
		require.Len(t, sliceResult, 3)
	})

	t.Run("convertStructToJSONFriendly_with_path_prefix", func(t *testing.T) {
		val := reflect.ValueOf(OldListTestStruct{Array: []int32{42}})
		schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(OldListTestStruct))
		require.NoError(t, err)

		ctx := createConversionContext()

		result, err := convertStructToJSONFriendly(val, schemaHandler, "root.nested", ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("unmarshal_nil_value_handling", func(t *testing.T) {
		type TestStruct struct {
			Value *int32 `parquet:"name=value, type=INT32, repetitiontype=OPTIONAL"`
		}

		schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(TestStruct))
		require.NoError(t, err)

		tableMap := createOldListTable([]string{"value"}, []interface{}{nil}, []int32{0}, []int32{0})
		result := make([]TestStruct, 0)
		err = Unmarshal(tableMap, 0, 1, &result, schemaHandler, "")
		require.NoError(t, err)
	})

	t.Run("convertStructToJSONFriendly_unexported_fields", func(t *testing.T) {
		type MixedFieldStruct struct {
			ExportedField   int32 `json:"exported"`
			unexportedField int32 // This should be skipped
		}

		val := reflect.ValueOf(MixedFieldStruct{
			ExportedField:   42,
			unexportedField: 100,
		})

		schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(MixedFieldStruct))
		require.NoError(t, err)
		ctx := createConversionContext()
		result, err := convertStructToJSONFriendly(val, schemaHandler, "", ctx)
		require.NoError(t, err)

		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok)

		require.Contains(t, resultMap, "exported")
		require.Equal(t, int32(42), resultMap["exported"])
		require.NotContains(t, resultMap, "unexportedField")
	})
}

func TestUnmarshal_ErrorHandling_TypeConversion(t *testing.T) {
	// This test simulates a case where type conversion might fail.
	// We want to ensure that Unmarshal handles this gracefully by returning an error,
	// instead of panicking.

	type DestStruct struct {
		Field int `parquet:"name=field, type=INT64"`
	}

	// Create a schema handler for the destination struct
	sh, err := schema.NewSchemaHandlerFromStruct(new(DestStruct))
	require.NoError(t, err)

	// Create a table map that mimics a valid parquet structure
	// but provides a value that cannot be converted to the destination type
	// in a way that might trigger a panic if not checked.
	type IncompatibleType struct {
		Something string
	}

	tableMap := map[string]*layout.Table{
		common.ParGoRootInName + ".Field": {
			Path:             []string{common.ParGoRootInName, "Field"},
			Values:           []interface{}{IncompatibleType{Something: "bad"}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		},
	}

	dst := make([]DestStruct, 0)
	err = Unmarshal(&tableMap, 0, 1, &dst, sh, "")

	// We expect an error here, not a panic.
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot convert value of type")
}

func TestUnmarshal_ByteSlice(t *testing.T) {
	type ByteSliceStruct struct {
		Data []byte `parquet:"name=data, type=BYTE_ARRAY"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(ByteSliceStruct))
	require.NoError(t, err)

	original := []any{
		ByteSliceStruct{Data: []byte("hello")},
	}

	tmap, err := Marshal(original, sh)
	require.NoError(t, err)

	dst := make([]ByteSliceStruct, 0)
	err = Unmarshal(tmap, 0, 1, &dst, sh, "")
	require.NoError(t, err)

	require.Len(t, dst, 1)
	require.Equal(t, []byte("hello"), dst[0].Data)
}

func TestHandleOldListLeaf_Coverage(t *testing.T) {
	type OldList struct {
		Array []int32 `json:"array"`
	}

	// Test direct call to handleOldListLeaf
	ol := OldList{Array: []int32{}}
	handleOldListLeaf(reflect.ValueOf(&ol).Elem(), int32(42))
	require.Equal(t, []int32{42}, ol.Array)
}

func TestSetPrimitiveValue_ComplexTypes(t *testing.T) {
	type FixedLenStruct struct {
		Data string `parquet:"name=data, type=FIXED_LEN_BYTE_ARRAY, length=4"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(FixedLenStruct))
	require.NoError(t, err)

	original := []any{
		FixedLenStruct{Data: "test"},
	}
	tmap, err := Marshal(original, sh)
	require.NoError(t, err)

	dst := make([]FixedLenStruct, 0)
	err = Unmarshal(tmap, 0, 1, &dst, sh, "")
	require.NoError(t, err)
	require.Equal(t, "test", dst[0].Data)
}

func TestSetByteSliceValue_Error(t *testing.T) {
	var b []byte
	po := reflect.ValueOf(&b).Elem()
	done, err := setByteSliceValue(po, 123) // int is not assignable to []byte
	require.False(t, done)
	require.Error(t, err)
}

func TestHandleOldListLeaf_EdgeCases(t *testing.T) {
	// Not a struct
	handleOldListLeaf(reflect.ValueOf(123), 456)

	// Struct with 2 fields
	type TwoFields struct {
		Array []int32
		Other int
	}
	tf := TwoFields{}
	handleOldListLeaf(reflect.ValueOf(&tf).Elem(), int32(1))
	require.Empty(t, tf.Array)

	// Struct with wrong field name
	type WrongName struct {
		NotArray []int32
	}
	wn := WrongName{}
	handleOldListLeaf(reflect.ValueOf(&wn).Elem(), int32(1))
	require.Empty(t, wn.NotArray)

	// Struct with non-slice array field
	type NotSlice struct {
		Array int
	}
	ns := NotSlice{}
	handleOldListLeaf(reflect.ValueOf(&ns).Elem(), int32(1))
	require.Equal(t, 0, ns.Array)
}

func TestSetPrimitiveValue_Errors(t *testing.T) {
	// !po.IsValid()
	err := setPrimitiveValue(reflect.Value{}, 123)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid reflect value")

	// !po.CanSet()
	type S struct {
		Exported int32
	}
	s := S{}
	v := reflect.ValueOf(s).Field(0)
	err = setPrimitiveValue(v, int32(123))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot set value")

	// Incompatible types without conversion
	var b bool
	v2 := reflect.ValueOf(&b).Elem()
	err = setPrimitiveValue(v2, "not a bool")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot convert value of type")
}
