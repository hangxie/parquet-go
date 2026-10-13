package marshal

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

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

func TestIsOldListFormat(t *testing.T) {
	listGroup := &parquet.SchemaElement{Name: "array", ConvertedType: common.ToPtr(parquet.ConvertedType_LIST)}
	logicalGroup := &parquet.SchemaElement{Name: "array", LogicalType: &parquet.LogicalType{LIST: parquet.NewListType()}}
	plainGroup := &parquet.SchemaElement{Name: "array"}
	leaf := &parquet.SchemaElement{Name: "array", Type: common.ToPtr(parquet.Type_INT32)}
	path := []string{common.ParGoRootInName, "array", "array"}

	testCases := map[string]struct {
		path     []string
		index    int
		elements []*parquet.SchemaElement
		expected bool
	}{
		"list-annotated":   {path, 2, []*parquet.SchemaElement{{}, listGroup, leaf}, true},
		"logical-type":     {path, 2, []*parquet.SchemaElement{{}, logicalGroup, leaf}, true},
		"no-annotation":    {path, 2, []*parquet.SchemaElement{{}, plainGroup, leaf}, false},
		"inner-not-leaf":   {path, 2, []*parquet.SchemaElement{{}, listGroup, plainGroup}, false},
		"other-names":      {[]string{common.ParGoRootInName, "array", "item"}, 2, []*parquet.SchemaElement{{}, listGroup, leaf}, false},
		"case-insensitive": {[]string{common.ParGoRootInName, "Array", "ARRAY"}, 2, []*parquet.SchemaElement{{}, listGroup, leaf}, true},
		"index-zero":       {path, 0, []*parquet.SchemaElement{{}, listGroup, leaf}, false},
		"index-past-path":  {path, 3, []*parquet.SchemaElement{{}, listGroup, leaf}, false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			sh := &schema.SchemaHandler{SchemaElements: tc.elements}
			require.Equal(t, tc.expected, isOldListFormat(tc.path, tc.index, []int{0, 1, 2}, sh))
		})
	}
}
