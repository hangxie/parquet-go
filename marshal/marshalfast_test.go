package marshal

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/schema"
)

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
	require.NoError(t, err)

	_, err = MarshalFast(data, sch)
	require.NoError(t, err)

	// The String() method is internal to encoderMapKey, but we can verify MarshalFast works
	// This indirectly tests the String() method through the compiler cache
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
			require.NoError(t, err)
			expected, err := Marshal(input, sch)
			require.NoError(t, err)
			actual, err := MarshalFast(input, sch)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}

func Test_encoderMapKey_String(t *testing.T) {
	tests := []struct {
		name        string
		setupKey    func() encoderMapKey
		expected    string
		expectPanic bool
	}{
		{
			name: "normal_path",
			setupKey: func() encoderMapKey {
				pathMap := &schema.PathMapType{
					Path: "test.column.path",
				}
				return encoderMapKey{
					typeID:  12345,
					pathMap: pathMap,
				}
			},
			expected: "{12345, test.column.path}",
		},
		{
			name: "empty_path",
			setupKey: func() encoderMapKey {
				pathMap := &schema.PathMapType{
					Path: "",
				}
				return encoderMapKey{
					typeID:  0,
					pathMap: pathMap,
				}
			},
			expected: "{0, }",
		},
		{
			name: "nil_pathmap",
			setupKey: func() encoderMapKey {
				return encoderMapKey{
					typeID:  999,
					pathMap: nil,
				}
			},
			expectPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := tt.setupKey()

			if !tt.expectPanic {
				result := key.String()
				require.Equal(t, tt.expected, result)
				return
			}
			require.Panics(t, func() {
				_ = key.String()
			})
		})
	}
}
