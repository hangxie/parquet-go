package common

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// Builder interface for Arrow array builders that support generic operations
type appendableBuilder[T any] interface {
	Append(T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

// Generic function to build Arrow arrays with null value support
func buildArray[T any, B appendableBuilder[T]](mem memory.Allocator, values []T, valid []bool, builderFunc func(memory.Allocator) B) arrow.Array {
	builder := builderFunc(mem)
	defer builder.Release()
	for i, v := range values {
		if valid[i] {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewArray()
}

func buildBinaryArray(mem memory.Allocator, values [][]byte, valid []bool) arrow.Array {
	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer builder.Release()
	for i, v := range values {
		if valid[i] {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewArray()
}

func buildTime32Array(mem memory.Allocator, values []arrow.Time32, valid []bool) arrow.Array {
	builder := array.NewTime32Builder(mem, &arrow.Time32Type{Unit: arrow.Second})
	defer builder.Release()
	for i, v := range values {
		if valid[i] {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewArray()
}

func buildTimestampArray(mem memory.Allocator, values []arrow.Timestamp, valid []bool) arrow.Array {
	builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()
	for i, v := range values {
		if valid[i] {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewArray()
}

func Test_ArrowColToParquetCol(t *testing.T) {
	mem := memory.NewGoAllocator()

	testCases := map[string]struct {
		field    arrow.Field
		col      arrow.Array
		expected []any
		errMsg   string
	}{
		"int8_to_int32": {
			field:    arrow.Field{Name: "test", Type: &arrow.Int8Type{}, Nullable: true},
			col:      buildArray(mem, []int8{1, 2, 3}, []bool{true, true, true}, array.NewInt8Builder),
			expected: []any{int32(1), int32(2), int32(3)},
		},
		"int16_to_int32": {
			field:    arrow.Field{Name: "test", Type: &arrow.Int16Type{}, Nullable: true},
			col:      buildArray(mem, []int16{100, 200, 300}, []bool{true, true, true}, array.NewInt16Builder),
			expected: []any{int32(100), int32(200), int32(300)},
		},
		"int32_direct": {
			field:    arrow.Field{Name: "test", Type: &arrow.Int32Type{}, Nullable: true},
			col:      buildArray(mem, []int32{1000, 2000, 3000}, []bool{true, true, true}, array.NewInt32Builder),
			expected: []any{int32(1000), int32(2000), int32(3000)},
		},
		"int64_direct": {
			field:    arrow.Field{Name: "test", Type: &arrow.Int64Type{}, Nullable: true},
			col:      buildArray(mem, []int64{10000, 20000, 30000}, []bool{true, true, true}, array.NewInt64Builder),
			expected: []any{int64(10000), int64(20000), int64(30000)},
		},
		"uint8_to_int32": {
			field:    arrow.Field{Name: "test", Type: &arrow.Uint8Type{}, Nullable: true},
			col:      buildArray(mem, []uint8{1, 2, 3}, []bool{true, true, true}, array.NewUint8Builder),
			expected: []any{int32(1), int32(2), int32(3)},
		},
		"uint16_to_int32": {
			field:    arrow.Field{Name: "test", Type: &arrow.Uint16Type{}, Nullable: true},
			col:      buildArray(mem, []uint16{100, 200, 300}, []bool{true, true, true}, array.NewUint16Builder),
			expected: []any{int32(100), int32(200), int32(300)},
		},
		"uint32_to_int32": {
			field:    arrow.Field{Name: "test", Type: &arrow.Uint32Type{}, Nullable: true},
			col:      buildArray(mem, []uint32{1000, 2000, 3000}, []bool{true, true, true}, array.NewUint32Builder),
			expected: []any{int32(1000), int32(2000), int32(3000)},
		},
		"uint64_to_int64": {
			field:    arrow.Field{Name: "test", Type: &arrow.Uint64Type{}, Nullable: true},
			col:      buildArray(mem, []uint64{10000, 20000, 30000}, []bool{true, true, true}, array.NewUint64Builder),
			expected: []any{int64(10000), int64(20000), int64(30000)},
		},
		"float32_direct": {
			field:    arrow.Field{Name: "test", Type: &arrow.Float32Type{}, Nullable: true},
			col:      buildArray(mem, []float32{1.1, 2.2, 3.3}, []bool{true, true, true}, array.NewFloat32Builder),
			expected: []any{float32(1.1), float32(2.2), float32(3.3)},
		},
		"float64_direct": {
			field:    arrow.Field{Name: "test", Type: &arrow.Float64Type{}, Nullable: true},
			col:      buildArray(mem, []float64{1.1, 2.2, 3.3}, []bool{true, true, true}, array.NewFloat64Builder),
			expected: []any{float64(1.1), float64(2.2), float64(3.3)},
		},
		"date32_to_int32": {
			field:    arrow.Field{Name: "test", Type: &arrow.Date32Type{}, Nullable: true},
			col:      buildArray(mem, []arrow.Date32{1, 2, 3}, []bool{true, true, true}, array.NewDate32Builder),
			expected: []any{int32(1), int32(2), int32(3)},
		},
		"date64_to_int32": {
			field:    arrow.Field{Name: "test", Type: &arrow.Date64Type{}, Nullable: true},
			col:      buildArray(mem, []arrow.Date64{1000, 2000, 3000}, []bool{true, true, true}, array.NewDate64Builder),
			expected: []any{int32(1000), int32(2000), int32(3000)},
		},
		"binary_to_string": {
			field:    arrow.Field{Name: "test", Type: &arrow.BinaryType{}, Nullable: true},
			col:      buildBinaryArray(mem, [][]byte{[]byte("hello"), []byte("world")}, []bool{true, true}),
			expected: []any{"hello", "world"},
		},
		"string_direct": {
			field:    arrow.Field{Name: "test", Type: &arrow.StringType{}, Nullable: true},
			col:      buildArray(mem, []string{"hello", "world"}, []bool{true, true}, array.NewStringBuilder),
			expected: []any{"hello", "world"},
		},
		"boolean_direct": {
			field:    arrow.Field{Name: "test", Type: &arrow.BooleanType{}, Nullable: true},
			col:      buildArray(mem, []bool{true, false, true}, []bool{true, true, true}, array.NewBooleanBuilder),
			expected: []any{true, false, true},
		},
		"time32_to_int32": {
			field:    arrow.Field{Name: "test", Type: &arrow.Time32Type{}, Nullable: true},
			col:      buildTime32Array(mem, []arrow.Time32{1000, 2000}, []bool{true, true}),
			expected: []any{int32(1000), int32(2000)},
		},
		"timestamp_to_int64": {
			field:    arrow.Field{Name: "test", Type: &arrow.TimestampType{}, Nullable: true},
			col:      buildTimestampArray(mem, []arrow.Timestamp{1000000, 2000000}, []bool{true, true}),
			expected: []any{int64(1000000), int64(2000000)},
		},
		"nullable_with_nulls": {
			field:    arrow.Field{Name: "test", Type: &arrow.Int32Type{}, Nullable: true},
			col:      buildArray(mem, []int32{1, 2, 3}, []bool{true, false, true}, array.NewInt32Builder),
			expected: []any{int32(1), nil, int32(3)},
		},
		"non_nullable_with_null_error": {
			field:  arrow.Field{Name: "test", Type: &arrow.Int32Type{}, Nullable: false},
			col:    buildArray(mem, []int32{1, 2, 3}, []bool{true, false, true}, array.NewInt32Builder),
			errMsg: "field with name 'test' is marked non-nullable but its column array contains Null value at index 1",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := ArrowColToParquetCol(tc.field, tc.col)
			if tc.errMsg == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, actual)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
			tc.col.Release()
		})
	}
}

func Test_TransposeTable(t *testing.T) {
	testCases := map[string]struct {
		table    [][]any
		expected [][]any
	}{
		"test-case-1": {[][]any{{1, 2, 3}}, [][]any{{1}, {2}, {3}}},
		"test-case-2": {[][]any{{1, 2, 3}, {4, 5, 6}}, [][]any{{1, 4}, {2, 5}, {3, 6}}},
		"test-case-3": {[][]any{{1}, {2}, {3}}, [][]any{{1, 2, 3}}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, TransposeTable(tc.table))
		})
	}
}

func Test_newTable(t *testing.T) {
	actual := newTable(5, 6)
	require.Equal(t, 5, len(actual))
	require.Equal(t, 6, len(actual[0]))
}

func Test_nonNullableFieldContainsNullError(t *testing.T) {
	err := nonNullableFieldContainsNullError(arrow.Field{Name: "unit-test"}, 3)
	require.Equal(t, "field with name 'unit-test' is marked non-nullable but its column array contains Null value at index 3", err.Error())
}
