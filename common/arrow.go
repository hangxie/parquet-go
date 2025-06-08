package common

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TransposeTable transposes a table's rows and columns once per arrow record.
// We need to transpose the rows and columns because parquet-go library writes
// data row by row while the arrow library provides the data column by column.
func TransposeTable(table [][]any) [][]any {
	transposedTable := newTable(len(table[0]), len(table))
	for i := range transposedTable {
		row := transposedTable[i]
		for j := range row {
			row[j] = table[j][i]
		}
	}
	return transposedTable
}

// ArrowColToParquetCol creates column with native go values from column
// with arrow values according to the rules described in the Type section in
// the project's README.md file.
//
// If `col` contains Null value but `field` is not marked as Nullable this
// results in an error.
func ArrowColToParquetCol(field arrow.Field, col arrow.Array) ([]any, error) {
	recs := make([]any, col.Len())
	switch field.Type.(type) {
	case *arrow.Int8Type:
		arr := col.(*array.Int8)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Int16Type:
		arr := col.(*array.Int16)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Int32Type:
		arr := col.(*array.Int32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Int64Type:
		arr := col.(*array.Int64)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Uint8Type:
		arr := col.(*array.Uint8)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Uint16Type:
		arr := col.(*array.Uint16)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Uint32Type:
		arr := col.(*array.Uint32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Uint64Type:
		arr := col.(*array.Uint64)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int64(arr.Value(i))
			}
		}
	case *arrow.Float32Type:
		arr := col.(*array.Float32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Float64Type:
		arr := col.(*array.Float64)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Date32Type:
		arr := col.(*array.Date32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.Date64Type:
		arr := col.(*array.Date64)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.BinaryType:
		arr := col.(*array.Binary)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = string(arr.Value(i))
			}
		}
	case *arrow.StringType:
		arr := col.(*array.String)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.BooleanType:
		arr := col.(*array.Boolean)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = arr.Value(i)
			}
		}
	case *arrow.Time32Type:
		arr := col.(*array.Time32)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int32(arr.Value(i))
			}
		}
	case *arrow.TimestampType:
		arr := col.(*array.Timestamp)
		for i := range arr.Len() {
			if arr.IsNull(i) {
				if !field.Nullable {
					return nil, nonNullableFieldContainsNullError(field, i)
				}
				recs[i] = nil
			} else {
				recs[i] = int64(arr.Value(i))
			}
		}
	}
	return recs, nil
}

func nonNullableFieldContainsNullError(field arrow.Field, idx int) error {
	return fmt.Errorf("field with name '%s' is marked non-nullable but its "+
		"column array contains Null value at index %d", field.Name, idx)
}

// newTable creates empty table with transposed columns and records
func newTable(rowLen, colLen int) [][]any {
	tableLen := make([]any, rowLen*colLen)
	// Need to reconsinder to avoid allocation and memcopy.
	table := make([][]any, rowLen)
	lo, hi := 0, colLen
	for i := range table {
		table[i] = tableLen[lo:hi:hi]
		lo, hi = hi, hi+colLen
	}
	return table
}
