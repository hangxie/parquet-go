package common

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
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

type arrowArrayWithValues[arrowValueT any] interface {
	arrow.Array
	Value(i int) arrowValueT
}

func arrowArrayToParquetList[arrowValueT any](field arrow.Field, col arrow.Array, toParquetType func(arrowValueT) any) ([]any, error) {
	recs := make([]any, col.Len())
	arr := col.(arrowArrayWithValues[arrowValueT])
	for i := range arr.Len() {
		if arr.IsNull(i) && !field.Nullable {
			return nil, nonNullableFieldContainsNullError(field, i)
		}
		if arr.IsNull(i) {
			recs[i] = nil
		} else {
			recs[i] = toParquetType(arr.Value(i))
		}
	}
	return recs, nil
}

// ArrowColToParquetCol creates column with native go values from column
// with arrow values according to the rules described in the Type section in
// the project's README.md file.
//
// If `col` contains Null value but `field` is not marked as Nullable this
// results in an error.
func ArrowColToParquetCol(field arrow.Field, col arrow.Array) ([]any, error) {
	recs := make([]any, col.Len())
	var err error
	switch field.Type.(type) {
	case *arrow.Int8Type:
		recs, err = arrowArrayToParquetList(field, col, func(v int8) any { return int32(v) })
	case *arrow.Int16Type:
		recs, err = arrowArrayToParquetList(field, col, func(v int16) any { return int32(v) })
	case *arrow.Int32Type:
		recs, err = arrowArrayToParquetList(field, col, func(v int32) any { return v })
	case *arrow.Int64Type:
		recs, err = arrowArrayToParquetList(field, col, func(v int64) any { return v })
	case *arrow.Uint8Type:
		recs, err = arrowArrayToParquetList(field, col, func(v uint8) any { return int32(v) })
	case *arrow.Uint16Type:
		recs, err = arrowArrayToParquetList(field, col, func(v uint16) any { return int32(v) })
	case *arrow.Uint32Type:
		recs, err = arrowArrayToParquetList(field, col, func(v uint32) any { return int32(v) })
	case *arrow.Uint64Type:
		recs, err = arrowArrayToParquetList(field, col, func(v uint64) any { return int64(v) })
	case *arrow.Float32Type:
		recs, err = arrowArrayToParquetList(field, col, func(v float32) any { return v })
	case *arrow.Float64Type:
		recs, err = arrowArrayToParquetList(field, col, func(v float64) any { return v })
	case *arrow.Date32Type:
		recs, err = arrowArrayToParquetList(field, col, func(v arrow.Date32) any { return int32(v) })
	case *arrow.Date64Type:
		recs, err = arrowArrayToParquetList(field, col, func(v arrow.Date64) any { return int32(v) })
	case *arrow.BinaryType:
		recs, err = arrowArrayToParquetList(field, col, func(v []byte) any { return string(v) })
	case *arrow.StringType:
		recs, err = arrowArrayToParquetList(field, col, func(v string) any { return v })
	case *arrow.BooleanType:
		recs, err = arrowArrayToParquetList(field, col, func(v bool) any { return v })
	case *arrow.Time32Type:
		recs, err = arrowArrayToParquetList(field, col, func(v arrow.Time32) any { return int32(v) })
	case *arrow.TimestampType:
		recs, err = arrowArrayToParquetList(field, col, func(v arrow.Timestamp) any { return int64(v) })
	}
	return recs, err
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
