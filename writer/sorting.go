package writer

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// WithSortingColumns declares the ordering of rows within every row group.
// ColumnIdx is the zero-based leaf-column ordinal. The writer records this
// metadata but does not sort or verify the rows supplied by the caller.
func WithSortingColumns(columns ...*parquet.SortingColumn) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		pw.sortingColumns = make([]*parquet.SortingColumn, len(columns))
		seen := make(map[int32]struct{}, len(columns))
		for i, column := range columns {
			if column == nil {
				pw.optionErrors = append(pw.optionErrors, fmt.Errorf("WithSortingColumns: sorting column %d is nil", i))
				continue
			}
			if column.ColumnIdx < 0 {
				pw.optionErrors = append(pw.optionErrors, fmt.Errorf("WithSortingColumns: sorting column %d column index must be non-negative, got %d", i, column.ColumnIdx))
			}
			if _, ok := seen[column.ColumnIdx]; ok {
				pw.optionErrors = append(pw.optionErrors, fmt.Errorf("WithSortingColumns: column index %d is duplicated", column.ColumnIdx))
			}
			seen[column.ColumnIdx] = struct{}{}
			clonedColumn := *column
			pw.sortingColumns[i] = &clonedColumn
		}
	})
}

func (pw *ParquetWriter) validateSortingColumns() error {
	if len(pw.sortingColumns) == 0 {
		return nil
	}
	if pw.SchemaHandler == nil {
		return fmt.Errorf("WithSortingColumns: schema handler is required")
	}
	columnCount := pw.SchemaHandler.GetColumnNum()
	for i, column := range pw.sortingColumns {
		if int64(column.ColumnIdx) >= columnCount {
			return fmt.Errorf("WithSortingColumns: sorting column %d column index %d out of range [0, %d)", i, column.ColumnIdx, columnCount)
		}
	}
	return nil
}

func cloneSortingColumns(columns []*parquet.SortingColumn) []*parquet.SortingColumn {
	if len(columns) == 0 {
		return nil
	}
	cloned := make([]*parquet.SortingColumn, len(columns))
	for i, column := range columns {
		clonedColumn := *column
		cloned[i] = &clonedColumn
	}
	return cloned
}
