package writer

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

func TestWithSortingColumns(t *testing.T) {
	type row struct {
		ID   int64  `parquet:"name=id, type=INT64"`
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	columns := []*parquet.SortingColumn{
		{ColumnIdx: 0, Descending: false, NullsFirst: true},
		{ColumnIdx: 1, Descending: true, NullsFirst: false},
	}
	pw, buf, err := createTestParquetWriter(new(row), WithNP(1), WithSortingColumns(columns...))
	require.NoError(t, err)

	// The option owns its configuration after construction.
	columns[0].ColumnIdx = 1
	columns[1] = nil

	require.NoError(t, pw.Write(row{ID: 1, Name: "a"}))
	require.NoError(t, pw.Flush(true))
	require.NoError(t, pw.Write(row{ID: 2, Name: "b"}))
	require.NoError(t, pw.WriteStop())

	pr, pf, err := createTestParquetReader(buf.Bytes(), new(row), reader.WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pf.Close()) }()

	want := []*parquet.SortingColumn{
		{ColumnIdx: 0, Descending: false, NullsFirst: true},
		{ColumnIdx: 1, Descending: true, NullsFirst: false},
	}
	require.Len(t, pr.Footer.RowGroups, 2)
	for rowGroupIndex := range pr.Footer.RowGroups {
		got, sortingErr := pr.RowGroupSortingColumns(rowGroupIndex)
		require.NoError(t, sortingErr)
		require.Equal(t, want, got)
	}
}

func TestWithSortingColumnsValidation(t *testing.T) {
	type row struct {
		ID   int64  `parquet:"name=id, type=INT64"`
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	tests := []struct {
		name    string
		columns []*parquet.SortingColumn
		wantErr string
	}{
		{name: "nil column", columns: []*parquet.SortingColumn{nil}, wantErr: "sorting column 0 is nil"},
		{name: "negative ordinal", columns: []*parquet.SortingColumn{{ColumnIdx: -1}}, wantErr: "column index must be non-negative"},
		{name: "ordinal out of range", columns: []*parquet.SortingColumn{{ColumnIdx: 2}}, wantErr: "column index 2 out of range [0, 2)"},
		{name: "duplicate ordinal", columns: []*parquet.SortingColumn{{ColumnIdx: 1}, {ColumnIdx: 1}}, wantErr: "column index 1 is duplicated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, buf, err := createTestParquetWriter(new(row), WithSortingColumns(tt.columns...))
			require.ErrorContains(t, err, tt.wantErr)
			require.Empty(t, buf.Bytes())
		})
	}
}

func TestWithSortingColumnsRequiresSchema(t *testing.T) {
	_, buf, err := createTestParquetWriter(nil, WithSortingColumns(&parquet.SortingColumn{ColumnIdx: 0}))
	require.ErrorContains(t, err, "WithSortingColumns: schema handler is required")
	require.Empty(t, buf.Bytes())
}

func TestSortingColumnsValidatedByAllWriters(t *testing.T) {
	jsonSchema := `{"Tag":"name=root","Fields":[{"Tag":"name=id, type=INT64"}]}`
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	invalid := WithSortingColumns(&parquet.SortingColumn{ColumnIdx: 1})

	tests := []struct {
		name string
		new  func() (*bytes.Buffer, error)
	}{
		{
			name: "ParquetWriter JSON schema",
			new: func() (*bytes.Buffer, error) {
				buf := new(bytes.Buffer)
				_, err := NewParquetWriter(writerfile.NewWriterFile(buf), jsonSchema, invalid)
				return buf, err
			},
		},
		{
			name: "JSONWriter",
			new: func() (*bytes.Buffer, error) {
				buf := new(bytes.Buffer)
				_, err := NewJSONWriter(jsonSchema, writerfile.NewWriterFile(buf), invalid)
				return buf, err
			},
		},
		{
			name: "CSVWriter",
			new: func() (*bytes.Buffer, error) {
				buf := new(bytes.Buffer)
				_, err := NewCSVWriter([]string{"name=id, type=INT64"}, writerfile.NewWriterFile(buf), invalid)
				return buf, err
			},
		},
		{
			name: "ArrowWriter",
			new: func() (*bytes.Buffer, error) {
				buf := new(bytes.Buffer)
				_, err := NewArrowWriter(arrowSchema, writerfile.NewWriterFile(buf), invalid)
				return buf, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf, err := tt.new()
			require.ErrorContains(t, err, "column index 1 out of range [0, 1)")
			require.Empty(t, buf.Bytes())
		})
	}
}
