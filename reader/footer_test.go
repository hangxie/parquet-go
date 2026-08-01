package reader

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
)

type footerRenamedColumnRecord struct {
	ColL1 string `parquet:"name=col_l1, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func TestParquetReader_RenameSchema_NilChecks(t *testing.T) {
	tests := []struct {
		name  string
		setup func() *ParquetReader
	}{
		{
			name: "nil_schema_handler",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: nil,
				}
			},
		},
		{
			name: "nil_schema_handler_infos",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos: nil,
					},
				}
			},
		},
		{
			name: "nil_footer",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos: []*common.Tag{{}},
					},
					Footer: nil,
				}
			},
		},
		{
			name: "nil_footer_schema",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos: []*common.Tag{{}},
					},
					Footer: &parquet.FileMetaData{
						Schema: nil,
					},
				}
			},
		},
		{
			name: "nil_elements_in_arrays",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos: []*common.Tag{nil, {InName: "test"}},
					},
					Footer: &parquet.FileMetaData{
						Schema: []*parquet.SchemaElement{nil, {Name: "old_name"}},
					},
				}
			},
		},
		{
			name: "nil_row_groups",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos:          []*common.Tag{{InName: "test"}},
						ExPathToInPath: map[string]string{"test": "test"},
						SchemaElements: []*parquet.SchemaElement{{Name: "test"}},
					},
					Footer: &parquet.FileMetaData{
						Schema:    []*parquet.SchemaElement{{Name: "old_name"}},
						RowGroups: nil,
					},
				}
			},
		},
		{
			name: "nil_row_group_columns",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos:          []*common.Tag{{InName: "test"}},
						ExPathToInPath: map[string]string{"test": "test"},
					},
					Footer: &parquet.FileMetaData{
						Schema: []*parquet.SchemaElement{{Name: "old_name"}},
						RowGroups: []*parquet.RowGroup{
							{Columns: nil},
						},
					},
				}
			},
		},
		{
			name: "nil_chunk_metadata",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos:          []*common.Tag{{InName: "test"}},
						ExPathToInPath: map[string]string{"test": "test"},
					},
					Footer: &parquet.FileMetaData{
						Schema: []*parquet.SchemaElement{{Name: "old_name"}},
						RowGroups: []*parquet.RowGroup{
							{
								Columns: []*parquet.ColumnChunk{
									{MetaData: nil},
								},
							},
						},
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setup()

			require.NotPanics(t, pr.RenameSchema)
		})
	}
}

func TestNewParquetReaderFooterPathInSchemaPreservesExternalNames(t *testing.T) {
	data := writeFooterRenamedColumnParquet(t)

	pf := buffer.NewBufferReaderFromBytesNoAlloc(data)
	pr, err := NewParquetReader(pf, new(footerRenamedColumnRecord), WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	require.NotEmpty(t, pr.Footer.GetRowGroups())
	require.NotEmpty(t, pr.Footer.RowGroups[0].GetColumns())
	require.Equal(t, "col_l1", pr.Footer.Schema[1].GetName())
	require.Equal(t, []string{"col_l1"}, pr.Footer.RowGroups[0].Columns[0].MetaData.GetPathInSchema())

	rows, err := pr.ReadByNumber(1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "value", rows[0].(footerRenamedColumnRecord).ColL1)
}

func TestParquetReaderInternalFooter(t *testing.T) {
	data := writeFooterRenamedColumnParquet(t)

	pf := buffer.NewBufferReaderFromBytesNoAlloc(data)
	pr, err := NewParquetReader(pf, new(footerRenamedColumnRecord), WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	internalFooter, err := pr.InternalFooter()
	require.NoError(t, err)
	require.NotSame(t, pr.Footer, internalFooter)

	require.Equal(t, "col_l1", pr.Footer.Schema[1].GetName())
	require.Equal(t, []string{"col_l1"}, pr.Footer.RowGroups[0].Columns[0].MetaData.GetPathInSchema())
	require.Equal(t, "ColL1", internalFooter.Schema[1].GetName())
	require.Equal(t, []string{"ColL1"}, internalFooter.RowGroups[0].Columns[0].MetaData.GetPathInSchema())

	internalFooter.Schema[1].Name = "Changed"
	internalFooter.RowGroups[0].Columns[0].MetaData.PathInSchema[0] = "Changed"
	require.Equal(t, "col_l1", pr.Footer.Schema[1].GetName())
	require.Equal(t, []string{"col_l1"}, pr.Footer.RowGroups[0].Columns[0].MetaData.GetPathInSchema())

	rows, err := pr.ReadByNumber(1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "value", rows[0].(footerRenamedColumnRecord).ColL1)
}

func TestParquetColumnReaderInternalFooter(t *testing.T) {
	data := writeFooterRenamedColumnParquet(t)

	pf := buffer.NewBufferReaderFromBytesNoAlloc(data)
	pr, err := NewParquetColumnReader(pf, WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	internalFooter, err := pr.InternalFooter()
	require.NoError(t, err)
	require.NotSame(t, pr.Footer, internalFooter)

	require.Equal(t, "col_l1", pr.Footer.Schema[1].GetName())
	require.Equal(t, []string{"col_l1"}, pr.Footer.RowGroups[0].Columns[0].MetaData.GetPathInSchema())
	require.Equal(t, "Col_l1", internalFooter.Schema[1].GetName())
	require.Equal(t, []string{"Col_l1"}, internalFooter.RowGroups[0].Columns[0].MetaData.GetPathInSchema())

	internalFooter.Schema[1].Name = "Changed"
	internalFooter.RowGroups[0].Columns[0].MetaData.PathInSchema[0] = "Changed"
	require.Equal(t, "col_l1", pr.Footer.Schema[1].GetName())
	require.Equal(t, []string{"col_l1"}, pr.Footer.RowGroups[0].Columns[0].MetaData.GetPathInSchema())
}

func TestParquetReaderInternalFooterNil(t *testing.T) {
	footer, err := (*ParquetReader)(nil).InternalFooter()
	require.NoError(t, err)
	require.Nil(t, footer)

	footer, err = (&ParquetReader{}).InternalFooter()
	require.NoError(t, err)
	require.Nil(t, footer)
}

func TestParquetReaderRowGroupSortingColumns(t *testing.T) {
	first := &parquet.SortingColumn{ColumnIdx: 1, Descending: true, NullsFirst: true}
	footer := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{SortingColumns: []*parquet.SortingColumn{first}},
			{},
		},
	}
	pr := &ParquetReader{Footer: footer}

	columns, err := pr.RowGroupSortingColumns(0)
	require.NoError(t, err)
	require.Equal(t, []*parquet.SortingColumn{first}, columns)
	require.NotSame(t, first, columns[0])

	columns[0].ColumnIdx = 0
	columns[0] = nil
	require.Equal(t, int32(1), pr.Footer.RowGroups[0].SortingColumns[0].ColumnIdx)

	columns, err = pr.RowGroupSortingColumns(1)
	require.NoError(t, err)
	require.Nil(t, columns)

	nilRowGroupFooter := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{nil}}
	nilRowGroupReader := &ParquetReader{Footer: nilRowGroupFooter}

	tests := []struct {
		name          string
		reader        *ParquetReader
		rowGroupIndex int
		wantErr       string
	}{
		{name: "nil reader", reader: nil, wantErr: "reader footer is unavailable"},
		{name: "nil footer", reader: &ParquetReader{}, wantErr: "reader footer is unavailable"},
		{name: "negative index", reader: pr, rowGroupIndex: -1, wantErr: "row group index -1 out of range [0, 2)"},
		{name: "large index", reader: pr, rowGroupIndex: 2, wantErr: "row group index 2 out of range [0, 2)"},
		{name: "nil row group", reader: nilRowGroupReader, wantErr: "row group 0 is nil"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.reader.RowGroupSortingColumns(tt.rowGroupIndex)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func writeFooterRenamedColumnParquet(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	//nolint:staticcheck
	pw, err := writer.NewParquetWriter(fw, new(footerRenamedColumnRecord), writer.WithNP(1))
	require.NoError(t, err)
	//nolint:staticcheck
	require.NoError(t, pw.Write(footerRenamedColumnRecord{ColL1: "value"}))
	//nolint:staticcheck
	require.NoError(t, pw.WriteStop())

	return buf.Bytes()
}
