package reader

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
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
			name: "nil_row_group",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos:          []*common.Tag{{InName: "test"}},
						ExPathToInPath: map[string]string{"test": "test"},
					},
					Footer: &parquet.FileMetaData{
						Schema:    []*parquet.SchemaElement{{Name: "old_name"}},
						RowGroups: []*parquet.RowGroup{nil},
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

func TestFooterConversionListWrapperAliases(t *testing.T) {
	rows := []listWrapperRecord{
		{V: []int32{4, 5}, N: []listWrapperItem{{X: []int32{6}}}},
		{V: []int32{7}, N: []listWrapperItem{{X: []int32{8, 9}}, {X: []int32{}}}},
	}
	data := writeListWrapperParquet(t, rows, renameListWrappers)
	wantPaths := [][]string{{"V", "List", "Element"}, {"N", "List", "Element", "X", "List", "Element"}}

	requireInternalPaths := func(t *testing.T, sh *schema.SchemaHandler, footer *parquet.FileMetaData) {
		t.Helper()
		require.Len(t, footer.RowGroups, len(rows))
		for _, rowGroup := range footer.RowGroups {
			for i, column := range rowGroup.Columns {
				path := column.MetaData.GetPathInSchema()
				require.Equal(t, wantPaths[i], path)
				require.Contains(t, sh.MapIndex, common.PathToStr(append([]string{sh.GetRootInName()}, path...)))
			}
		}
	}

	t.Run("internal_footer", func(t *testing.T) {
		pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), new(listWrapperRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { require.NoError(t, pr.ReadStop()) }()

		internalFooter, err := pr.InternalFooter()
		require.NoError(t, err)
		requireInternalPaths(t, pr.SchemaHandler, internalFooter)
		require.Equal(t, []string{"v", "bag", "array_element"}, pr.Footer.RowGroups[0].Columns[0].MetaData.GetPathInSchema())
	})

	t.Run("internal_footer_case_insensitive", func(t *testing.T) {
		pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), new(listWrapperRecord), WithNP(1), WithCaseInsensitive(true))
		require.NoError(t, err)
		defer func() { require.NoError(t, pr.ReadStop()) }()

		internalFooter, err := pr.InternalFooter()
		require.NoError(t, err)
		requireInternalPaths(t, pr.SchemaHandler, internalFooter)
	})

	t.Run("rename_schema", func(t *testing.T) {
		pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), new(listWrapperRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { require.NoError(t, pr.ReadStop()) }()

		// A second call must leave the already internal footer as it is.
		for range 2 {
			//nolint:staticcheck
			pr.RenameSchema()
			requireInternalPaths(t, pr.SchemaHandler, pr.Footer)
		}

		got := make([]listWrapperRecord, len(rows))
		require.NoError(t, pr.ReadWithContext(context.Background(), &got))
		require.Equal(t, rows, got)
	})
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

func TestParquetReader_GetFooterSize(t *testing.T) {
	tests := []struct {
		name     string
		reader   *ParquetReader
		expected uint32
		errMsg   string
	}{
		{
			name:   "nil_file",
			reader: &ParquetReader{},
			errMsg: "PFile is nil",
		},
		{
			name: "seek_error",
			reader: &ParquetReader{
				PFile: buffer.NewBufferReaderFromBytesNoAlloc([]byte("short")),
			},
			errMsg: "seek to negative location",
		},
		{
			name: "read_error",
			reader: &ParquetReader{
				PFile: &shortReadParquetFileReader{
					data: []byte{0, 0, 0, 0, 'P', 'A', 'R', '1'},
					err:  io.ErrUnexpectedEOF,
				},
			},
			errMsg: "unexpected EOF",
		},
		{
			name: "success",
			reader: &ParquetReader{
				PFile: buffer.NewBufferReaderFromBytesNoAlloc([]byte{
					'd', 'a', 't', 'a',
					0x78, 0x56, 0x34, 0x12,
					'P', 'A', 'R', '1',
				}),
			},
			expected: 0x12345678,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := tc.reader.GetFooterSize()

			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func rewriteFooter(t *testing.T, data []byte, mutate func(*parquet.FileMetaData)) []byte {
	t.Helper()

	size := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	start := len(data) - 8 - size
	deserializer := thrift.NewTDeserializer()
	deserializer.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(deserializer.Transport)
	footer := parquet.NewFileMetaData()
	require.NoError(t, deserializer.Read(context.Background(), footer, data[start:len(data)-8]))
	mutate(footer)

	footerBytes := serializeThrift(t, footer)
	out := append([]byte(nil), data[:start]...)
	out = append(out, footerBytes...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footerBytes)))
	return append(out, common.MagicBytes...)
}

func TestReadFooterRejectsMalformedTail(t *testing.T) {
	valid := writeFooterRenamedColumnParquet(t)

	tests := map[string]struct {
		data   []byte
		errMsg string
	}{
		"bad-magic": {
			data:   append(append([]byte(nil), valid[:len(valid)-4]...), "BAD!"...),
			errMsg: `invalid footer magic "BAD!"`,
		},
		"negative-file-rows": {
			data:   rewriteFooter(t, valid, func(f *parquet.FileMetaData) { f.NumRows = -1 }),
			errMsg: "file num_rows -1 is negative",
		},
		"negative-row-group-rows": {
			data:   rewriteFooter(t, valid, func(f *parquet.FileMetaData) { f.RowGroups[0].NumRows = -1 }),
			errMsg: "row group 0 num_rows -1 is negative",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pf := buffer.NewBufferReaderFromBytesNoAlloc(tc.data)
			_, err := NewParquetReader(pf, new(footerRenamedColumnRecord), WithNP(1))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)

			_, err = NewParquetColumnReader(buffer.NewBufferReaderFromBytesNoAlloc(tc.data), WithNP(1))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestReadFooterMalformedSchema(t *testing.T) {
	valid := writeFooterRenamedColumnParquet(t)
	tests := []struct {
		name   string
		mutate func(*parquet.FileMetaData)
		errMsg string
	}{
		{"trailing-leaf", func(f *parquet.FileMetaData) {
			f.Schema = append(f.Schema, &parquet.SchemaElement{Name: "extra", Type: common.ToPtr(parquet.Type_INT32)})
		}, "outside the root subtree"},
		{"trailing-group", func(f *parquet.FileMetaData) {
			f.Schema = append(f.Schema, &parquet.SchemaElement{Name: "extra", NumChildren: common.ToPtr(int32(0))})
		}, "outside the root subtree"},
		{"root-too-few", func(f *parquet.FileMetaData) {
			f.Schema[0].NumChildren = common.ToPtr(int32(0))
		}, "outside the root subtree"},
		{"root-too-many", func(f *parquet.FileMetaData) {
			f.Schema[0].NumChildren = common.ToPtr(int32(2))
		}, "unfulfilled child"},
		{"nested-too-many", func(f *parquet.FileMetaData) {
			f.Schema[1].NumChildren = common.ToPtr(int32(1))
		}, "unfulfilled child"},
		{"negative-count", func(f *parquet.FileMetaData) {
			f.Schema[0].NumChildren = common.ToPtr(int32(-1))
		}, "negative num_children"},
		{"empty-schema", func(f *parquet.FileMetaData) { f.Schema = []*parquet.SchemaElement{} }, "empty schema"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := rewriteFooter(t, valid, tt.mutate)
			for _, mode := range []string{"inferred", "struct", "json", "list", "column", "footer"} {
				t.Run(mode, func(t *testing.T) {
					file := buffer.NewBufferReaderFromBytesNoAlloc(data)
					var err error
					switch mode {
					case "column":
						_, err = NewParquetColumnReaderWithContext(context.Background(), file)
					case "footer":
						pr := &ParquetReader{PFile: file}
						err = pr.ReadFooterWithContext(context.Background())
					default:
						var obj any
						switch mode {
						case "struct":
							obj = new(footerRenamedColumnRecord)
						case "json":
							obj = `{"Tag":"name=root", "Fields":[{"Tag":"name=col_l1, type=BYTE_ARRAY"}]}`
						case "list":
							obj = []*parquet.SchemaElement{{Name: "root"}}
						}
						_, err = NewParquetReaderWithContext(context.Background(), file, obj)
					}
					require.ErrorContains(t, err, "malformed schema")
					require.ErrorContains(t, err, tt.errMsg)
				})
			}
		})
	}
}

func TestReadFooterRequireEncryption(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	write := func(opts ...writer.WriterOption) []byte {
		ctx := context.Background()
		var out bytes.Buffer
		opts = append([]writer.WriterOption{writer.WithNP(1)}, opts...)
		pw, err := writer.NewParquetWriterWithContext(ctx, writerfile.NewWriterFile(&out), new(footerRenamedColumnRecord), opts...)
		require.NoError(t, err)
		require.NoError(t, pw.WriteWithContext(ctx, footerRenamedColumnRecord{ColL1: "value"}))
		require.NoError(t, pw.WriteStopWithContext(ctx))
		return out.Bytes()
	}
	columnKey := []byte("abcdef0123456789")
	plain := write()
	encryptedFooter := write(writer.WithFooterKey(footerKey))
	plaintextFooter := write(writer.WithFooterKey(footerKey), writer.WithPlaintextFooter(true))
	plaintextFooterColumnKey := write(writer.WithFooterKey(footerKey), writer.WithPlaintextFooter(true),
		writer.WithColumnEncrypted("col_l1", writer.ColumnKey(columnKey)))

	// A plaintext file whose footer claims encryption and carries a junk
	// signature; only the footer signature can tell it apart from a real one.
	forged := rewriteFooter(t, plain, func(f *parquet.FileMetaData) {
		f.EncryptionAlgorithm = &parquet.EncryptionAlgorithm{AES_GCM_V1: &parquet.AesGcmV1{}}
	})
	forgedSize := binary.LittleEndian.Uint32(forged[len(forged)-8:])
	forged = append(forged[:len(forged)-8], make([]byte, 28)...)
	forged = binary.LittleEndian.AppendUint32(forged, forgedSize+28)
	forged = append(forged, common.MagicBytes...)

	required := []ReaderOption{WithRequireEncryption(true)}
	withFooterKey := []ReaderOption{WithRequireEncryption(true), WithFooterKey(footerKey)}

	tests := []struct {
		name    string
		data    []byte
		opts    []ReaderOption
		wantErr string
	}{
		{name: "unencrypted allowed by default", data: plain},
		{name: "unencrypted allowed when disabled", data: plain, opts: []ReaderOption{WithRequireEncryption(false)}},
		{name: "unencrypted rejected", data: plain, opts: required, wantErr: "file is not encrypted"},
		{name: "encrypted footer accepted", data: encryptedFooter, opts: withFooterKey},
		{name: "encrypted footer without key", data: encryptedFooter, opts: required, wantErr: "decryption key required"},
		{name: "plaintext footer accepted", data: plaintextFooter, opts: withFooterKey},
		{name: "plaintext footer without key", data: plaintextFooter, opts: required, wantErr: "cannot be authenticated without the footer key"},
		{name: "plaintext footer with only column key", data: plaintextFooterColumnKey, opts: append([]ReaderOption{WithColumnKey("col_l1", columnKey)}, required...), wantErr: "cannot be authenticated without the footer key"},
		{name: "plaintext footer with only column key allowed when not required", data: plaintextFooterColumnKey, opts: []ReaderOption{WithColumnKey("col_l1", columnKey)}},
		{name: "forged footer without key", data: forged, opts: required, wantErr: "cannot be authenticated without the footer key"},
		{name: "forged footer with key", data: forged, opts: withFooterKey, wantErr: "verify plaintext footer signature"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := append([]ReaderOption{WithNP(1)}, tt.opts...)
			pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(tt.data), new(footerRenamedColumnRecord), opts...)
			cr, crErr := NewParquetColumnReader(buffer.NewBufferReaderFromBytesNoAlloc(tt.data), opts...)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.ErrorContains(t, crErr, tt.wantErr)
				return
			}
			require.NoError(t, err)
			got := make([]footerRenamedColumnRecord, 1)
			require.NoError(t, pr.Read(&got))
			require.Equal(t, "value", got[0].ColL1)
			require.NoError(t, pr.ReadStop())

			require.NoError(t, crErr)
			values, _, _, err := cr.ReadColumnByPath(valueColumnPathWithLeaf(t, cr, "col_l1"), 1)
			require.NoError(t, err)
			require.Equal(t, []any{"value"}, values)
			require.NoError(t, cr.ReadStop())
		})
	}
}
