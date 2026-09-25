package reader

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Mock ParquetFileReader for testing NewColumnBuffer
type mockColumnBufferFileReader struct {
	data       []byte
	offset     int64
	closed     bool
	shouldFail bool
	cloneFails bool
	openFails  bool
	// clones records readers handed out by Clone, letting tests assert the
	// cloned handle is closed on cleanup paths.
	clones []*mockColumnBufferFileReader
}

func newMockColumnBufferFileReader(data []byte) *mockColumnBufferFileReader {
	return &mockColumnBufferFileReader{
		data:   data,
		offset: 0,
		closed: false,
	}
}

func (m *mockColumnBufferFileReader) Read(p []byte) (n int, err error) {
	if m.shouldFail {
		return 0, fmt.Errorf("mock read error")
	}
	if m.closed {
		return 0, fmt.Errorf("reader is closed")
	}
	if m.offset >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.offset:])
	m.offset += int64(n)
	return n, nil
}

func TestReadRows(t *testing.T) {
	tests := []struct {
		name           string
		setup          func() *ColumnBufferType
		numRows        int64
		expectedRows   int64
		expectError    bool
		validateResult func(t *testing.T, tbl *layout.Table, n int64)
	}{
		{
			name: "empty_footer_fast_path",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 0}}
			},
			numRows:      10,
			expectedRows: 0,
			expectError:  false,
			validateResult: func(t *testing.T, tbl *layout.Table, n int64) {
				require.NotNil(t, tbl)
				require.Len(t, tbl.Values, 0)
			},
		},
		{
			name: "negative_datatable_numrows",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 10}, DataTableNumRows: -1}
			},
			numRows:      1,
			expectedRows: 0,
			expectError:  false,
		},
		{
			name: "request_more_than_available",
			setup: func() *ColumnBufferType {
				dt := &layout.Table{
					Values:           []any{int64(1), int64(2), int64(3)},
					DefinitionLevels: []int32{1, 1, 1},
					RepetitionLevels: []int32{0, 0, 0},
				}
				return &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 10}, DataTable: dt, DataTableNumRows: 2}
			},
			numRows:      10,
			expectedRows: 2,
			expectError:  false,
			validateResult: func(t *testing.T, tbl *layout.Table, n int64) {
				require.Len(t, tbl.Values, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := tt.setup()
			tbl, n, err := cb.ReadRows(tt.numRows)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedRows, n)
			if tt.validateResult != nil {
				tt.validateResult(t, tbl, n)
			}
		})
	}
}

// TestAppendNullChunk_FooterOnlyColumnErrors guards against a nil-pointer panic:
// newColumnBuffer tolerates a column whose PathStr is absent from the schema map
// (footer-only access). An empty chunk for such a column cannot synthesize typed
// nulls, so appendNullChunk must return an error instead of appending through a
// nil DataTable.
func TestAppendNullChunk_FooterOnlyColumnErrors(t *testing.T) {
	data := make([]byte, 64)
	footer := &parquet.FileMetaData{
		NumRows: 3,
		RowGroups: []*parquet.RowGroup{
			{NumRows: 3, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema:   []string{"absent"},
				DataPageOffset: int64(len(data)),
				NumValues:      3,
				Type:           parquet.Type_INT64,
				Codec:          parquet.CompressionCodec_UNCOMPRESSED,
			}}}},
		},
	}
	// The schema handler only knows "leaf"; the absent path is footer-only.
	cb, err := NewColumnBuffer(newMockColumnBufferFileReader(data), footer, newSchemaHandlerWithPath("leaf"), common.PathToStr([]string{"root", "absent"}), nil)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		n, serr := cb.SkipRows(1)
		require.Error(t, serr)
		require.Contains(t, serr.Error(), "no schema element")
		require.Equal(t, int64(0), n)
	})
}

func TestReadRows_DictionaryOnlyChunkIsTruncated(t *testing.T) {
	table, n, err := newDictionaryOnlyChunkBuffer(t).ReadRows(3)

	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(0), n)
	require.Empty(t, table.Values)
}

// TestReadRows_TruncatedChunkSurfacesErrorWithBufferedRows covers a chunk that declares
// more values than its pages hold: after the real page is read, the next read reaches a
// clean EOF before the declared count is met. That is a truncation and must surface as
// an error, but every already-decoded row is still exposed (the one-below-actual count
// is normalized so the final decoded row is not hidden).
func TestReadRows_TruncatedChunkSurfacesErrorWithBufferedRows(t *testing.T) {
	cb := newTruncatedChunkBuffer(t)

	tbl, n, err := cb.ReadRows(5)
	require.ErrorIs(t, err, io.EOF)
	require.NotErrorIs(t, err, errColumnExhausted)
	require.Equal(t, int64(3), n, "all buffered rows must be exposed, not one fewer")
	require.Equal(t, []any{int64(0), int64(1), int64(2)}, tbl.Values[:n])
}

// TestReadRows_RepeatedReadAfterTruncation guards that a truncated chunk keeps surfacing
// the error on subsequent reads without fabricating phantom rows: the terminal
// normalization runs at most once, so later reads report zero rows (not a phantom that
// could panic callers slicing values by the returned count).
func TestReadRows_RepeatedReadAfterTruncation(t *testing.T) {
	cb := newTruncatedChunkBuffer(t)

	_, n, err := cb.ReadRows(5)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(3), n)

	for range 3 {
		tbl, n, err := cb.ReadRows(1)
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, int64(0), n, "no rows remain in a truncated chunk")
		require.Empty(t, tbl.Values[:n])
	}
}

// TestReadRows_EmptyChunkUsesRowGroupNumRows guards that an empty chunk synthesizes one
// null per top-level row, using the row group's NumRows rather than ColumnMetaData's
// NumValues. For a repeated column NumValues counts leaf values and can exceed the row
// count, so using it would create phantom rows and desynchronize columns.
func TestReadRows_EmptyChunkUsesRowGroupNumRows(t *testing.T) {
	data := make([]byte, 64)
	footer := &parquet.FileMetaData{
		NumRows: 2,
		RowGroups: []*parquet.RowGroup{
			// Two rows, but the chunk declares five leaf values (a repeated column).
			{NumRows: 2, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema:   []string{"leaf"},
				DataPageOffset: int64(len(data)),
				NumValues:      5,
				Type:           parquet.Type_INT64,
				Codec:          parquet.CompressionCodec_UNCOMPRESSED,
			}}}},
		},
	}
	cb, err := NewColumnBuffer(newMockColumnBufferFileReader(data), footer, newSchemaHandlerWithPath("leaf"), common.PathToStr([]string{"root", "leaf"}), nil)
	require.NoError(t, err)

	tbl, n, err := cb.ReadRows(10)
	require.NoError(t, err)
	require.Equal(t, int64(2), n, "one null row per row-group row, not one per declared value")
	require.Equal(t, []any{nil, nil}, tbl.Values[:n])
	require.Equal(t, int64(5), cb.ChunkReadValues, "the declared value count is still accounted")
}

// TestReadRows_RepeatedReadAfterExhaustion is the plain-file counterpart: a fully-read
// column must also report zero rows on every subsequent over-read, i.e. the terminal
// EOF normalization is not repeated per call.
func TestReadRows_RepeatedReadAfterExhaustion(t *testing.T) {
	fw := buffer.NewBufferWriter()
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(skipCountRecord), writer.WithRowGroupSize(1<<30))
	require.NoError(t, err)
	for i := range int64(3) {
		require.NoError(t, pw.WriteWithContext(context.Background(), skipCountRecord{V: i}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))
	pr, err := NewParquetReader(buffer.NewBufferReaderFromBytes(fw.Bytes()), new(skipCountRecord))
	require.NoError(t, err)
	cb, err := pr.newColumnBuffer(pr.SchemaHandler.ValueColumns[0])
	require.NoError(t, err)

	tbl, n, err := cb.ReadRows(3)
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
	require.Equal(t, []any{int64(0), int64(1), int64(2)}, tbl.Values[:n])

	for range 3 {
		tbl, n, err := cb.ReadRows(1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n, "over-read past the end must report zero rows")
		require.Empty(t, tbl.Values[:n])
	}
}

// TestReadRows_LaterEmptyRowGroup covers reviewer concern #2: a chunk with no page
// bytes that follows a populated row group must still contribute its declared rows as
// nulls. Detecting the empty chunk via DataTable == nil would skip it (DataTable is
// already non-nil from the first row group) and silently drop those rows.
func TestReadRows_LaterEmptyRowGroup(t *testing.T) {
	pageBytes, md, src := buildThreeRowPage(t)
	footer := &parquet.FileMetaData{
		NumRows: 5,
		Schema:  src.Footer.Schema,
		RowGroups: []*parquet.RowGroup{
			// Row group 1: the real 3-row page at offset 0.
			{NumRows: 3, Columns: []*parquet.ColumnChunk{chunkFor(md, 0, 3, int64(len(pageBytes)))}},
			// Row group 2: an empty chunk (offset at EOF) declaring 2 values, no page data.
			{NumRows: 2, Columns: []*parquet.ColumnChunk{chunkFor(md, int64(len(pageBytes)), 2, 0)}},
		},
	}
	cb, err := NewColumnBuffer(buffer.NewBufferReaderFromBytes(pageBytes), footer, src.SchemaHandler, src.SchemaHandler.ValueColumns[0], nil)
	require.NoError(t, err)

	tbl, n, err := cb.ReadRows(5)
	require.NoError(t, err)
	require.Equal(t, int64(5), n, "row group 2's declared null rows must not be dropped")
	require.Equal(t, []any{int64(0), int64(1), int64(2), nil, nil}, tbl.Values[:n])
}

func TestReadRows_EmptyChunkBeforeLaterBytes(t *testing.T) {
	pageBytes, md, src := buildThreeRowPage(t)
	footer := &parquet.FileMetaData{
		NumRows: 5,
		Schema:  src.Footer.Schema,
		RowGroups: []*parquet.RowGroup{
			// The empty chunk starts where later page bytes exist in the backing file.
			{NumRows: 2, Columns: []*parquet.ColumnChunk{chunkFor(md, 0, 2, 0)}},
			{NumRows: 3, Columns: []*parquet.ColumnChunk{chunkFor(md, 0, 3, int64(len(pageBytes)))}},
		},
	}
	cb, err := NewColumnBuffer(buffer.NewBufferReaderFromBytes(pageBytes), footer, src.SchemaHandler, src.SchemaHandler.ValueColumns[0], nil)
	require.NoError(t, err)

	table, n, err := cb.ReadRows(5)
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	require.Equal(t, []any{nil, nil, int64(0), int64(1), int64(2)}, table.Values[:n])
}

func TestReadRows_EmptyChunkRowsCanExceedFileBytes(t *testing.T) {
	const numRows int64 = 10_000
	data := make([]byte, 64)
	footer := &parquet.FileMetaData{
		NumRows: numRows,
		RowGroups: []*parquet.RowGroup{{
			NumRows: numRows,
			Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema:        []string{"leaf"},
				DataPageOffset:      int64(len(data)),
				NumValues:           numRows,
				TotalCompressedSize: 0,
				Type:                parquet.Type_INT64,
				Codec:               parquet.CompressionCodec_UNCOMPRESSED,
			}}},
		}},
	}
	cb, err := NewColumnBuffer(newMockColumnBufferFileReader(data), footer, newSchemaHandlerWithPath("leaf"), common.PathToStr([]string{"root", "leaf"}), nil)
	require.NoError(t, err)

	table, n, err := cb.ReadRows(numRows)
	require.NoError(t, err)
	require.Equal(t, numRows, n)
	require.Len(t, table.Values, int(numRows))
}

func TestReadRows_EmptyChunkHonorsSyntheticAllocationLimit(t *testing.T) {
	const numRows int64 = 2
	data := make([]byte, 64)
	footer := &parquet.FileMetaData{
		NumRows: numRows,
		RowGroups: []*parquet.RowGroup{{
			NumRows: numRows,
			Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema:        []string{"leaf"},
				DataPageOffset:      int64(len(data)),
				NumValues:           numRows,
				TotalCompressedSize: 0,
				Type:                parquet.Type_INT64,
				Codec:               parquet.CompressionCodec_UNCOMPRESSED,
			}}},
		}},
	}
	opts := &layout.PageReadOptions{MaxPageSize: 24}
	cb, err := NewColumnBuffer(newMockColumnBufferFileReader(data), footer, newSchemaHandlerWithPath("leaf"), common.PathToStr([]string{"root", "leaf"}), opts)
	require.NoError(t, err)

	_, n, err := cb.ReadRows(numRows)
	require.ErrorContains(t, err, "synthetic null row count 2 exceeds allocation limit 24")
	require.Equal(t, int64(0), n)
}

func TestEmptyChunkAtCursor_NilReader(t *testing.T) {
	cb := &ColumnBufferType{}
	require.False(t, cb.emptyChunkAtCursor(), "a nil ThriftReader is not an empty chunk")
}

func TestAppendNullChunk_NilSchemaHandler(t *testing.T) {
	cb := &ColumnBufferType{
		ChunkHeader: &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
			PathInSchema: []string{"leaf"}, NumValues: 1,
		}},
	}
	err := cb.appendNullChunk()
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema handler is nil")
}

func TestReadPage_ChunkHeaderConditions(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *ColumnBufferType
		expectError bool
	}{
		{
			name: "chunk_header_nil",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{
					Footer:           &parquet.FileMetaData{NumRows: 0},
					SchemaHandler:    newSchemaHandlerWithPath("leaf"),
					PathStr:          common.PathToStr([]string{"root", "leaf"}),
					ChunkHeader:      nil,
					DataTableNumRows: -1,
				}
			},
			expectError: true,
		},
		{
			name: "all_values_read",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{
					Footer:        &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}},
					SchemaHandler: newSchemaHandlerWithPath("leaf"),
					PathStr:       common.PathToStr([]string{"root", "leaf"}),
					ChunkHeader: &parquet.ColumnChunk{
						MetaData: &parquet.ColumnMetaData{
							PathInSchema:   []string{"leaf"},
							DataPageOffset: 0,
							NumValues:      5,
						},
					},
					ChunkReadValues:  5,
					DataTableNumRows: -1,
				}
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := tt.setup()
			err := cb.ReadPage()
			if tt.expectError {
				// No row group can be advanced to: normal completion, not a hard error.
				require.ErrorIs(t, err, errColumnExhausted)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReadPage_EOF_FallbackCreatesEmptyTable(t *testing.T) {
	// A file whose bytes are only padding, with the chunk's page offset at EOF, so the
	// first page read finds no bytes at all. The padding gives the file enough size to
	// satisfy appendNullChunk's file-size ceiling.
	data := make([]byte, 64)
	pFile := newMockColumnBufferFileReader(data)

	const metaNumValues int64 = 3
	footer := &parquet.FileMetaData{
		NumRows: metaNumValues,
		RowGroups: []*parquet.RowGroup{
			{NumRows: metaNumValues, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema:   []string{"leaf"},
				DataPageOffset: int64(len(data)),
				NumValues:      metaNumValues,
				Type:           parquet.Type_INT64,
				Codec:          parquet.CompressionCodec_UNCOMPRESSED,
			}}}},
		},
	}
	sh := newSchemaHandlerWithPath("leaf")

	cb, err := NewColumnBuffer(pFile, footer, sh, common.PathToStr([]string{"root", "leaf"}), nil)
	require.NoError(t, err)
	require.NotNil(t, cb)

	// The first read synthesizes the declared rows as nulls and marks the chunk
	// consumed, returning no error.
	require.NoError(t, cb.ReadPage())
	require.NotNil(t, cb.DataTable)
	require.Len(t, cb.DataTable.Values, int(metaNumValues))
	for i := range int(metaNumValues) {
		require.Nil(t, cb.DataTable.Values[i])
		require.Equal(t, int32(0), cb.DataTable.DefinitionLevels[i])
		require.Equal(t, int32(0), cb.DataTable.RepetitionLevels[i])
	}
	require.Equal(t, metaNumValues, cb.ChunkReadValues)

	// The next read advances past the now-exhausted row group and reports completion,
	// normalizing the "one less than actual" count. Completion stays io.EOF-compatible
	// for external callers of the exported method.
	rerr := cb.ReadPage()
	require.ErrorIs(t, rerr, errColumnExhausted)
	require.ErrorIs(t, rerr, io.EOF)
	require.Equal(t, metaNumValues, cb.DataTableNumRows)
}

func TestReadPage_RecursiveCall(t *testing.T) {
	// Test the else branch that calls NextRowGroup and recursively calls ReadPage
	footer := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}}
	sh := newSchemaHandlerWithPath("leaf")
	mockFile := newMockColumnBufferFileReader([]byte{})

	cb := &ColumnBufferType{
		PFile:         mockFile,
		Footer:        footer,
		SchemaHandler: sh,
		PathStr:       common.PathToStr([]string{"root", "leaf"}),
		ChunkHeader: &parquet.ColumnChunk{
			MetaData: &parquet.ColumnMetaData{
				PathInSchema:   []string{"leaf"},
				DataPageOffset: 0,
				NumValues:      5,
			},
		},
		ChunkReadValues:  5, // All values read, will trigger NextRowGroup
		DataTableNumRows: -1,
	}

	err := cb.ReadPage()
	// No more row groups to advance to: normal completion via the exhausted sentinel.
	require.ErrorIs(t, err, errColumnExhausted)
}
