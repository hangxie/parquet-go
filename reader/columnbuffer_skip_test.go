package reader

import (
	"context"
	"io"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/writer"
)

func TestSkipRows(t *testing.T) {
	tests := []struct {
		name         string
		setup        func() *ColumnBufferType
		numRows      int64
		expectedRows int64
		expectError  bool
	}{
		{
			name: "zero_rows",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 10}, DataTableNumRows: -1}
			},
			numRows:      0,
			expectedRows: 0,
			expectError:  false,
		},
		{
			name: "negative_rows",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 10}, DataTableNumRows: -1}
			},
			numRows:      -5,
			expectedRows: 0,
			expectError:  false,
		},
		{
			name: "partial_buffer",
			setup: func() *ColumnBufferType {
				dt := &layout.Table{
					Values:           []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
					DefinitionLevels: []int32{1, 1, 1, 1, 1},
					RepetitionLevels: []int32{0, 0, 0, 0, 0},
				}
				return &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 10}, DataTable: dt, DataTableNumRows: 4}
			},
			numRows:      3,
			expectedRows: 3,
			expectError:  false,
		},
		{
			name: "exact_buffer",
			setup: func() *ColumnBufferType {
				sh := newSchemaHandlerWithPath("leaf")
				dt := &layout.Table{
					Values:           []any{int64(1), int64(2), int64(3)},
					DefinitionLevels: []int32{1, 1, 1},
					RepetitionLevels: []int32{0, 0, 0},
				}
				return &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 10}, SchemaHandler: sh, PathStr: common.PathToStr([]string{"root", "leaf"}), DataTable: dt, DataTableNumRows: 2}
			},
			numRows:      2,
			expectedRows: 2,
			expectError:  false,
		},
		{
			name: "skip_more_than_buffer_with_schema",
			setup: func() *ColumnBufferType {
				sh := newSchemaHandlerWithPath("leaf")
				dt := &layout.Table{
					Values:           []any{int64(1), int64(2), int64(3)},
					DefinitionLevels: []int32{1, 1, 1},
					RepetitionLevels: []int32{0, 0, 0},
				}
				mockFile := newMockColumnBufferFileReader([]byte{})
				return &ColumnBufferType{
					PFile: mockFile,
					Footer: &parquet.FileMetaData{
						NumRows: 100,
						RowGroups: []*parquet.RowGroup{
							{NumRows: 50, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
								PathInSchema: []string{"leaf"}, DataPageOffset: 0, NumValues: 50,
							}}}},
							{NumRows: 50, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
								PathInSchema: []string{"leaf"}, DataPageOffset: 1000, NumValues: 50,
							}}}},
						},
					},
					SchemaHandler:    sh,
					PathStr:          common.PathToStr([]string{"root", "leaf"}),
					DataTable:        dt,
					DataTableNumRows: 2,
					RowGroupIndex:    0,
				}
			},
			numRows: 10,
			// The 3 buffered rows are skipped, then the first row group's empty chunk
			// (declaring 50 values with no page data) is synthesized as nulls, matching
			// what ReadRows would return; 7 of those are skipped for a total of 10.
			expectedRows: 10,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := tt.setup()
			n, err := cb.SkipRows(tt.numRows)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "EOF")
				// When we expect errors, we may have skipped some rows before the error
				require.True(t, n >= 0)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedRows, n)
			}
		})
	}
}

func TestSkipRows_ReadPageForSkipErrorReturnsZero(t *testing.T) {
	mockFile := newMockColumnBufferFileReader([]byte{})
	footer := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema:   []string{"leaf"},
				DataPageOffset: 0,
				NumValues:      3,
				Type:           parquet.Type_INT64,
				Codec:          parquet.CompressionCodec_UNCOMPRESSED,
			}}}},
		},
	}
	sh := newSchemaHandlerWithPath("leaf")

	cb, err := NewColumnBuffer(mockFile, footer, sh, common.PathToStr([]string{"root", "leaf"}), nil)
	require.NoError(t, err)
	require.NotNil(t, cb)

	n, _ := cb.SkipRows(1)
	require.Equal(t, int64(0), n)
}

// TestSkipRows_ReadRows_AgreeOnEmptyChunk covers issue #349: a column chunk whose
// metadata declares NumValues > 0 but has no readable page bytes yields an io.EOF on
// the first page read. ReadPage/ReadRows and ReadPageForSkip/SkipRows must recover it
// identically (synthesizing all-null top-level rows) so skip- and read-based access
// agree on both row counts and positions instead of one succeeding and the other
// erroring.
func TestSkipRows_ReadRows_AgreeOnEmptyChunk(t *testing.T) {
	const numValues int64 = 3

	// The backing file is large enough to satisfy appendNullChunk's file-size
	// ceiling, but DataPageOffset points at its end so the first page read hits EOF.
	data := make([]byte, 64)
	newCB := func() *ColumnBufferType {
		footer := &parquet.FileMetaData{
			NumRows: numValues,
			RowGroups: []*parquet.RowGroup{
				{NumRows: numValues, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
					PathInSchema:   []string{"leaf"},
					DataPageOffset: int64(len(data)),
					NumValues:      numValues,
					Type:           parquet.Type_INT64,
					Codec:          parquet.CompressionCodec_UNCOMPRESSED,
				}}}},
			},
		}
		cb, err := NewColumnBuffer(newMockColumnBufferFileReader(data), footer, newSchemaHandlerWithPath("leaf"), common.PathToStr([]string{"root", "leaf"}), nil)
		require.NoError(t, err)
		return cb
	}

	// ReadRows synthesizes all-null rows for the whole chunk.
	tbl, n, err := newCB().ReadRows(numValues)
	require.NoError(t, err)
	require.Equal(t, numValues, n)
	require.Len(t, tbl.Values, int(numValues))
	for i := range tbl.Values {
		require.Nil(t, tbl.Values[i])
		require.Equal(t, int32(0), tbl.DefinitionLevels[i])
		require.Equal(t, int32(0), tbl.RepetitionLevels[i])
	}

	// SkipRows agrees: it can skip every declared row without erroring, and caps a
	// request larger than the chunk at the number of rows that actually exist.
	skipped, err := newCB().SkipRows(1)
	require.NoError(t, err)
	require.Equal(t, int64(1), skipped)

	skipped, err = newCB().SkipRows(numValues)
	require.NoError(t, err)
	require.Equal(t, numValues, skipped)

	skipped, err = newCB().SkipRows(numValues + 10)
	require.NoError(t, err)
	require.Equal(t, numValues, skipped)

	// Positions line up: skipping k rows then reading the remainder yields the
	// remaining numValues-k rows, all still synthesized nulls.
	cb := newCB()
	skipped, err = cb.SkipRows(1)
	require.NoError(t, err)
	require.Equal(t, int64(1), skipped)
	tbl, n, err = cb.ReadRows(numValues - 1)
	require.NoError(t, err)
	require.Equal(t, numValues-1, n)
	require.Len(t, tbl.Values, int(numValues-1))
	for i := range tbl.Values {
		require.Nil(t, tbl.Values[i])
	}
}

// TestReadPageForSkip_EmptyChunkReturnsCompletion guards the exported contract that a
// nil page is only ever returned alongside a non-nil error. On an empty chunk the
// method synthesizes rows but returns them via SkipRows/ReadRows, so it reports
// completion (errColumnExhausted, which wraps io.EOF) rather than a nil page with a nil
// error that a caller could dereference.
func TestReadPageForSkip_EmptyChunkReturnsCompletion(t *testing.T) {
	data := make([]byte, 64)
	footer := &parquet.FileMetaData{
		NumRows: 3,
		RowGroups: []*parquet.RowGroup{
			{NumRows: 3, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema:   []string{"leaf"},
				DataPageOffset: int64(len(data)),
				NumValues:      3,
				Type:           parquet.Type_INT64,
				Codec:          parquet.CompressionCodec_UNCOMPRESSED,
			}}}},
		},
	}
	cb, err := NewColumnBuffer(newMockColumnBufferFileReader(data), footer, newSchemaHandlerWithPath("leaf"), common.PathToStr([]string{"root", "leaf"}), nil)
	require.NoError(t, err)

	page, err := cb.ReadPageForSkip()
	require.Nil(t, page)
	require.ErrorIs(t, err, errColumnExhausted)
	require.ErrorIs(t, err, io.EOF)
}

// TestSkipRows_ConsecutiveSkipsOnEmptyChunk guards that skipping an empty chunk keeps
// the buffered rows in the "one less than actual" convention that consumeExistingRows
// relies on. Skipping one of three synthesized rows must leave exactly two, so a
// following large skip reports two — not three.
func TestSkipRows_ConsecutiveSkipsOnEmptyChunk(t *testing.T) {
	const numValues int64 = 3
	data := make([]byte, 64)
	newCB := func() *ColumnBufferType {
		footer := &parquet.FileMetaData{
			NumRows: numValues,
			RowGroups: []*parquet.RowGroup{
				{NumRows: numValues, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
					PathInSchema:   []string{"leaf"},
					DataPageOffset: int64(len(data)),
					NumValues:      numValues,
					Type:           parquet.Type_INT64,
					Codec:          parquet.CompressionCodec_UNCOMPRESSED,
				}}}},
			},
		}
		cb, err := NewColumnBuffer(newMockColumnBufferFileReader(data), footer, newSchemaHandlerWithPath("leaf"), common.PathToStr([]string{"root", "leaf"}), nil)
		require.NoError(t, err)
		return cb
	}

	// Reviewer scenario: skip 1, then a large skip reports only the remaining 2.
	cb := newCB()
	skipped, err := cb.SkipRows(1)
	require.NoError(t, err)
	require.Equal(t, int64(1), skipped)
	skipped, err = cb.SkipRows(10)
	require.NoError(t, err)
	require.Equal(t, int64(2), skipped, "only two rows remain after skipping one")

	// Skipping one row at a time drains exactly numValues rows and no more.
	cb = newCB()
	for range numValues {
		skipped, err = cb.SkipRows(1)
		require.NoError(t, err)
		require.Equal(t, int64(1), skipped)
	}
	skipped, err = cb.SkipRows(1)
	require.NoError(t, err)
	require.Equal(t, int64(0), skipped, "no rows remain")

	// Skipping part of the chunk then reading returns the rest as nulls.
	cb = newCB()
	skipped, err = cb.SkipRows(2)
	require.NoError(t, err)
	require.Equal(t, int64(2), skipped)
	tbl, n, err := cb.ReadRows(5)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, []any{nil}, tbl.Values[:n])

	// Fully skipping the chunk then reading past the end reports zero rows on every
	// call: terminal exhaustion is recorded even though the buffer was already drained,
	// so the count is not re-normalized into a phantom row.
	cb = newCB()
	skipped, err = cb.SkipRows(numValues)
	require.NoError(t, err)
	require.Equal(t, numValues, skipped)
	for range 3 {
		tbl, n, err := cb.ReadRows(1)
		require.NoError(t, err)
		require.Equal(t, int64(0), n, "over-read after a full skip must report zero rows")
		require.Empty(t, tbl.Values[:n])
	}
}

// TestSkipRows_NoFabricationOnTruncatedPage guards that recovery is limited to a
// genuinely empty chunk: a page with a valid header but no body bytes (a truncated
// or corrupt page) yields io.EOF only after parsing has begun, and must surface as
// an error rather than being silently turned into fabricated null rows.
func TestSkipRows_NoFabricationOnTruncatedPage(t *testing.T) {
	ph := parquet.NewPageHeader()
	ph.Type = parquet.PageType_DATA_PAGE
	ph.CompressedPageSize = 10
	ph.UncompressedPageSize = 10
	ph.DataPageHeader = parquet.NewDataPageHeader()
	ph.DataPageHeader.NumValues = 2
	ph.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	ph.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	ph.DataPageHeader.Encoding = parquet.Encoding_PLAIN
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	headerBytes, err := ts.Write(context.TODO(), ph)
	require.NoError(t, err)

	newFooter := func() *parquet.FileMetaData {
		return &parquet.FileMetaData{
			NumRows: 3,
			RowGroups: []*parquet.RowGroup{
				{NumRows: 3, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
					PathInSchema:        []string{"leaf"},
					DataPageOffset:      0,
					NumValues:           3,
					TotalCompressedSize: int64(len(headerBytes)),
					Type:                parquet.Type_INT64,
					Codec:               parquet.CompressionCodec_UNCOMPRESSED,
				}}}},
			},
		}
	}

	// The page-body EOF surfaces as a real error (wrapping io.EOF) and must not be
	// mistaken for normal completion, nor fabricated into null rows.
	cb, err := NewColumnBuffer(newMockColumnBufferFileReader(headerBytes), newFooter(), newSchemaHandlerWithPath("leaf"), common.PathToStr([]string{"root", "leaf"}), nil)
	require.NoError(t, err)
	rerr := cb.ReadPage()
	require.ErrorIs(t, rerr, io.EOF)
	require.NotErrorIs(t, rerr, errColumnExhausted)
	require.Nil(t, cb.DataTable, "a truncated page must not synthesize null rows")

	// SkipRows sees the same truncated page and must surface the error rather than
	// silently reporting a successful skip.
	cb, err = NewColumnBuffer(newMockColumnBufferFileReader(headerBytes), newFooter(), newSchemaHandlerWithPath("leaf"), common.PathToStr([]string{"root", "leaf"}), nil)
	require.NoError(t, err)
	n, err := cb.SkipRows(1)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(0), n)
}

func TestSkipRows_DictionaryOnlyChunkIsTruncated(t *testing.T) {
	n, err := newDictionaryOnlyChunkBuffer(t).SkipRows(3)

	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(0), n)
}

func TestSkipRows_AfterReadRowsExhaustionReturnsZero(t *testing.T) {
	pageBytes, md, src := buildThreeRowPage(t)
	footer := &parquet.FileMetaData{
		NumRows: 3,
		Schema:  src.Footer.Schema,
		RowGroups: []*parquet.RowGroup{{
			NumRows: 3,
			Columns: []*parquet.ColumnChunk{
				chunkFor(md, 0, 3, int64(len(pageBytes))),
			},
		}},
	}
	cb, err := NewColumnBuffer(buffer.NewBufferReaderFromBytes(pageBytes), footer, src.SchemaHandler, src.SchemaHandler.ValueColumns[0], nil)
	require.NoError(t, err)

	_, n, err := cb.ReadRows(3)
	require.NoError(t, err)
	require.Equal(t, int64(3), n)

	n, err = cb.SkipRows(1)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}

// TestSkipRows_ReportsCountAcrossPages verifies SkipRows returns the true number of
// rows skipped even when the skip spans multiple pages, and leaves the cursor aligned
// so a subsequent read resumes at the correct row.
func TestSkipRows_ReportsCountAcrossPages(t *testing.T) {
	fw := buffer.NewBufferWriter()
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(skipCountRecord), writer.WithPageSize(8), writer.WithRowGroupSize(1<<30))
	require.NoError(t, err)
	for i := range int64(100) {
		require.NoError(t, pw.WriteWithContext(context.Background(), skipCountRecord{V: i}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	pr, err := NewParquetReader(buffer.NewBufferReaderFromBytes(fw.Bytes()), new(skipCountRecord))
	require.NoError(t, err)
	require.Equal(t, 1, len(pr.Footer.RowGroups), "fixture should be a single multi-page row group")

	cb, err := pr.newColumnBuffer(pr.SchemaHandler.ValueColumns[0])
	require.NoError(t, err)

	n, err := cb.SkipRows(50)
	require.NoError(t, err)
	require.Equal(t, int64(50), n)

	tbl, m, err := cb.ReadRows(3)
	require.NoError(t, err)
	require.Equal(t, []any{int64(50), int64(51), int64(52)}, tbl.Values[:m])
}

// TestSkipRows_ExactlyDrainsBuffer guards that when the buffered rows exactly satisfy
// the requested skip, SkipRows stops without reading the next page. The backing file
// is set to fail on I/O, so a spurious skipByReadingPages(0) call would surface an
// error from data beyond the skip boundary.
func TestSkipRows_ExactlyDrainsBuffer(t *testing.T) {
	mockFile := newMockColumnBufferFileReader([]byte{})
	mockFile.SetShouldFail(true) // any read/seek past the buffer errors

	cb := &ColumnBufferType{
		PFile:         mockFile,
		SchemaHandler: newSchemaHandlerWithPath("leaf"),
		PathStr:       common.PathToStr([]string{"root", "leaf"}),
		Footer: &parquet.FileMetaData{
			NumRows: 3,
			RowGroups: []*parquet.RowGroup{
				{NumRows: 3, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
					PathInSchema: []string{"leaf"}, DataPageOffset: 0, NumValues: 3,
				}}}},
			},
		},
		DataTable: &layout.Table{
			Values:           []any{int64(1), int64(2), int64(3)},
			DefinitionLevels: []int32{1, 1, 1},
			RepetitionLevels: []int32{0, 0, 0},
		},
		DataTableNumRows: 2, // three buffered rows, one-less convention
		RowGroupIndex:    0,
	}

	n, err := cb.SkipRows(3)
	require.NoError(t, err, "must not read past the exactly-drained buffer")
	require.Equal(t, int64(3), n)
}

// TestSkipRows_TruncatedChunkReportsPartialProgress covers a chunk that declares more
// values than its pages hold: skipping it surfaces a truncation error, but the count of
// rows actually skipped (the cursor's real advance) is still reported so columns do not
// silently desynchronize.
func TestSkipRows_TruncatedChunkReportsPartialProgress(t *testing.T) {
	pageBytes, md, src := buildThreeRowPage(t)
	footer := &parquet.FileMetaData{
		NumRows: 5,
		Schema:  src.Footer.Schema,
		RowGroups: []*parquet.RowGroup{
			// Declares 5 values but is backed by a single 3-row page, then EOF.
			{NumRows: 5, Columns: []*parquet.ColumnChunk{chunkFor(md, 0, 5, int64(len(pageBytes)))}},
		},
	}
	cb, err := NewColumnBuffer(buffer.NewBufferReaderFromBytes(pageBytes), footer, src.SchemaHandler, src.SchemaHandler.ValueColumns[0], nil)
	require.NoError(t, err)

	n, err := cb.SkipRows(5)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(3), n, "the three skipped rows must be reported despite the error")
}

// TestSkipRows_EmptyRowGroupThenPopulated covers a skip that spans an empty row group
// into the populated one that follows. ReadPageForSkip must continue past the empty
// (nonterminal) chunk rather than reporting completion, so the skip stays in sync with
// columns that do have data — otherwise ParquetReader.SkipRows, which ignores the
// returned count, would leave this column misaligned. It must also return a real
// (non-nil) page for the populated row group per the exported contract.
func TestSkipRows_EmptyRowGroupThenPopulated(t *testing.T) {
	pageBytes, md, src := buildThreeRowPage(t)
	newCB := func() *ColumnBufferType {
		footer := &parquet.FileMetaData{
			NumRows: 5,
			Schema:  src.Footer.Schema,
			RowGroups: []*parquet.RowGroup{
				// Row group 1: a zero-byte chunk whose offset points at later page bytes.
				{NumRows: 2, Columns: []*parquet.ColumnChunk{chunkFor(md, 0, 2, 0)}},
				// Row group 2: the real 3-row page at offset 0.
				{NumRows: 3, Columns: []*parquet.ColumnChunk{chunkFor(md, 0, 3, int64(len(pageBytes)))}},
			},
		}
		cb, err := NewColumnBuffer(buffer.NewBufferReaderFromBytes(pageBytes), footer, src.SchemaHandler, src.SchemaHandler.ValueColumns[0], nil)
		require.NoError(t, err)
		return cb
	}

	// Skipping past the empty row group leaves the populated row group's data intact
	// and in order.
	cb := newCB()
	skipped, err := cb.SkipRows(2)
	require.NoError(t, err)
	require.Equal(t, int64(2), skipped)
	tbl, n, err := cb.ReadRows(5)
	require.NoError(t, err)
	require.Equal(t, int64(3), n, "the following row group's rows must remain readable")
	require.Equal(t, []any{int64(0), int64(1), int64(2)}, tbl.Values[:n])

	// ReadPageForSkip crosses the empty row group and returns the populated page, never
	// a nil page on success.
	cb = newCB()
	page, err := cb.ReadPageForSkip()
	require.NoError(t, err)
	require.NotNil(t, page)
	require.Equal(t, int64(2), cb.RowGroupIndex, "the page must come from the populated row group")
}

func TestReadPageForSkip_Conditions(t *testing.T) {
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
			page, err := cb.ReadPageForSkip()
			if tt.expectError {
				// No row group can be advanced to: normal completion, not a hard error.
				require.ErrorIs(t, err, errColumnExhausted)
				require.Nil(t, page)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestSkipByReadingPages_ReturnsCountPopped documents that skipByReadingPages returns
// the count of rows actually popped (not the remaining-to-skip count). This is important
// because SkipRows must account for this when computing the total-skipped return value.
func TestSkipByReadingPages_ReturnsCountPopped(t *testing.T) {
	dt := &layout.Table{
		Values:           []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
		DefinitionLevels: []int32{1, 1, 1, 1, 1},
		RepetitionLevels: []int32{0, 0, 0, 0, 0},
	}
	cb := &ColumnBufferType{DataTable: dt, DataTableNumRows: 4} // 4 >= 3, loop won't read new pages
	n, err := cb.skipByReadingPages(3)
	require.NoError(t, err)
	require.Equal(t, int64(3), n) // must return the count popped
	require.Equal(t, int64(1), cb.DataTableNumRows)
}

func TestSkipRows_RowGroupSkipping(t *testing.T) {
	// Test skipping entire row groups
	mockFile := newMockColumnBufferFileReader([]byte{})
	footer := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{NumRows: 100, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema: []string{"leaf"}, DataPageOffset: 0, NumValues: 100,
			}}}},
			{NumRows: 100, Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema: []string{"leaf"}, DataPageOffset: 1000, NumValues: 100,
			}}}},
		},
	}
	sh := newSchemaHandlerWithPath("leaf")

	cb := &ColumnBufferType{
		PFile:            mockFile,
		Footer:           footer,
		SchemaHandler:    sh,
		PathStr:          common.PathToStr([]string{"root", "leaf"}),
		DataTableNumRows: -1,
		RowGroupIndex:    0,
	}

	// Skip across row groups
	n, err := cb.SkipRows(150)
	// This will fail because we can't actually read pages, but it exercises the row group skipping logic
	if err == nil || n > 0 {
		// Some rows were skipped
		require.True(t, cb.RowGroupIndex > 0 || n > 0)
	}
}

func TestReadPageForSkip_RecursiveCall(t *testing.T) {
	// Test the else branch that calls NextRowGroup and recursively calls ReadPageForSkip
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

	page, err := cb.ReadPageForSkip()
	// No more row groups to advance to: normal completion via the exhausted sentinel.
	require.ErrorIs(t, err, errColumnExhausted)
	require.Nil(t, page)
}
