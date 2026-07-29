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
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
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

func (m *mockColumnBufferFileReader) SetShouldFail(shouldFail bool) {
	m.shouldFail = shouldFail
}

func (m *mockColumnBufferFileReader) SetCloneFails(cloneFails bool) {
	m.cloneFails = cloneFails
}

func (m *mockColumnBufferFileReader) SetOpenFails(openFails bool) {
	m.openFails = openFails
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

func (m *mockColumnBufferFileReader) Seek(offset int64, whence int) (int64, error) {
	if m.shouldFail {
		return 0, fmt.Errorf("mock seek error")
	}
	if m.closed {
		return 0, fmt.Errorf("reader is closed")
	}
	switch whence {
	case io.SeekStart:
		m.offset = offset
	case io.SeekCurrent:
		m.offset += offset
	case io.SeekEnd:
		m.offset = int64(len(m.data)) + offset
	}
	return m.offset, nil
}

func (m *mockColumnBufferFileReader) Close() error {
	m.closed = true
	return nil
}

func (m *mockColumnBufferFileReader) Open(name string) (source.ParquetFileReader, error) {
	if m.shouldFail || m.openFails {
		return nil, fmt.Errorf("mock open error")
	}
	return newMockColumnBufferFileReader(m.data), nil
}

func (m *mockColumnBufferFileReader) Clone() (source.ParquetFileReader, error) {
	if m.cloneFails {
		return nil, fmt.Errorf("mock clone error")
	}
	if m.shouldFail {
		return nil, fmt.Errorf("mock clone error")
	}
	newReader := newMockColumnBufferFileReader(m.data)
	newReader.offset = m.offset
	// propagate flags so behaviors on the cloned reader remain consistent
	newReader.closed = m.closed
	newReader.shouldFail = m.shouldFail
	newReader.cloneFails = m.cloneFails
	newReader.openFails = m.openFails
	m.clones = append(m.clones, newReader)
	return newReader, nil
}

// Helper function to create a mock schema handler with basic setup
func newMockSchemaHandler() *schema.SchemaHandler {
	return &schema.SchemaHandler{
		SchemaElements: []*parquet.SchemaElement{
			{
				Name: "root",
			},
		},
		Infos: []*common.Tag{
			{
				InName: "root",
				ExName: "root",
			},
		},
		MapIndex:       make(map[string]int32),
		IndexMap:       make(map[int32]string),
		InPathToExPath: make(map[string]string),
		ExPathToInPath: make(map[string]string),
	}
}

// helper to build a minimal schema handler containing a root and one leaf at the given path
func newSchemaHandlerWithPath(path string) *schema.SchemaHandler {
	sh := &schema.SchemaHandler{
		SchemaElements: []*parquet.SchemaElement{
			{ // 0: root
				Name: "root",
			},
			{ // 1: leaf
				Name: path,
				Type: common.ToPtr(parquet.Type_INT64),
			},
		},
		Infos:          []*common.Tag{{InName: "root", ExName: "root"}},
		MapIndex:       make(map[string]int32),
		IndexMap:       make(map[int32]string),
		InPathToExPath: make(map[string]string),
		ExPathToInPath: make(map[string]string),
	}
	fq := common.PathToStr([]string{"root", path})
	sh.MapIndex[fq] = 1
	sh.IndexMap[1] = fq
	sh.InPathToExPath[fq] = fq
	sh.ExPathToInPath[fq] = fq
	return sh
}

func TestNewColumnBuffer(t *testing.T) {
	tests := []struct {
		name           string
		setupFile      func() source.ParquetFileReader
		setupFooter    func() *parquet.FileMetaData
		setupSchema    func() *schema.SchemaHandler
		pathStr        string
		expectError    bool
		expectedError  string
		validateResult func(t *testing.T, cb *ColumnBufferType)
	}{
		{
			name: "nil_file",
			setupFile: func() source.ParquetFileReader {
				return nil
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "pFile is nil",
		},
		{
			name: "clone_fails",
			setupFile: func() source.ParquetFileReader {
				mock := newMockColumnBufferFileReader([]byte{})
				mock.SetCloneFails(true)
				return mock
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "mock clone error",
		},
		{
			name: "nil_footer",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return nil
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "footer is nil",
		},
		{
			name: "nil_schema_handler",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return nil
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "schema handler is nil",
		},
		{
			name: "empty_path",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     "",
			expectError: false, // Empty footer means NextRowGroup returns EOF which is handled
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Empty(t, cb.PathStr)
				require.Equal(t, int64(-1), cb.DataTableNumRows)
				require.Equal(t, int64(0), cb.RowGroupIndex)
			},
		},
		{
			name: "empty_footer_success",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{}, // Empty row groups
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     "test.field",
			expectError: false, // Empty footer means NextRowGroup returns EOF which is handled
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, "test.field", cb.PathStr)
				require.Equal(t, int64(-1), cb.DataTableNumRows)
				require.Equal(t, int64(0), cb.RowGroupIndex)
			},
		},
		{
			name: "single_row_group_column_not_found",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						{
							Columns: []*parquet.ColumnChunk{
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema: []string{"other_field"},
									},
								},
							},
						},
					},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "[NextRowGroup] Column not found: test.field",
		},
		{
			name: "single_row_group_column_found",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						{
							Columns: []*parquet.ColumnChunk{
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:   []string{"test_field"},
										DataPageOffset: int64(100),
									},
									FilePath: nil,
								},
							},
						},
					},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     common.PathToStr([]string{"root", "test_field"}),
			expectError: false,
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, common.PathToStr([]string{"root", "test_field"}), cb.PathStr)
				require.Equal(t, int64(1), cb.RowGroupIndex)
				require.NotNil(t, cb.ChunkHeader)
				require.Equal(t, int64(-1), cb.DataTableNumRows)
			},
		},
		{
			name: "multiple_columns_correct_match",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						{
							Columns: []*parquet.ColumnChunk{
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:   []string{"field1"},
										DataPageOffset: int64(50),
									},
								},
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:   []string{"field2"},
										DataPageOffset: int64(100),
									},
								},
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:   []string{"target_field"},
										DataPageOffset: int64(150),
									},
								},
							},
						},
					},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     common.PathToStr([]string{"root", "target_field"}),
			expectError: false,
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, common.PathToStr([]string{"root", "target_field"}), cb.PathStr)
				// Should find the third column (index 2)
				require.NotNil(t, cb.ChunkHeader)
				expectedPath := []string{"target_field"}
				actualPath := cb.ChunkHeader.MetaData.GetPathInSchema()
				require.Len(t, actualPath, len(expectedPath))
				require.Equal(t, expectedPath[0], actualPath[0])
			},
		},
		{
			name: "with_dictionary_page_offset",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						{
							Columns: []*parquet.ColumnChunk{
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:         []string{"dict_field"},
										DataPageOffset:       int64(200),
										DictionaryPageOffset: common.ToPtr(int64(100)), // Dictionary comes before data
									},
								},
							},
						},
					},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     common.PathToStr([]string{"root", "dict_field"}),
			expectError: false,
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, common.PathToStr([]string{"root", "dict_field"}), cb.PathStr)
				require.NotNil(t, cb.ChunkHeader)
				// The function should use dictionary page offset when available
				require.NotNil(t, cb.ChunkHeader.MetaData.DictionaryPageOffset)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pFile := tt.setupFile()
			footer := tt.setupFooter()
			schemaHandler := tt.setupSchema()

			result, err := NewColumnBuffer(pFile, footer, schemaHandler, tt.pathStr, nil)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)

				// Validate basic fields are set correctly
				require.Equal(t, footer, result.Footer)
				require.Equal(t, schemaHandler, result.SchemaHandler)
				require.Equal(t, tt.pathStr, result.PathStr)
				require.NotNil(t, result.PFile)

				if tt.validateResult != nil {
					tt.validateResult(t, result)
				}
			}
		})
	}
}

func TestNewColumnBuffer_EdgeCases(t *testing.T) {
	t.Run("complex_nested_path", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		footer := &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{
					Columns: []*parquet.ColumnChunk{
						{
							MetaData: &parquet.ColumnMetaData{
								PathInSchema:   []string{"nested", "deep", "field"},
								DataPageOffset: int64(100),
							},
						},
					},
				},
			},
		}
		schemaHandler := newMockSchemaHandler()

		result, err := NewColumnBuffer(mockFile, footer, schemaHandler, common.PathToStr([]string{"root", "nested", "deep", "field"}), nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, common.PathToStr([]string{"root", "nested", "deep", "field"}), result.PathStr)
	})

	t.Run("file_path_specified", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		filePath := "external_file.parquet"
		footer := &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{
					Columns: []*parquet.ColumnChunk{
						{
							MetaData: &parquet.ColumnMetaData{
								PathInSchema:   []string{"external_field"},
								DataPageOffset: int64(100),
							},
							FilePath: &filePath,
						},
					},
				},
			},
		}
		schemaHandler := newMockSchemaHandler()

		result, err := NewColumnBuffer(mockFile, footer, schemaHandler, common.PathToStr([]string{"root", "external_field"}), nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		// When FilePath is specified, the function should handle opening the external file
		require.NotNil(t, result.ChunkHeader.FilePath)
		require.Equal(t, filePath, *result.ChunkHeader.FilePath)
	})

	t.Run("readrows_with_error_propagates", func(t *testing.T) {
		footer := &parquet.FileMetaData{
			NumRows: 1,
			RowGroups: []*parquet.RowGroup{{
				Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{PathInSchema: []string{"leaf"}, DataPageOffset: 0, NumValues: 1, Type: parquet.Type_INT64, Codec: parquet.CompressionCodec_UNCOMPRESSED}}},
			}},
		}
		sh := newSchemaHandlerWithPath("bogus") // MapIndex includes root.bogus
		cb := &ColumnBufferType{Footer: footer, SchemaHandler: sh, PathStr: common.PathToStr([]string{"root", "bogus"}), DataTableNumRows: -1}

		_, _, err := cb.ReadRows(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Column not found")
	})

	t.Run("skiprows_with_error_propagates", func(t *testing.T) {
		footer := &parquet.FileMetaData{
			NumRows: 1,
			RowGroups: []*parquet.RowGroup{{
				Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{PathInSchema: []string{"leaf"}, DataPageOffset: 0, NumValues: 1, Type: parquet.Type_INT64, Codec: parquet.CompressionCodec_UNCOMPRESSED}}},
			}},
		}
		sh := newSchemaHandlerWithPath("bogus")
		cb := &ColumnBufferType{Footer: footer, SchemaHandler: sh, PathStr: common.PathToStr([]string{"root", "bogus"}), DataTableNumRows: -1}

		_, err := cb.SkipRows(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Column not found")
	})

	t.Run("gettype_error_on_corrupted_schema", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		footer := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}}
		sh := &schema.SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{{Name: "root"}, {Name: "badfield"}}, // No Type set
			MapIndex:       map[string]int32{common.PathToStr([]string{"root", "badfield"}): 1},
		}

		cb, err := NewColumnBuffer(mockFile, footer, sh, common.PathToStr([]string{"root", "badfield"}), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "path not found")
		require.Nil(t, cb)
	})

	t.Run("nextrowgroup_no_increment_when_empty", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		footer := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}}
		sh := newSchemaHandlerWithPath("leaf")

		cb := &ColumnBufferType{
			PFile: mockFile, Footer: footer, SchemaHandler: sh,
			PathStr: common.PathToStr([]string{"root", "leaf"}), DataTableNumRows: -1, RowGroupIndex: 0,
		}

		err := cb.NextRowGroup()
		require.Equal(t, io.EOF, err)
		require.Equal(t, int64(-1), cb.DataTableNumRows) // Should NOT increment
	})
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

func TestNewColumnBuffer_FilePathOpenError(t *testing.T) {
	mockFile := newMockColumnBufferFileReader([]byte{})
	mockFile.SetOpenFails(true)

	filePath := "external.parquet"
	footer := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{Columns: []*parquet.ColumnChunk{{
				MetaData: &parquet.ColumnMetaData{
					PathInSchema:   []string{"leaf"},
					DataPageOffset: 0,
					NumValues:      1,
					Type:           parquet.Type_INT64,
					Codec:          parquet.CompressionCodec_UNCOMPRESSED,
				},
				FilePath: &filePath,
			}}},
		},
	}
	sh := newSchemaHandlerWithPath("leaf")

	cb, err := NewColumnBuffer(mockFile, footer, sh, common.PathToStr([]string{"root", "leaf"}), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock open error")
	require.Nil(t, cb)
	// The constructor discards the buffer on failure, so the cloned reader it
	// created must be closed rather than leaked.
	require.Len(t, mockFile.clones, 1)
	require.True(t, mockFile.clones[0].closed, "cloned reader must be closed on constructor failure")
}

// A failed Open on an external column-chunk file must leave cbt.PFile intact
// rather than clobbering it with the nil interface Open returns on error;
// otherwise a subsequent ReadStop would call Close on a nil interface and panic.
func TestNextRowGroup_OpenFailurePreservesPFile(t *testing.T) {
	mockFile := newMockColumnBufferFileReader([]byte{})
	mockFile.SetOpenFails(true)

	filePath := "external.parquet"
	cbt := &ColumnBufferType{
		PFile: mockFile,
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{{
					MetaData: &parquet.ColumnMetaData{
						PathInSchema:   []string{"leaf"},
						DataPageOffset: 0,
						NumValues:      1,
						Type:           parquet.Type_INT64,
						Codec:          parquet.CompressionCodec_UNCOMPRESSED,
					},
					FilePath: &filePath,
				}}},
			},
		},
		SchemaHandler: newSchemaHandlerWithPath("leaf"),
		PathStr:       common.PathToStr([]string{"root", "leaf"}),
		RowGroupIndex: 0,
	}

	err := cbt.NextRowGroup()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock open error")
	// PFile must still point at the original reader, not a nil interface.
	require.NotNil(t, cbt.PFile)
	require.Same(t, mockFile, cbt.PFile)
	require.NotPanics(t, func() { _ = cbt.PFile.Close() })
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

// skipCountRecord is a single-column row used to build real multi-page fixtures.
type skipCountRecord struct {
	V int64 `parquet:"name=v, type=INT64"`
}

type dictionaryRecord struct {
	V string `parquet:"name=v, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// newDictionaryOnlyChunkBuffer builds a chunk containing a valid dictionary page
// followed immediately by EOF, with metadata that still declares data values.
func newDictionaryOnlyChunkBuffer(t *testing.T) *ColumnBufferType {
	t.Helper()

	ctx := context.Background()
	fw := buffer.NewBufferWriter()
	pw, err := writer.NewParquetWriterWithContext(ctx, fw, new(dictionaryRecord))
	require.NoError(t, err)
	for _, value := range []string{"a", "b", "a"} {
		require.NoError(t, pw.WriteWithContext(ctx, dictionaryRecord{V: value}))
	}
	require.NoError(t, pw.WriteStopWithContext(ctx))

	src, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(fw.Bytes()), new(dictionaryRecord))
	require.NoError(t, err)
	md := src.Footer.RowGroups[0].Columns[0].MetaData
	require.NotNil(t, md.DictionaryPageOffset)

	dictionaryBytes := fw.Bytes()[*md.DictionaryPageOffset:md.DataPageOffset]
	dictionaryOffset := int64(0)
	footer := &parquet.FileMetaData{
		NumRows: 3,
		Schema:  src.Footer.Schema,
		RowGroups: []*parquet.RowGroup{{
			NumRows: 3,
			Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				Type:                 md.Type,
				Encodings:            md.Encodings,
				PathInSchema:         md.PathInSchema,
				Codec:                md.Codec,
				NumValues:            md.NumValues,
				TotalCompressedSize:  int64(len(dictionaryBytes)),
				DataPageOffset:       int64(len(dictionaryBytes)),
				DictionaryPageOffset: &dictionaryOffset,
			}}},
		}},
	}
	cb, err := NewColumnBuffer(buffer.NewBufferReaderFromBytes(dictionaryBytes), footer, src.SchemaHandler, src.SchemaHandler.ValueColumns[0], nil)
	require.NoError(t, err)
	return cb
}

func TestReadRows_DictionaryOnlyChunkIsTruncated(t *testing.T) {
	table, n, err := newDictionaryOnlyChunkBuffer(t).ReadRows(3)

	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(0), n)
	require.Empty(t, table.Values)
}

// buildThreeRowPage writes a single uncompressed data page holding rows 0,1,2 and
// returns just that page's bytes plus the source reader (for its schema/metadata), so
// tests can splice the page into synthetic footers.
func buildThreeRowPage(t *testing.T) ([]byte, *parquet.ColumnMetaData, *ParquetReader) {
	t.Helper()
	fw := buffer.NewBufferWriter()
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(skipCountRecord),
		writer.WithPageSize(1<<20), writer.WithRowGroupSize(1<<30),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED))
	require.NoError(t, err)
	for i := range int64(3) {
		require.NoError(t, pw.WriteWithContext(context.Background(), skipCountRecord{V: i}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	src, err := NewParquetReader(buffer.NewBufferReaderFromBytes(fw.Bytes()), new(skipCountRecord))
	require.NoError(t, err)
	md := src.Footer.RowGroups[0].Columns[0].MetaData
	require.Nil(t, md.DictionaryPageOffset, "fixture assumes a single data page with no dictionary")
	return fw.Bytes()[md.DataPageOffset : md.DataPageOffset+md.TotalCompressedSize], md, src
}

func chunkFor(md *parquet.ColumnMetaData, offset, numValues, totalCompressedSize int64) *parquet.ColumnChunk {
	return &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
		PathInSchema:        md.PathInSchema,
		Type:                md.Type,
		Codec:               md.Codec,
		Encodings:           md.Encodings,
		DataPageOffset:      offset,
		NumValues:           numValues,
		TotalCompressedSize: totalCompressedSize,
	}}
}

// newTruncatedChunkBuffer builds a column buffer over a chunk that declares 5 values
// but is backed by a single real 3-row data page followed immediately by EOF.
func newTruncatedChunkBuffer(t *testing.T) *ColumnBufferType {
	t.Helper()
	pageBytes, md, src := buildThreeRowPage(t)
	footer := &parquet.FileMetaData{
		NumRows: 5,
		Schema:  src.Footer.Schema,
		RowGroups: []*parquet.RowGroup{
			{NumRows: 5, Columns: []*parquet.ColumnChunk{chunkFor(md, 0, 5, int64(len(pageBytes)))}},
		},
	}
	cb, err := NewColumnBuffer(buffer.NewBufferReaderFromBytes(pageBytes), footer, src.SchemaHandler, src.SchemaHandler.ValueColumns[0], nil)
	require.NoError(t, err)
	return cb
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

// TestNextRowGroup_NilColumnMetaData guards the nil-pointer panic fuzzing found:
// a footer row group containing a column chunk with no MetaData must be skipped
// rather than dereferenced.
func TestNextRowGroup_NilColumnMetaData(t *testing.T) {
	sh := schema.NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
		{Name: "parquet_go_root", NumChildren: common.ToPtr(int32(1)), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
		{Name: "col", Type: common.ToPtr(parquet.Type_INT32), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
	})
	footer := &parquet.FileMetaData{
		NumRows: 1,
		RowGroups: []*parquet.RowGroup{
			{NumRows: 1, Columns: []*parquet.ColumnChunk{{MetaData: nil}}},
		},
	}
	pf := buffer.NewBufferReaderFromBytesNoAlloc([]byte("PAR1\x00\x00\x00\x00PAR1"))

	require.NotPanics(t, func() {
		// Unmatched path: the nil-metadata chunk is skipped, yielding a
		// "column not found" error rather than a panic.
		_, err := NewColumnBuffer(pf, footer, sh, "Parquet_go_root\x01Missing", nil)
		require.Error(t, err)
	})
}
