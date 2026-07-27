package reader

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/buffer"
)

// Mock ParquetFileReader for testing NewColumnBuffer
type mockColumnBufferFileReader struct {
	data       []byte
	offset     int64
	closed     bool
	shouldFail bool
	cloneFails bool
	openFails  bool
	// openReturnsSelf mimics backends such as HDFS whose Open mutates the
	// receiver in place and returns it, rather than a distinct reader.
	openReturnsSelf bool
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
	if m.openReturnsSelf {
		// Mutate in place and return the same receiver, as HDFS does.
		m.offset = 0
		return m, nil
	}
	return newMockColumnBufferFileReader(m.data), nil
}

// ReopensInPlace declares the source.InPlaceReopener capability when the mock is
// configured to return itself from Open, matching real in-place backends.
func (m *mockColumnBufferFileReader) ReopensInPlace() bool { return m.openReturnsSelf }

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
	newReader.openReturnsSelf = m.openReturnsSelf
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
			numRows:      10,
			expectedRows: 3,
			expectError:  true, // Will error when trying to read pages from mock data, but covers the skip path
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

// When a backend's Open mutates and returns the same receiver (as HDFS does)
// and declares the source.InPlaceReopener capability, NextRowGroup must not
// close the reader it just opened; doing so would hand a closed reader to
// ConvertToThriftReader.
func TestNextRowGroup_MutatingOpenKeepsReaderOpen(t *testing.T) {
	mockFile := newMockColumnBufferFileReader([]byte{})
	mockFile.openReturnsSelf = true

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

	_ = cbt.NextRowGroup()
	// The reader returned by Open is the same object as before; it must remain
	// open and installed on the column buffer.
	require.Same(t, mockFile, cbt.PFile)
	require.False(t, mockFile.closed, "the newly opened reader must not be closed")
}

// readerState is backing state shared between copies of a valueStructReader.
type readerState struct {
	closed bool
	reopen bool
}

// valueStructReader is a value-type ParquetFileReader that holds a pointer to
// shared state and returns itself from Open — a class of reader whose ownership
// cannot be inferred by identity. It opts into the explicit
// source.InPlaceReopener contract via ReopensInPlace so callers know not to
// close it across Open.
type valueStructReader struct {
	state *readerState
}

func (valueStructReader) Read([]byte) (int, error)                        { return 0, io.EOF }
func (valueStructReader) Seek(int64, int) (int64, error)                  { return 0, nil }
func (r valueStructReader) Close() error                                  { r.state.closed = true; return nil }
func (r valueStructReader) Open(string) (source.ParquetFileReader, error) { return r, nil }
func (r valueStructReader) Clone() (source.ParquetFileReader, error)      { return r, nil }
func (r valueStructReader) ReopensInPlace() bool                          { return r.state.reopen }

// A value-struct reader that shares state and returns itself from Open must be
// kept open by NextRowGroup when it declares the InPlaceReopener capability —
// the case reflection-based identity could not handle.
func TestNextRowGroup_InPlaceReopenerKeptOpen(t *testing.T) {
	state := &readerState{reopen: true}
	cbt := newExternalChunkColumnBuffer(valueStructReader{state})

	_ = cbt.NextRowGroup()
	require.False(t, state.closed, "a reader declaring InPlaceReopener must not be closed")
}

// The capability is opt-in: a reader that returns itself from Open but does not
// declare InPlaceReopener is treated as a distinct handle and closed, so backends
// that share state across Open must declare it explicitly.
func TestNextRowGroup_SelfReturningWithoutCapabilityClosed(t *testing.T) {
	state := &readerState{reopen: false}
	cbt := newExternalChunkColumnBuffer(valueStructReader{state})

	_ = cbt.NextRowGroup()
	require.True(t, state.closed, "a self-returning reader without the capability is closed")
}

// newExternalChunkColumnBuffer builds a column buffer positioned to open a
// single external-FilePath column chunk on the next NextRowGroup call.
func newExternalChunkColumnBuffer(pFile source.ParquetFileReader) *ColumnBufferType {
	filePath := "external.parquet"
	return &ColumnBufferType{
		PFile: pFile,
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
				require.Error(t, err)
				require.Contains(t, err.Error(), "move to next row group")
			} else {
				require.NoError(t, err)
			}
		})
	}
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
				require.Error(t, err)
				require.Contains(t, err.Error(), "EOF")
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

func TestReadPage_EOF_FallbackCreatesEmptyTable_HeaderOnly(t *testing.T) {
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

	pFile := newMockColumnBufferFileReader(headerBytes)

	const metaNumValues int64 = 3
	footer := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				PathInSchema:   []string{"leaf"},
				DataPageOffset: 0,
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

	rerr := cb.ReadPage()
	require.Error(t, rerr)

	if rerr == io.EOF {
		require.NotNil(t, cb.DataTable)
		require.Equal(t, metaNumValues, cb.DataTableNumRows)
		require.Len(t, cb.DataTable.Values, int(metaNumValues))
		for i := 0; i < int(metaNumValues); i++ {
			require.Nil(t, cb.DataTable.Values[i])
			require.Equal(t, int32(0), cb.DataTable.DefinitionLevels[i])
			require.Equal(t, int32(0), cb.DataTable.RepetitionLevels[i])
		}
		require.Equal(t, metaNumValues, cb.ChunkReadValues)
	} else {
		require.Contains(t, rerr.Error(), "EOF")
	}
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
	// Should error because NextRowGroup will fail (no more row groups)
	require.Error(t, err)
	require.Contains(t, err.Error(), "move to next row group")
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
	// Should error because NextRowGroup will fail (no more row groups)
	require.Error(t, err)
	require.Contains(t, err.Error(), "EOF")
	require.Nil(t, page)
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
