package reader

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
)

// Mock ParquetFileReader for testing NewColumnBuffer
type mockColumnBufferFileReader struct {
	data       []byte
	offset     int64
	closed     bool
	shouldFail bool
	cloneFails bool
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
	if m.shouldFail {
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

func Test_NewColumnBuffer(t *testing.T) {
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
			pathStr:     "test.field",
			expectError: true,
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
			pathStr:     "test.field",
			expectError: true, // Will fail when NextRowGroup tries to access footer
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
			pathStr:     "test.field",
			expectError: true, // Will fail when NextRowGroup tries to access schema handler
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
			pathStr:     "root.test_field",
			expectError: false,
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, "root.test_field", cb.PathStr)
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
			pathStr:     "root.target_field",
			expectError: false,
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, "root.target_field", cb.PathStr)
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
			pathStr:     "root.dict_field",
			expectError: false,
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, "root.dict_field", cb.PathStr)
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

			result, err := NewColumnBuffer(pFile, footer, schemaHandler, tt.pathStr)

			if tt.expectError {
				require.Error(t, err)
				if tt.expectedError != "" {
					require.Equal(t, tt.expectedError, err.Error())
				}
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

func Test_NewColumnBuffer_EdgeCases(t *testing.T) {
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

		result, err := NewColumnBuffer(mockFile, footer, schemaHandler, "root.nested.deep.field")
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, "root.nested.deep.field", result.PathStr)
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

		result, err := NewColumnBuffer(mockFile, footer, schemaHandler, "root.external_field")
		require.NoError(t, err)
		require.NotNil(t, result)
		// When FilePath is specified, the function should handle opening the external file
		require.NotNil(t, result.ChunkHeader.FilePath)
		require.Equal(t, filePath, *result.ChunkHeader.FilePath)
	})
}
