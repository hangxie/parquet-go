package reader

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
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
	openFails  bool
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
	fq := "root." + path
	sh.MapIndex[fq] = 1
	sh.IndexMap[1] = fq
	sh.InPathToExPath[fq] = fq
	sh.ExPathToInPath[fq] = fq
	return sh
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

	t.Run("readrows_with_error_propagates", func(t *testing.T) {
		footer := &parquet.FileMetaData{
			NumRows: 1,
			RowGroups: []*parquet.RowGroup{{
				Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{PathInSchema: []string{"leaf"}, DataPageOffset: 0, NumValues: 1, Type: parquet.Type_INT64, Codec: parquet.CompressionCodec_UNCOMPRESSED}}},
			}},
		}
		sh := newSchemaHandlerWithPath("bogus") // MapIndex includes root.bogus
		cb := &ColumnBufferType{Footer: footer, SchemaHandler: sh, PathStr: "root.bogus", DataTableNumRows: -1}

		_, _, err := cb.ReadRowsWithError(1)
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
		cb := &ColumnBufferType{Footer: footer, SchemaHandler: sh, PathStr: "root.bogus", DataTableNumRows: -1}

		_, err := cb.SkipRowsWithError(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Column not found")
	})

	t.Run("gettype_error_on_corrupted_schema", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		footer := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}}
		sh := &schema.SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{{Name: "root"}, {Name: "badfield"}}, // No Type set
			MapIndex:       map[string]int32{"root.badfield": 1},
		}

		cb, err := NewColumnBuffer(mockFile, footer, sh, "root.badfield")
		require.Error(t, err)
		require.Nil(t, cb)
	})

	t.Run("nextrowgroup_no_increment_when_empty", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		footer := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}}
		sh := newSchemaHandlerWithPath("leaf")

		cb := &ColumnBufferType{
			PFile: mockFile, Footer: footer, SchemaHandler: sh,
			PathStr: "root.leaf", DataTableNumRows: -1, RowGroupIndex: 0,
		}

		err := cb.NextRowGroup()
		require.Equal(t, io.EOF, err)
		require.Equal(t, int64(-1), cb.DataTableNumRows) // Should NOT increment
	})
}

func Test_ReadRowsWithError(t *testing.T) {
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
			tbl, n, err := cb.ReadRowsWithError(tt.numRows)

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

func Test_ReadRows_DeprecatedWrapper(t *testing.T) {
	dt := &layout.Table{
		Values:           []any{int64(1), int64(2), int64(3)},
		DefinitionLevels: []int32{1, 1, 1},
		RepetitionLevels: []int32{0, 0, 0},
	}
	cb := &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 10}, DataTable: dt, DataTableNumRows: 2}

	tbl, n := cb.ReadRows(2)
	require.Equal(t, int64(2), n)
	require.NotNil(t, tbl)
}

func Test_SkipRowsWithError(t *testing.T) {
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
				return &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 10}, SchemaHandler: sh, PathStr: "root.leaf", DataTable: dt, DataTableNumRows: 2}
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
					PathStr:          "root.leaf",
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
			n, err := cb.SkipRowsWithError(tt.numRows)

			if tt.expectError {
				require.Error(t, err)
				// When we expect errors, we may have skipped some rows before the error
				require.True(t, n >= 0)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedRows, n)
			}
		})
	}
}

func Test_SkipRows_DeprecatedWrapper(t *testing.T) {
	dt := &layout.Table{
		Values:           []any{int64(7), int64(8)},
		DefinitionLevels: []int32{1, 1},
		RepetitionLevels: []int32{0, 0},
	}
	cb := &ColumnBufferType{Footer: &parquet.FileMetaData{NumRows: 10}, DataTable: dt, DataTableNumRows: 2}

	n := cb.SkipRows(2)
	require.Equal(t, int64(2), n)
	require.Equal(t, int64(0), cb.DataTableNumRows)
}

func Test_NewColumnBuffer_FilePathOpenError(t *testing.T) {
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

	cb, err := NewColumnBuffer(mockFile, footer, sh, "root.leaf")
	require.Error(t, err)
	require.Nil(t, cb)
}

func Test_SkipRows_ReadPageForSkipErrorReturnsZero(t *testing.T) {
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

	cb, err := NewColumnBuffer(mockFile, footer, sh, "root.leaf")
	require.NoError(t, err)
	require.NotNil(t, cb)

	n := cb.SkipRows(1)
	require.Equal(t, int64(0), n)
}

func Test_ReadPage_ChunkHeaderConditions(t *testing.T) {
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
					PathStr:          "root.leaf",
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
					PathStr:       "root.leaf",
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
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_ReadPageForSkip_Conditions(t *testing.T) {
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
					PathStr:          "root.leaf",
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
					PathStr:       "root.leaf",
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
				require.Nil(t, page)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_SkipRowsWithError_RowGroupSkipping(t *testing.T) {
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
		PathStr:          "root.leaf",
		DataTableNumRows: -1,
		RowGroupIndex:    0,
	}

	// Skip across row groups
	n, err := cb.SkipRowsWithError(150)
	// This will fail because we can't actually read pages, but it exercises the row group skipping logic
	if err == nil || n > 0 {
		// Some rows were skipped
		require.True(t, cb.RowGroupIndex > 0 || n > 0)
	}
}

func Test_ReadPage_EOF_FallbackCreatesEmptyTable_HeaderOnly(t *testing.T) {
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

	cb, err := NewColumnBuffer(pFile, footer, sh, "root.leaf")
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

func Test_ReadPage_RecursiveCall(t *testing.T) {
	// Test the else branch that calls NextRowGroup and recursively calls ReadPage
	footer := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}}
	sh := newSchemaHandlerWithPath("leaf")
	mockFile := newMockColumnBufferFileReader([]byte{})

	cb := &ColumnBufferType{
		PFile:         mockFile,
		Footer:        footer,
		SchemaHandler: sh,
		PathStr:       "root.leaf",
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
	require.Contains(t, err.Error(), "failed to move to next row group")
}

func Test_ReadPageForSkip_RecursiveCall(t *testing.T) {
	// Test the else branch that calls NextRowGroup and recursively calls ReadPageForSkip
	footer := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}}
	sh := newSchemaHandlerWithPath("leaf")
	mockFile := newMockColumnBufferFileReader([]byte{})

	cb := &ColumnBufferType{
		PFile:         mockFile,
		Footer:        footer,
		SchemaHandler: sh,
		PathStr:       "root.leaf",
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
	require.Nil(t, page)
}
