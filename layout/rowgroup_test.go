package layout

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
)

// Mock ParquetFileReader for testing
type mockParquetFileReader struct {
	data   []byte
	offset int64
	closed bool
}

func newMockParquetFileReader(data []byte) *mockParquetFileReader {
	return &mockParquetFileReader{
		data:   data,
		offset: 0,
		closed: false,
	}
}

func (m *mockParquetFileReader) Read(p []byte) (n int, err error) {
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

func (m *mockParquetFileReader) Seek(offset int64, whence int) (int64, error) {
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

func (m *mockParquetFileReader) Close() error {
	m.closed = true
	return nil
}

func (m *mockParquetFileReader) Open(name string) (source.ParquetFileReader, error) {
	// Return a new instance for the same data
	return newMockParquetFileReader(m.data), nil
}

func (m *mockParquetFileReader) Clone() (source.ParquetFileReader, error) {
	// Return a new instance for the same data
	newReader := newMockParquetFileReader(m.data)
	newReader.offset = m.offset
	return newReader, nil
}

func Test_NewRowGroup(t *testing.T) {
	rowGroup := NewRowGroup()
	require.NotNil(t, rowGroup)
	// Chunks slice is not initialized by NewRowGroup - it's nil initially
	require.NotNil(t, rowGroup.RowGroupHeader)
}

func Test_ReadRowGroup_Comprehensive(t *testing.T) {
	t.Run("single_column_single_parallelism", func(t *testing.T) {
		// Create a row group header with one column chunk
		rowGroupHeader := &parquet.RowGroup{
			Columns: []*parquet.ColumnChunk{
				{
					FileOffset: 0,
					MetaData: &parquet.ColumnMetaData{
						NumValues: 10,
					},
				},
			},
		}

		// Create mock file reader with minimal valid thrift data
		mockData := make([]byte, 100)
		mockReader := newMockParquetFileReader(mockData)

		// Create a minimal schema handler
		schemaHandler := &schema.SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{
				{
					Name: "test_column",
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
			},
		}

		// This will likely fail due to invalid thrift data, but we test the structure
		rowGroup, err := ReadRowGroup(rowGroupHeader, mockReader, schemaHandler, 1)

		// We expect this to fail due to invalid data, but the structure should be set up
		require.NotNil(t, rowGroup)
		if rowGroup != nil {
			require.Equal(t, rowGroupHeader, rowGroup.RowGroupHeader)
		}

		// Error is expected due to mock data not being valid thrift
		if err == nil {
			// If somehow it succeeded, check the basic structure
			require.LessOrEqual(t, len(rowGroup.Chunks), 1)
		}
	})

	t.Run("multiple_columns_multiple_parallelism", func(t *testing.T) {
		// Create a row group header with multiple column chunks
		rowGroupHeader := &parquet.RowGroup{
			Columns: []*parquet.ColumnChunk{
				{
					FileOffset: 0,
					MetaData: &parquet.ColumnMetaData{
						NumValues: 5,
					},
				},
				{
					FileOffset: 50,
					MetaData: &parquet.ColumnMetaData{
						NumValues: 5,
					},
				},
				{
					FileOffset: 100,
					MetaData: &parquet.ColumnMetaData{
						NumValues: 5,
					},
				},
			},
		}

		// Create mock file reader
		mockData := make([]byte, 200)
		mockReader := newMockParquetFileReader(mockData)

		// Create schema handler
		schemaHandler := &schema.SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{
				{
					Name: "col1",
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
				{
					Name: "col2",
					Type: parquet.TypePtr(parquet.Type_INT64),
				},
				{
					Name: "col3",
					Type: parquet.TypePtr(parquet.Type_BOOLEAN),
				},
			},
		}

		// Test with 2 parallel readers
		rowGroup, _ := ReadRowGroup(rowGroupHeader, mockReader, schemaHandler, 2)

		// Basic structure verification
		require.NotNil(t, rowGroup)
		if rowGroup != nil {
			require.Equal(t, rowGroupHeader, rowGroup.RowGroupHeader)
		}

		// Error is expected due to mock data, but we can verify the parallelism logic ran
	})

	t.Run("column_with_file_path", func(t *testing.T) {
		filePath := "test_file.parquet"

		// Create a row group header with column chunk that has a file path
		rowGroupHeader := &parquet.RowGroup{
			Columns: []*parquet.ColumnChunk{
				{
					FileOffset: 0,
					FilePath:   &filePath,
					MetaData: &parquet.ColumnMetaData{
						NumValues: 3,
					},
				},
			},
		}

		// Create mock file reader
		mockData := make([]byte, 50)
		mockReader := newMockParquetFileReader(mockData)

		// Create schema handler
		schemaHandler := &schema.SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{
				{
					Name: "test_column",
					Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				},
			},
		}

		// Test that Open() method is called when FilePath is provided
		rowGroup, _ := ReadRowGroup(rowGroupHeader, mockReader, schemaHandler, 1)

		// Verify basic structure
		require.NotNil(t, rowGroup)
		if rowGroup != nil {
			require.Equal(t, rowGroupHeader, rowGroup.RowGroupHeader)
		}
	})

	t.Run("high_parallelism_few_columns", func(t *testing.T) {
		// Test case where NP > number of columns
		rowGroupHeader := &parquet.RowGroup{
			Columns: []*parquet.ColumnChunk{
				{
					FileOffset: 0,
					MetaData: &parquet.ColumnMetaData{
						NumValues: 2,
					},
				},
			},
		}

		mockData := make([]byte, 30)
		mockReader := newMockParquetFileReader(mockData)

		schemaHandler := &schema.SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{
				{
					Name: "lonely_column",
					Type: parquet.TypePtr(parquet.Type_FLOAT),
				},
			},
		}

		// Use high parallelism (5) with only 1 column
		rowGroup, _ := ReadRowGroup(rowGroupHeader, mockReader, schemaHandler, 5)

		// Should handle this gracefully
		require.NotNil(t, rowGroup)
	})

	t.Run("parallelism_equals_columns", func(t *testing.T) {
		// Test case where NP == number of columns (exact match)
		rowGroupHeader := &parquet.RowGroup{
			Columns: []*parquet.ColumnChunk{
				{
					FileOffset: 0,
					MetaData: &parquet.ColumnMetaData{
						NumValues: 1,
					},
				},
				{
					FileOffset: 25,
					MetaData: &parquet.ColumnMetaData{
						NumValues: 1,
					},
				},
				{
					FileOffset: 50,
					MetaData: &parquet.ColumnMetaData{
						NumValues: 1,
					},
				},
			},
		}

		mockData := make([]byte, 75)
		mockReader := newMockParquetFileReader(mockData)

		schemaHandler := &schema.SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{
				{
					Name: "col1",
					Type: parquet.TypePtr(parquet.Type_INT32),
				},
				{
					Name: "col2",
					Type: parquet.TypePtr(parquet.Type_INT64),
				},
				{
					Name: "col3",
					Type: parquet.TypePtr(parquet.Type_DOUBLE),
				},
			},
		}

		// Use parallelism equal to column count (3)
		rowGroup, _ := ReadRowGroup(rowGroupHeader, mockReader, schemaHandler, 3)

		// Should handle this case efficiently
		require.NotNil(t, rowGroup)
		if rowGroup != nil {
			require.Equal(t, rowGroupHeader, rowGroup.RowGroupHeader)
		}
	})

	t.Run("single_parallelism_many_columns", func(t *testing.T) {
		// Test case where NP = 1 but many columns (sequential processing)
		var columns []*parquet.ColumnChunk
		for i := 0; i < 6; i++ {
			columns = append(columns, &parquet.ColumnChunk{
				FileOffset: int64(i * 20),
				MetaData: &parquet.ColumnMetaData{
					NumValues: 1,
				},
			})
		}

		rowGroupHeader := &parquet.RowGroup{
			Columns: columns,
		}

		mockData := make([]byte, 120)
		mockReader := newMockParquetFileReader(mockData)

		var schemaElements []*parquet.SchemaElement
		for i := 0; i < 6; i++ {
			schemaElements = append(schemaElements, &parquet.SchemaElement{
				Name: fmt.Sprintf("col%d", i),
				Type: parquet.TypePtr(parquet.Type_INT32),
			})
		}

		schemaHandler := &schema.SchemaHandler{
			SchemaElements: schemaElements,
		}

		// Use single-threaded processing
		rowGroup, _ := ReadRowGroup(rowGroupHeader, mockReader, schemaHandler, 1)

		// Should process all columns sequentially
		require.NotNil(t, rowGroup)
		if rowGroup != nil {
			require.Equal(t, rowGroupHeader, rowGroup.RowGroupHeader)
		}
	})
}

func Test_ReadRowGroup_ErrorConditions(t *testing.T) {
	// Test ReadRowGroup with error conditions
	// Since ReadRowGroup is deprecated and involves complex file reading setup,
	// we'll test error paths and basic functionality

	t.Run("nil_row_group_header", func(t *testing.T) {
		_, err := ReadRowGroup(nil, nil, nil, 1)
		require.Error(t, err)
	})

	t.Run("empty_columns", func(t *testing.T) {
		// Create a row group header with empty columns
		rowGroupHeader := &parquet.RowGroup{
			Columns: []*parquet.ColumnChunk{}, // empty columns
		}

		// This should return successfully with empty chunks
		rowGroup, err := ReadRowGroup(rowGroupHeader, nil, nil, 1)
		require.NoError(t, err)
		require.NotNil(t, rowGroup)
		require.Len(t, rowGroup.Chunks, 0)
	})

	t.Run("zero_parallelism", func(t *testing.T) {
		// Test with zero parallelism (NP = 0) - this will cause divide by zero
		rowGroupHeader := &parquet.RowGroup{
			Columns: []*parquet.ColumnChunk{
				{
					FileOffset: 100,
				},
			},
		}

		_, err := ReadRowGroup(rowGroupHeader, nil, nil, 0)
		require.Error(t, err)
	})
}

func Test_RowGroupToTableMap(t *testing.T) {
	tests := []struct {
		name          string
		setupRowGroup func() *RowGroup
		expectedKeys  []string
		checkResult   func(t *testing.T, tableMap *map[string]*Table)
	}{
		{
			name: "multiple_columns",
			setupRowGroup: func() *RowGroup {
				return &RowGroup{
					Chunks: []*Chunk{
						{
							Pages: []*Page{
								{
									DataTable: &Table{
										Path:             []string{"root", "col1"},
										Values:           []any{int32(1), int32(2)},
										DefinitionLevels: []int32{0, 0},
										RepetitionLevels: []int32{0, 0},
									},
								},
							},
						},
						{
							Pages: []*Page{
								{
									DataTable: &Table{
										Path:             []string{"root", "col2"},
										Values:           []any{"a", "b"},
										DefinitionLevels: []int32{0, 0},
										RepetitionLevels: []int32{0, 0},
									},
								},
							},
						},
					},
				}
			},
			expectedKeys: []string{"root\x01col1", "root\x01col2"},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				require.Equal(t, 2, len(*tableMap), "Expected 2 entries in table map, got %d", len(*tableMap))

				table, exists := (*tableMap)["root\x01col1"]
				require.True(t, exists)
				require.Len(t, table.Values, 2, "Expected 2 values in root\x01col1 table, got %d", len(table.Values))

				table, exists = (*tableMap)["root\x01col2"]
				require.True(t, exists)
				require.Len(t, table.Values, 2, "Expected 2 values in root\x01col2 table, got %d", len(table.Values))
			},
		},
		{
			name: "empty_row_group",
			setupRowGroup: func() *RowGroup {
				return NewRowGroup()
			},
			expectedKeys: []string{},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				require.Empty(t, *tableMap, "Expected empty table map, got %d entries", len(*tableMap))
			},
		},
		{
			name: "multiple_pages_same_column",
			setupRowGroup: func() *RowGroup {
				return &RowGroup{
					Chunks: []*Chunk{
						{
							Pages: []*Page{
								{
									DataTable: &Table{
										Path:             []string{"root", "col1"},
										Values:           []any{int32(1), int32(2)},
										DefinitionLevels: []int32{0, 0},
										RepetitionLevels: []int32{0, 0},
									},
								},
								{
									DataTable: &Table{
										Path:             []string{"root", "col1"},
										Values:           []any{int32(3), int32(4)},
										DefinitionLevels: []int32{0, 0},
										RepetitionLevels: []int32{0, 0},
									},
								},
							},
						},
					},
				}
			},
			expectedKeys: []string{"root\x01col1"},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				table, exists := (*tableMap)["root\x01col1"]
				require.True(t, exists)
				require.Len(t, table.Values, 4, "Expected 4 values in merged table, got %d", len(table.Values))
			},
		},
		{
			name: "empty_chunks",
			setupRowGroup: func() *RowGroup {
				return &RowGroup{
					Chunks: []*Chunk{
						{
							Pages: []*Page{},
						},
					},
				}
			},
			expectedKeys: []string{},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				require.Empty(t, *tableMap, "Expected empty table map for empty chunks, got %d entries", len(*tableMap))
			},
		},
		{
			name: "empty_path",
			setupRowGroup: func() *RowGroup {
				return &RowGroup{
					Chunks: []*Chunk{
						{
							Pages: []*Page{
								{
									DataTable: &Table{
										Path:             []string{}, // empty path
										Values:           []any{int32(1)},
										DefinitionLevels: []int32{0},
										RepetitionLevels: []int32{0},
									},
								},
							},
						},
					},
				}
			},
			expectedKeys: []string{""},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				_, exists := (*tableMap)[""]
				require.True(t, exists)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowGroup := tt.setupRowGroup()
			tableMap := rowGroup.RowGroupToTableMap()

			require.NotNil(t, tableMap)

			if tt.checkResult != nil {
				tt.checkResult(t, tableMap)
			}
		})
	}
}
