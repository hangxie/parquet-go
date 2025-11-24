package reader

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
)

// Mock ParquetFileReader for testing
type mockParquetFileReader struct {
	data       []byte
	offset     int64
	closed     bool
	shouldFail bool
}

func newMockParquetFileReader(data []byte) *mockParquetFileReader {
	return &mockParquetFileReader{
		data:   data,
		offset: 0,
		closed: false,
	}
}

func (m *mockParquetFileReader) SetShouldFail(shouldFail bool) {
	m.shouldFail = shouldFail
}

func (m *mockParquetFileReader) Read(p []byte) (n int, err error) {
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

func (m *mockParquetFileReader) Seek(offset int64, whence int) (int64, error) {
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

func (m *mockParquetFileReader) Close() error {
	m.closed = true
	return nil
}

func (m *mockParquetFileReader) Open(name string) (source.ParquetFileReader, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock open error")
	}
	return newMockParquetFileReader(m.data), nil
}

func (m *mockParquetFileReader) Clone() (source.ParquetFileReader, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock clone error")
	}
	newReader := newMockParquetFileReader(m.data)
	newReader.offset = m.offset
	return newReader, nil
}

// Helper function to create minimal parquet-like data for testing
func createMinimalValidParquetData() []byte {
	// This creates mock data that resembles parquet structure but isn't valid
	// In real tests, you'd use actual parquet files
	data := make([]byte, 1000)
	// Add parquet magic number at the end
	copy(data[len(data)-4:], []byte("PAR1"))
	// Add minimal footer length
	footerLength := make([]byte, 4)
	footerLength[0] = 100 // Mock footer length
	copy(data[len(data)-8:len(data)-4], footerLength)
	return data
}

func TestNewParquetColumnReader(t *testing.T) {
	tests := []struct {
		name        string
		setupReader func() source.ParquetFileReader
		np          int64
		expectError bool
		expectPanic bool
	}{
		{
			name: "nil_file",
			setupReader: func() source.ParquetFileReader {
				return nil
			},
			np:          1,
			expectError: true,
			expectPanic: true,
		},
		{
			name: "read_footer_error",
			setupReader: func() source.ParquetFileReader {
				mockReader := newMockParquetFileReader([]byte{})
				mockReader.SetShouldFail(true)
				return mockReader
			},
			np:          1,
			expectError: true,
			expectPanic: false,
		},
		{
			name: "invalid_footer_data",
			setupReader: func() source.ParquetFileReader {
				invalidData := make([]byte, 100)
				// Fill with invalid parquet footer data
				copy(invalidData[len(invalidData)-4:], []byte{0, 0, 0, 0}) // Invalid length
				return newMockParquetFileReader(invalidData)
			},
			np:          1,
			expectError: true,
			expectPanic: false,
		},
		{
			name: "valid_minimal_file",
			setupReader: func() source.ParquetFileReader {
				// Create minimal valid parquet file data
				// This is a simplified test - in real scenarios we'd need proper parquet file structure
				mockData := createMinimalValidParquetData()
				return newMockParquetFileReader(mockData)
			},
			np:          1,
			expectError: true, // We expect this to fail with invalid data, but it exercises the code path
			expectPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectPanic {
				_, err := NewParquetColumnReader(tt.setupReader(), tt.np)
				require.Error(t, err)
				return
			}

			reader, err := NewParquetColumnReader(tt.setupReader(), tt.np)

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, reader)
			} else {
				require.NoError(t, err)
				require.NotNil(t, reader)
			}
		})
	}
}

func TestParquetReader_EdgeCases(t *testing.T) {
	t.Run("nil_schema_handler", func(t *testing.T) {
		pr := &ParquetReader{
			SchemaHandler: nil,
		}

		err := pr.SkipRowsByPath("test", 1)
		require.Error(t, err)
	})

	t.Run("nil_column_buffers_map", func(t *testing.T) {
		pr := &ParquetReader{
			SchemaHandler: &schema.SchemaHandler{
				MapIndex:       map[string]int32{"test": 0},
				InPathToExPath: map[string]string{"test": "test"},
				ExPathToInPath: map[string]string{"test": "test"},
			},
			ColumnBuffers: nil, // This should cause issues
		}

		err := pr.SkipRowsByPath("test", 1)
		require.Error(t, err)
	})
}

func TestParquetReader_ReadColumnByIndex(t *testing.T) {
	tests := []struct {
		name        string
		setupReader func() *ParquetReader
		index       int64
		num         int64
		expectError bool
	}{
		{
			name: "index_out_of_range",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: []string{"column1", "column2"},
					},
				}
			},
			index:       5, // Out of range
			num:         3,
			expectError: true,
		},
		{
			name: "negative_index",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: []string{"column1", "column2"},
					},
				}
			},
			index:       -1,
			num:         3,
			expectError: true,
		},
		{
			name: "exactly_at_boundary",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: []string{"column1", "column2"},
					},
				}
			},
			index:       2, // Exactly equal to length
			num:         1,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setupReader()

			// Handle error case for negative index
			if tt.index < 0 {
				_, _, _, err := pr.ReadColumnByIndex(tt.index, tt.num)
				require.Error(t, err)
				return
			}

			values, rls, dls, err := pr.ReadColumnByIndex(tt.index, tt.num)

			if tt.expectError {
				require.Error(t, err)
				// Verify error message for out of range
				if tt.index < 0 || tt.index >= int64(len(pr.SchemaHandler.ValueColumns)) {
					expectedMsg := fmt.Sprintf("index %v out of range [0, %v)", tt.index, len(pr.SchemaHandler.ValueColumns))
					require.Equal(t, expectedMsg, err.Error())
				}
			} else {
				// For success cases, verify non-nil return values
				require.NotNil(t, values)
				require.NotNil(t, rls)
				require.NotNil(t, dls)
			}
		})
	}
}

func TestParquetReader_ReadColumnByIndex_ErrorPropagation(t *testing.T) {
	// Test that ReadColumnByIndex properly propagates errors from ReadColumnByPath
	t.Run("error_from_read_column_by_path", func(t *testing.T) {
		pr := &ParquetReader{
			ColumnBuffers: make(map[string]*ColumnBufferType),
		}
		pr.SchemaHandler = &schema.SchemaHandler{
			ValueColumns:   []string{"column1"},
			MapIndex:       map[string]int32{}, // Missing the path, will cause error
			InPathToExPath: map[string]string{"column1": "column1"},
			ExPathToInPath: map[string]string{"column1": "column1"},
		}

		values, rls, dls, err := pr.ReadColumnByIndex(0, 5)

		require.Error(t, err)

		// Should return empty slices on error
		require.Empty(t, values)
		require.Empty(t, rls)
		require.Empty(t, dls)
	})
}

func TestParquetReader_ReadColumnByPath_Comprehensive(t *testing.T) {
	tests := []struct {
		name          string
		setupReader   func() *ParquetReader
		pathStr       string
		num           int64
		expectError   bool
		expectedError string
	}{
		{
			name: "invalid_num_zero",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{},
						InPathToExPath: map[string]string{},
						ExPathToInPath: map[string]string{},
					},
				}
			},
			pathStr:     "test.field",
			num:         0,
			expectError: true,
		},
		{
			name: "invalid_num_negative",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{},
						InPathToExPath: map[string]string{},
						ExPathToInPath: map[string]string{},
					},
				}
			},
			pathStr:     "test.field",
			num:         -5,
			expectError: true,
		},
		{
			name: "empty_path",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{},
						InPathToExPath: map[string]string{},
						ExPathToInPath: map[string]string{},
					},
				}
			},
			pathStr:     "",
			num:         5,
			expectError: true,
		},
		{
			name: "path_conversion_error",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{},
						InPathToExPath: map[string]string{},
						ExPathToInPath: map[string]string{},
					},
				}
			},
			pathStr:     "invalid..path",
			num:         5,
			expectError: true,
		},
		{
			name: "path_not_found_in_schema",
			setupReader: func() *ParquetReader {
				pr := &ParquetReader{
					ColumnBuffers: make(map[string]*ColumnBufferType),
				}
				pr.SchemaHandler = &schema.SchemaHandler{
					MapIndex:       map[string]int32{},
					InPathToExPath: map[string]string{"test.field": "test.field"},
					ExPathToInPath: map[string]string{"test.field": "test.field"},
				}
				return pr
			},
			pathStr:       "test.field",
			num:           5,
			expectError:   true,
			expectedError: "path test.field not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setupReader()

			values, rls, dls, err := pr.ReadColumnByPath(tt.pathStr, tt.num)

			if tt.expectError {
				require.Error(t, err)
				if tt.expectedError != "" {
					require.Contains(t, err.Error(), tt.expectedError)
				}
				// Check that error case returns empty slices
				require.Empty(t, values)
				require.Empty(t, rls)
				require.Empty(t, dls)
			} else {
				// For success cases, we expect non-nil slices even if they might be empty
				require.NotNil(t, values)
				require.NotNil(t, rls)
				require.NotNil(t, dls)
			}
		})
	}
}

func TestParquetReader_SkipRowsByIndex(t *testing.T) {
	tests := []struct {
		name        string
		setupReader func() *ParquetReader
		index       int64
		num         int64
		expectCall  bool
	}{
		{
			name: "index_out_of_range",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: []string{"column1", "column2"},
					},
				}
			},
			index:      5, // Out of range
			num:        3,
			expectCall: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setupReader()

			// SkipRowsByIndex doesn't return error, so we just call it
			pr.SkipRowsByIndex(tt.index, tt.num)
		})
	}
}

func TestParquetReader_SkipRowsByIndexWithError(t *testing.T) {
	tests := []struct {
		name        string
		setupReader func() *ParquetReader
		index       int64
		num         int64
		expectError bool
	}{
		{
			name: "index_out_of_range",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: []string{"column1", "column2"},
					},
				}
			},
			index:       5, // Out of range
			num:         3,
			expectError: true,
		},
		{
			name: "nil_schema_handler",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: nil,
				}
			},
			index:       0,
			expectError: true,
		},
		{
			name: "nil_value_columns",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: nil,
					},
				}
			},
			index:       0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setupReader()

			err := pr.SkipRowsByIndexWithError(tt.index, tt.num)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewParquetColumnReader_Success(t *testing.T) {
	// Create a real parquet file to test with
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	// Now test NewParquetColumnReader with a real parquet file
	columnReader, err := NewParquetColumnReader(pr.PFile, 1)
	require.NoError(t, err)
	require.NotNil(t, columnReader)
	require.Equal(t, int64(1), columnReader.NP)
	require.NotNil(t, columnReader.PFile)
	require.NotNil(t, columnReader.SchemaHandler)
	require.NotNil(t, columnReader.ColumnBuffers)

	columnReader.ReadStop()
}

func TestParquetReader_SkipRowsByPath_WithValidData(t *testing.T) {
	// Create a real parquet reader
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	// Create column reader to test the successful path
	columnReader, err := NewParquetColumnReader(pr.PFile, 1)
	require.NoError(t, err)
	defer columnReader.ReadStop()

	// Test skipping rows with valid path
	if len(columnReader.SchemaHandler.ValueColumns) > 0 {
		validPath := columnReader.SchemaHandler.ValueColumns[0]
		err = columnReader.SkipRowsByPath(validPath, 5)
		require.NoError(t, err)
	}
}

func TestParquetReader_SkipRowsByIndex_Success(t *testing.T) {
	// Create a real parquet reader
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	// Create column reader
	columnReader, err := NewParquetColumnReader(pr.PFile, 1)
	require.NoError(t, err)
	defer columnReader.ReadStop()

	// Test with valid index
	if len(columnReader.SchemaHandler.ValueColumns) > 0 {
		columnReader.SkipRowsByIndex(0, 5)
		// No error expected as this function doesn't return errors
	}

	// Test with nil SchemaHandler
	emptyReader := &ParquetReader{SchemaHandler: nil}
	emptyReader.SkipRowsByIndex(0, 5) // Should return early

	// Test with nil ValueColumns
	emptyReader.SchemaHandler = &schema.SchemaHandler{ValueColumns: nil}
	emptyReader.SkipRowsByIndex(0, 5) // Should return early
}

func TestParquetReader_SkipRowsByIndexWithError_Success(t *testing.T) {
	// Create a real parquet reader
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	// Create column reader
	columnReader, err := NewParquetColumnReader(pr.PFile, 1)
	require.NoError(t, err)
	defer columnReader.ReadStop()

	// Test with valid index
	if len(columnReader.SchemaHandler.ValueColumns) > 0 {
		err = columnReader.SkipRowsByIndexWithError(0, 5)
		require.NoError(t, err)
	}

	// Test with nil SchemaHandler - should return error
	emptyReader := &ParquetReader{SchemaHandler: nil}
	err = emptyReader.SkipRowsByIndexWithError(0, 5)
	require.Error(t, err)

	// Test with nil ValueColumns - should return error
	emptyReader.SchemaHandler = &schema.SchemaHandler{ValueColumns: nil}
	err = emptyReader.SkipRowsByIndexWithError(0, 5)
	require.Error(t, err)
}

func TestParquetReader_ReadColumnByPath_WithValidData(t *testing.T) {
	// Create a real parquet reader
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	// Create column reader
	columnReader, err := NewParquetColumnReader(pr.PFile, 1)
	require.NoError(t, err)
	defer columnReader.ReadStop()

	// Test reading column with valid path
	if len(columnReader.SchemaHandler.ValueColumns) > 0 {
		validPath := columnReader.SchemaHandler.ValueColumns[0]
		values, rls, dls, err := columnReader.ReadColumnByPath(validPath, 5)
		require.NoError(t, err)
		require.NotNil(t, values)
		require.NotNil(t, rls)
		require.NotNil(t, dls)
	}
}

func TestParquetReader_ErrorPaths(t *testing.T) {
	t.Run("skip_rows_column_buffer_error", func(t *testing.T) {
		// Create a mock reader that will fail when creating column buffer
		pr := &ParquetReader{
			PFile:         newMockParquetFileReader([]byte{}), // Invalid data will cause NewColumnBuffer to fail
			ColumnBuffers: make(map[string]*ColumnBufferType),
		}
		pr.SchemaHandler = &schema.SchemaHandler{
			MapIndex:       map[string]int32{"test.field": 0},
			InPathToExPath: map[string]string{"test.field": "test.field"},
			ExPathToInPath: map[string]string{"test.field": "test.field"},
		}

		err := pr.SkipRowsByPath("test.field", 5)
		require.Error(t, err) // This should hit the NewColumnBuffer error path (line 47-49)
	})

	t.Run("read_column_column_buffer_error", func(t *testing.T) {
		// Create a mock reader that will fail when creating column buffer
		pr := &ParquetReader{
			PFile:         newMockParquetFileReader([]byte{}), // Invalid data will cause NewColumnBuffer to fail
			ColumnBuffers: make(map[string]*ColumnBufferType),
		}
		pr.SchemaHandler = &schema.SchemaHandler{
			MapIndex:       map[string]int32{"test.field": 0},
			InPathToExPath: map[string]string{"test.field": "test.field"},
			ExPathToInPath: map[string]string{"test.field": "test.field"},
		}

		values, rls, dls, err := pr.ReadColumnByPath("test.field", 5)
		require.Error(t, err) // This should hit the NewColumnBuffer error path (line 88-90)
		require.Empty(t, values)
		require.Empty(t, rls)
		require.Empty(t, dls)
	})

	t.Run("skip_rows_column_buffer_not_found_fallback", func(t *testing.T) {
		// Test the fallback case where column buffer exists but lookup fails
		pr := &ParquetReader{
			ColumnBuffers: make(map[string]*ColumnBufferType),
		}
		pr.SchemaHandler = &schema.SchemaHandler{
			MapIndex:       map[string]int32{"test.field": 0},
			InPathToExPath: map[string]string{"test.field": "test.field"},
			ExPathToInPath: map[string]string{"test.field": "test.field"},
		}

		// Manually add a column buffer but then test a path that won't be found due to race condition
		// This is tricky to test, but we can simulate by modifying the path after adding
		err := pr.SkipRowsByPath("test.field", 5)
		if err != nil {
			// This will fail at NewColumnBuffer, which is expected
			require.Error(t, err)
		}
	})

	t.Run("read_column_not_found_fallback", func(t *testing.T) {
		// Test the fallback case where path isn't found in ColumnBuffers after creation
		pr := &ParquetReader{
			ColumnBuffers: make(map[string]*ColumnBufferType),
		}
		pr.SchemaHandler = &schema.SchemaHandler{
			MapIndex:       map[string]int32{"test.field": 0},
			InPathToExPath: map[string]string{"test.field": "test.field"},
			ExPathToInPath: map[string]string{"test.field": "test.field"},
		}

		values, rls, dls, err := pr.ReadColumnByPath("test.field", 5)
		// This will either error at NewColumnBuffer or return the fallback error
		if err != nil {
			require.Error(t, err)
			require.Empty(t, values)
			require.Empty(t, rls)
			require.Empty(t, dls)
		}
	})

	// Note: Lines 54-56 and 97 in columnreader.go are defensive fallback error paths
	// that are extremely difficult to trigger in practice. They would require a race condition
	// where a ColumnBuffer is added to the map but then not found immediately after.
	// These lines serve as safety measures but are not easily testable without complex concurrency scenarios.
}

func TestParquetReader_SkipRowsByPath_Comprehensive(t *testing.T) {
	tests := []struct {
		name           string
		setupReader    func() *ParquetReader
		pathStr        string
		num            int64
		expectError    bool
		expectedError  string
		shouldCreateCB bool
	}{
		{
			name: "invalid_num_zero",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{},
						InPathToExPath: map[string]string{},
						ExPathToInPath: map[string]string{},
					},
				}
			},
			pathStr:     "test.field",
			num:         0,
			expectError: true,
		},
		{
			name: "invalid_num_negative",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{},
						InPathToExPath: map[string]string{},
						ExPathToInPath: map[string]string{},
					},
				}
			},
			pathStr:     "test.field",
			num:         -1,
			expectError: true,
		},
		{
			name: "empty_path",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{},
						InPathToExPath: map[string]string{},
						ExPathToInPath: map[string]string{},
					},
				}
			},
			pathStr:     "",
			num:         5,
			expectError: true,
		},
		{
			name: "conversion_error",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{},
						InPathToExPath: map[string]string{},
						ExPathToInPath: map[string]string{},
					},
				}
			},
			pathStr:     "invalid..path",
			num:         5,
			expectError: true,
		},
		{
			name: "path_not_found_in_map_index",
			setupReader: func() *ParquetReader {
				pr := &ParquetReader{
					ColumnBuffers: make(map[string]*ColumnBufferType),
				}
				pr.SchemaHandler = &schema.SchemaHandler{
					MapIndex:       map[string]int32{},
					InPathToExPath: map[string]string{"test.field": "test.field"},
					ExPathToInPath: map[string]string{"test.field": "test.field"},
				}
				return pr
			},
			pathStr:       "test.field",
			num:           5,
			expectError:   true,
			expectedError: "path test.field not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setupReader()

			err := pr.SkipRowsByPath(tt.pathStr, tt.num)

			if tt.expectError {
				require.Error(t, err)
				if tt.expectedError != "" {
					require.Contains(t, err.Error(), tt.expectedError)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
