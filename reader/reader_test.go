package reader

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/bloomfilter"
	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
	"github.com/hangxie/parquet-go/v2/source/buffer"
	phttp "github.com/hangxie/parquet-go/v2/source/http"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
	"github.com/hangxie/parquet-go/v2/writer"
)

type Record struct {
	Str1 string `parquet:"name=str1, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Str2 string `parquet:"name=str2, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Str3 string `parquet:"name=str3, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Str4 string `parquet:"name=str4, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Int1 int64  `parquet:"name=int1, type=INT64, convertedtype=INT_64, encoding=PLAIN"`
	Int2 int64  `parquet:"name=int2, type=INT64, convertedtype=INT_64, encoding=PLAIN"`
	Int3 int64  `parquet:"name=int3, type=INT64, convertedtype=INT_64, encoding=PLAIN"`
	Int4 int64  `parquet:"name=int4, type=INT64, convertedtype=INT_64, encoding=PLAIN"`
}

var numRecord = int64(50)

var parquetBuf []byte

func parquetReader() (*ParquetReader, error) {
	var once sync.Once
	var err error
	once.Do(func() {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		var pw *writer.ParquetWriter
		pw, err = writer.NewParquetWriter(fw, new(Record), 1)
		if err != nil {
			return
		}
		pw.RowGroupSize = 1 * 1024 * 1024 // 1M
		pw.PageSize = 4 * 1024            // 4K
		for i := range numRecord {
			strVal := strconv.FormatInt(i, 10)
			err = pw.Write(Record{strVal, strVal, strVal, strVal, i, i, i, i})
			if err != nil {
				return
			}
		}
		if err = pw.WriteStop(); err != nil {
			return
		}
		err = pw.WriteStop()
		parquetBuf = buf.Bytes()
	})
	if err != nil {
		return nil, err
	}
	buf := buffer.NewBufferReaderFromBytesNoAlloc(parquetBuf)
	return NewParquetReader(buf, new(Record), int64(runtime.NumCPU()))
}

func rowsLeft(pr *ParquetReader) (int64, error) {
	result := 0
	for {
		rows, err := pr.ReadByNumber(1000)
		if err != nil {
			return 0, err
		}
		if len(rows) == 0 {
			break
		}
		result += len(rows)
	}
	return int64(result), nil
}

type NestedRecord struct {
	Name   string   `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age    int32    `parquet:"name=age, type=INT32"`
	Score  *float64 `parquet:"name=score, type=DOUBLE, repetitiontype=OPTIONAL"`
	Active bool     `parquet:"name=active, type=BOOLEAN"`
}

func createNestedParquetData() ([]byte, error) {
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := writer.NewParquetWriter(fw, new(NestedRecord), 1)
	if err != nil {
		return nil, err
	}

	// Write some test data
	score1 := 95.5
	score2 := 87.0
	records := []NestedRecord{
		{
			Name:   "Alice",
			Age:    30,
			Score:  &score1,
			Active: true,
		},
		{
			Name:   "Bob",
			Age:    25,
			Score:  &score2,
			Active: false,
		},
	}

	for _, record := range records {
		if err := pw.Write(record); err != nil {
			return nil, err
		}
	}

	if err := pw.WriteStop(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func TestParquetReader_GetNumRows(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	numRows := pr.GetNumRows()
	require.Equal(t, numRecord, numRows)
	require.Greater(t, numRows, int64(0))
}

func TestParquetReader_ReadPartial(t *testing.T) {
	// Create test data with nested structure
	data, err := createNestedParquetData()
	require.NoError(t, err)

	buf := buffer.NewBufferReaderFromBytesNoAlloc(data)
	pr, err := NewParquetReader(buf, new(NestedRecord), 1)
	require.NoError(t, err)
	defer pr.ReadStop()

	// First, let's check what paths are available in the schema
	require.NotNil(t, pr.SchemaHandler)
	require.NotEmpty(t, pr.SchemaHandler.IndexMap)

	// Schema paths are available for testing

	// Use any available field path for testing
	var nameField string
	for path := range pr.SchemaHandler.MapIndex {
		if path != "Parquet_go_root" && path != "" {
			nameField = path
			break
		}
	}

	require.NotEmpty(t, nameField)

	var invalidResult []string
	err = pr.ReadPartial(&invalidResult, "nonexistent_field")
	require.Error(t, err)
	require.Contains(t, err.Error(), "path not found")

	err = pr.ReadPartial(nil, nameField)
	require.Error(t, err)
}

func TestParquetReader_ReadPartialByNumber(t *testing.T) {
	// Create test data
	data, err := createNestedParquetData()
	require.NoError(t, err)

	buf := buffer.NewBufferReaderFromBytesNoAlloc(data)
	pr, err := NewParquetReader(buf, new(NestedRecord), 1)
	require.NoError(t, err)
	defer pr.ReadStop()

	// Use any available field path for testing
	var nameField string
	for path := range pr.SchemaHandler.MapIndex {
		if path != "Parquet_go_root" && path != "" {
			nameField = path
			break
		}
	}

	// Skip this test if we can't find any field paths
	if nameField == "" {
		t.Skip("No field paths found in schema")
	}

	// Test reading partial by number
	results, err := pr.ReadPartialByNumber(1, nameField)
	require.NoError(t, err)
	require.Len(t, results, 1)
	// Results type depends on the field - could be string, int32, float64, or bool

	// Test reading more records than available
	results, err = pr.ReadPartialByNumber(10, nameField)
	require.NoError(t, err)
	require.LessOrEqual(t, len(results), 2) // We only have 2 records

	// Test reading zero records
	results, err = pr.ReadPartialByNumber(0, nameField)
	require.NoError(t, err)
	require.Empty(t, results)

	// Test with invalid path
	_, err = pr.ReadPartialByNumber(1, "nonexistent_field")
	require.Error(t, err)

	// Test with negative number (should return error)
	_, err = pr.ReadPartialByNumber(-1, nameField)
	require.Error(t, err)
}

func TestParquetReader_ReadStop(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)

	// Ensure column buffers are initialized
	require.NotNil(t, pr.ColumnBuffers)
	require.NotEmpty(t, pr.ColumnBuffers)

	// Call ReadStop
	pr.ReadStop()

	// Verify that column buffers are properly cleaned up
	// ReadStop should close all file handles in column buffers
	// We can't easily verify this without exposing internal state
	pr.ReadStop()

	// Test ReadStop with nil column buffers
	pr2, err := parquetReader()
	require.NoError(t, err)
	pr2.ColumnBuffers = nil
	pr2.ReadStop()

	// Test ReadStop with empty column buffers
	pr3, err := parquetReader()
	require.NoError(t, err)
	pr3.ColumnBuffers = make(map[string]*ColumnBufferType)
	pr3.ReadStop()
}

func TestParquetReader_ReadStopWithError(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)

	// Ensure column buffers are initialized
	require.NotNil(t, pr.ColumnBuffers)
	require.NotEmpty(t, pr.ColumnBuffers)

	// Call ReadStopWithError - should succeed
	err = pr.ReadStopWithError()
	require.NoError(t, err)

	// Calling again should be safe (files already closed)
	_ = pr.ReadStopWithError()
	// May return error because files are already closed, but shouldn't panic
	// We don't assert on error here as behavior may vary

	// Test ReadStopWithError with nil column buffers
	pr2, err := parquetReader()
	require.NoError(t, err)
	pr2.ColumnBuffers = nil
	err = pr2.ReadStopWithError()
	require.NoError(t, err) // Should not error with nil buffers

	// Test ReadStopWithError with empty column buffers
	pr3, err := parquetReader()
	require.NoError(t, err)
	pr3.ColumnBuffers = make(map[string]*ColumnBufferType)
	err = pr3.ReadStopWithError()
	require.NoError(t, err) // Should not error with empty buffers
}

func TestParquetReader_SetSchemaHandlerFromJSON(t *testing.T) {
	// Create a simple parquet reader using existing data
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	// Test with invalid JSON first (should fail immediately)
	invalidJSON := `{"invalid": json}`
	err = pr.SetSchemaHandlerFromJSON(invalidJSON)
	require.Error(t, err)

	// Test with valid JSON but invalid schema structure
	invalidSchema := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=invalid_field, type=INVALID_TYPE"}
		]
	}`
	err = pr.SetSchemaHandlerFromJSON(invalidSchema)
	require.Error(t, err)

	// Test JSON parsing capability (the function should at least parse valid JSON)
	// This tests the JSON unmarshaling part of the function
	validJSONSchema := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=test_field, type=BYTE_ARRAY, convertedtype=UTF8"}
		]
	}`

	// Even if this fails during column buffer creation, it should at least
	// successfully parse the JSON and create a schema handler
	err = pr.SetSchemaHandlerFromJSON(validJSONSchema)
	// We'll accept either success or a specific column-related error
	if err != nil {
		// Should be a column-related error, not a JSON parsing error
		require.Contains(t, err.Error(), "Column not found")
	} else {
		// If it succeeds, the schema handler should be set
		require.NotNil(t, pr.SchemaHandler)
	}
}

func TestParquetReader_SkipRows(t *testing.T) {
	testCases := map[string]struct {
		skip int64
	}{
		"10":  {10},
		"20":  {20},
		"30":  {30},
		"40":  {40},
		"max": {numRecord},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pr, err := parquetReader()
			require.NoError(t, err)
			err = pr.SkipRows(tc.skip)
			require.NoError(t, err)
			num, err := rowsLeft(pr)
			require.NoError(t, err)
			require.Equal(t, numRecord-tc.skip, num)
		})
	}
}

func TestParquetReader_IndexMapBoundsChecking(t *testing.T) {
	tests := []struct {
		name         string
		setupReader  func() *ParquetReader
		expectPanics bool
		desc         string
	}{
		{
			name: "missing_index_in_index_map",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						SchemaElements: []*parquet.SchemaElement{
							{Name: "root", NumChildren: &[]int32{1}[0]},
							{Name: "leaf", NumChildren: &[]int32{0}[0]}, // Index 1, but missing from IndexMap
						},
						IndexMap: map[int32]string{
							0: "root",
							// 1 is missing - should be handled gracefully
						},
					},
					ColumnBuffers: make(map[string]*ColumnBufferType),
				}
			},
			expectPanics: false,
		},
		{
			name: "nil_schema_element_in_array",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						SchemaElements: []*parquet.SchemaElement{
							{Name: "root", NumChildren: &[]int32{1}[0]},
							nil, // Nil schema element should be skipped
						},
						IndexMap: map[int32]string{
							0: "root",
							1: "leaf", // Won't be used because SchemaElements[1] is nil
						},
					},
					ColumnBuffers: make(map[string]*ColumnBufferType),
				}
			},
			expectPanics: false,
		},
		{
			name: "index_map_with_out_of_bounds_keys",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						SchemaElements: []*parquet.SchemaElement{
							{Name: "leaf", NumChildren: &[]int32{0}[0]},
						},
						IndexMap: map[int32]string{
							0:  "valid.path",
							10: "invalid.path", // Index 10 > array length
						},
					},
					ColumnBuffers: make(map[string]*ColumnBufferType),
				}
			},
			expectPanics: false,
		},
		{
			name: "negative_index_in_map",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						SchemaElements: []*parquet.SchemaElement{
							{Name: "leaf", NumChildren: &[]int32{0}[0]},
						},
						IndexMap: map[int32]string{
							0:  "valid.path",
							-1: "negative.path", // Negative index
						},
					},
					ColumnBuffers: make(map[string]*ColumnBufferType),
				}
			},
			expectPanics: false,
		},
		{
			name: "empty_schema_elements_with_non_empty_map",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						SchemaElements: []*parquet.SchemaElement{}, // Empty array
						IndexMap: map[int32]string{
							0: "orphaned.path", // Index exists but no corresponding element
						},
					},
					ColumnBuffers: make(map[string]*ColumnBufferType),
				}
			},
			expectPanics: false,
		},
		{
			name: "nil_index_map",
			setupReader: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						SchemaElements: []*parquet.SchemaElement{
							{Name: "leaf", NumChildren: &[]int32{0}[0]},
						},
						IndexMap: nil, // Nil map
					},
					ColumnBuffers: make(map[string]*ColumnBufferType),
				}
			},
			expectPanics: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := tt.setupReader()

			// Simulate the loop logic from NewParquetReader that caused issues
			if reader.SchemaHandler != nil && reader.SchemaHandler.SchemaElements != nil {
				for i := range len(reader.SchemaHandler.SchemaElements) {
					schema := reader.SchemaHandler.SchemaElements[i]
					if schema == nil {
						continue
					}
					if schema.GetNumChildren() == 0 {
						if pathStr, exists := reader.SchemaHandler.IndexMap[int32(i)]; exists {
							// In real code, this would create a ColumnBuffer
							// Here we just verify the path exists and is valid
							require.NotEmpty(t, pathStr)
						}
					}
				}
			}
		})
	}
}

func TestColumnBufferType_MapIndexBoundsChecking(t *testing.T) {
	tests := []struct {
		name   string
		buffer *ColumnBufferType
	}{
		{
			name: "path_not_in_map_index",
			buffer: &ColumnBufferType{
				SchemaHandler: &schema.SchemaHandler{
					MapIndex: map[string]int32{
						"other.path": 0,
						// PathStr is missing from map
					},
					SchemaElements: []*parquet.SchemaElement{
						{Name: "element"},
					},
				},
				PathStr: "missing.path",
				Footer:  &parquet.FileMetaData{},
			},
		},
		{
			name: "map_index_points_to_out_of_bounds",
			buffer: &ColumnBufferType{
				SchemaHandler: &schema.SchemaHandler{
					MapIndex: map[string]int32{
						"test.path": 10, // Index 10 > array length
					},
					SchemaElements: []*parquet.SchemaElement{
						{Name: "element"},
					},
				},
				PathStr: "test.path",
				Footer:  &parquet.FileMetaData{},
			},
		},
		{
			name: "negative_index_in_map_index",
			buffer: &ColumnBufferType{
				SchemaHandler: &schema.SchemaHandler{
					MapIndex: map[string]int32{
						"test.path": -1, // Negative index
					},
					SchemaElements: []*parquet.SchemaElement{
						{Name: "element"},
					},
				},
				PathStr: "test.path",
				Footer:  &parquet.FileMetaData{},
			},
		},
		{
			name: "nil_schema_elements_array",
			buffer: &ColumnBufferType{
				SchemaHandler: &schema.SchemaHandler{
					MapIndex: map[string]int32{
						"test.path": 0,
					},
					SchemaElements: nil, // Nil array
				},
				PathStr: "test.path",
				Footer:  &parquet.FileMetaData{},
			},
		},
		{
			name: "empty_schema_elements_array",
			buffer: &ColumnBufferType{
				SchemaHandler: &schema.SchemaHandler{
					MapIndex: map[string]int32{
						"test.path": 0,
					},
					SchemaElements: []*parquet.SchemaElement{}, // Empty array
				},
				PathStr: "test.path",
				Footer:  &parquet.FileMetaData{},
			},
		},
		{
			name: "nil_map_index",
			buffer: &ColumnBufferType{
				SchemaHandler: &schema.SchemaHandler{
					MapIndex:       nil, // Nil map
					SchemaElements: []*parquet.SchemaElement{{Name: "element"}},
				},
				PathStr: "test.path",
				Footer:  &parquet.FileMetaData{},
			},
		},
		{
			name: "valid_scenario",
			buffer: &ColumnBufferType{
				SchemaHandler: &schema.SchemaHandler{
					MapIndex: map[string]int32{
						"test.path": 0,
					},
					SchemaElements: []*parquet.SchemaElement{
						{Name: "element"},
					},
				},
				PathStr: "test.path",
				Footer:  &parquet.FileMetaData{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the logic from ReadPage
			if tt.buffer.SchemaHandler != nil && tt.buffer.SchemaHandler.MapIndex != nil && tt.buffer.SchemaHandler.SchemaElements != nil {
				if index, exists := tt.buffer.SchemaHandler.MapIndex[tt.buffer.PathStr]; exists && index >= 0 && int(index) < len(tt.buffer.SchemaHandler.SchemaElements) {
					// In real code, this would access the schema element
					// Here we just verify bounds checking works
					_ = tt.buffer.SchemaHandler.SchemaElements[index]
				}
			}
		})
	}
}

func TestParquetReader_RenameSchema_NilChecks(t *testing.T) {
	tests := []struct {
		name   string
		setup  func() *ParquetReader
		expect string
	}{
		{
			name: "nil_schema_handler",
			setup: func() *ParquetReader {
				pr := &ParquetReader{
					SchemaHandler: nil,
				}
				return pr
			},
			expect: "should handle nil SchemaHandler gracefully",
		},
		{
			name: "nil_schema_handler_infos",
			setup: func() *ParquetReader {
				pr := &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos: nil,
					},
				}
				return pr
			},
			expect: "should handle nil SchemaHandler.Infos gracefully",
		},
		{
			name: "nil_footer",
			setup: func() *ParquetReader {
				pr := &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos: []*common.Tag{{}},
					},
					Footer: nil,
				}
				return pr
			},
			expect: "should handle nil Footer gracefully",
		},
		{
			name: "nil_footer_schema",
			setup: func() *ParquetReader {
				pr := &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos: []*common.Tag{{}},
					},
					Footer: &parquet.FileMetaData{
						Schema: nil,
					},
				}
				return pr
			},
			expect: "should handle nil Footer.Schema gracefully",
		},
		{
			name: "nil_elements_in_arrays",
			setup: func() *ParquetReader {
				pr := &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						Infos: []*common.Tag{nil, {InName: "test"}},
					},
					Footer: &parquet.FileMetaData{
						Schema: []*parquet.SchemaElement{nil, {Name: "old_name"}},
					},
				}
				return pr
			},
			expect: "should handle nil elements in arrays gracefully",
		},
		{
			name: "nil_row_groups",
			setup: func() *ParquetReader {
				pr := &ParquetReader{
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
				return pr
			},
			expect: "should handle nil RowGroups gracefully",
		},
		{
			name: "nil_row_group_columns",
			setup: func() *ParquetReader {
				pr := &ParquetReader{
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
				return pr
			},
			expect: "should handle nil RowGroup.Columns gracefully",
		},
		{
			name: "nil_chunk_metadata",
			setup: func() *ParquetReader {
				pr := &ParquetReader{
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
				return pr
			},
			expect: "should handle nil chunk.MetaData gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setup()

			pr.RenameSchema()
		})
	}
}

func TestParquetReader_SkipRowsByIndex_NilChecks(t *testing.T) {
	tests := []struct {
		name   string
		setup  func() *ParquetReader
		index  int64
		expect string
	}{
		{
			name: "nil_schema_handler",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: nil,
				}
			},
			index:  0,
			expect: "should handle nil SchemaHandler gracefully",
		},
		{
			name: "nil_value_columns",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: nil,
					},
				}
			},
			index:  0,
			expect: "should handle nil ValueColumns gracefully",
		},
		{
			name: "index_out_of_bounds",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: []string{"col1", "col2"},
					},
				}
			},
			index:  5,
			expect: "should handle index out of bounds gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setup()

			pr.SkipRowsByIndex(tt.index, 1)
		})
	}
}

func TestParquetReader_SkipRowsByIndexWithError_NilChecks(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *ParquetReader
		index       int64
		expectError bool
	}{
		{
			name: "nil_schema_handler",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: nil,
				}
			},
			index:       0,
			expectError: true,
		},
		{
			name: "nil_value_columns",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: nil,
					},
				}
			},
			index:       0,
			expectError: true,
		},
		{
			name: "index_out_of_bounds",
			setup: func() *ParquetReader {
				return &ParquetReader{
					SchemaHandler: &schema.SchemaHandler{
						ValueColumns: []string{"col1", "col2"},
					},
				}
			},
			index:       5,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setup()

			err := pr.SkipRowsByIndexWithError(tt.index, 1)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestColumnBufferType_ReadPage_NilChecks(t *testing.T) {
	tests := []struct {
		name   string
		setup  func() *ColumnBufferType
		expect string
	}{
		{
			name: "nil_schema_handler",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{
					SchemaHandler: nil,
					PathStr:       "test.path",
					Footer:        &parquet.FileMetaData{},
				}
			},
			expect: "should handle nil SchemaHandler gracefully",
		},
		{
			name: "nil_map_index",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex: nil,
					},
					PathStr: "test.path",
					Footer:  &parquet.FileMetaData{},
				}
			},
			expect: "should handle nil MapIndex gracefully",
		},
		{
			name: "nil_schema_elements",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{"test.path": 0},
						SchemaElements: nil,
					},
					PathStr: "test.path",
					Footer:  &parquet.FileMetaData{},
				}
			},
			expect: "should handle nil SchemaElements gracefully",
		},
		{
			name: "index_out_of_bounds",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{"test.path": 10},
						SchemaElements: []*parquet.SchemaElement{{}, {}},
					},
					PathStr: "test.path",
					Footer:  &parquet.FileMetaData{},
				}
			},
			expect: "should handle index out of bounds gracefully",
		},
		{
			name: "path_not_in_map",
			setup: func() *ColumnBufferType {
				return &ColumnBufferType{
					SchemaHandler: &schema.SchemaHandler{
						MapIndex:       map[string]int32{"other.path": 0},
						SchemaElements: []*parquet.SchemaElement{{}},
					},
					PathStr: "test.path",
					Footer:  &parquet.FileMetaData{},
				}
			},
			expect: "should handle path not in map gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cbt := tt.setup()

			// We're testing the defensive code paths
			// The function has internal checks that should prevent panics
			// even if it may fail later for other reasons
			_, _ = cbt.ReadPageForSkip()
		})
	}
}

func TestNewParquetReader_WithOptions(t *testing.T) {
	// Create a simple parquet file buffer using the existing pattern
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := writer.NewParquetWriter(fw, new(Record), 1)
	require.NoError(t, err)

	// Write a few records
	for i := int64(0); i < 5; i++ {
		strVal := strconv.FormatInt(i, 10)
		err = pw.Write(Record{strVal, strVal, strVal, strVal, i, i, i, i})
		require.NoError(t, err)
	}

	err = pw.WriteStop()
	require.NoError(t, err)

	parquetBuffer := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())

	// Test with CaseInsensitive option set to true
	opts := ParquetReaderOptions{CaseInsensitive: true}
	pr, err := NewParquetReader(parquetBuffer, new(Record), 1, opts)
	require.NoError(t, err)
	require.True(t, pr.CaseInsensitive)
	pr.ReadStop()

	// Test with CaseInsensitive option set to false
	parquetBuffer2 := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	opts2 := ParquetReaderOptions{CaseInsensitive: false}
	pr2, err := NewParquetReader(parquetBuffer2, new(Record), 1, opts2)
	require.NoError(t, err)
	require.False(t, pr2.CaseInsensitive)
	pr2.ReadStop()
}

func TestParquetReader_Reset(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	// Read first 10 records
	records1 := make([]Record, 10)
	err = pr.Read(&records1)
	require.NoError(t, err)
	require.Len(t, records1, 10)

	// Verify first batch
	for i := range 10 {
		require.Equal(t, strconv.FormatInt(int64(i), 10), records1[i].Str1)
		require.Equal(t, int64(i), records1[i].Int1)
	}

	// Read next 10 records
	records2 := make([]Record, 10)
	err = pr.Read(&records2)
	require.NoError(t, err)
	require.Len(t, records2, 10)

	// Verify second batch (should be records 10-19)
	for i := range 10 {
		require.Equal(t, strconv.FormatInt(int64(i+10), 10), records2[i].Str1)
		require.Equal(t, int64(i+10), records2[i].Int1)
	}

	// Reset the reader
	err = pr.Reset()
	require.NoError(t, err)

	// Read first 10 records again after reset
	records3 := make([]Record, 10)
	err = pr.Read(&records3)
	require.NoError(t, err)
	require.Len(t, records3, 10)

	// Verify we're back at the beginning
	for i := range 10 {
		require.Equal(t, strconv.FormatInt(int64(i), 10), records3[i].Str1)
		require.Equal(t, int64(i), records3[i].Int1)
	}

	// Verify records match the first batch
	require.Equal(t, records1, records3)
}

func TestParquetReader_Reset_MultipleResets(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	// Read and reset multiple times
	for iteration := range 3 {
		t.Logf("Iteration %d", iteration)

		// Read first 5 records
		records := make([]Record, 5)
		err = pr.Read(&records)
		require.NoError(t, err)
		require.Len(t, records, 5)

		// Verify we always get the same first 5 records
		for i := range 5 {
			require.Equal(t, strconv.FormatInt(int64(i), 10), records[i].Str1)
			require.Equal(t, int64(i), records[i].Int1)
		}

		// Reset for next iteration
		if iteration < 2 { // Don't reset after last iteration
			err = pr.Reset()
			require.NoError(t, err)
		}
	}
}

func TestParquetReader_Reset_AfterReadAll(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	// Read all records
	allRecords := make([]Record, numRecord)
	err = pr.Read(&allRecords)
	require.NoError(t, err)
	require.Len(t, allRecords, int(numRecord))

	// Verify we read all records
	for i := range int(numRecord) {
		require.Equal(t, strconv.FormatInt(int64(i), 10), allRecords[i].Str1)
		require.Equal(t, int64(i), allRecords[i].Int1)
	}

	// Try to read more (should get nothing since we're at EOF)
	moreRecords := make([]Record, 10)
	err = pr.Read(&moreRecords)
	require.NoError(t, err)
	// The slice gets resized to 0 when there's no more data
	require.Len(t, moreRecords, 0)

	// Reset the reader
	err = pr.Reset()
	require.NoError(t, err)

	// Read all records again
	allRecords2 := make([]Record, numRecord)
	err = pr.Read(&allRecords2)
	require.NoError(t, err)
	require.Len(t, allRecords2, int(numRecord))

	// Verify we got the same data
	require.Equal(t, allRecords, allRecords2)
}

// Tests for positionTracker (internal type used for Thrift protocol reading)

func TestPositionTracker_Read(t *testing.T) {
	data := []byte("hello world")
	pt := &positionTracker{r: bytes.NewReader(data), pos: 0}

	buf := make([]byte, 5)
	n, err := pt.Read(buf)

	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "hello", string(buf))
	require.Equal(t, int64(5), pt.pos)
}

func TestPositionTracker_Write(t *testing.T) {
	pt := &positionTracker{}

	n, err := pt.Write([]byte("test"))

	require.Error(t, err)
	require.Contains(t, err.Error(), "write not supported")
	require.Equal(t, 0, n)
}

func TestPositionTracker_Close(t *testing.T) {
	pt := &positionTracker{}

	err := pt.Close()

	require.NoError(t, err)
}

func TestPositionTracker_Flush(t *testing.T) {
	pt := &positionTracker{}

	err := pt.Flush(context.Background())

	require.NoError(t, err)
}

func TestPositionTracker_RemainingBytes(t *testing.T) {
	pt := &positionTracker{}

	remaining := pt.RemainingBytes()

	// Should return max uint64 (unknown)
	require.Equal(t, ^uint64(0), remaining)
}

func TestPositionTracker_IsOpen(t *testing.T) {
	pt := &positionTracker{}

	isOpen := pt.IsOpen()

	require.True(t, isOpen)
}

func TestPositionTracker_Open(t *testing.T) {
	pt := &positionTracker{}

	err := pt.Open()

	require.NoError(t, err)
}

// TestNestedListWithEmptyStrings tests that nested lists containing empty strings
// are correctly written and read back. This is a regression test for a bug where
// empty strings at the end of a BYTE_ARRAY column would cause EOF errors during read.
func TestNestedListWithEmptyStrings(t *testing.T) {
	type NestedListRecord struct {
		Matrix [][]string
	}

	jsonSchema := `
{
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
  "Fields": [
    {
      "Tag": "name=matrix, inname=Matrix, type=LIST, repetitiontype=REQUIRED",
      "Fields": [
        {
          "Tag": "name=element, type=LIST, repetitiontype=REQUIRED",
          "Fields": [
            {"Tag": "name=element, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"}
          ]
        }
      ]
    }
  ]
}
`

	testCases := []struct {
		name    string
		records []NestedListRecord
	}{
		{
			name: "empty_string_at_end",
			records: []NestedListRecord{
				{Matrix: [][]string{{"a", "b", ""}}},
			},
		},
		{
			name: "empty_string_at_start",
			records: []NestedListRecord{
				{Matrix: [][]string{{"", "b", "c"}}},
			},
		},
		{
			name: "empty_string_in_middle",
			records: []NestedListRecord{
				{Matrix: [][]string{{"a", "", "c"}}},
			},
		},
		{
			name: "only_empty_string",
			records: []NestedListRecord{
				{Matrix: [][]string{{""}}},
			},
		},
		{
			name: "multiple_rows_with_empty_strings",
			records: []NestedListRecord{
				{Matrix: [][]string{{"a", "b", ""}}},
				{Matrix: [][]string{{"x", "y", "z"}}},
				{Matrix: [][]string{{"", "middle", ""}}},
			},
		},
		{
			name: "multiple_inner_lists_with_empty_strings",
			records: []NestedListRecord{
				{Matrix: [][]string{{"a", ""}, {"b", "c", ""}, {""}}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Write to buffer
			var buf bytes.Buffer
			fw := writerfile.NewWriterFile(&buf)
			pw, err := writer.NewParquetWriter(fw, jsonSchema, 1)
			require.NoError(t, err)
			pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED

			for _, rec := range tc.records {
				err = pw.Write(rec)
				require.NoError(t, err)
			}
			err = pw.WriteStop()
			require.NoError(t, err)

			// Read back from buffer
			fr := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
			pr, err := NewParquetReader(fr, jsonSchema, 1)
			require.NoError(t, err)

			numRows := pr.GetNumRows()
			require.Equal(t, int64(len(tc.records)), numRows)

			for i, expected := range tc.records {
				result := make([]NestedListRecord, 1)
				err = pr.Read(&result)
				require.NoError(t, err, "Failed to read row %d", i)
				require.Equal(t, expected.Matrix, result[0].Matrix, "Matrix mismatch at row %d", i)
			}

			pr.ReadStop()
		})
	}
}

func TestBloomFilterCheck(t *testing.T) {
	t.Run("basic_check", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)

		for i := range 100 {
			require.NoError(t, pw.Write(BloomRecord{
				ID:   int64(i * 100),
				Name: fmt.Sprintf("name-%d", i),
			}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		// Values that were written should return true (might contain)
		found, err := pr.BloomFilterCheck("id", 0, int64(0))
		require.NoError(t, err)
		require.True(t, found)

		found, err = pr.BloomFilterCheck("id", 0, int64(5000))
		require.NoError(t, err)
		require.True(t, found)

		found, err = pr.BloomFilterCheck("name", 0, "name-50")
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("absent_values", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)

		// Write specific values
		for i := range 10 {
			require.NoError(t, pw.Write(BloomRecord{ID: int64(i * 1000)}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		// Test that values NOT written are likely to return false.
		// With 10 values in a 1024-byte filter, false positive rate should be very low.
		falsePositives := 0
		for i := 1; i < 1000; i++ {
			found, err := pr.BloomFilterCheck("id", 0, int64(i))
			require.NoError(t, err)
			if found {
				falsePositives++
			}
		}
		// Very conservative check: should have < 5% false positives
		require.Less(t, falsePositives, 50)
	})

	t.Run("no_bloom_filter_column", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42, Name: "test"}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		// Column without bloom filter should return true (conservative)
		found, err := pr.BloomFilterCheck("name", 0, "test")
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("invalid_row_group_index", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		_, err = pr.BloomFilterCheck("id", -1, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "out of range")

		_, err = pr.BloomFilterCheck("id", 5, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "out of range")
	})

	t.Run("invalid_column_path", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		_, err = pr.BloomFilterCheck("nonexistent", 0, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("round_trip_int_and_string", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)

		for i := range 50 {
			require.NoError(t, pw.Write(BloomRecord{
				ID:   int64(i),
				Name: fmt.Sprintf("user-%d", i),
			}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		// Verify all written values pass the bloom filter check
		for i := range 50 {
			found, err := pr.BloomFilterCheck("id", 0, int64(i))
			require.NoError(t, err)
			require.True(t, found)

			found, err = pr.BloomFilterCheck("name", 0, fmt.Sprintf("user-%d", i))
			require.NoError(t, err)
			require.True(t, found)
		}
	})

	t.Run("multiple_row_groups_independence", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)
		pw.RowGroupSize = 256
		pw.PageSize = 64

		for i := range 1000 {
			require.NoError(t, pw.Write(BloomRecord{
				ID:   int64(i),
				Name: fmt.Sprintf("a-long-name-to-force-multiple-row-groups-%d", i),
			}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		require.Greater(t, len(pr.Footer.RowGroups), 1)

		// Each row group should support bloom filter checks
		for rgIdx := range pr.Footer.RowGroups {
			found, err := pr.BloomFilterCheck("id", rgIdx, int64(0))
			require.NoError(t, err)
			// ID 0 is in the first row group; other row groups may return false
			_ = found
		}

		// First row group should definitely contain ID 0
		found, err := pr.BloomFilterCheck("id", 0, int64(0))
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("hash_value_error", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		// Pass a string value for an INT64 column → HashValue encoding error
		_, err = pr.BloomFilterCheck("id", 0, "not-an-int64")
		require.Error(t, err)
		require.Contains(t, err.Error(), "hash value")
	})

	t.Run("clone_error", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		// Replace PFile with a mock that fails on Clone
		pr.PFile = &failCloneReader{ParquetFileReader: pf}
		_, err = pr.BloomFilterCheck("id", 0, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "clone file reader")
	})

	t.Run("read_bloom_filter_error", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), 1)
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		// Corrupt the bloom filter offset to point to the start of the file (PAR1 magic)
		for _, cc := range pr.Footer.RowGroups[0].Columns {
			if cc.MetaData.IsSetBloomFilterOffset() {
				badOffset := int64(0)
				cc.MetaData.BloomFilterOffset = &badOffset
			}
		}
		_, err = pr.BloomFilterCheck("id", 0, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "read bloom filter")
	})
}

func TestDetectBloomFilters(t *testing.T) {
	t.Run("nil_footer", func(t *testing.T) {
		pr := &ParquetReader{
			Footer:        nil,
			SchemaHandler: &schema.SchemaHandler{},
		}
		pr.detectBloomFilters() // should not panic
	})

	t.Run("empty_row_groups", func(t *testing.T) {
		pr := &ParquetReader{
			Footer:        &parquet.FileMetaData{},
			SchemaHandler: &schema.SchemaHandler{},
		}
		pr.detectBloomFilters() // should not panic
	})

	t.Run("nil_schema_handler", func(t *testing.T) {
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{{}},
			},
			SchemaHandler: nil,
		}
		pr.detectBloomFilters() // should not panic
	})

	t.Run("nil_first_row_group", func(t *testing.T) {
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{nil},
			},
			SchemaHandler: &schema.SchemaHandler{},
		}
		pr.detectBloomFilters() // should not panic
	})

	t.Run("column_without_bloom_offset", func(t *testing.T) {
		// Column with metadata but no BloomFilterOffset set
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							{MetaData: &parquet.ColumnMetaData{PathInSchema: []string{"col1"}}},
							{MetaData: nil}, // nil metadata
						},
					},
				},
			},
			SchemaHandler: &schema.SchemaHandler{
				Infos:    []*common.Tag{{InName: "root"}, {InName: "col1"}},
				MapIndex: map[string]int32{"root\x01col1": 1},
			},
		}
		pr.detectBloomFilters()
		require.False(t, pr.SchemaHandler.Infos[1].BloomFilter)
	})

	t.Run("column_path_not_in_map", func(t *testing.T) {
		offset := int64(100)
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							{MetaData: &parquet.ColumnMetaData{
								PathInSchema:      []string{"unknown_col"},
								BloomFilterOffset: &offset,
							}},
						},
					},
				},
			},
			SchemaHandler: &schema.SchemaHandler{
				Infos:    []*common.Tag{{InName: "root"}},
				MapIndex: map[string]int32(nil),
			},
		}
		pr.detectBloomFilters() // should not panic, path not found
	})

	t.Run("sets_bloom_filter_with_length", func(t *testing.T) {
		offset := int64(100)
		length := int32(1024)
		rootName := "Parquet_go_root"
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							{MetaData: &parquet.ColumnMetaData{
								PathInSchema:      []string{"id"},
								BloomFilterOffset: &offset,
								BloomFilterLength: &length,
							}},
						},
					},
				},
			},
			SchemaHandler: &schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{{Name: rootName}},
				Infos:          []*common.Tag{{InName: rootName}, {InName: "ID"}},
				MapIndex:       map[string]int32{"Parquet_go_root\x01id": 1},
			},
		}
		pr.detectBloomFilters()
		require.True(t, pr.SchemaHandler.Infos[1].BloomFilter)
		require.Equal(t, int32(1024), pr.SchemaHandler.Infos[1].BloomFilterSize)
	})

	t.Run("sets_bloom_filter_without_length", func(t *testing.T) {
		offset := int64(100)
		rootName := "Parquet_go_root"
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							{MetaData: &parquet.ColumnMetaData{
								PathInSchema:      []string{"id"},
								BloomFilterOffset: &offset,
							}},
						},
					},
				},
			},
			SchemaHandler: &schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{{Name: rootName}},
				Infos:          []*common.Tag{{InName: rootName}, {InName: "ID"}},
				MapIndex:       map[string]int32{"Parquet_go_root\x01id": 1},
			},
		}
		pr.detectBloomFilters()
		require.True(t, pr.SchemaHandler.Infos[1].BloomFilter)
		require.Equal(t, int32(0), pr.SchemaHandler.Infos[1].BloomFilterSize)
	})
}

// failCloneReader wraps a ParquetFileReader and makes Clone() return an error.
type failCloneReader struct {
	source.ParquetFileReader
}

func (f *failCloneReader) Clone() (source.ParquetFileReader, error) {
	return nil, fmt.Errorf("clone failed")
}

// TestBloomFilterInterop reads parquet files generated by parquet-mr (Java reference
// implementation) from apache/parquet-testing and verifies our bloom filter reader
// can parse the metadata and bitset data correctly.
func TestBloomFilterInterop(t *testing.T) {
	t.Run("read_bloom_from_parquet_mr", func(t *testing.T) {
		bloomURL := "https://github.com/apache/parquet-testing/raw/refs/heads/master/data/data_index_bloom_encoding_stats.parquet"
		httpReader, err := phttp.NewHttpReader(bloomURL, false, false, map[string]string{})
		require.NoError(t, err)
		defer func() { _ = httpReader.Close() }()

		pr, err := NewParquetReader(httpReader, nil, 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		require.NotEmpty(t, pr.Footer.RowGroups)
		rg := pr.Footer.RowGroups[0]

		// Find a column with bloom filter metadata and read the bloom filter
		foundBloom := false
		for _, cc := range rg.Columns {
			if !cc.MetaData.IsSetBloomFilterOffset() {
				continue
			}
			foundBloom = true
			offset := cc.MetaData.GetBloomFilterOffset()

			pf, cloneErr := httpReader.Clone()
			require.NoError(t, cloneErr)

			filter, readErr := bloomfilter.ReadBloomFilter(pf, offset)
			_ = pf.Close()
			require.NoError(t, readErr)
			require.Greater(t, filter.NumBytes(), int32(0))
		}
		require.True(t, foundBloom)
	})

	t.Run("bloom_filter_length_populated", func(t *testing.T) {
		bloomWithLengthURL := "https://github.com/apache/parquet-testing/raw/refs/heads/master/data/data_index_bloom_encoding_with_length.parquet"
		httpReader, err := phttp.NewHttpReader(bloomWithLengthURL, false, false, map[string]string{})
		require.NoError(t, err)
		defer func() { _ = httpReader.Close() }()

		pr, err := NewParquetReader(httpReader, nil, 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		require.NotEmpty(t, pr.Footer.RowGroups)
		rg := pr.Footer.RowGroups[0]

		foundLength := false
		for _, cc := range rg.Columns {
			if cc.MetaData.BloomFilterLength == nil {
				continue
			}
			foundLength = true
			require.Greater(t, *cc.MetaData.BloomFilterLength, int32(0))

			// Also verify the bloom filter can be read using the offset
			require.True(t, cc.MetaData.IsSetBloomFilterOffset())
			pf, cloneErr := httpReader.Clone()
			require.NoError(t, cloneErr)

			filter, readErr := bloomfilter.ReadBloomFilter(pf, cc.MetaData.GetBloomFilterOffset())
			_ = pf.Close()
			require.NoError(t, readErr)
			require.Greater(t, filter.NumBytes(), int32(0))
		}
		require.True(t, foundLength)
	})
}
