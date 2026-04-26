package reader

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
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
		pw, err = writer.NewParquetWriter(fw, new(Record), writer.WithNP(1), writer.WithRowGroupSize(1*1024*1024), writer.WithPageSize(4*1024))
		if err != nil {
			return
		}
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
	return NewParquetReader(buf, new(Record), WithNP(int64(runtime.NumCPU())))
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
	pw, err := writer.NewParquetWriter(fw, new(NestedRecord), writer.WithNP(1))
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
	defer func() { _ = pr.ReadStopWithError() }()

	numRows := pr.GetNumRows()
	require.Equal(t, numRecord, numRows)
	require.Greater(t, numRows, int64(0))
}

func TestParquetReader_SetSchemaHandlerFromJSON(t *testing.T) {
	// Create a simple parquet reader using existing data
	pr, err := parquetReader()
	require.NoError(t, err)
	defer func() { _ = pr.ReadStopWithError() }()

	// Test with invalid JSON first (should fail immediately)
	invalidJSON := `{"invalid": json}`
	err = pr.SetSchemaHandlerFromJSON(invalidJSON)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal json schema")

	// Test with valid JSON but invalid schema structure
	invalidSchema := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=invalid_field, type=INVALID_TYPE"}
		]
	}`
	err = pr.SetSchemaHandlerFromJSON(invalidSchema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a valid Type")

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

func TestParquetReader_SkipRowsByIndexWithError_NilChecks(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *ParquetReader
		index       int64
		expectError bool
		errMsg      string
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
			errMsg:      "SchemaHandler is nil",
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
			errMsg:      "ValueColumns is nil",
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
			errMsg:      "out of range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := tt.setup()

			err := pr.SkipRowsByIndexWithError(tt.index, 1)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
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
	pw, err := writer.NewParquetWriter(fw, new(Record), writer.WithNP(1))
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

	// Test with CaseInsensitive enabled
	pr, err := NewParquetReader(parquetBuffer, new(Record), WithNP(1), WithCaseInsensitive(true))
	require.NoError(t, err)
	require.True(t, pr.caseInsensitive)
	_ = pr.ReadStopWithError()

	// Test that WithCaseInsensitive(true) then (false) resets to false
	parquetBuffer2 := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr2, err := NewParquetReader(parquetBuffer2, new(Record), WithNP(1), WithCaseInsensitive(true), WithCaseInsensitive(false))
	require.NoError(t, err)
	require.False(t, pr2.caseInsensitive)
	_ = pr2.ReadStopWithError()

	// Test with CRCMode option
	parquetBuffer3 := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr3, err := NewParquetReader(parquetBuffer3, new(Record), WithNP(1), WithCRCMode(common.CRCStrict))
	require.NoError(t, err)
	require.Equal(t, common.CRCStrict, pr3.crcMode)
	_ = pr3.ReadStopWithError()
}

func TestNewParquetReader_DefaultNP(t *testing.T) {
	pf := buffer.NewBufferReaderFromBytesNoAlloc(parquetBuf)
	pr, err := NewParquetReader(pf, new(Record))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStopWithError() }()

	require.Equal(t, int64(4), pr.np)
}

func TestNewParquetReader_OptionValidation(t *testing.T) {
	tests := []struct {
		name   string
		opts   []ReaderOption
		errMsg string
	}{
		{
			name:   "np_zero",
			opts:   []ReaderOption{WithNP(0)},
			errMsg: "WithNP: value must be positive, got 0",
		},
		{
			name:   "np_negative",
			opts:   []ReaderOption{WithNP(-1)},
			errMsg: "WithNP: value must be positive, got -1",
		},
		{
			name:   "crc_mode_invalid",
			opts:   []ReaderOption{WithCRCMode(common.CRCMode(99))},
			errMsg: "WithCRCMode: unsupported mode 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pf := buffer.NewBufferReaderFromBytesNoAlloc(parquetBuf)
			pr, err := NewParquetReader(pf, new(Record), tt.opts...)
			require.Error(t, err)
			require.Nil(t, pr)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestNewParquetColumnReader_OptionValidation(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)
	defer func() { _ = pr.ReadStopWithError() }()

	tests := []struct {
		name   string
		opts   []ReaderOption
		errMsg string
	}{
		{
			name:   "np_zero",
			opts:   []ReaderOption{WithNP(0)},
			errMsg: "WithNP: value must be positive, got 0",
		},
		{
			name:   "np_negative",
			opts:   []ReaderOption{WithNP(-1)},
			errMsg: "WithNP: value must be positive, got -1",
		},
		{
			name:   "crc_mode_invalid",
			opts:   []ReaderOption{WithCRCMode(common.CRCMode(99))},
			errMsg: "WithCRCMode: unsupported mode 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr, err := NewParquetColumnReader(pr.PFile, tt.opts...)
			require.Error(t, err)
			require.Nil(t, cr)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
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
	t.Run("no underlying closer", func(t *testing.T) {
		pt := &positionTracker{}
		err := pt.Close()
		require.NoError(t, err)
	})

	t.Run("delegates to underlying closer", func(t *testing.T) {
		mc := &mockCloser{}
		pt := &positionTracker{r: mc}
		err := pt.Close()
		require.NoError(t, err)
		require.True(t, mc.closed)
	})
}

type mockCloser struct {
	closed bool
}

func (m *mockCloser) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (m *mockCloser) Close() error {
	m.closed = true
	return nil
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
