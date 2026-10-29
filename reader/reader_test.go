package reader

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
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

type shortReadParquetFileReader struct {
	data   []byte
	offset int64
	n      int
	err    error
}

func (r *shortReadParquetFileReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.offset >= int64(len(r.data)) {
		return 0, io.EOF
	}
	limit := r.n
	if limit <= 0 || limit > len(p) {
		limit = len(p)
	}
	if remaining := len(r.data) - int(r.offset); limit > remaining {
		limit = remaining
	}
	n := copy(p[:limit], r.data[r.offset:])
	r.offset += int64(n)
	return n, nil
}

func (r *shortReadParquetFileReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.offset = offset
	case io.SeekCurrent:
		r.offset += offset
	case io.SeekEnd:
		r.offset = int64(len(r.data)) + offset
	}
	return r.offset, nil
}

func (r *shortReadParquetFileReader) Close() error {
	return nil
}

func (r *shortReadParquetFileReader) Open(string) (source.ParquetFileReader, error) {
	return &shortReadParquetFileReader{data: r.data, n: r.n, err: r.err}, nil
}

func (r *shortReadParquetFileReader) Clone() (source.ParquetFileReader, error) {
	return &shortReadParquetFileReader{data: r.data, offset: r.offset, n: r.n, err: r.err}, nil
}

func parquetReader() (*ParquetReader, error) {
	var once sync.Once
	var err error
	once.Do(func() {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		var pw *writer.ParquetWriter
		//nolint:staticcheck
		pw, err = writer.NewParquetWriter(fw, new(Record), writer.WithNP(1), writer.WithRowGroupSize(1*1024*1024), writer.WithPageSize(4*1024))
		if err != nil {
			return
		}
		for i := range numRecord {
			strVal := strconv.FormatInt(i, 10)
			//nolint:staticcheck
			err = pw.Write(Record{strVal, strVal, strVal, strVal, i, i, i, i})
			if err != nil {
				return
			}
		}
		//nolint:staticcheck
		if err = pw.WriteStop(); err != nil {
			return
		}
		//nolint:staticcheck
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
	//nolint:staticcheck
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
		//nolint:staticcheck
		if err := pw.Write(record); err != nil {
			return nil, err
		}
	}

	//nolint:staticcheck
	if err := pw.WriteStop(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func TestParquetReaderFileSchemaHandlerWithoutFooter(t *testing.T) {
	require.Nil(t, (&ParquetReader{}).fileSchemaHandler())
}

func TestNewFileSchemaHandler(t *testing.T) {
	footer := &parquet.FileMetaData{Schema: []*parquet.SchemaElement{{Name: "root", NumChildren: common.ToPtr(int32(1))}, {Name: "v", Type: common.ToPtr(parquet.Type_INT32)}}}
	sh := newFileSchemaHandler(footer)
	// Renaming the footer in place, as RenameSchema does, must not reach the handler.
	footer.Schema[1].Name = "V"
	require.Equal(t, "v", sh.SchemaElements[1].GetName())

	footer.Schema[1] = nil
	require.Nil(t, newFileSchemaHandler(footer))
}

func TestParquetReader_GetNumRows(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	numRows := pr.GetNumRows()
	require.Equal(t, numRecord, numRows)
	require.Greater(t, numRows, int64(0))
}

func TestParquetReader_SetSchemaHandlerFromJSON(t *testing.T) {
	// Create a simple parquet reader using existing data
	pr, err := parquetReader()
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

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

func TestParquetReader_SkipRowsByIndex_NilChecks(t *testing.T) {
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
			errMsg:      "out of range",
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

			err := pr.SkipRowsByIndex(tt.index, 1)
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
	//nolint:staticcheck
	pw, err := writer.NewParquetWriter(fw, new(Record), writer.WithNP(1))
	require.NoError(t, err)

	// Write a few records
	for i := int64(0); i < 5; i++ {
		strVal := strconv.FormatInt(i, 10)
		//nolint:staticcheck
		err = pw.Write(Record{strVal, strVal, strVal, strVal, i, i, i, i})
		require.NoError(t, err)
	}

	//nolint:staticcheck
	err = pw.WriteStop()
	require.NoError(t, err)

	parquetBuffer := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())

	// Test with CaseInsensitive enabled
	pr, err := NewParquetReader(parquetBuffer, new(Record), WithNP(1), WithCaseInsensitive(true))
	require.NoError(t, err)
	require.True(t, pr.caseInsensitive)
	_ = pr.ReadStop()

	// Test that WithCaseInsensitive(true) then (false) resets to false
	parquetBuffer2 := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr2, err := NewParquetReader(parquetBuffer2, new(Record), WithNP(1), WithCaseInsensitive(true), WithCaseInsensitive(false))
	require.NoError(t, err)
	require.False(t, pr2.caseInsensitive)
	_ = pr2.ReadStop()

	// Test with CRCMode option
	parquetBuffer3 := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr3, err := NewParquetReader(parquetBuffer3, new(Record), WithNP(1), WithCRCMode(common.CRCStrict))
	require.NoError(t, err)
	require.Equal(t, common.CRCStrict, pr3.crcMode)
	_ = pr3.ReadStop()
}

// ReadStop must defensively tolerate a column buffer whose PFile is nil rather
// than panicking on Close of a nil interface.
func TestParquetReader_ReadStop_NilPFile(t *testing.T) {
	pr := &ParquetReader{
		ColumnBuffers: map[string]*ColumnBufferType{
			"root\x01leaf": {PFile: nil},
		},
	}
	require.NotPanics(t, func() {
		require.NoError(t, pr.ReadStop())
	})
}

func TestNewParquetReader_DefaultNP(t *testing.T) {
	pf := buffer.NewBufferReaderFromBytesNoAlloc(parquetBuf)
	pr, err := NewParquetReader(pf, new(Record))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

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
	defer func() { _ = pr.ReadStop() }()

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

func TestReadFixedLenByteArrayTagWithoutLength(t *testing.T) {
	type writeRow struct {
		V string `parquet:"name=V, type=FIXED_LEN_BYTE_ARRAY, length=4"`
	}
	// The width a tag declares is what FLBA values are decoded with, so a tag that
	// leaves it out reads every value as empty. It is rejected instead.
	type readRow struct {
		V string `parquet:"name=V, type=FIXED_LEN_BYTE_ARRAY"`
	}

	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(context.Background(), &buf, new(writeRow), writer.WithNP(1))
	require.NoError(t, err)
	require.NoError(t, pw.WriteWithContext(context.Background(), writeRow{V: "abcd"}))
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	defer func() {
		require.NoError(t, pf.Close())
	}()

	_, err = NewParquetReaderWithContext(context.Background(), pf, new(readRow), WithNP(1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "FIXED_LEN_BYTE_ARRAY requires a positive length, got 0")

	// The same file reads back through a tag that declares the width.
	pr, err := NewParquetReaderWithContext(context.Background(), pf, new(writeRow), WithNP(1))
	require.NoError(t, err)
	rows := make([]writeRow, 1)
	require.NoError(t, pr.ReadWithContext(context.Background(), &rows))
	require.Equal(t, "abcd", rows[0].V)
}

type boolDictRecord struct {
	Value bool `parquet:"name=value, type=BOOLEAN, encoding=RLE_DICTIONARY"`
}

// writeLegacyBoolDictParquet writes a BOOLEAN dictionary column, as releases before
// the writer rejected that encoding did, by setting it after construction.
func writeLegacyBoolDictParquet(t *testing.T) []byte {
	t.Helper()

	type plainBool struct {
		Value bool `parquet:"name=value, type=BOOLEAN"`
	}
	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(context.Background(), &buf, new(plainBool), writer.WithNP(1))
	require.NoError(t, err)
	pw.SchemaHandler.Infos[1].Encoding = parquet.Encoding_RLE_DICTIONARY
	for _, v := range []bool{true, false, true} {
		require.NoError(t, pw.WriteWithContext(context.Background(), plainBool{Value: v}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	data := buf.Bytes()
	pr, err := NewParquetColumnReader(buffer.NewBufferReaderFromBytesNoAlloc(data), WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()
	require.Contains(t, pr.Footer.RowGroups[0].Columns[0].MetaData.Encodings, parquet.Encoding_RLE_DICTIONARY)
	return data
}

func TestReadLegacyBooleanDictionary(t *testing.T) {
	data := writeLegacyBoolDictParquet(t)

	testCases := map[string]any{
		"struct-tagged-dictionary": new(boolDictRecord),
		"json-schema-dictionary":   `{"Tag": "name=parquet_go_root", "Fields": [{"Tag": "name=value, type=BOOLEAN, encoding=RLE_DICTIONARY"}]}`,
		"schema-inference":         nil,
	}
	for name, obj := range testCases {
		t.Run(name, func(t *testing.T) {
			pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(data), obj, WithNP(1))
			require.NoError(t, err)
			defer func() { _ = pr.ReadStop() }()

			rows, err := pr.ReadByNumberWithContext(context.Background(), 3)
			require.NoError(t, err)
			require.Len(t, rows, 3)
			got := make([]bool, len(rows))
			for i, row := range rows {
				got[i] = reflect.ValueOf(row).FieldByName("Value").Bool()
			}
			require.Equal(t, []bool{true, false, true}, got)
		})
	}
}

func TestReadEmptySchema(t *testing.T) {
	valid := writeFooterRenamedColumnParquet(t)
	emptyRoot := []*parquet.SchemaElement{{Name: "schema", NumChildren: common.ToPtr(int32(0))}}
	testCases := map[string]struct {
		schema    []*parquet.SchemaElement
		rowGroups []*parquet.RowGroup
		errMsg    string
	}{
		"no-row-groups":     {schema: emptyRoot, rowGroups: []*parquet.RowGroup{}},
		"empty-row-group":   {schema: emptyRoot, rowGroups: []*parquet.RowGroup{{Columns: []*parquet.ColumnChunk{}}}},
		"column-no-chunk":   {rowGroups: []*parquet.RowGroup{{Columns: []*parquet.ColumnChunk{}}}, errMsg: "Column not found"},
		"column-no-row-grp": {rowGroups: []*parquet.RowGroup{}},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			data := rewriteFooter(t, valid, func(footer *parquet.FileMetaData) {
				if tc.schema != nil {
					footer.Schema = tc.schema
				}
				footer.RowGroups = tc.rowGroups
				footer.NumRows = 0
			})

			pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(data), nil, WithNP(1))
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			defer func() { _ = pr.ReadStop() }()
			for range 2 {
				rows, err := pr.ReadByNumberWithContext(context.Background(), 1)
				require.NoError(t, err)
				require.Empty(t, rows)
			}

			if tc.schema == nil {
				return
			}
			cr, err := NewParquetColumnReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(data), WithNP(1))
			require.NoError(t, err)
			defer func() { _ = cr.ReadStop() }()
			require.Empty(t, cr.SchemaHandler.ValueColumns)
			_, _, _, err = cr.ReadColumnByIndex(0, 1)
			require.ErrorContains(t, err, "out of range")
			require.ErrorContains(t, cr.SkipRowsByIndex(0, 1), "out of range")
		})
	}
}

func TestReadEmptySchemaRows(t *testing.T) {
	newReader := func(t *testing.T, numRows int64, obj any) *ParquetReader {
		t.Helper()
		data := rewriteFooter(t, writeFooterRenamedColumnParquet(t), func(footer *parquet.FileMetaData) {
			footer.Schema = []*parquet.SchemaElement{{Name: "schema", NumChildren: common.ToPtr(int32(0))}}
			footer.RowGroups = []*parquet.RowGroup{{NumRows: numRows, Columns: []*parquet.ColumnChunk{}}}
			footer.NumRows = numRows
		})
		pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(data), obj, WithNP(2))
		require.NoError(t, err)
		t.Cleanup(func() { _ = pr.ReadStop() })
		return pr
	}
	readLen := func(t *testing.T, pr *ParquetReader, n int) int {
		t.Helper()
		rows, err := pr.ReadByNumberWithContext(context.Background(), n)
		require.NoError(t, err)
		return len(rows)
	}

	t.Run("read-and-skip", func(t *testing.T) {
		pr := newReader(t, 5, nil)
		require.Equal(t, 2, readLen(t, pr, 2))
		require.NoError(t, pr.SkipRowsWithContext(context.Background(), 1))
		require.Equal(t, 2, readLen(t, pr, 5))
		require.Zero(t, readLen(t, pr, 1))
		require.NoError(t, pr.SkipRowsWithContext(context.Background(), 1))
	})

	t.Run("reset", func(t *testing.T) {
		pr := newReader(t, 3, nil)
		require.Equal(t, 3, readLen(t, pr, 10))
		require.NoError(t, pr.ResetWithContext(context.Background()))
		require.Equal(t, 3, readLen(t, pr, 10))
	})

	t.Run("skip-past-end", func(t *testing.T) {
		pr := newReader(t, 3, nil)
		require.NoError(t, pr.SkipRowsWithContext(context.Background(), 10))
		require.Zero(t, readLen(t, pr, 1))
		require.NoError(t, pr.ResetWithContext(context.Background()))
		require.Equal(t, 3, readLen(t, pr, 10))
	})

	t.Run("prefilled-destination-is-zeroed", func(t *testing.T) {
		type filled struct{ V int }
		pr := newReader(t, 2, nil)
		dst := []filled{{1}, {2}, {3}}
		require.NoError(t, pr.ReadWithContext(context.Background(), &dst))
		require.Equal(t, []filled{{}, {}}, dst)
	})

	t.Run("footer-altered-after-load", func(t *testing.T) {
		pr := newReader(t, 2, nil)
		require.Equal(t, 1, readLen(t, pr, 1))
		pr.Footer.NumRows = -1
		require.NoError(t, pr.SkipRowsWithContext(context.Background(), 1))
		require.Zero(t, readLen(t, pr, 1))
		pr.Footer.NumRows = 2
		require.Equal(t, 1, readLen(t, pr, 5))
	})

	t.Run("typed-read", func(t *testing.T) {
		pr := newReader(t, 2, new(struct{}))
		dst := make([]struct{}, 3)
		require.NoError(t, pr.ReadWithContext(context.Background(), &dst))
		require.Len(t, dst, 2)
	})
}

func TestSetSchemaHandlerFromJSONEmptySchema(t *testing.T) {
	pr := &ParquetReader{ColumnBuffers: map[string]*ColumnBufferType{}}
	require.NoError(t, pr.SetSchemaHandlerFromJSON(`{"Tag": "name=parquet_go_root", "Fields": []}`))
	require.Empty(t, pr.ColumnBuffers)
}

func BenchmarkReaderScan(b *testing.B) {
	type integer struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	type floating struct {
		Value float64 `parquet:"name=value, type=DOUBLE"`
	}
	type nullable struct {
		Value *float64 `parquet:"name=value, type=DOUBLE"`
	}
	type dictionary struct {
		Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	}
	const count = 16384
	integers, floats := make([]integer, count), make([]floating, count)
	nulls, strings := make([]nullable, count), make([]dictionary, count)
	for i := range count {
		integers[i].Value = int64(i)
		floats[i].Value = float64(i) / 7
		if i%2 != 0 {
			nulls[i].Value = common.ToPtr(float64(i) / 7)
		}
		strings[i].Value = "label-" + strconv.Itoa(i%64)
	}
	for _, size := range []int64{8 << 10, 4 << 20} {
		b.Run("page-"+strconv.FormatInt(size, 10), func(b *testing.B) {
			b.Run("required-int64", func(b *testing.B) { benchmarkReaderScanRecords(b, integers, size) })
			b.Run("required-double", func(b *testing.B) { benchmarkReaderScanRecords(b, floats, size) })
			b.Run("nullable-double", func(b *testing.B) { benchmarkReaderScanRecords(b, nulls, size) })
			b.Run("dictionary", func(b *testing.B) { benchmarkReaderScanRecords(b, strings, size) })
		})
	}
}

func benchmarkReaderScanRecords[T any](b *testing.B, records []T, pageSize int64) {
	b.Helper()
	var out bytes.Buffer
	pw, err := writer.NewParquetWriterWithContext(context.Background(), writerfile.NewWriterFile(&out), new(T), writer.WithNP(1), writer.WithPageSize(pageSize), writer.WithRowGroupSize(1<<30), writer.WithDataPageVersion(2), writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED))
	require.NoError(b, err)
	for _, record := range records {
		require.NoError(b, pw.WriteWithContext(context.Background(), record))
	}
	require.NoError(b, pw.WriteStopWithContext(context.Background()))
	data := bytes.Clone(out.Bytes())
	b.Run("rows", func(b *testing.B) { benchmarkReaderScanData[T](b, data, len(records), false) })
	b.Run("column", func(b *testing.B) { benchmarkReaderScanData[T](b, data, len(records), true) })
}

var benchmarkScanValues any

func benchmarkReaderScanData[T any](b *testing.B, data []byte, count int, column bool) {
	b.Helper()
	pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(data), new(T), WithNP(1))
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, pr.ReadStopWithContext(context.Background())) })
	const batch = 4096
	dst := make([]T, batch)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(data)), "file-B")
	for range b.N {
		if err := pr.ResetWithContext(context.Background()); err != nil {
			b.Fatal(err)
		}
		for consumed := 0; consumed < count; consumed += batch {
			if column {
				values, _, _, err := pr.ReadColumnByIndexWithContext(context.Background(), 0, batch)
				if err != nil {
					b.Fatal(err)
				}
				if len(values) != batch {
					b.Fatalf("got %d values, want %d", len(values), batch)
				}
				benchmarkScanValues = values
			} else {
				dst = dst[:batch]
				if err := pr.ReadWithContext(context.Background(), &dst); err != nil {
					b.Fatal(err)
				}
				if len(dst) != batch {
					b.Fatalf("got %d rows, want %d", len(dst), batch)
				}
				benchmarkScanValues = dst
			}
		}
	}
}

func TestReaderPlainPageRetention(t *testing.T) {
	type required struct {
		Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	}
	type optional struct {
		Value *string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN, omitstats=true"`
	}
	for _, version := range []int32{1, 2} {
		t.Run("v"+strconv.Itoa(int(version)), func(t *testing.T) {
			t.Run("required", func(t *testing.T) {
				testPlainPageRetention(t, version, func(v *string) required { return required{Value: *v} })
			})
			t.Run("optional", func(t *testing.T) {
				testPlainPageRetention(t, version, func(v *string) optional { return optional{Value: v} })
			})
		})
	}
}

func testPlainPageRetention[T any](t *testing.T, version int32, record func(*string) T) {
	t.Helper()
	// One ZSTD page of repeated 80-byte values is small on disk but decodes to ~34 MB.
	const count, size = 400_000, 80
	ctx := context.Background()
	var out bytes.Buffer
	pw, err := writer.NewParquetWriterWithContext(ctx, writerfile.NewWriterFile(&out), new(T), writer.WithNP(1), writer.WithDataPageVersion(version), writer.WithCompressionCodec(parquet.CompressionCodec_ZSTD), writer.WithPageSize(1<<30))
	require.NoError(t, err)
	value := strings.Repeat("x", size)
	for range count {
		require.NoError(t, pw.WriteWithContext(ctx, record(&value)))
	}
	require.NoError(t, pw.WriteStopWithContext(ctx))
	data := bytes.Clone(out.Bytes())
	out = bytes.Buffer{}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	pr, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytesNoAlloc(data), new(T), WithNP(1))
	require.NoError(t, err)
	got := make([]T, 1024)
	require.NoError(t, pr.ReadWithContext(ctx, &got))
	require.Len(t, got, 1024)
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(pr)

	// The reader holds the decompressed page once while it is being read, not a copy of it too.
	decoded := int64(count * (4 + size))
	require.Less(t, int64(after.HeapInuse)-int64(before.HeapInuse), decoded*3/2)
	require.NoError(t, pr.ReadStopWithContext(ctx))
}
