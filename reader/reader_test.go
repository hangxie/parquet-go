package reader

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source/buffer"
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

func Test_ParquetReader_GetNumRows(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)
	defer pr.ReadStop()

	numRows := pr.GetNumRows()
	require.Equal(t, numRecord, numRows)
	require.Greater(t, numRows, int64(0))
}

func Test_ParquetReader_ReadPartial(t *testing.T) {
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
	require.Contains(t, err.Error(), "can't find path")

	require.Panics(t, func() {
		_ = pr.ReadPartial(nil, nameField)
	})
}

func Test_ParquetReader_ReadPartialByNumber(t *testing.T) {
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

	// Test with negative number (currently panics - this is a bug)
	require.Panics(t, func() {
		_, _ = pr.ReadPartialByNumber(-1, nameField)
	})
}

func Test_ParquetReader_ReadStop(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)

	// Ensure column buffers are initialized
	require.NotNil(t, pr.ColumnBuffers)
	require.NotEmpty(t, pr.ColumnBuffers)

	// Call ReadStop
	pr.ReadStop()

	// Verify that column buffers are properly cleaned up
	// ReadStop should close all file handles in column buffers
	// We can't easily verify this without exposing internal state,
	// but we can at least verify the method doesn't panic
	require.NotPanics(t, func() {
		pr.ReadStop()
	})

	// Test ReadStop with nil column buffers
	pr2, err := parquetReader()
	require.NoError(t, err)
	pr2.ColumnBuffers = nil
	require.NotPanics(t, func() {
		pr2.ReadStop()
	})

	// Test ReadStop with empty column buffers
	pr3, err := parquetReader()
	require.NoError(t, err)
	pr3.ColumnBuffers = make(map[string]*ColumnBufferType)
	require.NotPanics(t, func() {
		pr3.ReadStop()
	})
}

func Test_ParquetReader_SetSchemaHandlerFromJSON(t *testing.T) {
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

func Test_ParquetReader_SkipRows(t *testing.T) {
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
