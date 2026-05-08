package reader

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/types"
	"github.com/hangxie/parquet-go/v3/writer"
)

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

func TestParquetReader_ReadPartial(t *testing.T) {
	// Create test data with nested structure
	data, err := createNestedParquetData()
	require.NoError(t, err)

	buf := buffer.NewBufferReaderFromBytesNoAlloc(data)
	pr, err := NewParquetReader(buf, new(NestedRecord), WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	// First, let's check what paths are available in the schema
	require.NotNil(t, pr.SchemaHandler)
	require.NotEmpty(t, pr.SchemaHandler.IndexMap)

	// Schema paths are available for testing

	// Use any available field path for testing
	var nameField string
	for path := range pr.SchemaHandler.MapIndex {
		if path != common.ParGoRootInName && path != "" {
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
	require.Contains(t, err.Error(), "dstInterface is nil")
}

func TestParquetReader_ReadPartialByNumber(t *testing.T) {
	// Create test data
	data, err := createNestedParquetData()
	require.NoError(t, err)

	buf := buffer.NewBufferReaderFromBytesNoAlloc(data)
	pr, err := NewParquetReader(buf, new(NestedRecord), WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	// Use any available field path for testing
	var nameField string
	for path := range pr.SchemaHandler.MapIndex {
		if path != common.ParGoRootInName && path != "" {
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
	require.Contains(t, err.Error(), "path not found")

	// Test with negative number (should return error)
	_, err = pr.ReadPartialByNumber(-1, nameField)
	require.Error(t, err)
	require.Contains(t, err.Error(), "negative maxReadNumber")
}

func TestParquetReader_ReadByNumber_Negative(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	_, err = pr.ReadByNumber(-1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "negative maxReadNumber")
}

func TestParquetReader_ReadStop(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)

	// Ensure column buffers are initialized
	require.NotNil(t, pr.ColumnBuffers)
	require.NotEmpty(t, pr.ColumnBuffers)

	// Call ReadStop - should succeed
	err = pr.ReadStop()
	require.NoError(t, err)

	// Calling again should be safe (files already closed)
	_ = pr.ReadStop()
	// May return error because files are already closed, but shouldn't panic
	// We don't assert on error here as behavior may vary

	// Test ReadStop with nil column buffers
	pr2, err := parquetReader()
	require.NoError(t, err)
	pr2.ColumnBuffers = nil
	err = pr2.ReadStop()
	require.NoError(t, err) // Should not error with nil buffers

	// Test ReadStop with empty column buffers
	pr3, err := parquetReader()
	require.NoError(t, err)
	pr3.ColumnBuffers = make(map[string]*ColumnBufferType)
	err = pr3.ReadStop()
	require.NoError(t, err) // Should not error with empty buffers
}

func TestParquetReader_Reset(t *testing.T) {
	pr, err := parquetReader()
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

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
	defer func() { _ = pr.ReadStop() }()

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
	defer func() { _ = pr.ReadStop() }()

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
			pw, err := writer.NewParquetWriter(fw, jsonSchema, writer.WithNP(1), writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED))
			require.NoError(t, err)

			for _, rec := range tc.records {
				err = pw.Write(rec)
				require.NoError(t, err)
			}
			err = pw.WriteStop()
			require.NoError(t, err)

			// Read back from buffer
			fr := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
			pr, err := NewParquetReader(fr, jsonSchema, WithNP(1))
			require.NoError(t, err)

			numRows := pr.GetNumRows()
			require.Equal(t, int64(len(tc.records)), numRows)

			for i, expected := range tc.records {
				result := make([]NestedListRecord, 1)
				err = pr.Read(&result)
				require.NoError(t, err, "Failed to read row %d", i)
				require.Equal(t, expected.Matrix, result[0].Matrix, "Matrix mismatch at row %d", i)
			}

			_ = pr.ReadStop()
		})
	}
}

func TestGeospatialConfigRoundTrip(t *testing.T) {
	type GeoRow struct {
		Geom string `parquet:"name=geom, type=BYTE_ARRAY, logicaltype=GEOMETRY"`
	}

	// WKB Point(1, 2) little-endian
	wkbPoint := string([]byte{
		0x01, 0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // 1.0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // 2.0
	})

	// Write
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := writer.NewParquetWriter(fw, new(GeoRow), writer.WithNP(1))
	require.NoError(t, err)
	require.NoError(t, pw.Write(GeoRow{Geom: wkbPoint}))
	require.NoError(t, pw.WriteStop())
	require.NoError(t, fw.Close())

	// Read
	fr := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr, err := NewParquetReader(fr, nil, WithNP(1))
	require.NoError(t, err)
	data, err := pr.ReadByNumber(1)
	require.NoError(t, err)
	require.NoError(t, pr.ReadStop())

	tests := []struct {
		name     string
		cfg      *types.GeospatialConfig
		expected map[string]any
	}{
		{
			name: "default_hex",
			cfg:  nil,
			expected: map[string]any{
				"wkb_hex": "0101000000000000000000f03f0000000000000040",
				"crs":     "OGC:CRS84",
			},
		},
		{
			name: "geojson",
			cfg:  types.NewGeospatialConfig(types.WithGeometryJSONMode(types.GeospatialModeGeoJSON)),
			expected: map[string]any{
				"type":       "Feature",
				"geometry":   map[string]any{"type": "Point", "coordinates": []float64{1, 2}},
				"properties": map[string]any{"crs": "OGC:CRS84"},
			},
		},
		{
			name: "base64",
			cfg:  types.NewGeospatialConfig(types.WithGeometryJSONMode(types.GeospatialModeBase64)),
			expected: map[string]any{
				"wkb_b64": "AQEAAAAAAAAAAADwPwAAAAAAAABA",
				"crs":     "OGC:CRS84",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []marshal.JSONConvertOption
			if tt.cfg != nil {
				opts = append(opts, marshal.WithGeospatialConfig(tt.cfg))
			}
			out, err := marshal.ConvertToJSONFriendly(data, pr.SchemaHandler, opts...)
			require.NoError(t, err)
			slice, ok := out.([]any)
			require.True(t, ok)
			require.Len(t, slice, 1)
			row, ok := slice[0].(map[string]any)
			require.True(t, ok)
			require.Equal(t, tt.expected, row["geom"])
		})
	}
}
