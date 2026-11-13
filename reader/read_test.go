package reader

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
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

func TestParquetReader_ReadPartial_SiblingPrefix(t *testing.T) {
	// Reproduces issue #325: reading prefix "name" must not pick up data from
	// the sibling column "name2" whose path shares the same string prefix.
	type siblingInner struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	type siblingRecord struct {
		Name  *siblingInner `parquet:"name=name"`
		Name2 *siblingInner `parquet:"name=name2"`
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	//nolint:staticcheck
	pw, err := writer.NewParquetWriter(fw, new(siblingRecord), writer.WithNP(1))
	require.NoError(t, err)

	const rows = 3
	for i := range int64(rows) {
		//nolint:staticcheck
		require.NoError(t, pw.Write(siblingRecord{
			Name:  &siblingInner{Value: i},
			Name2: &siblingInner{Value: i + 100},
		}))
	}
	//nolint:staticcheck
	require.NoError(t, pw.WriteStop())

	// Run multiple times: before the fix, map iteration order made the sibling
	// column's data win nondeterministically.
	for range 20 {
		br := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(br, new(siblingRecord), WithNP(1))
		require.NoError(t, err)

		out := make([]siblingInner, rows)
		err = pr.ReadPartial(&out, common.PathToStr([]string{common.ParGoRootExName, "name"}))
		require.NoError(t, err)
		_ = pr.ReadStop()

		for i := range int64(rows) {
			require.Equal(t, i, out[i].Value)
		}
	}
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

type rtInner struct {
	A int32  `parquet:"name=a, type=INT32"`
	B string `parquet:"name=b, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type rtComplexRow struct {
	ID       int64            `parquet:"name=id, type=INT64, compression=SNAPPY"`
	Name     string           `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, compression=SNAPPY"`
	Score    *float64         `parquet:"name=score, type=DOUBLE, repetitiontype=OPTIONAL, compression=SNAPPY"`
	Flag     bool             `parquet:"name=flag, type=BOOLEAN"`
	Tags     []string         `parquet:"name=tags, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8, valuecompression=SNAPPY"`
	Scores   map[string]int32 `parquet:"name=scores, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	Repeated []int32          `parquet:"name=repeated, type=INT32, repetitiontype=REPEATED"`
	Nested   rtInner          `parquet:"name=nested, type=STRUCT"`
	Children []rtInner        `parquet:"name=children, type=LIST, valuetype=STRUCT"`
}

func roundTripComplexRows(t *testing.T, dataPageVersion int32) []rtComplexRow {
	t.Helper()
	score := 9.5
	in := []rtComplexRow{
		{
			ID: 1, Name: "alice", Score: &score, Flag: true,
			Tags:     []string{"x", "y"},
			Scores:   map[string]int32{"math": 90},
			Repeated: []int32{1, 2, 3},
			Nested:   rtInner{A: 7, B: "inner"},
			Children: []rtInner{{A: 1, B: "c1"}, {A: 2, B: "c2"}},
		},
		{
			ID: 2, Name: "bob", Score: nil, Flag: false,
			Tags:     []string{},
			Scores:   map[string]int32{"sci": 80, "art": 70},
			Repeated: []int32{},
			Nested:   rtInner{A: 0, B: ""},
			Children: []rtInner{},
		},
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	opts := []writer.WriterOption{writer.WithNP(1)}
	if dataPageVersion == 2 {
		opts = append(opts, writer.WithDataPageVersion(2))
	}
	//nolint:staticcheck
	pw, err := writer.NewParquetWriter(fw, new(rtComplexRow), opts...)
	require.NoError(t, err)
	for _, r := range in {
		//nolint:staticcheck
		require.NoError(t, pw.Write(r))
	}
	//nolint:staticcheck
	require.NoError(t, pw.WriteStop())

	fr := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr, err := NewParquetReader(fr, new(rtComplexRow), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	out := make([]rtComplexRow, len(in))
	require.NoError(t, pr.Read(&out))
	return out
}

func TestRoundTrip_ComplexNested_V1(t *testing.T) {
	out := roundTripComplexRows(t, 1)
	require.Len(t, out, 2)
	require.Equal(t, int64(1), out[0].ID)
	require.Equal(t, "alice", out[0].Name)
	require.NotNil(t, out[0].Score)
	require.InDelta(t, 9.5, *out[0].Score, 1e-9)
	require.Equal(t, []string{"x", "y"}, out[0].Tags)
	require.Equal(t, map[string]int32{"math": 90}, out[0].Scores)
	require.Equal(t, []int32{1, 2, 3}, out[0].Repeated)
	require.Equal(t, rtInner{A: 7, B: "inner"}, out[0].Nested)
	require.Equal(t, []rtInner{{A: 1, B: "c1"}, {A: 2, B: "c2"}}, out[0].Children)

	require.Equal(t, "bob", out[1].Name)
	require.Nil(t, out[1].Score)
	require.Equal(t, map[string]int32{"sci": 80, "art": 70}, out[1].Scores)
}

func TestRoundTrip_ComplexNested_V2(t *testing.T) {
	out := roundTripComplexRows(t, 2)
	require.Len(t, out, 2)
	require.Equal(t, "alice", out[0].Name)
	require.Equal(t, []string{"x", "y"}, out[0].Tags)
	require.Equal(t, []rtInner{{A: 1, B: "c1"}, {A: 2, B: "c2"}}, out[0].Children)
	require.Nil(t, out[1].Score)
}

func TestReadUnknownLogicalType(t *testing.T) {
	type Row struct {
		Name    string `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING"`
		NullCol *int32 `parquet:"name=null_col, type=INT32, logicaltype=UNKNOWN, repetitiontype=OPTIONAL"`
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	//nolint:staticcheck
	pw, err := writer.NewParquetWriter(fw, new(Row), writer.WithNP(1))
	require.NoError(t, err)
	//nolint:staticcheck
	require.NoError(t, pw.Write(Row{Name: "foo", NullCol: nil}))
	//nolint:staticcheck
	require.NoError(t, pw.Write(Row{Name: "bar", NullCol: nil}))
	//nolint:staticcheck
	require.NoError(t, pw.WriteStop())

	fr := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr, err := NewParquetReader(fr, new(Row), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rows := make([]Row, 2)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, "foo", rows[0].Name)
	require.Nil(t, rows[0].NullCol)
	require.Equal(t, "bar", rows[1].Name)
	require.Nil(t, rows[1].NullCol)
}

// TestReadUnknownLogicalType_LooseRead verifies that a malformed parquet file
// whose UNKNOWN column contains a non-null INT32 value is read back as-is
// rather than being forced to nil.
func TestReadUnknownLogicalType_LooseRead(t *testing.T) {
	// Write with a plain OPTIONAL INT32 (no UNKNOWN) so the writer accepts a non-nil value.
	type WriterRow struct {
		Name    string `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING"`
		NullCol *int32 `parquet:"name=null_col, type=INT32, repetitiontype=OPTIONAL"`
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	//nolint:staticcheck
	pw, err := writer.NewParquetWriter(fw, new(WriterRow), writer.WithNP(1))
	require.NoError(t, err)
	val := int32(42)
	//nolint:staticcheck
	require.NoError(t, pw.Write(WriterRow{Name: "foo", NullCol: &val}))
	//nolint:staticcheck
	require.NoError(t, pw.WriteStop())

	// Patch the footer: mark null_col as UNKNOWN logical type.
	patched, err := patchSchemaElementUnknown(buf.Bytes(), "null_col")
	require.NoError(t, err)

	// Read back via a struct that declares null_col as UNKNOWN — the non-nil value must survive.
	type ReaderRow struct {
		Name    string `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING"`
		NullCol *int32 `parquet:"name=null_col, type=INT32, logicaltype=UNKNOWN, repetitiontype=OPTIONAL"`
	}

	fr := buffer.NewBufferReaderFromBytesNoAlloc(patched)
	pr, err := NewParquetReader(fr, new(ReaderRow), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rows := make([]ReaderRow, 1)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, "foo", rows[0].Name)
	require.NotNil(t, rows[0].NullCol)
	require.Equal(t, int32(42), *rows[0].NullCol)
}

// patchSchemaElementUnknown rewrites the Thrift footer of a parquet byte slice,
// setting LogicalType.UNKNOWN on the schema element named columnName.
func patchSchemaElementUnknown(data []byte, columnName string) ([]byte, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("data too short to be a parquet file")
	}
	// Footer length sits at data[len-8:len-4]; trailing magic is data[len-4:].
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	footerStart := len(data) - 8 - footerLen
	if footerStart < 4 {
		return nil, fmt.Errorf("computed footer start %d is out of range", footerStart)
	}

	// Decode footer.
	footerBytes := data[footerStart : len(data)-8]
	footer := parquet.NewFileMetaData()
	proto := thrift.NewTCompactProtocolConf(
		thrift.NewTBufferedTransport(thrift.NewStreamTransportR(bytes.NewReader(footerBytes)), len(footerBytes)),
		&thrift.TConfiguration{},
	)
	if err := footer.Read(context.TODO(), proto); err != nil {
		return nil, fmt.Errorf("decode footer: %w", err)
	}

	// Patch the target schema element.
	for _, se := range footer.Schema {
		if se.Name == columnName {
			se.LogicalType = &parquet.LogicalType{UNKNOWN: parquet.NewNullType()}
			break
		}
	}

	// Re-encode footer.
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	newFooter, err := ts.Write(context.TODO(), footer)
	if err != nil {
		return nil, fmt.Errorf("encode footer: %w", err)
	}

	// Reconstruct: original data up to old footer + new footer + length + magic.
	out := make([]byte, 0, footerStart+len(newFooter)+8)
	out = append(out, data[:footerStart]...)
	out = append(out, newFooter...)
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(newFooter)))
	out = append(out, lenBuf...)
	out = append(out, []byte("PAR1")...)
	return out, nil
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
			//nolint:staticcheck
			pw, err := writer.NewParquetWriter(fw, jsonSchema, writer.WithNP(1), writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED))
			require.NoError(t, err)

			for _, rec := range tc.records {
				//nolint:staticcheck
				err = pw.Write(rec)
				require.NoError(t, err)
			}
			//nolint:staticcheck
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
	//nolint:staticcheck
	pw, err := writer.NewParquetWriter(fw, new(GeoRow), writer.WithNP(1))
	require.NoError(t, err)
	//nolint:staticcheck
	require.NoError(t, pw.Write(GeoRow{Geom: wkbPoint}))
	//nolint:staticcheck
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

// TestNonFiniteFloatJSONRoundTrip verifies that non-finite FLOAT, DOUBLE, and FLOAT16 values
// written through JSONWriter using the Go-style "+Inf"/"-Inf" input tokens come back out of
// marshal.ConvertToJSONFriendly as the portable "Infinity"/"-Infinity" spellings, and that the
// converted result can always be passed to encoding/json.Marshal.
func TestNonFiniteFloatJSONRoundTrip(t *testing.T) {
	jsonSchema := `{
		"Tag": "name=parquet-go-root",
		"Fields": [
			{"Tag": "name=score, type=DOUBLE"},
			{"Tag": "name=score32, type=FLOAT"},
			{"Tag": "name=score16, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16"}
		]
	}`

	ctx := context.Background()
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	jw, err := writer.NewJSONWriterWithContext(ctx, jsonSchema, fw, writer.WithNP(1))
	require.NoError(t, err)

	rows := []string{
		`{"score": "NaN", "score32": "NaN", "score16": "NaN"}`,
		`{"score": "+Inf", "score32": "+Inf", "score16": "+Inf"}`,
		`{"score": "-Inf", "score32": "-Inf", "score16": "-Inf"}`,
		`{"score": 1.5, "score32": 1.5, "score16": 1.5}`,
	}
	for _, row := range rows {
		require.NoError(t, jw.WriteWithContext(ctx, row))
	}
	require.NoError(t, jw.WriteStopWithContext(ctx))
	require.NoError(t, fw.Close())

	fr := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr, err := NewParquetReader(fr, nil, WithNP(1))
	require.NoError(t, err)
	data, err := pr.ReadByNumber(len(rows))
	require.NoError(t, err)
	require.NoError(t, pr.ReadStop())

	out, err := marshal.ConvertToJSONFriendly(data, pr.SchemaHandler)
	require.NoError(t, err)

	rowsOut, ok := out.([]any)
	require.True(t, ok)
	require.Len(t, rowsOut, len(rows))

	expected := []map[string]any{
		{"score": "NaN", "score32": "NaN", "score16": "NaN"},
		{"score": "Infinity", "score32": "Infinity", "score16": "Infinity"},
		{"score": "-Infinity", "score32": "-Infinity", "score16": "-Infinity"},
		{"score": 1.5, "score32": float32(1.5), "score16": float32(1.5)},
	}
	for i, exp := range expected {
		rowMap, ok := rowsOut[i].(map[string]any)
		require.True(t, ok)
		require.Equal(t, exp["score"], rowMap["score"])
		require.Equal(t, exp["score32"], rowMap["score32"])
		require.Equal(t, exp["score16"], rowMap["score16"])
	}

	marshaled, err := json.Marshal(out)
	require.NoError(t, err)
	require.Contains(t, string(marshaled), `"score":"NaN"`)
	require.Contains(t, string(marshaled), `"score":"Infinity"`)
	require.Contains(t, string(marshaled), `"score":"-Infinity"`)
}

// TestLegacyRepeatedJSONRoundTrip verifies that legacy REPEATED columns, which are repeated in
// place instead of being wrapped in a three-level LIST, still get their logical type conversions
// applied by marshal.ConvertToJSONFriendly.
func TestLegacyRepeatedJSONRoundTrip(t *testing.T) {
	jsonSchema := `{
		"Tag": "name=parquet-go-root",
		"Fields": [
			{"Tag": "name=scores, type=DOUBLE, repetitiontype=REPEATED"},
			{"Tag": "name=days, type=INT32, convertedtype=DATE, repetitiontype=REPEATED"},
			{"Tag": "name=groups, repetitiontype=REPEATED", "Fields": [{"Tag": "name=value, type=DOUBLE"}]}
		]
	}`

	ctx := context.Background()
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	jw, err := writer.NewJSONWriterWithContext(ctx, jsonSchema, fw, writer.WithNP(1))
	require.NoError(t, err)
	require.NoError(t, jw.WriteWithContext(ctx, `{"scores": ["NaN", "+Inf", 1.5], "days": [19000], "groups": [{"value": "-Inf"}]}`))
	require.NoError(t, jw.WriteStopWithContext(ctx))
	require.NoError(t, fw.Close())

	fr := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr, err := NewParquetReader(fr, nil, WithNP(1))
	require.NoError(t, err)
	data, err := pr.ReadByNumber(1)
	require.NoError(t, err)
	require.NoError(t, pr.ReadStop())

	out, err := marshal.ConvertToJSONFriendly(data, pr.SchemaHandler)
	require.NoError(t, err)

	rowsOut, ok := out.([]any)
	require.True(t, ok)
	require.Len(t, rowsOut, 1)
	require.Equal(t, map[string]any{
		"scores": []any{"NaN", "Infinity", 1.5},
		"days":   []any{"2022-01-08"},
		"groups": []any{map[string]any{"value": "-Infinity"}},
	}, rowsOut[0])

	marshaled, err := json.Marshal(out)
	require.NoError(t, err)
	require.JSONEq(t, `[{"scores":["NaN","Infinity",1.5],"days":["2022-01-08"],"groups":[{"value":"-Infinity"}]}]`, string(marshaled))
}

func TestReadInferredRowsIgnoreRootRepetition(t *testing.T) {
	type row struct {
		V int32 `parquet:"name=v, type=INT32"`
	}
	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(context.Background(), &buf, new(row), writer.WithNP(1))
	require.NoError(t, err)
	for _, v := range []int32{1, 2, 3} {
		require.NoError(t, pw.WriteWithContext(context.Background(), row{V: v}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	for _, rt := range []parquet.FieldRepetitionType{parquet.FieldRepetitionType_REPEATED, parquet.FieldRepetitionType_OPTIONAL} {
		t.Run(rt.String(), func(t *testing.T) {
			data := rewriteFooter(t, buf.Bytes(), func(f *parquet.FileMetaData) { f.Schema[0].RepetitionType = &rt })
			pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(data), nil, WithNP(1))
			require.NoError(t, err)
			defer func() { _ = pr.ReadStop() }()

			rows, err := pr.ReadByNumberWithContext(context.Background(), 3)
			require.NoError(t, err)
			got, err := json.Marshal(rows)
			require.NoError(t, err)
			require.JSONEq(t, `[{"v":1},{"v":2},{"v":3}]`, string(got))
		})
	}
}

type nestedArrayInner struct {
	Array []int32 `parquet:"name=array, type=INT32, repetitiontype=REPEATED"`
}

type nestedArrayRow struct {
	Array []nestedArrayInner `parquet:"name=array, repetitiontype=REPEATED"`
}

// A repeated group and leaf both named array, with no LIST annotation, are plain nested lists.
func TestRoundTrip_NestedRepeatedNamedArray(t *testing.T) {
	ctx := context.Background()
	src := []nestedArrayRow{
		{Array: []nestedArrayInner{{Array: []int32{1, 2}}, {Array: []int32{3, 4}}}},
		{Array: []nestedArrayInner{{Array: []int32{5}}, {}, {Array: []int32{6, 7, 8}}}},
	}
	fw := buffer.NewBufferWriter()
	pw, err := writer.NewParquetWriterWithContext(ctx, fw, new(nestedArrayRow))
	require.NoError(t, err)
	for _, row := range src {
		require.NoError(t, pw.WriteWithContext(ctx, row))
	}
	require.NoError(t, pw.WriteStopWithContext(ctx))

	t.Run("typed", func(t *testing.T) {
		pr, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(fw.Bytes()), new(nestedArrayRow))
		require.NoError(t, err)
		got := make([]nestedArrayRow, len(src))
		require.NoError(t, pr.ReadWithContext(ctx, &got))
		require.Equal(t, src, got)
	})

	t.Run("schema-less", func(t *testing.T) {
		pr, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(fw.Bytes()), nil)
		require.NoError(t, err)
		got, err := pr.ReadByNumberWithContext(ctx, len(src))
		require.NoError(t, err)
		raw, err := json.Marshal(got)
		require.NoError(t, err)
		require.JSONEq(t, `[{"array":[{"array":[1,2]},{"array":[3,4]}]},{"array":[{"array":[5]},{"array":null},{"array":[6,7,8]}]}]`, string(raw))

		friendly, err := marshal.ConvertToJSONFriendly(got, pr.SchemaHandler)
		require.NoError(t, err)
		raw, err = json.Marshal(friendly)
		require.NoError(t, err)
		require.JSONEq(t, `[{"array":[{"array":[1,2]},{"array":[3,4]}]},{"array":[{"array":[5]},{"array":[]},{"array":[6,7,8]}]}]`, string(raw))
	})
}

// A LIST-annotated repeated group of a repeated leaf, both named array, is a parquet-mr two-level list.
func TestRoundTrip_TwoLevelList(t *testing.T) {
	ctx := context.Background()
	src := []nestedArrayRow{
		{Array: []nestedArrayInner{{Array: []int32{1, 2}}, {Array: []int32{3, 4}}}},
		{Array: []nestedArrayInner{{Array: []int32{5}}, {Array: []int32{6, 7, 8}}}},
	}

	testCases := map[string]func(*parquet.SchemaElement){
		"converted-type": func(e *parquet.SchemaElement) { e.ConvertedType = common.ToPtr(parquet.ConvertedType_LIST) },
		"logical-type":   func(e *parquet.SchemaElement) { e.LogicalType = &parquet.LogicalType{LIST: parquet.NewListType()} },
	}

	for name, annotate := range testCases {
		t.Run(name, func(t *testing.T) {
			fw := buffer.NewBufferWriter()
			pw, err := writer.NewParquetWriterWithContext(ctx, fw, new(nestedArrayRow))
			require.NoError(t, err)
			for _, row := range src {
				require.NoError(t, pw.WriteWithContext(ctx, row))
			}
			// Two-level levels match the unannotated nesting, so only the footer needs the annotation.
			group := *pw.Footer.Schema[1]
			annotate(&group)
			pw.Footer.Schema[1] = &group
			require.NoError(t, pw.WriteStopWithContext(ctx))

			pr, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(fw.Bytes()), nil)
			require.NoError(t, err)
			got, err := pr.ReadByNumberWithContext(ctx, len(src))
			require.NoError(t, err)
			friendly, err := marshal.ConvertToJSONFriendly(got, pr.SchemaHandler)
			require.NoError(t, err)
			raw, err := json.Marshal(friendly)
			require.NoError(t, err)
			require.JSONEq(t, `[{"array":[[1,2],[3,4]]},{"array":[[5],[6,7,8]]}]`, string(raw))
		})
	}
}

// parquet-testing's LIST<LIST<int32>> written by parquet-mr with nested two-level lists.
func TestRead_OldListStructure(t *testing.T) {
	const oldListFile = "../build/testdata/old_list_structure.parquet"
	data, err := os.ReadFile(oldListFile)
	require.NoError(t, err)

	ctx := context.Background()
	pr, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(data), nil)
	require.NoError(t, err)
	got, err := pr.ReadByNumberWithContext(ctx, int(pr.GetNumRows()))
	require.NoError(t, err)
	friendly, err := marshal.ConvertToJSONFriendly(got, pr.SchemaHandler)
	require.NoError(t, err)
	raw, err := json.Marshal(friendly)
	require.NoError(t, err)
	require.JSONEq(t, `[{"a":[[1,2],[3,4]]}]`, string(raw))
}

// Written by pyarrow 26.0.1 with no dictionary and DELTA_BYTE_ARRAY on every column, page_version "1.0" or "2.0":
//
//	ids = [uuid.UUID(int=0x0123456789abcdef0000000000000000 + i).bytes for i in range(5)]
//	t = pa.table({
//	    "u": pa.array(ids, pa.uuid()),
//	    "f": pa.array([b"abcd", b"abce", None, b"xbcd", b"xbcd"], pa.binary(4)),
//	    "d": pa.array([decimal.Decimal(x) for x in ["1.23", "-4.56", "1.24", "999.99", "0.00"]], pa.decimal128(10, 2)),
//	})
//	pq.write_table(t, path, use_dictionary=False, compression="NONE", data_page_version=page_version,
//	               store_schema=False, column_encoding={c: "DELTA_BYTE_ARRAY" for c in t.column_names})
const pyarrowFLBADeltaByteArrayV1 = "" +
	"UEFSMRUAFZwBFZwBLBUKFQ4VBhUGHDYAKBABI0VniavN7wAAAAAAAAAEGBABI0VniavN7wAAAAAAAAAAEREAAAACAAAACgGAAQQF" +
	"AAAEAAAADwAAAAAAAAAAAAAAAAAAAIABBAUgHQQAAADw/wAAAAAAAAAAAAAAAAAAASNFZ4mrze8AAAAAAAAAAAECAwQVABV2FXYs" +
	"FQoVDhUGFQYcNgIoBHhiY2QYBGFiY2QREQAAAAIAAAADG4ABBAQABQMAAADGAQAAAAAAAAAAAACAAQQECAcDAAAAOQAAAAAAAAAA" +
	"AAAAYWJjZGV4YmNkFQAVfhV+LBUKFQ4VBhUGHBgFAAABhp8YBf////44FgAoBQAAAYafGAX////+OBERAAAAAgAAAAoBgAEEBQAA" +
	"AgAAACAAAAAAAAAAgAEEBQoDAgAAAIoAAAAAAAAAAAAAAHv////+OAAAAAB8AYafAAAAFQQZTDUAGAZzY2hlbWEVBgAVDhUgFQIY" +
	"AXVs7AAAABUOFQgVAhgBZgAVDhUKFQIYAWQlChUEFRQsXBUEFRQAAAAWChkcGTwmABwVDhklBg4ZGAF1FQAWChaWAhaWAiYIPDYA" +
	"KBABI0VniavN7wAAAAAAAAAEGBABI0VniavN7wAAAAAAAAAAEREAGRwVABUOFQIAPCkGGSYACgAAACYAHBUOGSUGDhkYAWYVABYK" +
	"FrwBFrwBJp4CPDYCKAR4YmNkGARhYmNkEREAGRwVABUOFQIAPCkGGSYCCAAAACYAHBUOGSUGDhkYAWQVABYKFuQBFuQBJtoDPBgF" +
	"AAABhp8YBf////44FgAoBQAAAYafGAX////+OBERABkcFQAVDhUCADwpBhkmAAoAAAAWtgUWCiYIFrYFACggcGFycXVldC1jcHAt" +
	"YXJyb3cgdmVyc2lvbiAyNi4wLjEZPBwAABwAABwAAABkAQAAUEFSMQ=="

const pyarrowFLBADeltaByteArrayV2 = "" +
	"UEFSMRUGFZQBFZQBXBUKFQAVChUOFQQVABIcNgAoEAEjRWeJq83vAAAAAAAAAAQYEAEjRWeJq83vAAAAAAAAAAAREQAAAAoBgAEE" +
	"BQAABAAAAA8AAAAAAAAAAAAAAAAAAACAAQQFIB0EAAAA8P8AAAAAAAAAAAAAAAAAAAEjRWeJq83vAAAAAAAAAAABAgMEFQYVbhVu" +
	"XBUKFQIVChUOFQQVABIcNgIoBHhiY2QYBGFiY2QREQAAAAMbgAEEBAAFAwAAAMYBAAAAAAAAAAAAAIABBAQIBwMAAAA5AAAAAAAA" +
	"AAAAAABhYmNkZXhiY2QVBhV2FXZcFQoVABUKFQ4VBBUAEhwYBQAAAYafGAX////+OBYAKAUAAAGGnxgF/////jgREQAAAAoBgAEE" +
	"BQAAAgAAACAAAAAAAAAAgAEEBQoDAgAAAIoAAAAAAAAAAAAAAHv////+OAAAAAB8AYafAAAAFQQZTDUAGAZzY2hlbWEVBgAVDhUg" +
	"FQIYAXVs7AAAABUOFQgVAhgBZgAVDhUKFQIYAWQlChUEFRQsXBUEFRQAAAAWChkcGTwmABwVDhklBg4ZGAF1FQAWChaYAhaYAiYI" +
	"PDYAKBABI0VniavN7wAAAAAAAAAEGBABI0VniavN7wAAAAAAAAAAEREAGRwVABUOFQIAPCkGGSYACgAAACYAHBUOGSUGDhkYAWYV" +
	"ABYKFr4BFr4BJqACPDYCKAR4YmNkGARhYmNkEREAGRwVABUOFQIAPCkGGSYCCAAAACYAHBUOGSUGDhkYAWQVABYKFuYBFuYBJt4D" +
	"PBgFAAABhp8YBf////44FgAoBQAAAYafGAX////+OBERABkcFQAVDhUCADwpBhkmAAoAAAAWvAUWCiYIFrwFACggcGFycXVldC1j" +
	"cHAtYXJyb3cgdmVyc2lvbiAyNi4wLjEZPBwAABwAABwAAABkAQAAUEFSMQ=="

// Files from other writers; parquet-testing has no FIXED_LEN_BYTE_ARRAY column with DELTA_BYTE_ARRAY.
var fixedLenByteArrayDeltaByteArrayFiles = map[string]struct {
	data     string
	pageType parquet.PageType
}{
	"pyarrow-v1": {pyarrowFLBADeltaByteArrayV1, parquet.PageType_DATA_PAGE},
	"pyarrow-v2": {pyarrowFLBADeltaByteArrayV2, parquet.PageType_DATA_PAGE_V2},
}

func TestRead_FixedLenByteArrayDeltaByteArray(t *testing.T) {
	for name, tc := range fixedLenByteArrayDeltaByteArrayFiles {
		t.Run(name, func(t *testing.T) {
			data, err := base64.StdEncoding.DecodeString(tc.data)
			require.NoError(t, err)

			ctx := context.Background()
			pr, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(data), nil)
			require.NoError(t, err)
			for i, col := range pr.Footer.RowGroups[0].Columns {
				require.Equal(t, parquet.Type_FIXED_LEN_BYTE_ARRAY, col.MetaData.Type)
				require.Contains(t, col.MetaData.Encodings, parquet.Encoding_DELTA_BYTE_ARRAY)
				header, err := pr.GetFirstDataPageHeader(0, i)
				require.NoError(t, err)
				require.Equal(t, tc.pageType, header.PageType)
				require.Equal(t, parquet.Encoding_DELTA_BYTE_ARRAY, header.Encoding)
			}
			got, err := pr.ReadByNumberWithContext(ctx, int(pr.GetNumRows()))
			require.NoError(t, err)
			friendly, err := marshal.ConvertToJSONFriendly(got, pr.SchemaHandler)
			require.NoError(t, err)
			raw, err := json.Marshal(friendly)
			require.NoError(t, err)
			require.JSONEq(t, `[
				{"u":"01234567-89ab-cdef-0000-000000000000","f":"YWJjZA==","d":1.23},
				{"u":"01234567-89ab-cdef-0000-000000000001","f":"YWJjZQ==","d":-4.56},
				{"u":"01234567-89ab-cdef-0000-000000000002","f":null,"d":1.24},
				{"u":"01234567-89ab-cdef-0000-000000000003","f":"eGJjZA==","d":999.99},
				{"u":"01234567-89ab-cdef-0000-000000000004","f":"eGJjZA==","d":0}
			]`, string(raw))
		})
	}
}

func TestRead_FixedLenByteArrayDeltaByteArrayWidthMismatch(t *testing.T) {
	for name, tc := range fixedLenByteArrayDeltaByteArrayFiles {
		t.Run(name, func(t *testing.T) {
			data, err := base64.StdEncoding.DecodeString(tc.data)
			require.NoError(t, err)
			ctx := context.Background()
			pr, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(data), nil)
			require.NoError(t, err)

			// Declare f as 3 bytes wide while its pages still hold 4-byte values.
			footer := pr.Footer
			for _, se := range footer.Schema {
				if se.Name == "f" {
					se.TypeLength = common.ToPtr(int32(3))
				}
			}
			ts := thrift.NewTSerializer()
			ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
			footerBytes, err := ts.Write(ctx, footer)
			require.NoError(t, err)
			oldLen := binary.LittleEndian.Uint32(data[len(data)-8:])
			patched := append([]byte{}, data[:len(data)-8-int(oldLen)]...)
			patched = append(patched, footerBytes...)
			patched = binary.LittleEndian.AppendUint32(patched, uint32(len(footerBytes)))
			patched = append(patched, "PAR1"...)

			pr, err = NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(patched), nil)
			require.NoError(t, err)
			_, err = pr.ReadByNumberWithContext(ctx, int(pr.GetNumRows()))
			require.ErrorContains(t, err, "column [schema.f]")
			require.ErrorContains(t, err, "byte array of length 4 does not match column length 3")
		})
	}
}

func TestDeltaByteArrayRoundTrip(t *testing.T) {
	type record struct {
		Uuid    string   `parquet:"name=uuid, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID, encoding=DELTA_BYTE_ARRAY"`
		Decimal string   `parquet:"name=decimal, type=FIXED_LEN_BYTE_ARRAY, length=5, logicaltype=DECIMAL, logicaltype.precision=10, logicaltype.scale=2, encoding=DELTA_BYTE_ARRAY"`
		Fixed   *string  `parquet:"name=fixed, type=FIXED_LEN_BYTE_ARRAY, length=4, repetitiontype=OPTIONAL, encoding=DELTA_BYTE_ARRAY"`
		Tags    []string `parquet:"name=tags, type=FIXED_LEN_BYTE_ARRAY, length=2, repetitiontype=REPEATED, encoding=DELTA_BYTE_ARRAY"`
		Raw     []byte   `parquet:"name=raw, type=FIXED_LEN_BYTE_ARRAY, length=3, encoding=DELTA_BYTE_ARRAY"`
		Binary  []byte   `parquet:"name=binary, type=BYTE_ARRAY, encoding=DELTA_BYTE_ARRAY"`
	}
	records := make([]record, 300)
	for i := range records {
		// Consecutive rows share values, so pages hold full-prefix entries with empty suffixes.
		r := record{
			Uuid:    string(binary.BigEndian.AppendUint64([]byte("\x01\x23\x45\x67\x89\xab\xcd\xef"), uint64(i/2))),
			Decimal: types.StrIntToBinary(strconv.Itoa(i/3*37-5000), "BigEndian", 5, true),
			Raw:     []byte{0x00, 0xff, byte(i / 4)},
			Binary:  []byte(strings.Repeat("\xfe", i%5)),
		}
		if i%7 != 0 {
			r.Fixed = common.ToPtr(fmt.Sprintf("x%03d", i/3%50))
		}
		for j := range i % 3 {
			r.Tags = append(r.Tags, fmt.Sprintf("t%d", (i+j)%4))
		}
		records[i] = r
	}

	for _, version := range []int32{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			ctx := context.Background()
			var out bytes.Buffer
			pw, err := writer.NewParquetWriterWithContext(ctx, writerfile.NewWriterFile(&out), new(record), writer.WithNP(1), writer.WithDataPageVersion(version), writer.WithPageSize(1024))
			require.NoError(t, err)
			for _, r := range records {
				require.NoError(t, pw.WriteWithContext(ctx, r))
			}
			require.NoError(t, pw.WriteStopWithContext(ctx))

			pr, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(out.Bytes()), new(record), WithNP(1))
			require.NoError(t, err)
			pageType := map[int32]parquet.PageType{1: parquet.PageType_DATA_PAGE, 2: parquet.PageType_DATA_PAGE_V2}[version]
			for i, col := range pr.Footer.RowGroups[0].Columns {
				require.Equal(t, []parquet.Encoding{parquet.Encoding_RLE, parquet.Encoding_DELTA_BYTE_ARRAY}, col.MetaData.Encodings)
				header, err := pr.GetFirstDataPageHeader(0, i)
				require.NoError(t, err)
				require.Equal(t, pageType, header.PageType)
			}
			got := make([]record, len(records))
			require.NoError(t, pr.ReadWithContext(ctx, &got))
			require.Equal(t, records, got)
			require.NoError(t, pr.ReadStopWithContext(ctx))
		})
	}
}

func TestReadListWithRepeatedElementField(t *testing.T) {
	// v (LIST) > repeated list > repeated element: the spec reads list as the element itself.
	type record struct {
		V struct {
			List []struct {
				Element []int32 `parquet:"name=element, type=INT32, repetitiontype=REPEATED"`
			} `parquet:"name=list, repetitiontype=REPEATED"`
		} `parquet:"name=v"`
	}
	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(context.Background(), &buf, new(record), writer.WithNP(1))
	require.NoError(t, err)
	var row record
	row.V.List = append(row.V.List, struct {
		Element []int32 `parquet:"name=element, type=INT32, repetitiontype=REPEATED"`
	}{Element: []int32{1, 2}}, struct {
		Element []int32 `parquet:"name=element, type=INT32, repetitiontype=REPEATED"`
	}{Element: []int32{3}})
	require.NoError(t, pw.WriteWithContext(context.Background(), row))
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	// The writer leaves a struct group unannotated, so mark v as a LIST in the footer.
	data := rewriteFooter(t, buf.Bytes(), func(footer *parquet.FileMetaData) {
		footer.Schema[1].ConvertedType = common.ToPtr(parquet.ConvertedType_LIST)
		footer.Schema[1].LogicalType = &parquet.LogicalType{LIST: parquet.NewListType()}
	})

	pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), nil, WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()
	got, err := pr.ReadByNumberWithContext(context.Background(), 1)
	require.NoError(t, err)
	friendly, err := marshal.ConvertToJSONFriendly(got, pr.SchemaHandler)
	require.NoError(t, err)
	jsonBytes, err := json.Marshal(friendly)
	require.NoError(t, err)
	require.JSONEq(t, `[{"v":[{"element":[1,2]},{"element":[3]}]}]`, string(jsonBytes))
}

func TestReadMapAnnotatedByLogicalTypeOnly(t *testing.T) {
	type record struct {
		M map[string]int32 `parquet:"name=m, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	}
	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(context.Background(), &buf, new(record), writer.WithNP(1))
	require.NoError(t, err)
	require.NoError(t, pw.WriteWithContext(context.Background(), record{M: map[string]int32{"a": 1}}))
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	// converted_type is optional, so a writer may annotate the MAP with only its logical type.
	data := rewriteFooter(t, buf.Bytes(), func(footer *parquet.FileMetaData) {
		for _, element := range footer.Schema {
			if element.GetConvertedType() == parquet.ConvertedType_MAP {
				element.ConvertedType = nil
				element.LogicalType = &parquet.LogicalType{MAP: parquet.NewMapType()}
			}
		}
	})

	pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), nil, WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()
	got, err := pr.ReadByNumberWithContext(context.Background(), 1)
	require.NoError(t, err)
	jsonBytes, err := json.Marshal(got)
	require.NoError(t, err)
	require.JSONEq(t, `[{"m":{"a":1}}]`, string(jsonBytes))
}
