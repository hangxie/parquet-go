package writer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/types"
)

func TestCSVWriter(t *testing.T) {
	t.Run("new_csv_writer", func(t *testing.T) {
		testCases := map[string]struct {
			schema []string
			errMsg string
		}{
			"bad":   {[]string{"abc"}, "create schema from metadata"},
			"empty": {[]string{}, ""},
			"good":  {[]string{"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN"}, ""},
		}
		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				var buf bytes.Buffer
				bw := bufio.NewWriter(&buf)
				wf := writerfile.NewWriterFile(bw)
				cw, err := NewCSVWriter(tc.schema, wf)
				if tc.errMsg == "" {
					require.NoError(t, err)
					require.Equal(t, cw.np, int64(4))
					require.Equal(t, cw.pageSize, int64(8*1024))
					require.Equal(t, cw.rowGroupSize, int64(128*1024*1024))
					require.Equal(t, cw.compressionType, parquet.CompressionCodec_SNAPPY)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errMsg)
				}
			})
		}
	})

	t.Run("new_csv_writer_from_writer", func(t *testing.T) {
		schema := []string{
			"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
			"Name=Last, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, err := NewCSVWriterFromWriter(schema, bw)
		require.NoError(t, err)
		require.Equal(t, cw.np, int64(4))
		require.Equal(t, cw.pageSize, int64(8*1024))
		require.Equal(t, cw.rowGroupSize, int64(128*1024*1024))
		require.Equal(t, cw.compressionType, parquet.CompressionCodec_SNAPPY)
	})

	t.Run("new_csv_writer_with_options", func(t *testing.T) {
		schema := []string{
			"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, err := NewCSVWriterFromWriter(
			schema, bw,
			WithNP(2),
			WithPageSize(4096),
			WithCompressionCodec(parquet.CompressionCodec_GZIP),
		)
		require.NoError(t, err)
		require.Equal(t, int64(2), cw.np)
		require.Equal(t, int64(4096), cw.pageSize)
		require.Equal(t, parquet.CompressionCodec_GZIP, cw.compressionType)
	})

	t.Run("new_csv_writer_invalid_option", func(t *testing.T) {
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		_, err := NewCSVWriterFromWriter(nil, bw, WithNP(0))
		require.Error(t, err)
		require.Contains(t, err.Error(), "init CSV writer base")
	})

	t.Run("new_csv_writer_invalid_column_key_path", func(t *testing.T) {
		schema := []string{
			"Name=id, Type=INT32",
			"Name=name, Type=BYTE_ARRAY, ConvertedType=UTF8",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		_, err := NewCSVWriterFromWriter(
			schema,
			bw,
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("invalid_name", ColumnKey([]byte("abcdef0123456789"))),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "validate encryption column keys")
		require.Contains(t, err.Error(), "invalid_name")
	})

	t.Run("write_csv", func(t *testing.T) {
		testCases := map[string]struct {
			data   []*string
			errMsg string
		}{
			"empty": {[]*string{nil, nil}, ""},
			"good":  {[]*string{common.ToPtr("name"), common.ToPtr("123")}, ""},
			"bad":   {[]*string{common.ToPtr("name"), common.ToPtr("abc")}, `parse INT32 "abc"`},
		}
		schema := []string{
			"Name=Name, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
			"Name=id, Type=INT32",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, _ := NewCSVWriterFromWriter(schema, bw)

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				err := cw.WriteString(tc.data)
				if tc.errMsg == "" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errMsg)
				}
			})
		}
	})

	t.Run("write_csv_uuid", func(t *testing.T) {
		testCases := map[string]struct {
			value  string
			errMsg string
		}{
			"dashed":         {"550e8400-e29b-41d4-a716-446655440000", ""},
			"undashed":       {"550e8400e29b41d4a716446655440000", ""},
			"braced":         {"{550e8400-e29b-41d4-a716-446655440000}", ""},
			"urn":            {"urn:uuid:550e8400-e29b-41d4-a716-446655440000", ""},
			"not_a_uuid":     {"not-a-uuid", "parse UUID"},
			"truncated":      {"550e8400-e29b-41d4-a716-44665544", "parse UUID"},
			"square_bracket": {"[550e8400-e29b-41d4-a716-446655440000]", "parse UUID"},
			"raw_binary":     {"\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00", "parse UUID"},
		}
		schema := []string{"Name=Id, Type=FIXED_LEN_BYTE_ARRAY, Length=16, LogicalType=UUID"}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				var buf bytes.Buffer
				bw := bufio.NewWriter(&buf)
				cw, err := NewCSVWriterFromWriter(schema, bw)
				require.NoError(t, err)

				err = cw.WriteString([]*string{common.ToPtr(tc.value)})
				if tc.errMsg == "" {
					require.NoError(t, err)
					require.NoError(t, cw.WriteStop())
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errMsg)
				}
			})
		}
	})

	t.Run("uuid_column_length_omitted", func(t *testing.T) {
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, err := NewCSVWriterFromWriter([]string{"Name=Id, Type=FIXED_LEN_BYTE_ARRAY, LogicalType=UUID"}, bw)
		require.NoError(t, err)
		require.Equal(t, int32(16), cw.SchemaHandler.SchemaElements[1].GetTypeLength())

		require.NoError(t, cw.WriteString([]*string{common.ToPtr("550e8400-e29b-41d4-a716-446655440000")}))
		require.NoError(t, cw.WriteStop())
	})

	t.Run("uuid_column_wrong_length", func(t *testing.T) {
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		_, err := NewCSVWriterFromWriter([]string{"Name=Id, Type=FIXED_LEN_BYTE_ARRAY, Length=8, LogicalType=UUID"}, bw)
		require.Error(t, err)
		require.Contains(t, err.Error(), "LogicalType UUID requires FIXED_LEN_BYTE_ARRAY with length 16")
	})

	t.Run("write_string_wrong_type", func(t *testing.T) {
		testCases := map[string]struct {
			data   any
			errMsg string
		}{
			"string_slice":  {[]string{"name", "123"}, "WriteString: expected []*string, got []string"},
			"int_slice":     {[]int{1, 2}, "WriteString: expected []*string, got []int"},
			"nil":           {nil, "WriteString: expected []*string, got <nil>"},
			"single_string": {"name", "WriteString: expected []*string, got string"},
			"any_slice":     {[]any{"name", "123"}, "WriteString: expected []*string, got []interface {}"},
		}
		schema := []string{
			"Name=Name, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
			"Name=id, Type=INT32",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, err := NewCSVWriterFromWriter(schema, bw)
		require.NoError(t, err)

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				require.NotPanics(t, func() {
					err := cw.WriteString(tc.data)
					require.Error(t, err)
					require.Equal(t, tc.errMsg, err.Error())
				})
			})
		}
	})
}

func TestCSVWriterFixedLenByteArrayWidth(t *testing.T) {
	testCases := map[string]struct {
		value  string
		errMsg string
	}{
		"base64-width-match": {
			"YWJjZGVmZ2hpamtsbW5vcA==", "",
		},
		"decoded-width-decides": {
			// Valid base64, but it decodes to 12 bytes rather than the column's 16;
			// the 16 characters themselves are no longer a second reading.
			"0123456789abcdef",
			`FIXED_LEN_BYTE_ARRAY "0123456789abcdef" decodes to 12 bytes, column length is 16`,
		},
		"not-base64": {
			"abc", `FIXED_LEN_BYTE_ARRAY "abc" is not valid base64`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			cw, err := NewCSVWriter(
				[]string{"name=V, type=FIXED_LEN_BYTE_ARRAY, length=16"},
				writerfile.NewWriterFile(&buf), WithNP(1),
			)
			require.NoError(t, err)

			// A width the converter cannot satisfy is reported here, not once the
			// page is built from a value the caller never supplied.
			err = cw.WriteString([]*string{common.ToPtr(tc.value)})
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.NoError(t, cw.WriteStop())
		})
	}
}

// csvJSONCase is one column of the CSV/JSON equivalence sweep. Every physical value listed
// must survive both write paths identically, starting from the one rendering the read path
// produces for it.
type csvJSONCase struct {
	name   string
	tag    string // schema tag for the column, without the name
	values []any  // physical values as the reader hands them back
	issue  string // open defect that keeps this column from agreeing yet
}

// csvCell derives the CSV field for a rendered value: the JSON scalar with a string's quotes
// removed, which is what a CSV rendering of the same file writes into the cell.
func csvCell(rendered any) (string, error) {
	encoded, err := json.Marshal(rendered)
	if err != nil {
		return "", err
	}
	if len(encoded) > 0 && encoded[0] == '"' {
		var s string
		if err := json.Unmarshal(encoded, &s); err != nil {
			return "", err
		}
		return s, nil
	}
	return string(encoded), nil
}

// writeCSVColumn writes one column of CSV cells and reads the physical values back.
func writeCSVColumn(tag string, cells []string) ([]any, error) {
	var buf bytes.Buffer
	cw, err := NewCSVWriterFromWriter([]string{tag}, &buf, WithNP(1))
	if err != nil {
		return nil, fmt.Errorf("create CSV writer: %w", err)
	}
	for _, cell := range cells {
		if err := cw.WriteString([]*string{&cell}); err != nil {
			return nil, fmt.Errorf("write %q: %w", cell, err)
		}
	}
	if err := cw.WriteStop(); err != nil {
		return nil, fmt.Errorf("stop CSV writer: %w", err)
	}
	return readBackColumn(buf.Bytes(), len(cells))
}

// writeJSONColumn writes the same column as single-field JSON objects and reads it back.
func writeJSONColumn(tag string, rendered []any) ([]any, error) {
	schemaJSON := fmt.Sprintf(`{"Tag":"name=parquet-go-root","Fields":[{"Tag":%q}]}`, tag)
	var buf bytes.Buffer
	jw, err := NewJSONWriterFromWriter(schemaJSON, &buf, WithNP(1))
	if err != nil {
		return nil, fmt.Errorf("create JSON writer: %w", err)
	}
	for _, val := range rendered {
		row, err := json.Marshal(map[string]any{csvJSONColumn: val})
		if err != nil {
			return nil, err
		}
		if err := jw.Write(string(row)); err != nil {
			return nil, fmt.Errorf("write %s: %w", row, err)
		}
	}
	if err := jw.WriteStop(); err != nil {
		return nil, fmt.Errorf("stop JSON writer: %w", err)
	}
	return readBackColumn(buf.Bytes(), len(rendered))
}

// readBackColumn reads the only column of a single-column file.
func readBackColumn(raw []byte, num int) ([]any, error) {
	ctx := context.Background()
	fr := buffer.NewBufferReaderFromBytes(raw)
	pr, err := reader.NewParquetColumnReaderWithContext(ctx, fr, reader.WithNP(1))
	if err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}
	defer func() { _ = pr.ReadStopWithContext(ctx) }()
	values, _, _, err := pr.ReadColumnByIndexWithContext(ctx, 0, int64(num))
	if err != nil {
		return nil, fmt.Errorf("read column: %w", err)
	}
	return values, nil
}

const csvJSONColumn = "Col"

// TestCSVJSONEquivalence is criterion 3 of the write-path sweep: the CSV and JSON writers must
// store the same physical value for the same source value. The JSON side starts from the read
// path's rendering, so this also re-checks the exact physical round trip of
// types.TestPhysicalRoundTrip through the real writers rather than the conversion helpers.
//
// Columns where the two paths still disagree carry the issue that tracks the break; asserting
// the disagreement rather than skipping it means the case fails, and says so, the day the fix
// lands.
func TestCSVJSONEquivalence(t *testing.T) {
	float16 := func(bits uint16) string {
		return string([]byte{byte(bits), byte(bits >> 8)})
	}
	interval := func(months, days, millis uint32) string {
		b := make([]byte, common.IntervalByteLen)
		binary.LittleEndian.PutUint32(b[0:4], months)
		binary.LittleEndian.PutUint32(b[4:8], days)
		binary.LittleEndian.PutUint32(b[8:12], millis)
		return string(b)
	}

	tests := []csvJSONCase{
		{
			name:   "BOOLEAN",
			tag:    "type=BOOLEAN",
			values: []any{true, false},
		},
		{
			name:   "INT32",
			tag:    "type=INT32",
			values: []any{int32(0), int32(-1), int32(math.MaxInt32), int32(math.MinInt32)},
		},
		{
			name:   "INT64",
			tag:    "type=INT64",
			values: []any{int64(0), int64(-1), int64(math.MaxInt64), int64(math.MinInt64)},
		},
		{
			name:   "FLOAT",
			tag:    "type=FLOAT",
			values: []any{float32(0), float32(-0.5), float32(math.MaxFloat32)},
		},
		{
			name:   "DOUBLE",
			tag:    "type=DOUBLE",
			values: []any{float64(0), -0.5, math.MaxFloat64},
		},
		{
			name:   "BYTE_ARRAY unannotated",
			tag:    "type=BYTE_ARRAY",
			values: []any{"", "hello", "\x00\xff\x01\xfe", "TEST"},
		},
		{
			name:   "FIXED_LEN_BYTE_ARRAY unannotated",
			tag:    "type=FIXED_LEN_BYTE_ARRAY, length=4",
			values: []any{"\x00\xff\x01\xfe", "abcd", "\x00\x00\x00\x00"},
		},
		{
			name:   "INT96",
			tag:    "type=INT96",
			values: []any{string(make([]byte, 12)), "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"},
		},
		{
			name:   "FLOAT non-finite",
			tag:    "type=FLOAT",
			values: []any{float32(math.Inf(1)), float32(math.Inf(-1))},
		},
		{
			name:   "DOUBLE non-finite",
			tag:    "type=DOUBLE",
			values: []any{math.Inf(1), math.Inf(-1)},
		},
		{
			name:   "TIME NANOS",
			tag:    "type=INT64, logicaltype=TIME, logicaltype.unit=NANOS, logicaltype.isadjustedtoutc=true",
			values: []any{int64(0), int64(45296789012345), int64(86399999999999)},
		},
		{
			name:   "DECIMAL BYTE_ARRAY",
			tag:    "type=BYTE_ARRAY, convertedtype=DECIMAL, precision=20, scale=3",
			values: []any{"\x01", "\xff", "\x00\xde\xad\xbe\xef"},
		},
		{
			name:   "BSON",
			tag:    "type=BYTE_ARRAY, convertedtype=BSON",
			values: []any{"\x0e\x00\x00\x00\x10a\x00\x01\x00\x00\x00\x00"},
			issue:  "#417: BSON has no interpreted write form, the rendered document is not parsed back",
		},
		{
			name:   "GEOMETRY",
			tag:    "type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84",
			values: []any{"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\x00\x40"},
			issue:  "#418: geospatial has no textual write form, GeoJSON output cannot be written back",
		},
		{
			name:   "UTF8",
			tag:    "type=BYTE_ARRAY, convertedtype=UTF8",
			values: []any{"", "hello", "日本語", `{"a":1}`},
		},
		{
			name:   "ENUM",
			tag:    "type=BYTE_ARRAY, convertedtype=ENUM",
			values: []any{"ACTIVE", "TEST", "AAAA"},
		},
		{
			name:   "JSON",
			tag:    "type=BYTE_ARRAY, convertedtype=JSON",
			values: []any{`{"a":1}`, `null`, `"TEST"`},
		},
		{
			name:   "UUID",
			tag:    "type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID",
			values: []any{string(make([]byte, common.UUIDByteLen)), "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00"},
		},
		{
			name:   "FLOAT16",
			tag:    "type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16",
			values: []any{float16(0x0000), float16(0x3c00), float16(0xc900), float16(0x7bff)},
		},
		{
			name:   "DATE",
			tag:    "type=INT32, convertedtype=DATE",
			values: []any{int32(0), int32(-1), int32(19723)},
		},
		{
			name:   "TIME_MILLIS",
			tag:    "type=INT32, convertedtype=TIME_MILLIS",
			values: []any{int32(0), int32(45296789), int32(86399999)},
		},
		{
			name:   "TIME_MICROS",
			tag:    "type=INT64, convertedtype=TIME_MICROS",
			values: []any{int64(0), int64(45296789012), int64(86399999999)},
		},
		{
			name:   "TIMESTAMP_MILLIS",
			tag:    "type=INT64, convertedtype=TIMESTAMP_MILLIS",
			values: []any{int64(0), int64(-1), int64(1699999999999)},
		},
		{
			name:   "TIMESTAMP_MICROS",
			tag:    "type=INT64, convertedtype=TIMESTAMP_MICROS",
			values: []any{int64(0), int64(-1), int64(1699999999999999)},
		},
		{
			name:   "INTEGER signed 8",
			tag:    "type=INT32, logicaltype=INTEGER, logicaltype.bitwidth=8, logicaltype.issigned=true",
			values: []any{int32(0), int32(-128), int32(127)},
		},
		{
			name:   "INTEGER signed 64",
			tag:    "type=INT64, logicaltype=INTEGER, logicaltype.bitwidth=64, logicaltype.issigned=true",
			values: []any{int64(math.MinInt64), int64(math.MaxInt64)},
		},
		{
			name:   "INTEGER unsigned 8",
			tag:    "type=INT32, logicaltype=INTEGER, logicaltype.bitwidth=8, logicaltype.issigned=false",
			values: []any{int32(0), int32(127), int32(128), int32(255)},
		},
		{
			name:   "INTEGER unsigned 32",
			tag:    "type=INT32, logicaltype=INTEGER, logicaltype.bitwidth=32, logicaltype.issigned=false",
			values: []any{int32(0), int32(math.MaxInt32), int32(math.MinInt32), int32(-1)},
		},
		{
			name:   "INTEGER unsigned 64",
			tag:    "type=INT64, logicaltype=INTEGER, logicaltype.bitwidth=64, logicaltype.issigned=false",
			values: []any{int64(0), int64(math.MaxInt64), int64(math.MinInt64), int64(-1)},
		},
		{
			name:   "DECIMAL INT32",
			tag:    "type=INT32, convertedtype=DECIMAL, precision=9, scale=2",
			values: []any{int32(0), int32(-1), int32(123456789), int32(-123456789)},
		},
		{
			name:   "DECIMAL INT64",
			tag:    "type=INT64, convertedtype=DECIMAL, precision=18, scale=4",
			values: []any{int64(0), int64(-1), int64(123456789012345678)},
		},
		{
			name:   "DECIMAL FIXED_LEN_BYTE_ARRAY",
			tag:    "type=FIXED_LEN_BYTE_ARRAY, length=12, convertedtype=DECIMAL, precision=25, scale=5",
			values: []any{string(make([]byte, 12)), "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"},
		},
		{
			name:   "INTERVAL",
			tag:    "type=FIXED_LEN_BYTE_ARRAY, length=12, convertedtype=INTERVAL",
			values: []any{interval(0, 0, 0), interval(1, 2, 3), interval(math.MaxUint32, 0, 0)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag := "name=" + csvJSONColumn + ", " + tt.tag
			se, err := columnSchemaElement(tag)
			require.NoError(t, err)

			rendered := make([]any, len(tt.values))
			cells := make([]string, len(tt.values))
			for i, val := range tt.values {
				rendered[i] = types.ConvertToJSONType(val, se)
				cells[i], err = csvCell(rendered[i])
				require.NoError(t, err)
			}

			fromJSON, jsonErr := writeJSONColumn(tag, rendered)
			fromCSV, csvErr := writeCSVColumn(tag, cells)

			if tt.issue != "" {
				agrees := jsonErr == nil && csvErr == nil &&
					reflect.DeepEqual(tt.values, fromJSON) && reflect.DeepEqual(tt.values, fromCSV)
				require.False(t, agrees, "%s\nthis column now agrees; drop the issue from this case", tt.issue)
				return
			}

			require.NoError(t, jsonErr)
			require.NoError(t, csvErr)
			require.Equal(t, tt.values, fromJSON, "JSON write path")
			require.Equal(t, tt.values, fromCSV, "CSV write path")
		})
	}
}

// columnSchemaElement returns the schema element a one-column tag builds, which is what the
// read path needs to render the column's values.
func columnSchemaElement(tag string) (*parquet.SchemaElement, error) {
	sh, err := schema.NewSchemaHandlerFromMetadata([]string{tag})
	if err != nil {
		return nil, err
	}
	return sh.SchemaElements[1], nil
}

// TestInterpretedModeRefusesUnsupported pins the other half of #418: a geospatial or BSON
// value the write path cannot parse is reported instead of being stored as the bytes of
// the text it was given.
func TestInterpretedModeRefusesUnsupported(t *testing.T) {
	tests := map[string]struct {
		tag   string
		value string
	}{
		"GEOMETRY":  {"type=BYTE_ARRAY, logicaltype=GEOMETRY", "POINT (1 2)"},
		"GEOGRAPHY": {"type=BYTE_ARRAY, logicaltype=GEOGRAPHY", "POINT (1 2)"},
		"BSON":      {"type=BYTE_ARRAY, convertedtype=BSON", `{"a":1}`},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tag := "name=" + csvJSONColumn + ", " + tt.tag

			_, err := writeCSVColumn(tag, []string{tt.value})
			require.ErrorContains(t, err, name)
			require.ErrorContains(t, err, "not supported yet")

			_, err = writeJSONColumn(tag, []any{tt.value})
			require.ErrorContains(t, err, name)
		})
	}
}

// TestTextLogicalTypeStaysVerbatim covers a text column annotated only with a logical
// type, which both writers previously read as base64: "hello" failed the write and "TEST"
// was stored as the three bytes it decodes to.
func TestTextLogicalTypeStaysVerbatim(t *testing.T) {
	for _, logicalType := range []string{"STRING", "ENUM", "JSON"} {
		t.Run(logicalType, func(t *testing.T) {
			tag := "name=" + csvJSONColumn + ", type=BYTE_ARRAY, logicaltype=" + logicalType
			values := []any{"hello", "TEST", "AAAA"}

			fromCSV, err := writeCSVColumn(tag, []string{"hello", "TEST", "AAAA"})
			require.NoError(t, err)
			require.Equal(t, values, fromCSV, "CSV write path")

			fromJSON, err := writeJSONColumn(tag, values)
			require.NoError(t, err)
			require.Equal(t, values, fromJSON, "JSON write path")
		})
	}
}
