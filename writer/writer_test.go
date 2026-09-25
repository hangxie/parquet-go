package writer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

func TestParquetWriterContextCancellation(t *testing.T) {
	type row struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}

	tests := []struct {
		name string
		run  func(context.Context, source.ParquetFileWriter) error
	}{
		{
			name: "constructor",
			run: func(ctx context.Context, file source.ParquetFileWriter) error {
				_, err := NewParquetWriterWithContext(ctx, file, new(row), WithNP(1))
				return err
			},
		},
		{
			name: "write",
			run: func(ctx context.Context, file source.ParquetFileWriter) error {
				pw, err := NewParquetWriter(file, new(row), WithNP(1))
				require.NoError(t, err)
				return pw.WriteWithContext(ctx, row{Value: 1})
			},
		},
		{
			name: "flush",
			run: func(ctx context.Context, file source.ParquetFileWriter) error {
				pw, err := NewParquetWriter(file, new(row), WithNP(1))
				require.NoError(t, err)
				return pw.FlushWithContext(ctx, true)
			},
		},
		{
			name: "stop",
			run: func(ctx context.Context, file source.ParquetFileWriter) error {
				pw, err := NewParquetWriter(file, new(row), WithNP(1))
				require.NoError(t, err)
				return pw.WriteStopWithContext(ctx)
			},
		},
		{
			name: "CSV constructor",
			run: func(ctx context.Context, file source.ParquetFileWriter) error {
				_, err := NewCSVWriterWithContext(ctx, nil, file)
				return err
			},
		},
		{
			name: "JSON constructor",
			run: func(ctx context.Context, file source.ParquetFileWriter) error {
				_, err := NewJSONWriterWithContext(ctx, "", file)
				return err
			},
		},
		{
			name: "Arrow constructor",
			run: func(ctx context.Context, file source.ParquetFileWriter) error {
				_, err := NewArrowWriterWithContext(ctx, nil, file)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := tt.run(ctx, writerfile.NewWriterFile(new(bytes.Buffer)))
			require.ErrorIs(t, err, context.Canceled)
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewParquetWriterFromWriterWithContext(ctx, new(bytes.Buffer), new(row))
	require.ErrorIs(t, err, context.Canceled)
	_, err = NewCSVWriterFromWriterWithContext(ctx, nil, new(bytes.Buffer))
	require.ErrorIs(t, err, context.Canceled)
	_, err = NewJSONWriterFromWriterWithContext(ctx, "", new(bytes.Buffer))
	require.ErrorIs(t, err, context.Canceled)
	_, err = NewArrowWriterFromWriterWithContext(ctx, nil, new(bytes.Buffer))
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, new(CSVWriter).WriteStringWithContext(ctx, nil), context.Canceled)
	require.ErrorIs(t, new(ArrowWriter).WriteArrowWithContext(ctx, nil), context.Canceled)
	nilCtx := map[string]context.Context{}["missing"]
	_, err = NewParquetWriterWithContext(nilCtx, writerfile.NewWriterFile(new(bytes.Buffer)), new(row))
	require.ErrorContains(t, err, "context is nil")
}

func TestWriteStopWithContextCanceledFinalizesFile(t *testing.T) {
	type row struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}

	file := buffer.NewBufferWriter()
	pw, err := NewParquetWriter(file, new(row), WithNP(1))
	require.NoError(t, err)
	require.NoError(t, pw.Write(row{Value: 42}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, pw.WriteStopWithContext(ctx), context.Canceled)
	require.NoError(t, pw.WriteStop())

	//nolint:staticcheck
	pr, err := reader.NewParquetReader(buffer.NewBufferReaderFromBytes(file.Bytes()), new(row), reader.WithNP(1))
	require.NoError(t, err)
	//nolint:staticcheck
	defer func() { require.NoError(t, pr.ReadStop()) }()
	rows := make([]row, 1)
	//nolint:staticcheck
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, int64(42), rows[0].Value)
}

type contextTrackingWriter struct {
	bytes.Buffer
	contexts []context.Context
}

func (w *contextTrackingWriter) WriteContext(ctx context.Context, p []byte) (int, error) {
	w.contexts = append(w.contexts, ctx)
	return w.Write(p)
}

func (w *contextTrackingWriter) Close() error { return nil }

func (w *contextTrackingWriter) Create(string) (source.ParquetFileWriter, error) { return w, nil }

func TestParquetWriterWithContext(t *testing.T) {
	type row struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	type contextKey struct{}

	file := new(contextTrackingWriter)
	constructorCtx := context.WithValue(context.Background(), contextKey{}, "constructor")
	pw, err := NewParquetWriterWithContext(constructorCtx, file, new(row), WithNP(1))
	require.NoError(t, err)
	require.NotEmpty(t, file.contexts)
	require.Equal(t, "constructor", file.contexts[0].Value(contextKey{}))

	file.contexts = nil
	require.NoError(t, pw.Write(row{Value: 1}))
	require.Equal(t, "constructor", pw.context().Value(contextKey{}))

	file.contexts = nil
	stopCtx := context.WithValue(context.Background(), contextKey{}, "stop")
	require.NoError(t, pw.WriteStopWithContext(stopCtx))
	require.NotEmpty(t, file.contexts)
	for _, got := range file.contexts {
		require.Equal(t, "stop", got.Value(contextKey{}))
	}
}

func readColumnIndex(pf source.ParquetFileReader, offset int64) (*parquet.ColumnIndex, error) {
	colIdx := parquet.NewColumnIndex()
	tpf := thrift.NewTCompactProtocolFactoryConf(nil)
	triftReader, err := source.ConvertToThriftReaderWithContext(context.Background(), pf, offset)
	if err != nil {
		return nil, err
	}
	protocol := tpf.GetProtocol(triftReader)
	err = colIdx.Read(context.Background(), protocol)
	if err != nil {
		return nil, err
	}
	return colIdx, nil
}

// Helper function to create a parquet writer with buffer for testing
func createTestParquetWriter(schema any, opts ...WriterOption) (*ParquetWriter, *bytes.Buffer, error) {
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, schema, opts...)
	return pw, &buf, err
}

// Helper function to create a parquet reader from buffer
func createTestParquetReader(buf []byte, schema any, opts ...reader.ReaderOption) (*reader.ParquetReader, source.ParquetFileReader, error) {
	pf := buffer.NewBufferReaderFromBytesNoAlloc(buf)
	//nolint:staticcheck
	pr, err := reader.NewParquetReader(pf, schema, opts...)
	return pr, pf, err
}

type test struct {
	ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

var errWrite = errors.New("test error")

type invalidFileWriter struct {
	source.ParquetFileWriter
}

func (m *invalidFileWriter) Write(data []byte) (n int, err error) {
	return 0, errWrite
}

// firstSuccessThenFail writes succeed once (PAR1 magic) then always fail.
type firstSuccessThenFail struct {
	written bool
}

func (w *firstSuccessThenFail) Write(data []byte) (int, error) {
	if w.written {
		return 0, errWrite
	}
	w.written = true
	return len(data), nil
}

func (w *firstSuccessThenFail) Close() error { return nil }
func (w *firstSuccessThenFail) Create(_ string) (source.ParquetFileWriter, error) {
	return w, nil
}

func TestParquetWriter(t *testing.T) {
	t.Run("double_write_stop", func(t *testing.T) {
		pw, buf, err := createTestParquetWriter(new(test), WithNP(1))
		require.NoError(t, err)

		testData := []test{
			{ColA: "cola_0", ColB: "colb_0"},
			{ColA: "cola_1", ColB: "colb_1"},
			{ColA: "cola_2", ColB: "colb_2"},
		}

		for _, record := range testData {
			err = pw.Write(record)
			require.NoError(t, err)
		}

		err = pw.WriteStop()
		require.NoError(t, err)

		err = pw.WriteStop()
		require.NoError(t, err)

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(test), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		numRows := int(pr.GetNumRows())
		require.Equal(t, len(testData), numRows)

		actualRows := make([]test, numRows)
		//nolint:staticcheck
		err = pr.Read(&actualRows)
		require.NoError(t, err)

		//nolint:staticcheck
		_ = pr.ReadStop()
	})

	t.Run("set_schema_handler_from_json_valid", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(struct{}), WithNP(1))
		require.NoError(t, err)

		jsonSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
				{"Tag": "name=age, type=INT32"}
			]
		}`

		err = pw.SetSchemaHandlerFromJSON(jsonSchema)
		require.NoError(t, err)

		require.NotNil(t, pw.SchemaHandler)
		require.Greater(t, len(pw.Footer.Schema), 0)
	})

	t.Run("set_schema_handler_from_json_resets_state", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(struct{}), WithNP(1))
		require.NoError(t, err)

		// Simulate that encoding validation has already run
		pw.encodingsValidated = true

		jsonSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}
			]
		}`
		err = pw.SetSchemaHandlerFromJSON(jsonSchema)
		require.NoError(t, err)

		// encodingsValidated must be reset so the new schema gets validated on next Write
		require.False(t, pw.encodingsValidated)
	})

	t.Run("set_schema_handler_from_json_invalid", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(struct{}), WithNP(1))
		require.NoError(t, err)

		invalidJSON := `{"invalid": json}`
		err = pw.SetSchemaHandlerFromJSON(invalidJSON)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unmarshal json schema")
	})

	t.Run("set_schema_handler_from_json_empty", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(struct{}), WithNP(1))
		require.NoError(t, err)

		err = pw.SetSchemaHandlerFromJSON("")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unmarshal json schema")
	})

	t.Run("set_schema_handler_from_json_invalid_compression", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(struct{}), WithNP(1))
		require.NoError(t, err)

		jsonSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8, compression=GZIP:99"}
			]
		}`
		err = pw.SetSchemaHandlerFromJSON(jsonSchema)
		require.Error(t, err)
		require.Contains(t, err.Error(), "build column compressors")
	})

	t.Run("write_stop_race_condition_on_error", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewJSONWriter(`{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=x, type=INT64"}]}`, fw)
		require.NoError(t, err)

		for i := range 10 {
			entry := fmt.Sprintf(`{"not-x":%d}`, i)
			require.NoError(t, pw.Write(entry))
		}
		stopErr := pw.WriteStop()
		require.Error(t, stopErr)
		require.Contains(t, stopErr.Error(), "nil value encountered for REQUIRED field")

		stopErr = pw.WriteStop()
		require.Error(t, stopErr)
		require.Contains(t, stopErr.Error(), "previous WriteStop failed; file is incomplete")
	})

	t.Run("zero_rows", func(t *testing.T) {
		type TestSchema struct {
			ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
			ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		}

		pw, buf, err := createTestParquetWriter(new(TestSchema), WithNP(1))
		require.NoError(t, err)

		err = pw.WriteStop()
		require.NoError(t, err)

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestSchema), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		require.Equal(t, int64(0), pr.GetNumRows())
		require.Equal(t, int32(1), pr.Footer.Version)
		require.Equal(t, "github.com/hangxie/parquet-go/v3", *pr.Footer.CreatedBy)
	})

	t.Run("invalid_file", func(t *testing.T) {
		pw, err := NewParquetWriter(&invalidFileWriter{}, new(test), WithNP(1))
		require.Nil(t, pw)
		require.ErrorIs(t, err, errWrite)
	})
}

func TestOptionValidation(t *testing.T) {
	type S struct {
		ID int32 `parquet:"name=id, type=INT32"`
	}

	tests := map[string]struct {
		opts   []WriterOption
		errMsg string
	}{
		"np_zero":              {[]WriterOption{WithNP(0)}, "WithNP: value must be positive"},
		"np_negative":          {[]WriterOption{WithNP(-1)}, "WithNP: value must be positive"},
		"page_size_zero":       {[]WriterOption{WithPageSize(0)}, "WithPageSize: value must be positive"},
		"page_size_negative":   {[]WriterOption{WithPageSize(-100)}, "WithPageSize: value must be positive"},
		"truncate_length_zero": {[]WriterOption{WithBinaryMinMaxTruncateLength(0)}, "WithBinaryMinMaxTruncateLength: value must be positive"},
		"truncate_length_negative": {
			[]WriterOption{WithBinaryMinMaxTruncateLength(-1)},
			"WithBinaryMinMaxTruncateLength: value must be positive",
		},
		"row_group_size_zero":  {[]WriterOption{WithRowGroupSize(0)}, "WithRowGroupSize: value must be positive"},
		"dictionary_size_zero": {[]WriterOption{WithMaxDictionarySize(0)}, "WithMaxDictionarySize: value must be positive"},
		"dictionary_size_negative": {
			[]WriterOption{WithMaxDictionarySize(-1)},
			"WithMaxDictionarySize: value must be positive",
		},
		"data_page_version_0":  {[]WriterOption{WithDataPageVersion(0)}, "WithDataPageVersion: value must be 1 or 2"},
		"data_page_version_3":  {[]WriterOption{WithDataPageVersion(3)}, "WithDataPageVersion: value must be 1 or 2"},
		"data_page_version_-1": {[]WriterOption{WithDataPageVersion(-1)}, "WithDataPageVersion: value must be 1 or 2"},
		"compression_level_unsupported": {[]WriterOption{
			WithCompressionLevel(parquet.CompressionCodec_SNAPPY, 5),
		}, "WithCompressionLevel: codec SNAPPY does not support compression levels"},
		"compression_level_invalid": {[]WriterOption{
			WithCompressionLevel(parquet.CompressionCodec_GZIP, 100),
		}, "WithCompressionLevel: set compression level for GZIP"},
		"valid_defaults": {nil, ""},
		"valid_custom": {
			[]WriterOption{WithNP(2), WithPageSize(4096), WithRowGroupSize(1024), WithMaxDictionarySize(512), WithBinaryMinMaxTruncateLength(32), WithDataPageVersion(2)},
			"",
		},
		"valid_compression_level": {[]WriterOption{
			WithCompressionCodec(parquet.CompressionCodec_GZIP),
			WithCompressionLevel(parquet.CompressionCodec_GZIP, 1),
		}, ""},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			fw := writerfile.NewWriterFile(&buf)
			pw, err := NewParquetWriter(fw, new(S), tt.opts...)
			if tt.errMsg != "" {
				require.Nil(t, pw)
				require.ErrorContains(t, err, tt.errMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, pw)
			}
		})
	}
}

func TestOptionValidation_NoPartialOutput(t *testing.T) {
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	_, err := NewParquetWriter(fw, new(test), WithNP(0))
	require.Error(t, err)
	require.Contains(t, err.Error(), "value must be positive")
	// Invalid option must not produce any output (no PAR1 header written)
	require.Equal(t, 0, buf.Len())
}

// TestWrite_PointerInput covers the reflect pointer-dereference branch in Write.

func TestWriterCompressionLevel(t *testing.T) {
	type Entry struct {
		ID   int32  `parquet:"name=id, type=INT32"`
		Text string `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	pw, buf, err := createTestParquetWriter(
		new(Entry),
		WithNP(1),
		WithCompressionCodec(parquet.CompressionCodec_GZIP),
		WithCompressionLevel(parquet.CompressionCodec_GZIP, 1),
	)
	require.NoError(t, err)
	require.NotNil(t, pw.compressor)

	want := make([]Entry, 0, 128)
	for i := range 128 {
		row := Entry{
			ID:   int32(i),
			Text: fmt.Sprintf("compressible row %03d with repeated repeated repeated payload", i%8),
		}
		want = append(want, row)
		require.NoError(t, pw.Write(row))
	}
	require.NoError(t, pw.WriteStop())

	pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pf.Close())
	}()

	got := make([]Entry, len(want))
	//nolint:staticcheck
	require.NoError(t, pr.Read(&got))
	require.Equal(t, want, got)
	require.Equal(t, parquet.CompressionCodec_GZIP, pr.Footer.RowGroups[0].Columns[0].MetaData.GetCodec())
}

func TestParquetWriter_PerColumnCompressionLevel(t *testing.T) {
	t.Parallel()

	type Row struct {
		Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, compression=GZIP:5"`
		Value int32  `parquet:"name=value, type=INT32, compression=ZSTD:3"`
	}

	pw, buf, err := createTestParquetWriter(new(Row), WithNP(1))
	require.NoError(t, err)

	// Column compressors should be populated for columns with explicit levels
	require.NotNil(t, pw.columnCompressors, "columnCompressors should be built")
	require.Len(t, pw.columnCompressors, 2)

	want := []Row{
		{Name: "alice", Value: 1},
		{Name: "bob", Value: 2},
	}
	for _, r := range want {
		require.NoError(t, pw.Write(r))
	}
	require.NoError(t, pw.WriteStop())

	pr, pf, err := createTestParquetReader(buf.Bytes(), new(Row), reader.WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pf.Close()) }()

	// Verify correct codecs are recorded in the footer
	require.Equal(t, parquet.CompressionCodec_GZIP, pr.Footer.RowGroups[0].Columns[0].MetaData.GetCodec())
	require.Equal(t, parquet.CompressionCodec_ZSTD, pr.Footer.RowGroups[0].Columns[1].MetaData.GetCodec())

	// Round-trip validation
	got := make([]Row, len(want))
	//nolint:staticcheck
	require.NoError(t, pr.Read(&got))
	require.Equal(t, want, got)
}

func TestParquetWriter_PerColumnCompressionLevelInvalidLevel(t *testing.T) {
	t.Parallel()

	// An invalid compression level (e.g., GZIP level 99) should fail at writer creation
	type Row struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, compression=GZIP:99"`
	}

	_, _, err := createTestParquetWriter(new(Row), WithNP(1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "build column compressor")
}

func TestParquetWriter_UnknownLogicalType(t *testing.T) {
	type Row struct {
		ID      int32  `parquet:"name=id, type=INT32"`
		NullCol *int32 `parquet:"name=null_col, type=INT32, logicaltype=UNKNOWN, repetitiontype=OPTIONAL"`
	}

	pw, buf, err := createTestParquetWriter(new(Row), WithNP(1))
	require.NoError(t, err)

	require.NoError(t, pw.Write(Row{ID: 1, NullCol: nil}))
	require.NoError(t, pw.Write(Row{ID: 2, NullCol: nil}))
	require.NoError(t, pw.WriteStop())

	pr, pf, err := createTestParquetReader(buf.Bytes(), new(Row), reader.WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pf.Close()) }()

	// The footer schema must preserve the UNKNOWN logical type annotation.
	// Reader footers expose the external Parquet field name.
	var unknownElem *parquet.SchemaElement
	for _, se := range pr.Footer.Schema {
		if se.Name == "null_col" {
			unknownElem = se
			break
		}
	}
	require.NotNil(t, unknownElem, "null_col schema element not found in footer")
	require.NotNil(t, unknownElem.LogicalType, "null_col must have a LogicalType in the footer")
	require.NotNil(t, unknownElem.LogicalType.UNKNOWN, "null_col LogicalType must be UNKNOWN")

	// Round-trip: read rows back and confirm all values are nil.
	rows := make([]Row, int(pr.GetNumRows()))
	//nolint:staticcheck
	require.NoError(t, pr.Read(&rows))
	for _, row := range rows {
		require.Nil(t, row.NullCol)
	}
}

// TestDeprecatedMinMaxBySortOrder verifies that the deprecated Statistics
// Min/Max fields (PARQUET-251) are only written for columns whose sort order is
// signed. The current MinValue/MaxValue fields are written for every column.
func TestDeprecatedMinMaxBySortOrder(t *testing.T) {
	type Row struct {
		Signed   int64  `parquet:"name=signed, type=INT64"`
		Unsigned uint64 `parquet:"name=unsigned, type=INT64, convertedtype=UINT_64"`
		Str      string `parquet:"name=str, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	pw, buf, err := createTestParquetWriter(new(Row), WithNP(1))
	require.NoError(t, err)
	for i := range 3 {
		require.NoError(t, pw.Write(Row{
			Signed:   int64(i) - 1, // includes a negative value
			Unsigned: uint64(i),
			Str:      fmt.Sprintf("v%d", i),
		}))
	}
	require.NoError(t, pw.WriteStop())

	pr, pf, err := createTestParquetReader(buf.Bytes(), new(Row), reader.WithNP(1))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pf.Close())
	}()

	require.Len(t, pr.Footer.RowGroups, 1)
	columns := pr.Footer.RowGroups[0].Columns

	byName := func(name string) *parquet.Statistics {
		for _, c := range columns {
			if c.MetaData != nil && len(c.MetaData.PathInSchema) > 0 &&
				c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1] == name {
				return c.MetaData.Statistics
			}
		}
		t.Fatalf("column %q not found", name)
		return nil
	}

	// Signed column: both current and deprecated fields are populated.
	signed := byName("signed")
	require.NotNil(t, signed)
	require.NotNil(t, signed.MinValue)
	require.NotNil(t, signed.MaxValue)
	require.NotNil(t, signed.Min)
	require.NotNil(t, signed.Max)
	require.Equal(t, signed.MinValue, signed.Min)
	require.Equal(t, signed.MaxValue, signed.Max)

	// Unsigned integer and UTF8 columns: only the current fields are populated;
	// the deprecated signed Min/Max are omitted.
	for _, name := range []string{"unsigned", "str"} {
		stats := byName(name)
		require.NotNil(t, stats, name)
		require.NotNil(t, stats.MinValue, name)
		require.NotNil(t, stats.MaxValue, name)
		require.Nil(t, stats.Min, name)
		require.Nil(t, stats.Max, name)
	}
}

func TestWriteFixedLenByteArrayValueWidth(t *testing.T) {
	type uuidRow struct {
		ID string `parquet:"name=ID, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"`
	}
	type dictRow struct {
		ID string `parquet:"name=ID, type=FIXED_LEN_BYTE_ARRAY, length=16, encoding=RLE_DICTIONARY"`
	}

	testCases := map[string]struct {
		obj    any
		value  string
		opts   []WriterOption
		errMsg string
	}{
		"exact":              {new(uuidRow), "0123456789abcdef", nil, ""},
		"too-long":           {new(uuidRow), "0123456789abcdefGHIJ", nil, "value of length 20 does not match column length 16"},
		"too-short":          {new(uuidRow), "abc", nil, "value of length 3 does not match column length 16"},
		"empty":              {new(uuidRow), "", nil, "value of length 0 does not match column length 16"},
		"page-v2-exact":      {new(uuidRow), "0123456789abcdef", []WriterOption{WithDataPageVersion(2)}, ""},
		"page-v2-too-short":  {new(uuidRow), "abc", []WriterOption{WithDataPageVersion(2)}, "value of length 3 does not match column length 16"},
		"dictionary-exact":   {new(dictRow), "0123456789abcdef", nil, ""},
		"dictionary-short":   {new(dictRow), "abc", nil, "value of length 3 does not match column length 16"},
		"dictionary-v2-long": {new(dictRow), "0123456789abcdefGHIJ", []WriterOption{WithDataPageVersion(2)}, "value of length 20 does not match column length 16"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pw, buf, err := createTestParquetWriter(tc.obj, append([]WriterOption{WithNP(1)}, tc.opts...)...)
			require.NoError(t, err)

			// Pages are built at flush time, so a wrong-width value is reported by
			// Write or WriteStop depending on when the row group is flushed.
			err = errors.Join(pw.Write(rowWithValue(tc.obj, tc.value)), pw.WriteStop())
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)

			pr, pf, err := createTestParquetReader(buf.Bytes(), tc.obj, reader.WithNP(1))
			require.NoError(t, err)
			defer func() {
				require.NoError(t, pf.Close())
			}()
			require.Equal(t, int64(1), pr.GetNumRows())
			rows := reflect.New(reflect.SliceOf(reflect.TypeOf(tc.obj).Elem()))
			rows.Elem().Set(reflect.MakeSlice(rows.Type().Elem(), 1, 1))
			require.NoError(t, pr.ReadWithContext(context.Background(), rows.Interface()))
			require.Equal(t, tc.value, rows.Elem().Index(0).Field(0).String())
		})
	}
}

// rowWithValue returns a row of obj's element type with its single string field set to value.
func rowWithValue(obj any, value string) any {
	row := reflect.New(reflect.TypeOf(obj).Elem()).Elem()
	row.Field(0).SetString(value)
	return row.Interface()
}

// TestWriteIsDeterministic requires identical input to produce identical bytes. It guards
// the file rather than any one map-ordered field, so the next such field fails here too.
func TestWriteIsDeterministic(t *testing.T) {
	type GeometryEntry struct {
		Geom string `parquet:"name=geom, type=BYTE_ARRAY, logicaltype=GEOMETRY"`
	}

	le := binary.LittleEndian
	point := func(x, y float64) string {
		b := le.AppendUint32([]byte{1}, 1)
		b = le.AppendUint64(b, math.Float64bits(x))
		return string(le.AppendUint64(b, math.Float64bits(y)))
	}
	lineString := func(pts ...[2]float64) string {
		b := le.AppendUint32([]byte{1}, 2)
		b = le.AppendUint32(b, uint32(len(pts)))
		for _, p := range pts {
			b = le.AppendUint64(b, math.Float64bits(p[0]))
			b = le.AppendUint64(b, math.Float64bits(p[1]))
		}
		return string(b)
	}
	polygon := func(ring ...[2]float64) string {
		b := le.AppendUint32([]byte{1}, 3)
		b = le.AppendUint32(b, 1)
		b = le.AppendUint32(b, uint32(len(ring)))
		for _, p := range ring {
			b = le.AppendUint64(b, math.Float64bits(p[0]))
			b = le.AppendUint64(b, math.Float64bits(p[1]))
		}
		return string(b)
	}

	// Three geometry types, so the chunk's type list has more than one possible order.
	entries := []GeometryEntry{
		{Geom: polygon([2]float64{0, 0}, [2]float64{1, 0}, [2]float64{1, 1}, [2]float64{0, 0})},
		{Geom: point(10.5, 20.3)},
		{Geom: lineString([2]float64{-170, -80}, [2]float64{30, 40})},
		{Geom: point(-1, -2)},
	}

	write := func() []byte {
		pw, buf, err := createTestParquetWriter(new(GeometryEntry), WithNP(1))
		require.NoError(t, err)
		for _, e := range entries {
			require.NoError(t, pw.Write(e))
		}
		require.NoError(t, pw.WriteStop())
		return buf.Bytes()
	}

	first := write()
	require.NotEmpty(t, first)
	// One comparison catches a varying list; several make it near-certain.
	for i := 1; i < 8; i++ {
		require.Equal(t, first, write(), "run %d differs from the first", i)
	}
}
