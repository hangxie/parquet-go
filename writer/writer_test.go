package writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

func readColumnIndex(pf source.ParquetFileReader, offset int64) (*parquet.ColumnIndex, error) {
	colIdx := parquet.NewColumnIndex()
	tpf := thrift.NewTCompactProtocolFactoryConf(nil)
	triftReader, err := source.ConvertToThriftReader(pf, offset)
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
		err = pr.Read(&actualRows)
		require.NoError(t, err)

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

func TestNewParquetWriter_SchemaVariants(t *testing.T) {
	tests := map[string]struct {
		obj     any
		wantErr bool
	}{
		"invalid_json_schema_string": {
			obj:     `{"invalid": json}`,
			wantErr: true,
		},
		"valid_json_schema_string": {
			obj: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
					{"Tag": "name=age, type=INT32"}
				]
			}`,
			wantErr: false,
		},
		"nil_object": {
			obj:     nil,
			wantErr: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			fw := writerfile.NewWriterFile(&buf)
			pw, err := NewParquetWriter(fw, tt.obj, WithNP(1))
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "unmarshal json schema")
			} else {
				require.NoError(t, err)
				require.NotNil(t, pw)
			}
		})
	}
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
		"row_group_size_zero":  {[]WriterOption{WithRowGroupSize(0)}, "WithRowGroupSize: value must be positive"},
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
		"valid_custom":   {[]WriterOption{WithNP(2), WithPageSize(4096), WithRowGroupSize(1024), WithDataPageVersion(2)}, ""},
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

func TestNewParquetWriterFromWriter(t *testing.T) {
	type TestStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age  int32  `parquet:"name=age, type=INT32"`
	}

	t.Run("successful_creation", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriterFromWriter(&buf, new(TestStruct), WithNP(1))
		require.NoError(t, err)
		require.NotNil(t, pw)

		data := TestStruct{Name: "Alice", Age: 30}
		err = pw.Write(data)
		require.NoError(t, err)

		err = pw.WriteStop()
		require.NoError(t, err)

		require.Greater(t, buf.Len(), 0)
	})

	t.Run("invalid_object", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriterFromWriter(&buf, nil, WithNP(1))
		if err != nil {
			require.Error(t, err)
			require.Nil(t, pw)
		} else {
			require.NotNil(t, pw)
		}
	})
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

func TestNewParquetWriter_SchemaHandlerInput(t *testing.T) {
	type S struct {
		ID   int32  `parquet:"name=id, type=INT32"`
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(S))
	require.NoError(t, err)

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, sh, WithNP(1))
	require.NoError(t, err)
	require.NotNil(t, pw)
	require.NoError(t, pw.WriteStop())
}

// TestNewParquetWriter_SchemaElementsInput covers the []*parquet.SchemaElement branch.
func TestNewParquetWriter_SchemaElementsInput(t *testing.T) {
	type S struct {
		ID int32 `parquet:"name=id, type=INT32"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(S))
	require.NoError(t, err)

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, sh.SchemaElements, WithNP(1))
	require.NoError(t, err)
	require.NotNil(t, pw)
	require.NoError(t, pw.WriteStop())
}

// TestNewParquetWriter_InvalidStructInput covers the NewSchemaHandlerFromStruct error branch.
func TestNewParquetWriter_InvalidStructInput(t *testing.T) {
	type BadStruct struct {
		ID int32 `parquet:"name=id, type=INVALID_TYPE"`
	}
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	_, err := NewParquetWriter(fw, new(BadStruct), WithNP(1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "build schema handler")
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
	require.NoError(t, pr.Read(&got))
	require.Equal(t, want, got)
	require.Equal(t, parquet.CompressionCodec_GZIP, pr.Footer.RowGroups[0].Columns[0].MetaData.GetCodec())
}

func TestWriterOptionIsOpaque(t *testing.T) {
	t.Parallel()

	optionType := reflect.TypeOf((*WriterOption)(nil)).Elem()
	require.NotEqual(t, reflect.Func, optionType.Kind())
}
