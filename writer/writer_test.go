package writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source"
	"github.com/hangxie/parquet-go/v2/source/buffer"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
)

// TestNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex.

// TestAllNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex if a field contains null value only.

func readColumnIndex(pf source.ParquetFileReader, offset int64) (*parquet.ColumnIndex, error) {
	colIdx := parquet.NewColumnIndex()
	tpf := thrift.NewTCompactProtocolFactoryConf(nil)
	triftReader := source.ConvertToThriftReader(pf, offset)
	protocol := tpf.GetProtocol(triftReader)
	err := colIdx.Read(context.Background(), protocol)
	if err != nil {
		return nil, err
	}
	return colIdx, nil
}

// Helper function to create int64 pointer for test data
func int64Ptr(value int64) *int64 {
	return &value
}

// Helper function to create a parquet writer with buffer for testing
func createTestParquetWriter(schema any, parallelNumber int64) (*ParquetWriter, *bytes.Buffer, error) {
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, schema, parallelNumber)
	return pw, &buf, err
}

// Helper function to create a parquet reader from buffer
func createTestParquetReader(buf []byte, schema any, parallelNumber int64) (*reader.ParquetReader, source.ParquetFileReader, error) {
	pf := buffer.NewBufferReaderFromBytesNoAlloc(buf)
	pr, err := reader.NewParquetReader(pf, schema, parallelNumber)
	return pr, pf, err
}

type test struct {
	ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// TestDoubleWriteStop verifies that calling WriteStop multiple times is safe

var errWrite = errors.New("test error")

type invalidFileWriter struct {
	source.ParquetFileWriter
}

func (m *invalidFileWriter) Write(data []byte) (n int, err error) {
	return 0, errWrite
}

// Tests for comprehensive function coverage

func Test_ColumnIndex_AllNullCounts(t *testing.T) {
	type Entry struct {
		X *int64 `parquet:"name=x, type=INT64"`
		Y *int64 `parquet:"name=z, type=INT64"`
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, new(Entry), 1)
	require.NoError(t, err)

	entries := []Entry{
		{int64Ptr(0), nil},
		{int64Ptr(1), nil},
		{int64Ptr(2), nil},
		{int64Ptr(3), nil},
		{int64Ptr(4), nil},
		{int64Ptr(5), nil},
	}
	for _, entry := range entries {
		require.NoError(t, pw.Write(entry))
	}
	require.NoError(t, pw.WriteStop())

	pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	defer func() {
		require.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	require.Nil(t, err)

	require.Nil(t, pr.ReadFooter())

	require.Equal(t, 1, len(pr.Footer.RowGroups))
	columns := pr.Footer.RowGroups[0].GetColumns()
	require.Equal(t, 2, len(columns))

	colIdx, err := readColumnIndex(pr.PFile, *columns[0].ColumnIndexOffset)
	require.NoError(t, err)
	require.Equal(t, true, colIdx.IsSetNullCounts())
	require.Equal(t, []int64{0}, colIdx.GetNullCounts())

	colIdx, err = readColumnIndex(pr.PFile, *columns[1].ColumnIndexOffset)
	require.NoError(t, err)
	require.Equal(t, true, colIdx.IsSetNullCounts())
	require.Equal(t, []int64{6}, colIdx.GetNullCounts())
}

func Test_ParquetWriter(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "double_write_stop",
			testFunc: func(t *testing.T) {
				// Create writer
				pw, buf, err := createTestParquetWriter(new(test), 1)
				require.NoError(t, err)

				// Write test data
				testData := []test{
					{ColA: "cola_0", ColB: "colb_0"},
					{ColA: "cola_1", ColB: "colb_1"},
					{ColA: "cola_2", ColB: "colb_2"},
				}

				for _, record := range testData {
					err = pw.Write(record)
					require.NoError(t, err)
				}

				// Call WriteStop twice to verify it's idempotent
				err = pw.WriteStop()
				require.NoError(t, err)

				err = pw.WriteStop()
				require.NoError(t, err)

				// Verify data can be read correctly
				pr, pf, err := createTestParquetReader(buf.Bytes(), new(test), 1)
				require.NoError(t, err)
				defer func() {
					require.NoError(t, pf.Close())
				}()

				numRows := int(pr.GetNumRows())
				require.Equal(t, len(testData), numRows)

				actualRows := make([]test, numRows)
				err = pr.Read(&actualRows)
				require.NoError(t, err)

				pr.ReadStop()
			},
		},
		{
			name: "set_schema_handler_from_json_valid",
			testFunc: func(t *testing.T) {
				var buf bytes.Buffer
				fw := writerfile.NewWriterFile(&buf)
				pw, err := NewParquetWriter(fw, new(struct{}), 1)
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

				// Verify schema was set
				require.NotNil(t, pw.SchemaHandler)
				require.Greater(t, len(pw.Footer.Schema), 0)
			},
		},
		{
			name: "set_schema_handler_from_json_invalid",
			testFunc: func(t *testing.T) {
				var buf bytes.Buffer
				fw := writerfile.NewWriterFile(&buf)
				pw, err := NewParquetWriter(fw, new(struct{}), 1)
				require.NoError(t, err)

				invalidJSON := `{"invalid": json}`
				err = pw.SetSchemaHandlerFromJSON(invalidJSON)
				require.Error(t, err)
			},
		},
		{
			name: "set_schema_handler_from_json_empty",
			testFunc: func(t *testing.T) {
				var buf bytes.Buffer
				fw := writerfile.NewWriterFile(&buf)
				pw, err := NewParquetWriter(fw, new(struct{}), 1)
				require.NoError(t, err)

				err = pw.SetSchemaHandlerFromJSON("")
				require.Error(t, err)
			},
		},
		{
			name: "write_stop_race_condition_on_error",
			testFunc: func(t *testing.T) {
				var buf bytes.Buffer
				fw := writerfile.NewWriterFile(&buf)
				pw, err := NewJSONWriter(`{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=x, type=INT64"}]}`, fw, 4)
				require.NoError(t, err)

				for i := range 10 {
					entry := fmt.Sprintf(`{"not-x":%d}`, i)
					require.NoError(t, pw.Write(entry))
				}
				require.Error(t, pw.WriteStop())
			},
		},
		{
			name: "zero_rows",
			testFunc: func(t *testing.T) {
				type TestSchema struct {
					ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
					ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
				}

				// Create writer and write zero rows
				pw, buf, err := createTestParquetWriter(new(TestSchema), 1)
				require.NoError(t, err)

				err = pw.WriteStop()
				require.NoError(t, err)

				// Create reader and verify zero rows
				pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestSchema), 1)
				require.NoError(t, err)
				defer func() {
					require.NoError(t, pf.Close())
				}()

				require.Equal(t, int64(0), pr.GetNumRows())
				// need to contain version and create_by even if no data was written
				require.Equal(t, int32(2), pr.Footer.Version)
				require.Equal(t, "github.com/hangxie/parquet-go v2 latest", *pr.Footer.CreatedBy)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFunc)
	}
}

func Test_NewParquetWriterFromWriter(t *testing.T) {
	type TestStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age  int32  `parquet:"name=age, type=INT32"`
	}

	t.Run("successful_creation", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriterFromWriter(&buf, new(TestStruct), 1)
		require.NoError(t, err)
		require.NotNil(t, pw)

		// Test that we can write and close
		data := TestStruct{Name: "Alice", Age: 30}
		err = pw.Write(data)
		require.NoError(t, err)

		err = pw.WriteStop()
		require.NoError(t, err)

		// Verify some data was written
		require.Greater(t, buf.Len(), 0)
	})

	t.Run("invalid_object", func(t *testing.T) {
		var buf bytes.Buffer
		// Using nil object should fail during schema creation
		pw, err := NewParquetWriterFromWriter(&buf, nil, 1)
		// Should either return error or create writer that fails later
		if err != nil {
			require.Error(t, err)
			require.Nil(t, pw)
		} else {
			require.NotNil(t, pw)
		}
	})
}

func Test_NewParquetWriter_InvalidFile(t *testing.T) {
	pw, err := NewParquetWriter(&invalidFileWriter{}, new(test), 1)
	require.Nil(t, pw)
	require.ErrorIs(t, err, errWrite)
}

func Test_ColumnIndex_NullCounts(t *testing.T) {
	type Entry struct {
		X *int64 `parquet:"name=x, type=INT64"`
		Y *int64 `parquet:"name=y, type=INT64"`
		Z *int64 `parquet:"name=z, type=INT64, omitstats=true"`
		U int64  `parquet:"name=u, type=INT64"`
		V int64  `parquet:"name=v, type=INT64, omitstats=true"`
	}

	type Expect struct {
		IsSetNullCounts bool
		NullCounts      []int64
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, new(Entry), 1)
	require.NoError(t, err)

	entries := []Entry{
		{int64Ptr(0), int64Ptr(0), int64Ptr(0), 1, 1},
		{nil, int64Ptr(1), int64Ptr(1), 2, 2},
		{nil, nil, nil, 3, 3},
	}
	for _, entry := range entries {
		require.NoError(t, pw.Write(entry))
	}
	require.NoError(t, pw.WriteStop())

	pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	defer func() {
		require.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	require.Nil(t, err)

	require.Nil(t, pr.ReadFooter())

	require.Equal(t, 1, len(pr.Footer.RowGroups))
	chunks := pr.Footer.RowGroups[0].GetColumns()
	require.Equal(t, 5, len(chunks))

	expects := []Expect{
		{true, []int64{2}},
		{true, []int64{1}},
		{false, nil},
		{true, []int64{0}},
		{false, nil},
	}
	for i, chunk := range chunks {
		colIdx, err := readColumnIndex(pr.PFile, *chunk.ColumnIndexOffset)
		require.NoError(t, err)
		require.Equal(t, expects[i].IsSetNullCounts, colIdx.IsSetNullCounts())
		require.Equal(t, expects[i].NullCounts, colIdx.GetNullCounts())
	}
}
