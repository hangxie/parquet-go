package writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source"
	"github.com/hangxie/parquet-go/v2/source/buffer"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
)

// TestNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex.
func Test_NullCountsFromColumnIndex(t *testing.T) {
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
	assert.NoError(t, err)

	entries := []Entry{
		{int64Ptr(0), int64Ptr(0), int64Ptr(0), 1, 1},
		{nil, int64Ptr(1), int64Ptr(1), 2, 2},
		{nil, nil, nil, 3, 3},
	}
	for _, entry := range entries {
		assert.NoError(t, pw.Write(entry))
	}
	assert.NoError(t, pw.WriteStop())

	pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	assert.Nil(t, err)

	assert.Nil(t, pr.ReadFooter())

	assert.Equal(t, 1, len(pr.Footer.RowGroups))
	chunks := pr.Footer.RowGroups[0].GetColumns()
	assert.Equal(t, 5, len(chunks))

	expects := []Expect{
		{true, []int64{2}},
		{true, []int64{1}},
		{false, nil},
		{true, []int64{0}},
		{false, nil},
	}
	for i, chunk := range chunks {
		colIdx, err := readColumnIndex(pr.PFile, *chunk.ColumnIndexOffset)
		assert.NoError(t, err)
		assert.Equal(t, expects[i].IsSetNullCounts, colIdx.IsSetNullCounts())
		assert.Equal(t, expects[i].NullCounts, colIdx.GetNullCounts())
	}
}

// TestAllNullCountsFromColumnIndex tests that NullCounts is correctly set in the ColumnIndex if a field contains null value only.
func Test_AllNullCountsFromColumnIndex(t *testing.T) {
	type Entry struct {
		X *int64 `parquet:"name=x, type=INT64"`
		Y *int64 `parquet:"name=z, type=INT64"`
	}

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, new(Entry), 1)
	assert.NoError(t, err)

	entries := []Entry{
		{int64Ptr(0), nil},
		{int64Ptr(1), nil},
		{int64Ptr(2), nil},
		{int64Ptr(3), nil},
		{int64Ptr(4), nil},
		{int64Ptr(5), nil},
	}
	for _, entry := range entries {
		assert.NoError(t, pw.Write(entry))
	}
	assert.NoError(t, pw.WriteStop())

	pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	defer func() {
		assert.NoError(t, pf.Close())
	}()
	pr, err := reader.NewParquetReader(pf, nil, 1)
	assert.Nil(t, err)

	assert.Nil(t, pr.ReadFooter())

	assert.Equal(t, 1, len(pr.Footer.RowGroups))
	columns := pr.Footer.RowGroups[0].GetColumns()
	assert.Equal(t, 2, len(columns))

	colIdx, err := readColumnIndex(pr.PFile, *columns[0].ColumnIndexOffset)
	assert.NoError(t, err)
	assert.Equal(t, true, colIdx.IsSetNullCounts())
	assert.Equal(t, []int64{0}, colIdx.GetNullCounts())

	colIdx, err = readColumnIndex(pr.PFile, *columns[1].ColumnIndexOffset)
	assert.NoError(t, err)
	assert.Equal(t, true, colIdx.IsSetNullCounts())
	assert.Equal(t, []int64{6}, colIdx.GetNullCounts())
}

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

func Test_ZeroRows(t *testing.T) {
	type TestSchema struct {
		ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	}

	// Create writer and write zero rows
	pw, buf, err := createTestParquetWriter(new(TestSchema), 1)
	assert.NoError(t, err, "Should create parquet writer successfully")

	err = pw.WriteStop()
	assert.NoError(t, err, "WriteStop should succeed")

	// Create reader and verify zero rows
	pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestSchema), 1)
	assert.NoError(t, err, "Should create parquet reader successfully")
	defer func() {
		assert.NoError(t, pf.Close(), "Should close parquet file successfully")
	}()

	assert.Equal(t, int64(0), pr.GetNumRows(), "Should have zero rows")
}

type test struct {
	ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// TestDoubleWriteStop verifies that calling WriteStop multiple times is safe
func Test_DoubleWriteStop(t *testing.T) {
	// Create writer
	pw, buf, err := createTestParquetWriter(new(test), 1)
	assert.NoError(t, err, "Should create parquet writer successfully")

	// Write test data
	testData := []test{
		{ColA: "cola_0", ColB: "colb_0"},
		{ColA: "cola_1", ColB: "colb_1"},
		{ColA: "cola_2", ColB: "colb_2"},
	}

	for _, record := range testData {
		err = pw.Write(record)
		assert.NoError(t, err, "Should write record successfully")
	}

	// Call WriteStop twice to verify it's idempotent
	err = pw.WriteStop()
	assert.NoError(t, err, "First WriteStop should succeed")

	err = pw.WriteStop()
	assert.NoError(t, err, "Second WriteStop should also succeed")

	// Verify data can be read correctly
	pr, pf, err := createTestParquetReader(buf.Bytes(), new(test), 1)
	assert.NoError(t, err, "Should create parquet reader successfully")
	defer func() {
		assert.NoError(t, pf.Close(), "Should close parquet file successfully")
	}()

	numRows := int(pr.GetNumRows())
	assert.Equal(t, len(testData), numRows, "Should have correct number of rows")

	actualRows := make([]test, numRows)
	err = pr.Read(&actualRows)
	assert.NoError(t, err, "Should read data successfully")

	pr.ReadStop()
}

var errWrite = errors.New("test error")

type invalidFileWriter struct {
	source.ParquetFileWriter
}

func (m *invalidFileWriter) Write(data []byte) (n int, err error) {
	return 0, errWrite
}

func Test_NewWriterWithInvaidFile(t *testing.T) {
	pw, err := NewParquetWriter(&invalidFileWriter{}, new(test), 1)
	assert.Nil(t, pw)
	assert.ErrorIs(t, err, errWrite)
}

func Test_WriteStopRaceConditionOnError(t *testing.T) {
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewJSONWriter(`{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=x, type=INT64"}]}`, fw, 4)
	assert.NoError(t, err)

	for i := range 10 {
		entry := fmt.Sprintf(`{"not-x":%d}`, i)
		assert.NoError(t, pw.Write(entry))
	}
	assert.Error(t, pw.WriteStop())
}
