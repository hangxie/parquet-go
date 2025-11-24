package writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source"
	"github.com/hangxie/parquet-go/v2/source/buffer"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
)

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

var errWrite = errors.New("test error")

type invalidFileWriter struct {
	source.ParquetFileWriter
}

func (m *invalidFileWriter) Write(data []byte) (n int, err error) {
	return 0, errWrite
}

func TestColumnIndex(t *testing.T) {
	t.Run("all_null_counts", func(t *testing.T) {
		type Entry struct {
			X *int64 `parquet:"name=x, type=INT64"`
			Y *int64 `parquet:"name=z, type=INT64"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(Entry), 1)
		require.NoError(t, err)

		entries := []Entry{
			{common.ToPtr(int64(0)), nil},
			{common.ToPtr(int64(1)), nil},
			{common.ToPtr(int64(2)), nil},
			{common.ToPtr(int64(3)), nil},
			{common.ToPtr(int64(4)), nil},
			{common.ToPtr(int64(5)), nil},
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
	})

	t.Run("null_counts", func(t *testing.T) {
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
			{common.ToPtr(int64(0)), common.ToPtr(int64(0)), common.ToPtr(int64(0)), 1, 1},
			{nil, common.ToPtr(int64(1)), common.ToPtr(int64(1)), 2, 2},
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
	})
}

func TestParquetWriter(t *testing.T) {
	t.Run("double_write_stop", func(t *testing.T) {
		pw, buf, err := createTestParquetWriter(new(test), 1)
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

		_ = pr.ReadStopWithError()
	})

	t.Run("set_schema_handler_from_json_valid", func(t *testing.T) {
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

		require.NotNil(t, pw.SchemaHandler)
		require.Greater(t, len(pw.Footer.Schema), 0)
	})

	t.Run("set_schema_handler_from_json_invalid", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(struct{}), 1)
		require.NoError(t, err)

		invalidJSON := `{"invalid": json}`
		err = pw.SetSchemaHandlerFromJSON(invalidJSON)
		require.Error(t, err)
	})

	t.Run("set_schema_handler_from_json_empty", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(struct{}), 1)
		require.NoError(t, err)

		err = pw.SetSchemaHandlerFromJSON("")
		require.Error(t, err)
	})

	t.Run("write_stop_race_condition_on_error", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewJSONWriter(`{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=x, type=INT64"}]}`, fw, 4)
		require.NoError(t, err)

		for i := range 10 {
			entry := fmt.Sprintf(`{"not-x":%d}`, i)
			require.NoError(t, pw.Write(entry))
		}
		require.Error(t, pw.WriteStop())
	})

	t.Run("zero_rows", func(t *testing.T) {
		type TestSchema struct {
			ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
			ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		}

		pw, buf, err := createTestParquetWriter(new(TestSchema), 1)
		require.NoError(t, err)

		err = pw.WriteStop()
		require.NoError(t, err)

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestSchema), 1)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		require.Equal(t, int64(0), pr.GetNumRows())
		require.Equal(t, int32(2), pr.Footer.Version)
		require.Equal(t, "github.com/hangxie/parquet-go v2 latest", *pr.Footer.CreatedBy)
	})

	t.Run("invalid_file", func(t *testing.T) {
		pw, err := NewParquetWriter(&invalidFileWriter{}, new(test), 1)
		require.Nil(t, pw)
		require.ErrorIs(t, err, errWrite)
	})
}

func TestNewParquetWriterFromWriter(t *testing.T) {
	type TestStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age  int32  `parquet:"name=age, type=INT32"`
	}

	t.Run("successful_creation", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriterFromWriter(&buf, new(TestStruct), 1)
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
		pw, err := NewParquetWriterFromWriter(&buf, nil, 1)
		if err != nil {
			require.Error(t, err)
			require.Nil(t, pw)
		} else {
			require.NotNil(t, pw)
		}
	})
}

func TestDataPageVersion(t *testing.T) {
	type TestStruct struct {
		Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age   int32  `parquet:"name=age, type=INT32"`
		Score *int64 `parquet:"name=score, type=INT64"`
	}

	t.Run("v2_basic", func(t *testing.T) {
		pw, buf, err := createTestParquetWriter(new(TestStruct), 1)
		require.NoError(t, err)

		pw.DataPageVersion = 2

		testData := []TestStruct{
			{Name: "Alice", Age: 25, Score: common.ToPtr(int64(100))},
			{Name: "Bob", Age: 30, Score: common.ToPtr(int64(200))},
			{Name: "Charlie", Age: 35, Score: nil},
			{Name: "David", Age: 40, Score: common.ToPtr(int64(400))},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		require.Greater(t, buf.Len(), 0)

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		require.Equal(t, int64(4), pr.GetNumRows())

		results := make([]TestStruct, 4)
		require.NoError(t, pr.Read(&results))

		for i := range testData {
			require.Equal(t, testData[i].Name, results[i].Name)
			require.Equal(t, testData[i].Age, results[i].Age)
			if testData[i].Score == nil {
				require.Nil(t, results[i].Score)
			} else {
				require.NotNil(t, results[i].Score)
				require.Equal(t, *testData[i].Score, *results[i].Score)
			}
		}
	})

	t.Run("v2_with_dictionary", func(t *testing.T) {
		type DictStruct struct {
			Category string `parquet:"name=category, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
			Value    int32  `parquet:"name=value, type=INT32"`
		}

		pw, buf, err := createTestParquetWriter(new(DictStruct), 1)
		require.NoError(t, err)

		pw.DataPageVersion = 2

		testData := []DictStruct{
			{Category: "A", Value: 1},
			{Category: "B", Value: 2},
			{Category: "A", Value: 3},
			{Category: "C", Value: 4},
			{Category: "B", Value: 5},
			{Category: "A", Value: 6},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		require.Greater(t, buf.Len(), 0)

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(DictStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		require.Equal(t, int64(6), pr.GetNumRows())

		results := make([]DictStruct, 6)
		require.NoError(t, pr.Read(&results))

		for i := range testData {
			require.Equal(t, testData[i].Category, results[i].Category)
			require.Equal(t, testData[i].Value, results[i].Value)
		}
	})

	t.Run("v1_default_behavior", func(t *testing.T) {
		type SimpleStruct struct {
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		pw, buf, err := createTestParquetWriter(new(SimpleStruct), 1)
		require.NoError(t, err)

		require.Equal(t, int32(1), pw.DataPageVersion)

		testData := []SimpleStruct{
			{Name: "Test1"},
			{Name: "Test2"},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(SimpleStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		require.Equal(t, int64(2), pr.GetNumRows())
	})

	t.Run("version_switching", func(t *testing.T) {
		type ValueStruct struct {
			Value int32 `parquet:"name=value, type=INT32"`
		}

		pw, buf, err := createTestParquetWriter(new(ValueStruct), 1)
		require.NoError(t, err)

		require.Equal(t, int32(1), pw.DataPageVersion)

		pw.DataPageVersion = 2

		testData := []ValueStruct{
			{Value: 1},
			{Value: 2},
			{Value: 3},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(ValueStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		require.Equal(t, int64(3), pr.GetNumRows())

		results := make([]ValueStruct, 3)
		require.NoError(t, pr.Read(&results))

		for i := range testData {
			require.Equal(t, testData[i].Value, results[i].Value)
		}
	})

	t.Run("v2_page_header_type", func(t *testing.T) {
		type ValueStruct struct {
			Value int32 `parquet:"name=value, type=INT32"`
		}

		pw, buf, err := createTestParquetWriter(new(ValueStruct), 1)
		require.NoError(t, err)

		pw.DataPageVersion = 2

		for i := 0; i < 100; i++ {
			require.NoError(t, pw.Write(ValueStruct{Value: int32(i)}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		defer func() { _ = pf.Close() }()

		pr, err := reader.NewParquetReader(pf, new(ValueStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pr.ReadStopWithError() }()

		require.Equal(t, int64(100), pr.GetNumRows())

		results := make([]ValueStruct, 100)
		require.NoError(t, pr.Read(&results))

		for i := range results {
			require.Equal(t, int32(i), results[i].Value)
		}
	})
}
