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

func TestPerColumnCompression(t *testing.T) {
	t.Run("per_column_compression_basic", func(t *testing.T) {
		// Test struct with per-column compression specified
		type TestStruct struct {
			Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, compression=GZIP"`
			Age   int32  `parquet:"name=age, type=INT32, compression=SNAPPY"`
			Score int64  `parquet:"name=score, type=INT64"` // No compression specified - should use file default
		}

		pw, buf, err := createTestParquetWriter(new(TestStruct), 1)
		require.NoError(t, err)

		// Set file-level compression to ZSTD
		pw.CompressionType = parquet.CompressionCodec_ZSTD

		testData := []TestStruct{
			{Name: "Alice", Age: 25, Score: 100},
			{Name: "Bob", Age: 30, Score: 200},
			{Name: "Charlie", Age: 35, Score: 300},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		// Read back and verify
		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		require.Equal(t, int64(3), pr.GetNumRows())

		results := make([]TestStruct, 3)
		require.NoError(t, pr.Read(&results))

		for i := range testData {
			require.Equal(t, testData[i].Name, results[i].Name)
			require.Equal(t, testData[i].Age, results[i].Age)
			require.Equal(t, testData[i].Score, results[i].Score)
		}

		// Verify compression codecs in metadata
		require.NotNil(t, pr.Footer)
		require.Equal(t, 1, len(pr.Footer.RowGroups))
		columns := pr.Footer.RowGroups[0].GetColumns()
		require.Equal(t, 3, len(columns))

		// Name should use GZIP
		require.Equal(t, parquet.CompressionCodec_GZIP, columns[0].MetaData.GetCodec())
		// Age should use SNAPPY
		require.Equal(t, parquet.CompressionCodec_SNAPPY, columns[1].MetaData.GetCodec())
		// Score should use file default (ZSTD)
		require.Equal(t, parquet.CompressionCodec_ZSTD, columns[2].MetaData.GetCodec())
	})

	t.Run("per_column_compression_with_dictionary", func(t *testing.T) {
		type DictStruct struct {
			Category string `parquet:"name=category, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, compression=GZIP"`
			Value    int32  `parquet:"name=value, type=INT32, compression=SNAPPY"`
		}

		pw, buf, err := createTestParquetWriter(new(DictStruct), 1)
		require.NoError(t, err)

		// Set file-level compression to ZSTD
		pw.CompressionType = parquet.CompressionCodec_ZSTD

		testData := []DictStruct{
			{Category: "A", Value: 1},
			{Category: "B", Value: 2},
			{Category: "A", Value: 3},
			{Category: "C", Value: 4},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(DictStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		require.Equal(t, int64(4), pr.GetNumRows())

		results := make([]DictStruct, 4)
		require.NoError(t, pr.Read(&results))

		for i := range testData {
			require.Equal(t, testData[i].Category, results[i].Category)
			require.Equal(t, testData[i].Value, results[i].Value)
		}

		// Verify compression codecs
		columns := pr.Footer.RowGroups[0].GetColumns()
		require.Equal(t, parquet.CompressionCodec_GZIP, columns[0].MetaData.GetCodec())
		require.Equal(t, parquet.CompressionCodec_SNAPPY, columns[1].MetaData.GetCodec())
	})

	t.Run("per_column_compression_uncompressed", func(t *testing.T) {
		type TestStruct struct {
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, compression=UNCOMPRESSED"`
			Age  int32  `parquet:"name=age, type=INT32"` // Should use file default
		}

		pw, buf, err := createTestParquetWriter(new(TestStruct), 1)
		require.NoError(t, err)

		pw.CompressionType = parquet.CompressionCodec_SNAPPY

		testData := []TestStruct{
			{Name: "Alice", Age: 25},
			{Name: "Bob", Age: 30},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		columns := pr.Footer.RowGroups[0].GetColumns()
		require.Equal(t, parquet.CompressionCodec_UNCOMPRESSED, columns[0].MetaData.GetCodec())
		require.Equal(t, parquet.CompressionCodec_SNAPPY, columns[1].MetaData.GetCodec())
	})

	t.Run("compression_nil_fallback_to_file_default", func(t *testing.T) {
		// When compression tag is not specified (nil), should use file-level compression
		type TestStruct struct {
			ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8"` // No compression tag
			ColB int32  `parquet:"name=col_b, type=INT32"`                          // No compression tag
			ColC int64  `parquet:"name=col_c, type=INT64"`                          // No compression tag
		}

		pw, buf, err := createTestParquetWriter(new(TestStruct), 1)
		require.NoError(t, err)

		// Set file-level compression to GZIP
		pw.CompressionType = parquet.CompressionCodec_GZIP

		testData := []TestStruct{
			{ColA: "test1", ColB: 1, ColC: 100},
			{ColA: "test2", ColB: 2, ColC: 200},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		// All columns should use file-level compression (GZIP)
		columns := pr.Footer.RowGroups[0].GetColumns()
		require.Equal(t, 3, len(columns))
		require.Equal(t, parquet.CompressionCodec_GZIP, columns[0].MetaData.GetCodec(), "ColA should use file default GZIP")
		require.Equal(t, parquet.CompressionCodec_GZIP, columns[1].MetaData.GetCodec(), "ColB should use file default GZIP")
		require.Equal(t, parquet.CompressionCodec_GZIP, columns[2].MetaData.GetCodec(), "ColC should use file default GZIP")

		// Verify data integrity
		results := make([]TestStruct, 2)
		require.NoError(t, pr.Read(&results))
		for i := range testData {
			require.Equal(t, testData[i], results[i])
		}
	})

	t.Run("map_key_value_compression", func(t *testing.T) {
		// Test keycompression and valuecompression for map types
		type TestStruct struct {
			Data map[string]int32 `parquet:"name=data, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, keycompression=GZIP, valuetype=INT32, valuecompression=ZSTD"`
		}

		pw, buf, err := createTestParquetWriter(new(TestStruct), 1)
		require.NoError(t, err)

		// Set file-level compression to SNAPPY
		pw.CompressionType = parquet.CompressionCodec_SNAPPY

		testData := []TestStruct{
			{Data: map[string]int32{"a": 1, "b": 2}},
			{Data: map[string]int32{"c": 3}},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		// Verify compression codecs - map has key and value columns
		columns := pr.Footer.RowGroups[0].GetColumns()
		require.Equal(t, 2, len(columns))
		// Key should use GZIP
		require.Equal(t, parquet.CompressionCodec_GZIP, columns[0].MetaData.GetCodec(), "map key should use GZIP")
		// Value should use ZSTD
		require.Equal(t, parquet.CompressionCodec_ZSTD, columns[1].MetaData.GetCodec(), "map value should use ZSTD")
	})

	t.Run("list_value_compression", func(t *testing.T) {
		// Test valuecompression for list types
		type TestStruct struct {
			Items []string `parquet:"name=items, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8, valuecompression=GZIP"`
		}

		pw, buf, err := createTestParquetWriter(new(TestStruct), 1)
		require.NoError(t, err)

		// Set file-level compression to SNAPPY
		pw.CompressionType = parquet.CompressionCodec_SNAPPY

		testData := []TestStruct{
			{Items: []string{"a", "b", "c"}},
			{Items: []string{"d", "e"}},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		// Verify compression codec - list has one element column
		columns := pr.Footer.RowGroups[0].GetColumns()
		require.Equal(t, 1, len(columns))
		// Element should use GZIP
		require.Equal(t, parquet.CompressionCodec_GZIP, columns[0].MetaData.GetCodec(), "list element should use GZIP")
	})

	t.Run("per_column_compression_all_codecs", func(t *testing.T) {
		type TestStruct struct {
			ColA string `parquet:"name=col_a, type=BYTE_ARRAY, convertedtype=UTF8, compression=GZIP"`
			ColB string `parquet:"name=col_b, type=BYTE_ARRAY, convertedtype=UTF8, compression=SNAPPY"`
			ColC string `parquet:"name=col_c, type=BYTE_ARRAY, convertedtype=UTF8, compression=ZSTD"`
			ColD string `parquet:"name=col_d, type=BYTE_ARRAY, convertedtype=UTF8, compression=LZ4_RAW"`
		}

		pw, buf, err := createTestParquetWriter(new(TestStruct), 1)
		require.NoError(t, err)

		testData := []TestStruct{
			{ColA: "a1", ColB: "b1", ColC: "c1", ColD: "d1"},
			{ColA: "a2", ColB: "b2", ColC: "c2", ColD: "d2"},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		columns := pr.Footer.RowGroups[0].GetColumns()
		require.Equal(t, parquet.CompressionCodec_GZIP, columns[0].MetaData.GetCodec())
		require.Equal(t, parquet.CompressionCodec_SNAPPY, columns[1].MetaData.GetCodec())
		require.Equal(t, parquet.CompressionCodec_ZSTD, columns[2].MetaData.GetCodec())
		require.Equal(t, parquet.CompressionCodec_LZ4_RAW, columns[3].MetaData.GetCodec())

		// Verify data is read correctly
		results := make([]TestStruct, 2)
		require.NoError(t, pr.Read(&results))
		for i := range testData {
			require.Equal(t, testData[i], results[i])
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
			Category string `parquet:"name=category, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
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

	t.Run("variant_support", func(t *testing.T) {
		type Nested struct {
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
			Age  int32  `parquet:"name=age, type=INT32"`
		}
		type MyStruct struct {
			Val any `parquet:"name=val, type=VARIANT, repetitiontype=OPTIONAL"`
		}

		pw, buf, err := createTestParquetWriter(new(MyStruct), 1)
		require.NoError(t, err)

		records := []MyStruct{
			{Val: Nested{Name: "Alice", Age: 30}},
			{Val: map[string]any{"city": "New York"}},
			{Val: int64(123)},
			{Val: nil},
		}

		for _, rec := range records {
			require.NoError(t, pw.Write(rec))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(MyStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		res := make([]MyStruct, len(records))
		require.NoError(t, pr.Read(&res))

		for i, rec := range res {
			if i == 3 {
				require.Nil(t, rec.Val)
				continue
			}

			// Value is already decoded to interface{} (map, int64, etc)
			require.NotNil(t, rec.Val)

			if i == 0 {
				obj, ok := rec.Val.(map[string]any)
				require.True(t, ok, "Expected map[string]any for row 0")
				require.Equal(t, "Alice", obj["Name"])
			} else if i == 1 {
				obj, ok := rec.Val.(map[string]any)
				require.True(t, ok, "Expected map[string]any for row 1")
				require.Equal(t, "New York", obj["city"])
			} else if i == 2 {
				val, ok := rec.Val.(int64)
				require.True(t, ok, "Expected int64 for row 2")
				require.Equal(t, int64(123), val)
			}
		}
	})

	t.Run("json_writer_variant", func(t *testing.T) {
		jsonSchema := `{"Tag":"name=parquet-go-root","Fields":[{"Tag":"name=val, type=VARIANT, repetitiontype=OPTIONAL"}]}`
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter(jsonSchema, fw, 1)
		require.NoError(t, err)

		records := []string{
			`{"val": {"name": "Alice", "age": 30}}`,
			`{"val": null}`,
		}

		for _, rec := range records {
			require.NoError(t, jw.Write(rec))
		}
		require.NoError(t, jw.WriteStop())

		type MyStruct struct {
			Val any `parquet:"name=val, type=VARIANT, repetitiontype=OPTIONAL"`
		}
		pr, pf, err := createTestParquetReader(buf.Bytes(), new(MyStruct), 1)
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStopWithError() }()

		res := make([]MyStruct, 2)
		require.NoError(t, pr.Read(&res))

		require.NotNil(t, res[0].Val)
		// Value is already decoded
		obj, ok := res[0].Val.(map[string]any)
		require.True(t, ok, "Expected map[string]any")
		require.Equal(t, "Alice", obj["name"])

		require.Nil(t, res[1].Val)
	})
}
