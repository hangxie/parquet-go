package writer

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

func TestExtractPageStats(t *testing.T) {
	nullCount := int64(3)

	tests := []struct {
		name string
		page *layout.Page
		want pageStats
	}{
		{
			name: "data_page_v1",
			page: &layout.Page{
				Header: &parquet.PageHeader{
					DataPageHeader: &parquet.DataPageHeader{
						Statistics: &parquet.Statistics{
							Min:       []byte("a"),
							Max:       []byte("z"),
							NullCount: &nullCount,
						},
					},
				},
			},
			want: pageStats{minVal: []byte("a"), maxVal: []byte("z"), nullCount: &nullCount},
		},
		{
			name: "data_page_v2",
			page: &layout.Page{
				Header: &parquet.PageHeader{
					DataPageHeaderV2: &parquet.DataPageHeaderV2{
						Statistics: &parquet.Statistics{
							Min:       []byte("b"),
							Max:       []byte("y"),
							NullCount: &nullCount,
						},
					},
				},
			},
			want: pageStats{minVal: []byte("b"), maxVal: []byte("y"), nullCount: &nullCount},
		},
		{
			name: "no_stats",
			page: &layout.Page{Header: &parquet.PageHeader{}},
			want: pageStats{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPageStats(tt.page)
			require.Equal(t, tt.want.minVal, got.minVal)
			require.Equal(t, tt.want.maxVal, got.maxVal)
			if tt.want.nullCount == nil {
				require.Nil(t, got.nullCount)
			} else {
				require.NotNil(t, got.nullCount)
				require.Equal(t, *tt.want.nullCount, *got.nullCount)
			}
		})
	}
}

func TestDataPageNumValues(t *testing.T) {
	tests := []struct {
		name string
		page *layout.Page
		want int64
	}{
		{
			name: "data_page_v1",
			page: &layout.Page{Header: &parquet.PageHeader{
				DataPageHeader: &parquet.DataPageHeader{NumValues: 7},
			}},
			want: 7,
		},
		{
			name: "data_page_v2",
			page: &layout.Page{Header: &parquet.PageHeader{
				DataPageHeaderV2: &parquet.DataPageHeaderV2{NumValues: 11},
			}},
			want: 11,
		},
		{
			name: "not_data_page",
			page: &layout.Page{Header: &parquet.PageHeader{}},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, dataPageNumValues(tt.page))
		})
	}
}

func TestColumnIndex(t *testing.T) {
	t.Run("all_null_counts", func(t *testing.T) {
		type Entry struct {
			X *int64 `parquet:"name=x, type=INT64"`
			Y *int64 `parquet:"name=z, type=INT64"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(Entry), WithNP(1))
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
		pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
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
		pw, err := NewParquetWriter(fw, new(Entry), WithNP(1))
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
		pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
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

	t.Run("dict_encoded_column_index_size", func(t *testing.T) {
		// Dict-encoded columns have a dictionary page followed by data pages.
		// ColumnIndex entries must only correspond to data pages, not dictionary pages.
		type Entry struct {
			Val string `parquet:"name=val, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := NewParquetWriter(fw, new(Entry), WithNP(1))
		require.NoError(t, err)

		entries := []Entry{
			{Val: "alpha"},
			{Val: "beta"},
			{Val: "alpha"},
		}
		for _, entry := range entries {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		defer func() {
			require.NoError(t, pf.Close())
		}()
		pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pr.ReadFooter())

		require.Equal(t, 1, len(pr.Footer.RowGroups))
		columns := pr.Footer.RowGroups[0].GetColumns()
		require.Equal(t, 1, len(columns))

		colIdx, err := readColumnIndex(pr.PFile, *columns[0].ColumnIndexOffset)
		require.NoError(t, err)

		// There should be exactly 1 data page, so ColumnIndex arrays should have length 1.
		// A bug would produce length 2 (one phantom entry for the dict page + one for data).
		require.Equal(t, 1, len(colIdx.GetMinValues()))
		require.Equal(t, 1, len(colIdx.GetMaxValues()))
		require.Equal(t, 1, len(colIdx.GetNullPages()))
	})

	t.Run("level_histograms", func(t *testing.T) {
		// Optional field: maxDefinitionLevel=1, maxRepetitionLevel=0
		// X has values [ptr(1), nil, ptr(3)] → defLevels=[1,0,1]
		// Histogram: defLevel 0 → 1, defLevel 1 → 2
		type Entry struct {
			X *int64 `parquet:"name=x, type=INT64"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		entries := []Entry{
			{common.ToPtr(int64(1))},
			{nil},
			{common.ToPtr(int64(3))},
		}
		for _, entry := range entries {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		defer func() {
			require.NoError(t, pf.Close())
		}()
		pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pr.ReadFooter())

		columns := pr.Footer.RowGroups[0].GetColumns()
		colIdx, err := readColumnIndex(pr.PFile, *columns[0].ColumnIndexOffset)
		require.NoError(t, err)

		// One page, maxDefLevel=1 → histogram has 2 elements: [count_of_0, count_of_1]
		require.True(t, colIdx.IsSetDefinitionLevelHistograms())
		require.Equal(t, []int64{1, 2}, colIdx.GetDefinitionLevelHistograms())

		// maxRepLevel=0 for non-nested → no repetition histogram
		require.False(t, colIdx.IsSetRepetitionLevelHistograms())
	})

	t.Run("level_histograms_required_field", func(t *testing.T) {
		// Required field: maxDefinitionLevel=0, maxRepetitionLevel=0
		// No histograms should be set since they'd be trivial
		type Entry struct {
			X int64 `parquet:"name=x, type=INT64"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(Entry{X: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		defer func() {
			require.NoError(t, pf.Close())
		}()
		pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pr.ReadFooter())

		columns := pr.Footer.RowGroups[0].GetColumns()
		colIdx, err := readColumnIndex(pr.PFile, *columns[0].ColumnIndexOffset)
		require.NoError(t, err)

		// Both maxDefLevel and maxRepLevel are 0 → no histograms
		require.False(t, colIdx.IsSetDefinitionLevelHistograms())
		require.False(t, colIdx.IsSetRepetitionLevelHistograms())
	})
}

func TestSizeStatistics(t *testing.T) {
	t.Run("byte_array_column", func(t *testing.T) {
		// BYTE_ARRAY column should have unencoded_byte_array_data_bytes set.
		// Values: "ab" (2), "cde" (3), "f" (1) → total = 6 bytes (excludes 4-byte length prefixes)
		type Entry struct {
			S string `parquet:"name=s, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(Entry{S: "ab"}))
		require.NoError(t, pw.Write(Entry{S: "cde"}))
		require.NoError(t, pw.Write(Entry{S: "f"}))
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		columns := pr.Footer.RowGroups[0].GetColumns()
		ss := columns[0].MetaData.SizeStatistics
		require.NotNil(t, ss, "SizeStatistics should be set")
		require.True(t, ss.IsSetUnencodedByteArrayDataBytes())
		require.Equal(t, int64(6), ss.GetUnencodedByteArrayDataBytes())
	})

	t.Run("non_byte_array_required", func(t *testing.T) {
		// Required INT64 column: maxDefLevel=0, maxRepLevel=0, not BYTE_ARRAY.
		// SizeStatistics should be nil — nothing to report.
		type Entry struct {
			X int64 `parquet:"name=x, type=INT64"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(Entry{X: 42}))
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		columns := pr.Footer.RowGroups[0].GetColumns()
		require.Nil(t, columns[0].MetaData.SizeStatistics)
	})

	t.Run("non_byte_array_optional", func(t *testing.T) {
		// Optional INT64 column: maxDefLevel=1 → has def histogram but no byte array bytes.
		type Entry struct {
			X *int64 `parquet:"name=x, type=INT64"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(Entry{X: common.ToPtr(int64(42))}))
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		columns := pr.Footer.RowGroups[0].GetColumns()
		ss := columns[0].MetaData.SizeStatistics
		require.NotNil(t, ss)
		require.False(t, ss.IsSetUnencodedByteArrayDataBytes())
		require.True(t, ss.IsSetDefinitionLevelHistogram())
	})

	t.Run("level_histograms_aggregated", func(t *testing.T) {
		// Optional INT64 field: maxDefLevel=1.
		// Values: [ptr(1), nil, ptr(3)] → defLevels=[1,0,1]
		// Aggregated histogram: [1, 2] (one 0, two 1s)
		type Entry struct {
			X *int64 `parquet:"name=x, type=INT64"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(Entry{X: common.ToPtr(int64(1))}))
		require.NoError(t, pw.Write(Entry{X: nil}))
		require.NoError(t, pw.Write(Entry{X: common.ToPtr(int64(3))}))
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		columns := pr.Footer.RowGroups[0].GetColumns()
		ss := columns[0].MetaData.SizeStatistics
		require.NotNil(t, ss)
		require.True(t, ss.IsSetDefinitionLevelHistogram())
		require.Equal(t, []int64{1, 2}, ss.GetDefinitionLevelHistogram())
		// maxRepLevel=0 → no rep histogram
		require.False(t, ss.IsSetRepetitionLevelHistogram())
	})

	t.Run("required_field_no_histograms", func(t *testing.T) {
		// Required INT64 field: maxDefLevel=0, maxRepLevel=0
		type Entry struct {
			X int64 `parquet:"name=x, type=INT64"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(Entry{X: 42}))
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		columns := pr.Footer.RowGroups[0].GetColumns()
		ss := columns[0].MetaData.SizeStatistics
		// Both levels are 0 → SizeStatistics should be nil (no useful info)
		require.Nil(t, ss)
	})

	t.Run("nullable_byte_array", func(t *testing.T) {
		// Nullable BYTE_ARRAY: both unencoded bytes and def histogram
		type Entry struct {
			S *string `parquet:"name=s, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		s1, s2 := "hello", "go"
		require.NoError(t, pw.Write(Entry{S: &s1}))
		require.NoError(t, pw.Write(Entry{S: nil}))
		require.NoError(t, pw.Write(Entry{S: &s2}))
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		columns := pr.Footer.RowGroups[0].GetColumns()
		ss := columns[0].MetaData.SizeStatistics
		require.NotNil(t, ss)
		// "hello"=5 + "go"=2 = 7
		require.True(t, ss.IsSetUnencodedByteArrayDataBytes())
		require.Equal(t, int64(7), ss.GetUnencodedByteArrayDataBytes())
		// defLevels=[1,0,1] → histogram [1, 2]
		require.True(t, ss.IsSetDefinitionLevelHistogram())
		require.Equal(t, []int64{1, 2}, ss.GetDefinitionLevelHistogram())
	})
}

//nolint:gocognit
func TestDataPageVersion(t *testing.T) {
	type TestStruct struct {
		Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age   int32  `parquet:"name=age, type=INT32"`
		Score *int64 `parquet:"name=score, type=INT64"`
	}

	t.Run("v2_basic", func(t *testing.T) {
		pw, buf, err := createTestParquetWriter(new(TestStruct), WithNP(1), WithDataPageVersion(2))
		require.NoError(t, err)

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

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(DictStruct), WithNP(1), WithDataPageVersion(2))
		require.NoError(t, err)

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

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(DictStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(SimpleStruct), WithNP(1))
		require.NoError(t, err)

		require.Equal(t, int32(1), pw.dataPageVersion)

		testData := []SimpleStruct{
			{Name: "Test1"},
			{Name: "Test2"},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(SimpleStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

		require.Equal(t, int64(2), pr.GetNumRows())
	})

	t.Run("version_switching", func(t *testing.T) {
		type ValueStruct struct {
			Value int32 `parquet:"name=value, type=INT32"`
		}

		pw, buf, err := createTestParquetWriter(new(ValueStruct), WithNP(1))
		require.NoError(t, err)

		require.Equal(t, int32(1), pw.dataPageVersion)

		pw.dataPageVersion = 2

		testData := []ValueStruct{
			{Value: 1},
			{Value: 2},
			{Value: 3},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(ValueStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(ValueStruct), WithNP(1), WithDataPageVersion(2))
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			require.NoError(t, pw.Write(ValueStruct{Value: int32(i)}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		defer func() { _ = pf.Close() }()

		pr, err := reader.NewParquetReader(pf, new(ValueStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(MyStruct), WithNP(1))
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

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(MyStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

		res := make([]MyStruct, len(records))
		require.NoError(t, pr.Read(&res))

		for i, rec := range res {
			if i == 3 {
				require.Nil(t, rec.Val)
				continue
			}

			// Value is already decoded to interface{} (map, int64, etc)
			require.NotNil(t, rec.Val)

			switch i {
			case 0:
				obj, ok := rec.Val.(map[string]any)
				require.True(t, ok, "Expected map[string]any for row 0")
				require.Equal(t, "Alice", obj["Name"])
			case 1:
				obj, ok := rec.Val.(map[string]any)
				require.True(t, ok, "Expected map[string]any for row 1")
				require.Equal(t, "New York", obj["city"])
			case 2:
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
		jw, err := NewJSONWriter(jsonSchema, fw, WithNP(1))
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
		pr, pf, err := createTestParquetReader(buf.Bytes(), new(MyStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

		res := make([]MyStruct, 2)
		require.NoError(t, pr.Read(&res))

		require.NotNil(t, res[0].Val)
		// Value is already decoded
		obj, ok := res[0].Val.(map[string]any)
		require.True(t, ok, "Expected map[string]any")
		require.Equal(t, "Alice", obj["name"])

		require.Nil(t, res[1].Val)
	})

	t.Run("plain_dictionary_v2_rejected", func(t *testing.T) {
		type PlainDictStruct struct {
			Field string `parquet:"name=field, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`
		}
		pw, _, err := createTestParquetWriter(new(PlainDictStruct), WithNP(1), WithDataPageVersion(2))
		require.NoError(t, err)

		err = pw.Write(PlainDictStruct{Field: "test"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "PLAIN_DICTIONARY")
		require.Contains(t, err.Error(), "v1")
	})

	t.Run("delta_encoding_v1_rejected", func(t *testing.T) {
		type DeltaStruct struct {
			Field string `parquet:"name=field, type=BYTE_ARRAY, encoding=DELTA_BYTE_ARRAY"`
		}
		pw, _, err := createTestParquetWriter(new(DeltaStruct), WithNP(1))
		require.NoError(t, err)

		// Default is v1
		require.Equal(t, int32(1), pw.dataPageVersion)

		err = pw.Write(DeltaStruct{Field: "test"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "DELTA_BYTE_ARRAY")
		require.Contains(t, err.Error(), "v2")
	})

	t.Run("delta_binary_packed_v1_rejected", func(t *testing.T) {
		type DeltaIntStruct struct {
			Value int32 `parquet:"name=value, type=INT32, encoding=DELTA_BINARY_PACKED"`
		}
		pw, _, err := createTestParquetWriter(new(DeltaIntStruct), WithNP(1))
		require.NoError(t, err)

		// Default is v1
		err = pw.Write(DeltaIntStruct{Value: 123})
		require.Error(t, err)
		require.Contains(t, err.Error(), "DELTA_BINARY_PACKED")
		require.Contains(t, err.Error(), "v2")
	})

	t.Run("validation_only_happens_once", func(t *testing.T) {
		type SimpleStruct struct {
			Value int32 `parquet:"name=value, type=INT32"`
		}
		pw, buf, err := createTestParquetWriter(new(SimpleStruct), WithNP(1))
		require.NoError(t, err)

		// Write multiple records - validation should only happen on first write
		for i := range 100 {
			require.NoError(t, pw.Write(SimpleStruct{Value: int32(i)}))
		}
		require.NoError(t, pw.WriteStop())
		require.Greater(t, buf.Len(), 0)
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

		pw, buf, err := createTestParquetWriter(new(TestStruct), WithNP(1), WithCompressionCodec(parquet.CompressionCodec_ZSTD))
		require.NoError(t, err)

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
		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(DictStruct), WithNP(1), WithCompressionCodec(parquet.CompressionCodec_ZSTD))
		require.NoError(t, err)

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

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(DictStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(TestStruct), WithNP(1), WithCompressionCodec(parquet.CompressionCodec_SNAPPY))
		require.NoError(t, err)

		testData := []TestStruct{
			{Name: "Alice", Age: 25},
			{Name: "Bob", Age: 30},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(TestStruct), WithNP(1), WithCompressionCodec(parquet.CompressionCodec_GZIP))
		require.NoError(t, err)

		testData := []TestStruct{
			{ColA: "test1", ColB: 1, ColC: 100},
			{ColA: "test2", ColB: 2, ColC: 200},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(TestStruct), WithNP(1), WithCompressionCodec(parquet.CompressionCodec_SNAPPY))
		require.NoError(t, err)

		testData := []TestStruct{
			{Data: map[string]int32{"a": 1, "b": 2}},
			{Data: map[string]int32{"c": 3}},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(TestStruct), WithNP(1), WithCompressionCodec(parquet.CompressionCodec_SNAPPY))
		require.NoError(t, err)

		testData := []TestStruct{
			{Items: []string{"a", "b", "c"}},
			{Items: []string{"d", "e"}},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

		pw, buf, err := createTestParquetWriter(new(TestStruct), WithNP(1))
		require.NoError(t, err)

		testData := []TestStruct{
			{ColA: "a1", ColB: "b1", ColC: "c1", ColD: "d1"},
			{ColA: "a2", ColB: "b2", ColC: "c2", ColD: "d2"},
		}

		for _, entry := range testData {
			require.NoError(t, pw.Write(entry))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(TestStruct), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pf.Close() }()
		defer func() { _ = pr.ReadStop() }()

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

func TestWriteCRC_RoundTrip(t *testing.T) {
	type testRecord struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age  int32  `parquet:"name=age, type=INT32"`
	}

	testCases := []struct {
		name     string
		writeCRC bool
		crcMode  common.CRCMode
	}{
		{
			name:     "crc_enabled_strict_read",
			writeCRC: true,
			crcMode:  common.CRCStrict,
		},
		{
			name:     "crc_enabled_auto_read",
			writeCRC: true,
			crcMode:  common.CRCAuto,
		},
		{
			name:     "crc_disabled_auto_read",
			writeCRC: false,
			crcMode:  common.CRCAuto,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Write
			pw, buf, err := createTestParquetWriter(new(testRecord), WithNP(1), WithWriteCRC(tc.writeCRC))
			require.NoError(t, err)

			for i := range 10 {
				require.NoError(t, pw.Write(testRecord{
					Name: fmt.Sprintf("name_%d", i),
					Age:  int32(20 + i),
				}))
			}
			require.NoError(t, pw.WriteStop())

			// Read
			pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
			pr, err := reader.NewParquetReader(pf, new(testRecord),
				reader.WithNP(1), reader.WithCRCMode(tc.crcMode))
			require.NoError(t, err)

			results := make([]testRecord, 10)
			require.NoError(t, pr.Read(&results))
			require.NoError(t, pr.ReadStop())
			_ = pf.Close()

			require.Len(t, results, 10)
			for i, r := range results {
				require.Equal(t, fmt.Sprintf("name_%d", i), r.Name)
				require.Equal(t, int32(20+i), r.Age)
			}
		})
	}
}

func TestWriteCRC_DictEncoding_RoundTrip(t *testing.T) {
	type testRecord struct {
		Category string `parquet:"name=category, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	}

	// Write with CRC
	pw, buf, err := createTestParquetWriter(new(testRecord), WithNP(1), WithWriteCRC(true))
	require.NoError(t, err)

	for i := range 20 {
		require.NoError(t, pw.Write(testRecord{
			Category: fmt.Sprintf("cat_%d", i%5),
		}))
	}
	require.NoError(t, pw.WriteStop())

	// Read with strict CRC
	pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
	pr, err := reader.NewParquetReader(pf, new(testRecord),
		reader.WithNP(1), reader.WithCRCMode(common.CRCStrict))
	require.NoError(t, err)

	results := make([]testRecord, 20)
	require.NoError(t, pr.Read(&results))
	require.NoError(t, pr.ReadStop())
	_ = pf.Close()

	require.Len(t, results, 20)
	for i, r := range results {
		require.Equal(t, fmt.Sprintf("cat_%d", i%5), r.Category)
	}
}
