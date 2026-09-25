package writer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
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
							MinValue:  []byte("a"),
							MaxValue:  []byte("z"),
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
							MinValue:  []byte("b"),
							MaxValue:  []byte("y"),
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

func TestColumnIndexBoundaryOrderCalculation(t *testing.T) {
	tests := []struct {
		name   string
		schema *parquet.SchemaElement
		bounds []pageBounds
		want   parquet.BoundaryOrder
	}{
		{
			name:   "ascending",
			schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)},
			bounds: []pageBounds{{minVal: int32(1), maxVal: int32(2)}, {minVal: int32(3), maxVal: int32(4)}, {minVal: int32(3), maxVal: int32(5)}},
			want:   parquet.BoundaryOrder_ASCENDING,
		},
		{
			name:   "descending",
			schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)},
			bounds: []pageBounds{{minVal: int32(5), maxVal: int32(6)}, {minVal: int32(3), maxVal: int32(4)}, {minVal: int32(1), maxVal: int32(2)}},
			want:   parquet.BoundaryOrder_DESCENDING,
		},
		{
			name:   "unordered_when_only_minima_are_monotonic",
			schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)},
			bounds: []pageBounds{{minVal: int32(1), maxVal: int32(4)}, {minVal: int32(2), maxVal: int32(6)}, {minVal: int32(3), maxVal: int32(5)}},
			want:   parquet.BoundaryOrder_UNORDERED,
		},
		{
			name:   "equal_bounds_prefer_ascending",
			schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY)},
			bounds: []pageBounds{{minVal: "same", maxVal: "same"}, {minVal: "same", maxVal: "same"}},
			want:   parquet.BoundaryOrder_ASCENDING,
		},
		{
			name:   "null_pages_do_not_break_ordering",
			schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY)},
			bounds: []pageBounds{{minVal: "a", maxVal: "b"}, {}, {minVal: "c", maxVal: "d"}},
			want:   parquet.BoundaryOrder_ASCENDING,
		},
		{
			name:   "all_null_pages_are_ascending",
			schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)},
			bounds: []pageBounds{{}, {}},
			want:   parquet.BoundaryOrder_ASCENDING,
		},
		{
			name: "unsigned_logical_integer",
			schema: &parquet.SchemaElement{
				Type:        common.ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: false}},
			},
			bounds: []pageBounds{
				{minVal: int32(-2147483648), maxVal: int32(-2147483647)},
				{minVal: int32(-1), maxVal: int32(-1)},
			},
			want: parquet.BoundaryOrder_ASCENDING,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, columnIndexBoundaryOrder(tt.schema, tt.bounds))
		})
	}
}

func TestColumnIndexBoundaryOrder(t *testing.T) {
	type Entry struct {
		Value int32 `parquet:"name=value, type=INT32"`
	}

	tests := []struct {
		name   string
		values []int32
		want   parquet.BoundaryOrder
	}{
		{name: "ascending", values: []int32{1, 2, 3, 4, 5, 6, 7, 8}, want: parquet.BoundaryOrder_ASCENDING},
		{name: "descending", values: []int32{8, 7, 6, 5, 4, 3, 2, 1}, want: parquet.BoundaryOrder_DESCENDING},
		{name: "unordered", values: []int32{1, 2, 7, 8, 3, 4, 5, 6}, want: parquet.BoundaryOrder_UNORDERED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1), WithPageSize(8))
			require.NoError(t, err)
			for _, value := range tt.values {
				require.NoError(t, pw.Write(Entry{Value: value}))
			}
			require.NoError(t, pw.WriteStop())

			pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
			require.NoError(t, err)
			defer func() { require.NoError(t, pf.Close()) }()

			chunk := pr.Footer.RowGroups[0].Columns[0]
			index, err := readColumnIndex(pr.PFile, *chunk.ColumnIndexOffset)
			require.NoError(t, err)
			require.Greater(t, len(index.MinValues), 1)
			require.Equal(t, tt.want, index.BoundaryOrder)
		})
	}
}

func TestDictionaryColumnIndexBoundaryOrder(t *testing.T) {
	t.Run("utf8", func(t *testing.T) {
		type Entry struct {
			Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1), WithPageSize(8))
		require.NoError(t, err)
		for _, value := range []string{"azzz", "azzz", "baaa", "baaa", "czzz", "czzz", "daaa", "daaa"} {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { require.NoError(t, pf.Close()) }()
		chunk := pr.Footer.RowGroups[0].Columns[0]
		index, err := readColumnIndex(pr.PFile, *chunk.ColumnIndexOffset)
		require.NoError(t, err)
		require.Greater(t, len(index.MinValues), 1)
		require.Equal(t, parquet.BoundaryOrder_ASCENDING, index.BoundaryOrder)
	})

	t.Run("unsigned_int32", func(t *testing.T) {
		type Entry struct {
			Value uint32 `parquet:"name=value, type=INT32, convertedtype=UINT_32, encoding=PLAIN_DICTIONARY"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1), WithPageSize(8))
		require.NoError(t, err)
		values := []uint32{0x7ffffffe, 0x7ffffffe, 0x7fffffff, 0x7fffffff, 0x80000000, 0x80000000, 0xffffffff, 0xffffffff}
		for _, value := range values {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { require.NoError(t, pf.Close()) }()
		chunk := pr.Footer.RowGroups[0].Columns[0]
		index, err := readColumnIndex(pr.PFile, *chunk.ColumnIndexOffset)
		require.NoError(t, err)
		require.Greater(t, len(index.MinValues), 1)
		require.Equal(t, parquet.BoundaryOrder_ASCENDING, index.BoundaryOrder)
	})
}

// TestChunkOmitsStats pins that the tag is read from a data page. A dictionary chunk leads
// with a dictionary page the writer builds itself, which carries no tag of its own.
func TestChunkOmitsStats(t *testing.T) {
	newPage := func(pageType parquet.PageType, omitStats bool) *layout.Page {
		page := layout.NewDataPage()
		page.Header.Type = pageType
		info := &common.Tag{}
		info.OmitStats = omitStats
		page.Info = info
		return page
	}
	dictionaryPage := layout.NewDataPage()
	dictionaryPage.Header.Type = parquet.PageType_DICTIONARY_PAGE

	testCases := []struct {
		name  string
		pages []*layout.Page
		want  bool
	}{
		{"data page alone", []*layout.Page{newPage(parquet.PageType_DATA_PAGE, true)}, true},
		{"untagged data page", []*layout.Page{newPage(parquet.PageType_DATA_PAGE, false)}, false},
		{"behind an untagged dictionary page", []*layout.Page{dictionaryPage, newPage(parquet.PageType_DATA_PAGE, true)}, true},
		{"no pages", nil, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, chunkOmitsStats(tc.pages))
		})
	}
}
