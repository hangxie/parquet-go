package writer

import (
	"sync"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/bloomfilter"
	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

func TestWriteOffsetIndexes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		pw := &ParquetWriter{}
		ts := thrift.NewTSerializer()

		require.NoError(t, pw.writeOffsetIndexes(ts))
	})

	t.Run("write_error", func(t *testing.T) {
		pw := &ParquetWriter{
			PFile: &invalidFileWriter{},
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							parquet.NewColumnChunk(),
						},
					},
				},
			},
			offsetIndexes: []*parquet.OffsetIndex{
				parquet.NewOffsetIndex(),
			},
		}
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)

		err := pw.writeOffsetIndexes(ts)
		require.ErrorIs(t, err, errWrite)
		require.Contains(t, err.Error(), "write offset index")
	})
}

func TestWriteColumnIndexes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		pw := &ParquetWriter{}
		ts := thrift.NewTSerializer()

		require.NoError(t, pw.writeColumnIndexes(ts))
	})

	t.Run("write_error", func(t *testing.T) {
		pw := &ParquetWriter{
			PFile: &invalidFileWriter{},
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							parquet.NewColumnChunk(),
						},
					},
				},
			},
			columnIndexes: []*parquet.ColumnIndex{
				parquet.NewColumnIndex(),
			},
		}
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)

		err := pw.writeColumnIndexes(ts)
		require.ErrorIs(t, err, errWrite)
		require.Contains(t, err.Error(), "write column index")
	})
}

func TestInitBloomFilters(t *testing.T) {
	t.Run("nil_schema_handler_is_safe", func(t *testing.T) {
		pw := &ParquetWriter{}
		pw.initBloomFilters()
		require.Nil(t, pw.bloomFilters)
	})

	t.Run("bloom_filter_true_default_size", func(t *testing.T) {
		info := &common.Tag{}
		info.BloomFilter = true
		info.BloomFilterSize = 0 // use default

		pw := &ParquetWriter{
			SchemaHandler: &schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{}, // root (index 0)
					{}, // leaf (index 1)
				},
				IndexMap: map[int32]string{
					0: "root",
					1: "root.col",
				},
				Infos: []*common.Tag{
					nil,  // root has no info
					info, // leaf has bloom filter enabled
				},
			},
		}

		pw.initBloomFilters()
		require.NotNil(t, pw.bloomFilters)
		bf, ok := pw.bloomFilters["root.col"]
		require.True(t, ok)
		require.NotNil(t, bf)
		require.Equal(t, int32(bloomfilter.DefaultNumBytes), bf.NumBytes())
	})

	t.Run("bloom_filter_true_custom_size", func(t *testing.T) {
		info := &common.Tag{}
		info.BloomFilter = true
		info.BloomFilterSize = 2048

		pw := &ParquetWriter{
			SchemaHandler: &schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{},
					{},
				},
				IndexMap: map[int32]string{
					0: "root",
					1: "root.col",
				},
				Infos: []*common.Tag{
					nil,
					info,
				},
			},
		}

		pw.initBloomFilters()
		require.NotNil(t, pw.bloomFilters)
		bf, ok := pw.bloomFilters["root.col"]
		require.True(t, ok)
		require.Equal(t, int32(2048), bf.NumBytes())
	})

	t.Run("bloom_filter_false_skipped", func(t *testing.T) {
		info := &common.Tag{}
		info.BloomFilter = false

		pw := &ParquetWriter{
			SchemaHandler: &schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{},
					{},
				},
				IndexMap: map[int32]string{
					0: "root",
					1: "root.col",
				},
				Infos: []*common.Tag{
					nil,
					info,
				},
			},
		}

		pw.initBloomFilters()
		require.NotNil(t, pw.bloomFilters)
		require.Empty(t, pw.bloomFilters)
	})
}

func TestSerializeBloomFilters(t *testing.T) {
	t.Run("empty_bloom_filters_returns_nil", func(t *testing.T) {
		pw := &ParquetWriter{
			bloomFilters: map[string]*bloomfilter.Filter{},
		}
		err := pw.serializeBloomFilters()
		require.NoError(t, err)
		require.Empty(t, pw.bloomFilterData)
	})

	t.Run("happy_path", func(t *testing.T) {
		numChildren := int32(0)
		leafType := parquet.Type_INT64

		// Use BloomFilter=false so initBloomFilters (called at the end of
		// serializeBloomFilters) does not re-add an entry; that lets us
		// verify the map is reset to an empty (but non-nil) map.
		info := &common.Tag{}
		info.BloomFilter = false

		bf := bloomfilter.New(bloomfilter.DefaultNumBytes)
		h, err := bloomfilter.HashValue(int64(42), parquet.Type_INT64)
		require.NoError(t, err)
		bf.Insert(h)

		pw := &ParquetWriter{
			SchemaHandler: &schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{NumChildren: &numChildren, Type: &leafType},
				},
				IndexMap: map[int32]string{
					0: "root.col",
				},
				Infos: []*common.Tag{info},
			},
			bloomFilters: map[string]*bloomfilter.Filter{
				"root.col": bf,
			},
		}

		err = pw.serializeBloomFilters()
		require.NoError(t, err)

		// bloomFilterData should have been populated with one nil entry
		// (no matching key in bloomFilters for the leaf with BloomFilter=false
		// — wait, the key IS present; it was set before calling serializeBloomFilters).
		// The leaf path is in bloomFilters, so a non-nil entry is expected.
		require.NotEmpty(t, pw.bloomFilterData)
		require.NotNil(t, pw.bloomFilterData[0])

		// bloomFilters should have been reset to a fresh empty map by initBloomFilters.
		require.NotNil(t, pw.bloomFilters)
		require.Empty(t, pw.bloomFilters)
	})
}

func TestInsertBloomValues(t *testing.T) {
	t.Run("missing_path_is_noop", func(t *testing.T) {
		pw := &ParquetWriter{
			bloomFilters: map[string]*bloomfilter.Filter{},
		}
		typ := parquet.Type_INT64
		table := &layout.Table{
			Schema: &parquet.SchemaElement{Type: &typ},
			Values: []any{int64(1), int64(2)},
		}
		var mu sync.Mutex
		// Should not panic
		pw.insertBloomValues("nonexistent", table, &mu)
	})

	t.Run("nil_schema_type_is_noop", func(t *testing.T) {
		bf := bloomfilter.New(bloomfilter.DefaultNumBytes)
		pw := &ParquetWriter{
			bloomFilters: map[string]*bloomfilter.Filter{
				"root.col": bf,
			},
		}
		table := &layout.Table{
			Schema: &parquet.SchemaElement{Type: nil},
			Values: []any{int64(1)},
		}
		var mu sync.Mutex
		// Should not panic or insert anything
		pw.insertBloomValues("root.col", table, &mu)
		// The filter should still be empty (no inserts)
		h, err := bloomfilter.HashValue(int64(1), parquet.Type_INT64)
		require.NoError(t, err)
		require.False(t, bf.Check(h))
	})

	t.Run("non_nil_values_get_inserted", func(t *testing.T) {
		bf := bloomfilter.New(bloomfilter.DefaultNumBytes)
		pw := &ParquetWriter{
			bloomFilters: map[string]*bloomfilter.Filter{
				"root.col": bf,
			},
		}
		typ := parquet.Type_INT64
		table := &layout.Table{
			Schema: &parquet.SchemaElement{Type: &typ},
			Values: []any{int64(10), int64(20), int64(30)},
		}
		var mu sync.Mutex
		pw.insertBloomValues("root.col", table, &mu)

		for _, v := range []int64{10, 20, 30} {
			h, err := bloomfilter.HashValue(v, parquet.Type_INT64)
			require.NoError(t, err)
			require.True(t, bf.Check(h), "expected %d to be in bloom filter", v)
		}
	})

	t.Run("nil_values_are_skipped", func(t *testing.T) {
		bf := bloomfilter.New(bloomfilter.DefaultNumBytes)
		pw := &ParquetWriter{
			bloomFilters: map[string]*bloomfilter.Filter{
				"root.col": bf,
			},
		}
		typ := parquet.Type_INT64
		table := &layout.Table{
			Schema: &parquet.SchemaElement{Type: &typ},
			Values: []any{int64(99), nil, int64(88)},
		}
		var mu sync.Mutex
		pw.insertBloomValues("root.col", table, &mu)

		// Non-nil values should be found
		for _, v := range []int64{99, 88} {
			h, err := bloomfilter.HashValue(v, parquet.Type_INT64)
			require.NoError(t, err)
			require.True(t, bf.Check(h))
		}
	})
}
