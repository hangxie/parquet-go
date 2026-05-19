package writer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/bloomfilter"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/buffer"
)

func makeBloomPW(size int32) *ParquetWriter {
	info := &common.Tag{}
	info.BloomFilter = true
	info.BloomFilterSize = size
	return &ParquetWriter{
		SchemaHandler: &schema.SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{{}, {}},
			IndexMap:       map[int32]string{0: "root", 1: "root.col"},
			Infos:          []*common.Tag{nil, info},
		},
	}
}

func TestInitBloomFilters(t *testing.T) {
	t.Run("nil_schema_handler_is_safe", func(t *testing.T) {
		pw := &ParquetWriter{}
		require.NoError(t, pw.initBloomFilters())
		require.Nil(t, pw.bloomFilters)
	})

	t.Run("bloom_filter_true_default_size", func(t *testing.T) {
		pw := makeBloomPW(0) // 0 = use default
		require.NoError(t, pw.initBloomFilters())
		require.NotNil(t, pw.bloomFilters)
		bf, ok := pw.bloomFilters["root.col"]
		require.True(t, ok)
		require.NotNil(t, bf)
		require.Equal(t, int32(bloomfilter.DefaultNumBytes), bf.NumBytes())
	})

	t.Run("bloom_filter_true_custom_size", func(t *testing.T) {
		pw := makeBloomPW(2048)
		require.NoError(t, pw.initBloomFilters())
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
				SchemaElements: []*parquet.SchemaElement{{}, {}},
				IndexMap:       map[int32]string{0: "root", 1: "root.col"},
				Infos:          []*common.Tag{nil, info},
			},
		}
		require.NoError(t, pw.initBloomFilters())
		require.NotNil(t, pw.bloomFilters)
		require.Empty(t, pw.bloomFilters)
	})

	t.Run("size_below_min_returns_error", func(t *testing.T) {
		pw := makeBloomPW(bloomfilter.MinBytes - 1)
		err := pw.initBloomFilters()
		require.Error(t, err)
		require.Contains(t, err.Error(), "bloomfiltersize")
	})

	t.Run("size_above_max_returns_error", func(t *testing.T) {
		pw := makeBloomPW(bloomfilter.MaxBytes + 1)
		err := pw.initBloomFilters()
		require.Error(t, err)
		require.Contains(t, err.Error(), "bloomfiltersize")
	})

	t.Run("size_at_min_is_valid", func(t *testing.T) {
		pw := makeBloomPW(bloomfilter.MinBytes)
		require.NoError(t, pw.initBloomFilters())
		bf, ok := pw.bloomFilters["root.col"]
		require.True(t, ok)
		require.Equal(t, int32(bloomfilter.MinBytes), bf.NumBytes())
	})

	t.Run("size_at_max_is_valid", func(t *testing.T) {
		pw := makeBloomPW(bloomfilter.MaxBytes)
		require.NoError(t, pw.initBloomFilters())
		bf, ok := pw.bloomFilters["root.col"]
		require.True(t, ok)
		require.Equal(t, int32(bloomfilter.MaxBytes), bf.NumBytes())
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

		// PathInSchema uses the \x01 delimiter (common.PathToStr format).
		pathInSchema := []string{"root", "col"}
		internalPath := common.PathToStr(pathInSchema)

		pw := &ParquetWriter{
			SchemaHandler: &schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{NumChildren: &numChildren, Type: &leafType},
				},
				IndexMap: map[int32]string{
					0: internalPath,
				},
				Infos: []*common.Tag{info},
			},
			bloomFilters: map[string]*bloomfilter.Filter{
				internalPath: bf,
			},
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							{MetaData: &parquet.ColumnMetaData{PathInSchema: pathInSchema}},
						},
					},
				},
			},
		}

		err = pw.serializeBloomFilters()
		require.NoError(t, err)
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

// readBloomFilter reads a bloom filter at the given offset and returns the header and bitset.
func readBloomFilter(pf source.ParquetFileReader, offset int64) (*parquet.BloomFilterHeader, []byte, error) {
	header := parquet.NewBloomFilterHeader()
	tpf := thrift.NewTCompactProtocolFactoryConf(nil)
	triftReader, err := source.ConvertToThriftReader(pf, offset)
	if err != nil {
		return nil, nil, err
	}
	protocol := tpf.GetProtocol(triftReader)
	err = header.Read(context.Background(), protocol)
	if err != nil {
		return nil, nil, err
	}

	// Serialize the header to calculate its byte length so we can seek to the bitset.
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	headerBuf, err := ts.Write(context.TODO(), header)
	if err != nil {
		return nil, nil, err
	}
	headerLen := int64(len(headerBuf))

	// Seek to the bitset start and read it.
	if _, err := pf.Seek(offset+headerLen, 0); err != nil {
		return nil, nil, err
	}
	bitset := make([]byte, header.NumBytes)
	if _, err := pf.Read(bitset); err != nil {
		return nil, nil, err
	}
	return header, bitset, nil
}

func TestBloomFilter(t *testing.T) {
	t.Run("basic_bloom_filter_metadata", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		pw, buf, err := createTestParquetWriter(new(BloomRecord), WithNP(1))
		require.NoError(t, err)

		for i := range 100 {
			require.NoError(t, pw.Write(BloomRecord{ID: int64(i), Name: fmt.Sprintf("name-%d", i)}))
		}
		require.NoError(t, pw.WriteStop())

		// Read footer and verify bloom filter metadata
		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := reader.NewParquetReader(pf, new(BloomRecord), reader.WithNP(1))
		require.NoError(t, err)

		require.Len(t, pr.Footer.RowGroups, 1)
		rg := pr.Footer.RowGroups[0]

		// ID column (index 0) should have bloom filter
		idCol := rg.Columns[0]
		require.True(t, idCol.MetaData.IsSetBloomFilterOffset())
		require.True(t, idCol.MetaData.IsSetBloomFilterLength())
		require.Greater(t, idCol.MetaData.GetBloomFilterOffset(), int64(0))
		require.Greater(t, idCol.MetaData.GetBloomFilterLength(), int32(0))

		// Name column (index 1) should NOT have bloom filter
		nameCol := rg.Columns[1]
		require.False(t, nameCol.MetaData.IsSetBloomFilterOffset())

		_ = pr.ReadStop()
	})

	t.Run("bloom_filter_content_check", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		pw, buf, err := createTestParquetWriter(new(BloomRecord), WithNP(1))
		require.NoError(t, err)

		for i := range 50 {
			require.NoError(t, pw.Write(BloomRecord{ID: int64(i * 100)}))
		}
		require.NoError(t, pw.WriteStop())

		// Read the bloom filter data and verify it works
		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := reader.NewParquetReader(pf, new(BloomRecord), reader.WithNP(1))
		require.NoError(t, err)

		rg := pr.Footer.RowGroups[0]
		idCol := rg.Columns[0]

		header, bitset, err := readBloomFilter(pf, idCol.MetaData.GetBloomFilterOffset())
		require.NoError(t, err)
		require.NotNil(t, header.Algorithm.BLOCK)
		require.NotNil(t, header.Hash.XXHASH)
		require.NotNil(t, header.Compression.UNCOMPRESSED)

		f, err := bloomfilter.FromBitset(bitset)
		require.NoError(t, err)

		// Values that were written should be found
		for i := range 50 {
			h, err := bloomfilter.HashValue(int64(i*100), parquet.Type_INT64)
			require.NoError(t, err)
			require.True(t, f.Check(h))
		}

		_ = pr.ReadStop()
	})

	t.Run("bloom_filter_with_dict_encoding", func(t *testing.T) {
		type BloomRecord struct {
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, bloomfilter=true"`
		}

		pw, buf, err := createTestParquetWriter(new(BloomRecord), WithNP(1))
		require.NoError(t, err)

		names := []string{"alice", "bob", "charlie", "alice", "bob"}
		for _, name := range names {
			require.NoError(t, pw.Write(BloomRecord{Name: name}))
		}
		require.NoError(t, pw.WriteStop())

		// Read and verify bloom filter works for dict-encoded column
		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := reader.NewParquetReader(pf, new(BloomRecord), reader.WithNP(1))
		require.NoError(t, err)

		rg := pr.Footer.RowGroups[0]
		col := rg.Columns[0]
		require.True(t, col.MetaData.IsSetBloomFilterOffset())

		_, bitset, err := readBloomFilter(pf, col.MetaData.GetBloomFilterOffset())
		require.NoError(t, err)

		f, err := bloomfilter.FromBitset(bitset)
		require.NoError(t, err)

		// Written values should be found
		for _, name := range []string{"alice", "bob", "charlie"} {
			h, err := bloomfilter.HashValue(name, parquet.Type_BYTE_ARRAY)
			require.NoError(t, err)
			require.True(t, f.Check(h))
		}

		_ = pr.ReadStop()
	})

	t.Run("bloom_filter_custom_size", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true, bloomfiltersize=2048"`
		}

		pw, buf, err := createTestParquetWriter(new(BloomRecord), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := reader.NewParquetReader(pf, new(BloomRecord), reader.WithNP(1))
		require.NoError(t, err)

		rg := pr.Footer.RowGroups[0]
		col := rg.Columns[0]

		header, _, err := readBloomFilter(pf, col.MetaData.GetBloomFilterOffset())
		require.NoError(t, err)
		require.Equal(t, int32(2048), header.NumBytes)

		_ = pr.ReadStop()
	})

	t.Run("bloom_filter_with_nulls", func(t *testing.T) {
		type BloomRecord struct {
			ID *int64 `parquet:"name=id, type=INT64, repetitiontype=OPTIONAL, bloomfilter=true"`
		}

		pw, buf, err := createTestParquetWriter(new(BloomRecord), WithNP(1))
		require.NoError(t, err)

		v1 := int64(100)
		v2 := int64(200)
		require.NoError(t, pw.Write(BloomRecord{ID: &v1}))
		require.NoError(t, pw.Write(BloomRecord{ID: nil}))
		require.NoError(t, pw.Write(BloomRecord{ID: &v2}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := reader.NewParquetReader(pf, new(BloomRecord), reader.WithNP(1))
		require.NoError(t, err)

		rg := pr.Footer.RowGroups[0]
		col := rg.Columns[0]
		require.True(t, col.MetaData.IsSetBloomFilterOffset())

		_, bitset, err := readBloomFilter(pf, col.MetaData.GetBloomFilterOffset())
		require.NoError(t, err)

		f, err := bloomfilter.FromBitset(bitset)
		require.NoError(t, err)

		// Non-null values should be found
		h1, err := bloomfilter.HashValue(int64(100), parquet.Type_INT64)
		require.NoError(t, err)
		require.True(t, f.Check(h1))

		h2, err := bloomfilter.HashValue(int64(200), parquet.Type_INT64)
		require.NoError(t, err)
		require.True(t, f.Check(h2))

		_ = pr.ReadStop()
	})

	t.Run("bloom_filter_multiple_row_groups", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		pw, buf, err := createTestParquetWriter(new(BloomRecord), WithNP(1), WithRowGroupSize(256), WithPageSize(64))
		require.NoError(t, err)

		// Write enough data to force multiple row groups
		for i := range 1000 {
			require.NoError(t, pw.Write(BloomRecord{
				ID:   int64(i),
				Name: fmt.Sprintf("this-is-a-long-name-to-increase-row-size-%d", i),
			}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := reader.NewParquetReader(pf, new(BloomRecord), reader.WithNP(1))
		require.NoError(t, err)

		require.Greater(t, len(pr.Footer.RowGroups), 1)

		// Each row group should have bloom filter metadata on the ID column
		for _, rg := range pr.Footer.RowGroups {
			col := rg.Columns[0]
			require.True(t, col.MetaData.IsSetBloomFilterOffset())
		}

		_ = pr.ReadStop()
	})

	t.Run("no_bloom_filter_when_not_configured", func(t *testing.T) {
		type NoBloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		pw, buf, err := createTestParquetWriter(new(NoBloomRecord), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(NoBloomRecord{ID: 1, Name: "test"}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := reader.NewParquetReader(pf, new(NoBloomRecord), reader.WithNP(1))
		require.NoError(t, err)

		for _, col := range pr.Footer.RowGroups[0].Columns {
			require.False(t, col.MetaData.IsSetBloomFilterOffset())
		}

		_ = pr.ReadStop()
	})
}
