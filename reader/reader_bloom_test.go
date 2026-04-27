package reader

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/bloomfilter"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	phttp "github.com/hangxie/parquet-go/v3/source/http"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
)

func TestBloomFilterCheck(t *testing.T) {
	t.Run("basic_check", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)

		for i := range 100 {
			require.NoError(t, pw.Write(BloomRecord{
				ID:   int64(i * 100),
				Name: fmt.Sprintf("name-%d", i),
			}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		// Values that were written should return true (might contain)
		found, err := pr.BloomFilterCheck("id", 0, int64(0))
		require.NoError(t, err)
		require.True(t, found)

		found, err = pr.BloomFilterCheck("id", 0, int64(5000))
		require.NoError(t, err)
		require.True(t, found)

		found, err = pr.BloomFilterCheck("name", 0, "name-50")
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("absent_values", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)

		// Write specific values
		for i := range 10 {
			require.NoError(t, pw.Write(BloomRecord{ID: int64(i * 1000)}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		// Test that values NOT written are likely to return false.
		// With 10 values in a 1024-byte filter, false positive rate should be very low.
		falsePositives := 0
		for i := 1; i < 1000; i++ {
			found, err := pr.BloomFilterCheck("id", 0, int64(i))
			require.NoError(t, err)
			if found {
				falsePositives++
			}
		}
		// Very conservative check: should have < 5% false positives
		require.Less(t, falsePositives, 50)
	})

	t.Run("no_bloom_filter_column", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42, Name: "test"}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		// Column without bloom filter should return true (conservative)
		found, err := pr.BloomFilterCheck("name", 0, "test")
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("invalid_row_group_index", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		_, err = pr.BloomFilterCheck("id", -1, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "out of range")

		_, err = pr.BloomFilterCheck("id", 5, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "out of range")
	})

	t.Run("invalid_column_path", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		_, err = pr.BloomFilterCheck("nonexistent", 0, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("round_trip_int_and_string", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)

		for i := range 50 {
			require.NoError(t, pw.Write(BloomRecord{
				ID:   int64(i),
				Name: fmt.Sprintf("user-%d", i),
			}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		// Verify all written values pass the bloom filter check
		for i := range 50 {
			found, err := pr.BloomFilterCheck("id", 0, int64(i))
			require.NoError(t, err)
			require.True(t, found)

			found, err = pr.BloomFilterCheck("name", 0, fmt.Sprintf("user-%d", i))
			require.NoError(t, err)
			require.True(t, found)
		}
	})

	t.Run("multiple_row_groups_independence", func(t *testing.T) {
		type BloomRecord struct {
			ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1), writer.WithRowGroupSize(256), writer.WithPageSize(64))
		require.NoError(t, err)

		for i := range 1000 {
			require.NoError(t, pw.Write(BloomRecord{
				ID:   int64(i),
				Name: fmt.Sprintf("a-long-name-to-force-multiple-row-groups-%d", i),
			}))
		}
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		require.Greater(t, len(pr.Footer.RowGroups), 1)

		// Each row group should support bloom filter checks
		for rgIdx := range pr.Footer.RowGroups {
			found, err := pr.BloomFilterCheck("id", rgIdx, int64(0))
			require.NoError(t, err)
			// ID 0 is in the first row group; other row groups may return false
			_ = found
		}

		// First row group should definitely contain ID 0
		found, err := pr.BloomFilterCheck("id", 0, int64(0))
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("hash_value_error", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		// Pass a string value for an INT64 column → HashValue encoding error
		_, err = pr.BloomFilterCheck("id", 0, "not-an-int64")
		require.Error(t, err)
		require.Contains(t, err.Error(), "hash value")
	})

	t.Run("clone_error", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		// Replace PFile with a mock that fails on Clone
		pr.PFile = &failCloneReader{ParquetFileReader: pf}
		_, err = pr.BloomFilterCheck("id", 0, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "clone file reader")
	})

	t.Run("read_bloom_filter_error", func(t *testing.T) {
		type BloomRecord struct {
			ID int64 `parquet:"name=id, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		// Corrupt the bloom filter offset to point to the start of the file (PAR1 magic)
		for _, cc := range pr.Footer.RowGroups[0].Columns {
			if cc.MetaData.IsSetBloomFilterOffset() {
				badOffset := int64(0)
				cc.MetaData.BloomFilterOffset = &badOffset
			}
		}
		_, err = pr.BloomFilterCheck("id", 0, int64(42))
		require.Error(t, err)
		require.Contains(t, err.Error(), "read bloom filter")
	})
}

func TestDetectBloomFilters(t *testing.T) {
	t.Run("nil_footer", func(t *testing.T) {
		pr := &ParquetReader{
			Footer:        nil,
			SchemaHandler: &schema.SchemaHandler{},
		}
		pr.detectBloomFilters() // should not panic
	})

	t.Run("empty_row_groups", func(t *testing.T) {
		pr := &ParquetReader{
			Footer:        &parquet.FileMetaData{},
			SchemaHandler: &schema.SchemaHandler{},
		}
		pr.detectBloomFilters() // should not panic
	})

	t.Run("nil_schema_handler", func(t *testing.T) {
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{{}},
			},
			SchemaHandler: nil,
		}
		pr.detectBloomFilters() // should not panic
	})

	t.Run("nil_first_row_group", func(t *testing.T) {
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{nil},
			},
			SchemaHandler: &schema.SchemaHandler{},
		}
		pr.detectBloomFilters() // should not panic
	})

	t.Run("column_without_bloom_offset", func(t *testing.T) {
		// Column with metadata but no BloomFilterOffset set
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							{MetaData: &parquet.ColumnMetaData{PathInSchema: []string{"col1"}}},
							{MetaData: nil}, // nil metadata
						},
					},
				},
			},
			SchemaHandler: &schema.SchemaHandler{
				Infos:    []*common.Tag{{InName: "root"}, {InName: "col1"}},
				MapIndex: map[string]int32{"root\x01col1": 1},
			},
		}
		pr.detectBloomFilters()
		require.False(t, pr.SchemaHandler.Infos[1].BloomFilter)
	})

	t.Run("column_path_not_in_map", func(t *testing.T) {
		offset := int64(100)
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							{MetaData: &parquet.ColumnMetaData{
								PathInSchema:      []string{"unknown_col"},
								BloomFilterOffset: &offset,
							}},
						},
					},
				},
			},
			SchemaHandler: &schema.SchemaHandler{
				Infos:    []*common.Tag{{InName: "root"}},
				MapIndex: map[string]int32(nil),
			},
		}
		pr.detectBloomFilters() // should not panic, path not found
	})

	t.Run("sets_bloom_filter_no_pfile", func(t *testing.T) {
		offset := int64(100)
		rootName := common.ParGoRootInName
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							{MetaData: &parquet.ColumnMetaData{
								PathInSchema:      []string{"id"},
								BloomFilterOffset: &offset,
							}},
						},
					},
				},
			},
			SchemaHandler: &schema.SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{{Name: rootName}},
				Infos:          []*common.Tag{{InName: rootName}, {InName: "ID"}},
				MapIndex:       map[string]int32{common.ParGoRootInName + common.ParGoPathDelimiter + "id": 1},
			},
		}
		pr.detectBloomFilters()
		require.True(t, pr.SchemaHandler.Infos[1].BloomFilter)
		// BloomFilterSize stays 0 when PFile is nil (cannot read from file)
		require.Equal(t, int32(0), pr.SchemaHandler.Infos[1].BloomFilterSize)
	})

	t.Run("bloom_filter_size_round_trip", func(t *testing.T) {
		type BloomRecord struct {
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true, bloomfiltersize=4096"`
		}
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.Write(BloomRecord{Name: "test"}))
		require.NoError(t, pw.WriteStop())
		_ = fw.Close()

		fr := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(fr, nil, WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		// BloomFilterSize should be the bitset size (4096), not including Thrift header overhead.
		// After RenameSchema, the internal name "Name" (Go field name) is used in MapIndex.
		nameIdx, ok := pr.SchemaHandler.MapIndex[common.ParGoRootInName+common.ParGoPathDelimiter+"Name"]
		require.True(t, ok)
		require.True(t, pr.SchemaHandler.Infos[nameIdx].BloomFilter)
		require.Equal(t, int32(4096), pr.SchemaHandler.Infos[nameIdx].BloomFilterSize)
	})
}

// failCloneReader wraps a ParquetFileReader and makes Clone() return an error.
type failCloneReader struct {
	source.ParquetFileReader
}

func (f *failCloneReader) Clone() (source.ParquetFileReader, error) {
	return nil, fmt.Errorf("clone failed")
}

// TestBloomFilterInterop reads parquet files generated by parquet-mr (Java reference
// implementation) from apache/parquet-testing and verifies our bloom filter reader
// can parse the metadata and bitset data correctly.
func TestBloomFilterInterop(t *testing.T) {
	t.Run("read_bloom_from_parquet_mr", func(t *testing.T) {
		bloomURL := "https://github.com/apache/parquet-testing/raw/refs/heads/master/data/data_index_bloom_encoding_stats.parquet"
		httpReader, err := phttp.NewHttpReader(bloomURL, false, false, map[string]string{})
		require.NoError(t, err)
		defer func() { _ = httpReader.Close() }()

		pr, err := NewParquetReader(httpReader, nil, WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		require.NotEmpty(t, pr.Footer.RowGroups)
		rg := pr.Footer.RowGroups[0]

		// Find a column with bloom filter metadata and read the bloom filter
		foundBloom := false
		for _, cc := range rg.Columns {
			if !cc.MetaData.IsSetBloomFilterOffset() {
				continue
			}
			foundBloom = true
			offset := cc.MetaData.GetBloomFilterOffset()

			pf, cloneErr := httpReader.Clone()
			require.NoError(t, cloneErr)

			filter, readErr := bloomfilter.ReadBloomFilter(pf, offset)
			_ = pf.Close()
			require.NoError(t, readErr)
			require.Greater(t, filter.NumBytes(), int32(0))
		}
		require.True(t, foundBloom)
	})

	t.Run("bloom_filter_length_populated", func(t *testing.T) {
		bloomWithLengthURL := "https://github.com/apache/parquet-testing/raw/refs/heads/master/data/data_index_bloom_encoding_with_length.parquet"
		httpReader, err := phttp.NewHttpReader(bloomWithLengthURL, false, false, map[string]string{})
		require.NoError(t, err)
		defer func() { _ = httpReader.Close() }()

		pr, err := NewParquetReader(httpReader, nil, WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		require.NotEmpty(t, pr.Footer.RowGroups)
		rg := pr.Footer.RowGroups[0]

		foundLength := false
		for _, cc := range rg.Columns {
			if cc.MetaData.BloomFilterLength == nil {
				continue
			}
			foundLength = true
			require.Greater(t, *cc.MetaData.BloomFilterLength, int32(0))

			// Also verify the bloom filter can be read using the offset
			require.True(t, cc.MetaData.IsSetBloomFilterOffset())
			pf, cloneErr := httpReader.Clone()
			require.NoError(t, cloneErr)

			filter, readErr := bloomfilter.ReadBloomFilter(pf, cc.MetaData.GetBloomFilterOffset())
			_ = pf.Close()
			require.NoError(t, readErr)
			require.Greater(t, filter.NumBytes(), int32(0))
		}
		require.True(t, foundLength)
	})
}
