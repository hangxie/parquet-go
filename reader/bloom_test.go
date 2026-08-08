package reader

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/bloomfilter"
	"github.com/hangxie/parquet-go/v3/parquet"
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
		pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)

		for i := range 100 {
			require.NoError(t, pw.WriteWithContext(context.Background(), BloomRecord{
				ID:   int64(i * 100),
				Name: fmt.Sprintf("name-%d", i),
			}))
		}
		require.NoError(t, pw.WriteStopWithContext(context.Background()))

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
		pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)

		// Write specific values
		for i := range 10 {
			require.NoError(t, pw.WriteWithContext(context.Background(), BloomRecord{ID: int64(i * 1000)}))
		}
		require.NoError(t, pw.WriteStopWithContext(context.Background()))

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
		pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.WriteWithContext(context.Background(), BloomRecord{ID: 42, Name: "test"}))
		require.NoError(t, pw.WriteStopWithContext(context.Background()))

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
		pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.WriteWithContext(context.Background(), BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStopWithContext(context.Background()))

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
		pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		require.NoError(t, pw.WriteWithContext(context.Background(), BloomRecord{ID: 42}))
		require.NoError(t, pw.WriteStopWithContext(context.Background()))

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
		pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)

		for i := range 50 {
			require.NoError(t, pw.WriteWithContext(context.Background(), BloomRecord{
				ID:   int64(i),
				Name: fmt.Sprintf("user-%d", i),
			}))
		}
		require.NoError(t, pw.WriteStopWithContext(context.Background()))

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
		//nolint:staticcheck
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1), writer.WithRowGroupSize(256), writer.WithPageSize(64))
		require.NoError(t, err)

		for i := range 1000 {
			//nolint:staticcheck
			require.NoError(t, pw.Write(BloomRecord{
				ID:   int64(i),
				Name: fmt.Sprintf("a-long-name-to-force-multiple-row-groups-%d", i),
			}))
		}
		//nolint:staticcheck
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
		//nolint:staticcheck
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		//nolint:staticcheck
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		//nolint:staticcheck
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
		//nolint:staticcheck
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		//nolint:staticcheck
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		//nolint:staticcheck
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
		//nolint:staticcheck
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		//nolint:staticcheck
		require.NoError(t, pw.Write(BloomRecord{ID: 42}))
		//nolint:staticcheck
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

	t.Run("dotted_column_name", func(t *testing.T) {
		// A column name containing a dot is a single path component.
		type BloomRecord struct {
			Dotted int64 `parquet:"name=a.b, type=INT64, bloomfilter=true"`
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		//nolint:staticcheck
		pw, err := writer.NewParquetWriter(fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		for i := range 100 {
			//nolint:staticcheck
			require.NoError(t, pw.Write(BloomRecord{Dotted: int64(i * 100)}))
		}
		//nolint:staticcheck
		require.NoError(t, pw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		pr, err := NewParquetReader(pf, new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		found, err := pr.BloomFilterCheck("a.b", 0, int64(0))
		require.NoError(t, err)
		require.True(t, found)
	})
}

// failCloneReader wraps a ParquetFileReader and makes Clone() return an error.
type failCloneReader struct {
	source.ParquetFileReader
}

func (f *failCloneReader) Clone() (source.ParquetFileReader, error) {
	return nil, fmt.Errorf("clone failed")
}

// countingReader tallies the bytes read through a file reader and its clones.
type countingReader struct {
	source.ParquetFileReader
	bytesRead *int64
}

func (c *countingReader) Clone() (source.ParquetFileReader, error) {
	clone, err := c.ParquetFileReader.Clone()
	if err != nil {
		return nil, err
	}
	return &countingReader{ParquetFileReader: clone, bytesRead: c.bytesRead}, nil
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.ParquetFileReader.Read(p)
	*c.bytesRead += int64(n)
	return n, err
}

// TestBloomFilterInterop reads parquet files generated by parquet-mr (Java reference
// implementation) from apache/parquet-testing and verifies our bloom filter reader
// can parse the metadata and bitset data correctly.
func TestBloomFilterInterop(t *testing.T) {
	t.Run("read_bloom_from_parquet_mr", func(t *testing.T) {
		bloomURL := "https://github.com/apache/parquet-testing/raw/refs/heads/master/data/data_index_bloom_encoding_stats.parquet"
		httpReader, err := phttp.NewHttpReaderWithContext(context.Background(), bloomURL, false, false, map[string]string{})
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

			filter, readErr := bloomfilter.ReadBloomFilterWithContext(context.Background(), pf, offset)
			_ = pf.Close()
			require.NoError(t, readErr)
			require.Greater(t, filter.NumBytes(), int32(0))
		}
		require.True(t, foundBloom)
	})

	t.Run("bloom_filter_length_populated", func(t *testing.T) {
		bloomWithLengthURL := "https://github.com/apache/parquet-testing/raw/refs/heads/master/data/data_index_bloom_encoding_with_length.parquet"
		httpReader, err := phttp.NewHttpReaderWithContext(context.Background(), bloomWithLengthURL, false, false, map[string]string{})
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

			filter, readErr := bloomfilter.ReadBloomFilterWithContext(context.Background(), pf, cc.MetaData.GetBloomFilterOffset())
			_ = pf.Close()
			require.NoError(t, readErr)
			require.Greater(t, filter.NumBytes(), int32(0))
		}
		require.True(t, foundLength)
	})
}

func TestBloomFilterSize(t *testing.T) {
	type BloomRecord struct {
		ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true, bloomfiltersize=4096"`
		Big  int64  `parquet:"name=big, type=INT64, bloomfilter=true, bloomfiltersize=65536"`
		Age  int32  `parquet:"name=age, type=INT32"`
	}

	// writeMultiRowGroup writes rowGroups row groups, each holding 100 rows.
	writeMultiRowGroup := func(t *testing.T, rowGroups int) []byte {
		t.Helper()
		ctx := context.Background()
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		pw, err := writer.NewParquetWriterWithContext(ctx, fw, new(BloomRecord), writer.WithNP(1))
		require.NoError(t, err)
		for rg := range rowGroups {
			for i := range 100 {
				require.NoError(t, pw.WriteWithContext(ctx, BloomRecord{
					ID:   int64(rg*100 + i),
					Name: fmt.Sprintf("user-%d-%d", rg, i),
					Age:  int32(i % 50),
				}))
			}
			require.NoError(t, pw.FlushWithContext(ctx, true))
		}
		require.NoError(t, pw.WriteStopWithContext(ctx))
		require.Len(t, pw.Footer.RowGroups, rowGroups)
		return buf.Bytes()
	}

	openReader := func(t *testing.T, data []byte) *ParquetReader {
		t.Helper()
		pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(BloomRecord), WithNP(1))
		require.NoError(t, err)
		t.Cleanup(func() { _ = pr.ReadStop() })
		return pr
	}

	columnIndex := func(t *testing.T, rg *parquet.RowGroup, name string) int {
		t.Helper()
		for i, cc := range rg.Columns {
			if common.PathToStr(cc.MetaData.GetPathInSchema()) == name {
				return i
			}
		}
		t.Fatalf("column %q not found in row group", name)
		return -1
	}

	t.Run("every_row_group", func(t *testing.T) {
		ctx := context.Background()
		data := writeMultiRowGroup(t, 3)
		pr := openReader(t, data)

		for rgIndex, rg := range pr.Footer.RowGroups {
			for column, want := range map[string]int32{"id": bloomfilter.DefaultNumBytes, "name": 4096} {
				size, err := pr.BloomFilterSizeWithContext(ctx, column, rgIndex)
				require.NoError(t, err)
				require.Equal(t, want, size, "column %s row group %d", column, rgIndex)

				// The size must come from this row group's own filter, not row group 0's.
				cc := rg.Columns[columnIndex(t, rg, column)]
				filter, err := bloomfilter.ReadBloomFilterWithContext(ctx, bytes.NewReader(data), cc.MetaData.GetBloomFilterOffset())
				require.NoError(t, err)
				require.Equal(t, filter.NumBytes(), size)
			}
		}
	})

	t.Run("size_differs_between_row_groups", func(t *testing.T) {
		ctx := context.Background()
		pr := openReader(t, writeMultiRowGroup(t, 2))

		// Point the second row group's "id" chunk at that row group's 65536-byte filter,
		// so a lookup that sourced sizes from row group 0 would report 1024 instead.
		rg1 := pr.Footer.RowGroups[1]
		bigOffset := rg1.Columns[columnIndex(t, rg1, "big")].MetaData.GetBloomFilterOffset()
		rg1.Columns[columnIndex(t, rg1, "id")].MetaData.BloomFilterOffset = &bigOffset

		size, err := pr.BloomFilterSizeWithContext(ctx, "id", 0)
		require.NoError(t, err)
		require.Equal(t, int32(bloomfilter.DefaultNumBytes), size)

		size, err = pr.BloomFilterSizeWithContext(ctx, "id", 1)
		require.NoError(t, err)
		require.Equal(t, int32(65536), size)
	})

	t.Run("filter_only_outside_row_group_zero", func(t *testing.T) {
		ctx := context.Background()
		pr := openReader(t, writeMultiRowGroup(t, 2))

		// Simulate a writer that filtered "id" in the second row group only.
		rg0 := pr.Footer.RowGroups[0]
		rg0.Columns[columnIndex(t, rg0, "id")].MetaData.BloomFilterOffset = nil

		size, err := pr.BloomFilterSizeWithContext(ctx, "id", 0)
		require.NoError(t, err)
		require.Zero(t, size)

		size, err = pr.BloomFilterSizeWithContext(ctx, "id", 1)
		require.NoError(t, err)
		require.Equal(t, int32(bloomfilter.DefaultNumBytes), size)
	})

	t.Run("column_without_bloom_filter", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		size, err := pr.BloomFilterSizeWithContext(context.Background(), "age", 0)
		require.NoError(t, err)
		require.Zero(t, size)
	})

	t.Run("invalid_row_group_index", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		_, err := pr.BloomFilterSizeWithContext(context.Background(), "id", -1)
		require.ErrorContains(t, err, "out of range")

		_, err = pr.BloomFilterSizeWithContext(context.Background(), "id", 5)
		require.ErrorContains(t, err, "out of range")
	})

	t.Run("invalid_column_path", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		_, err := pr.BloomFilterSizeWithContext(context.Background(), "nonexistent", 0)
		require.ErrorContains(t, err, "not found")
	})

	t.Run("column_missing_from_row_group", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		// A chunk without metadata is skipped, leaving the column unresolvable.
		rg := pr.Footer.RowGroups[0]
		rg.Columns[columnIndex(t, rg, "id")].MetaData = nil

		_, err := pr.BloomFilterSizeWithContext(context.Background(), "id", 0)
		require.ErrorContains(t, err, "not found")
	})

	t.Run("canceled_context", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := pr.BloomFilterSizeWithContext(ctx, "id", 0)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("clone_error", func(t *testing.T) {
		data := writeMultiRowGroup(t, 1)
		pr := openReader(t, data)

		pr.PFile = &failCloneReader{ParquetFileReader: buffer.NewBufferReaderFromBytesNoAlloc(data)}
		_, err := pr.BloomFilterSizeWithContext(context.Background(), "id", 0)
		require.ErrorContains(t, err, "clone file reader")
	})

	t.Run("reads_header_only", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		pFile := pr.PFile
		defer func() { pr.PFile = pFile }()

		var sizeBytes int64
		pr.PFile = &countingReader{ParquetFileReader: pFile, bytesRead: &sizeBytes}
		size, err := pr.BloomFilterSizeWithContext(context.Background(), "big", 0)
		require.NoError(t, err)
		require.Equal(t, int32(65536), size)

		var checkBytes int64
		pr.PFile = &countingReader{ParquetFileReader: pFile, bytesRead: &checkBytes}
		_, err = pr.BloomFilterCheckWithContext(context.Background(), "big", 0, int64(0))
		require.NoError(t, err)

		// The size lookup reads a bounded header prefetch; a membership check reads the whole bitset.
		require.Less(t, sizeBytes, int64(8192))
		require.GreaterOrEqual(t, checkBytes, int64(size))
	})

	t.Run("read_bloom_filter_error", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		// Point the filter at the file magic so the header decode fails.
		rg := pr.Footer.RowGroups[0]
		badOffset := int64(0)
		rg.Columns[columnIndex(t, rg, "id")].MetaData.BloomFilterOffset = &badOffset

		_, err := pr.BloomFilterSizeWithContext(context.Background(), "id", 0)
		require.ErrorContains(t, err, "read bloom filter")
	})
}
