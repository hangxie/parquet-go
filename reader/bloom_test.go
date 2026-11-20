package reader

import (
	"bytes"
	"context"
	"fmt"
	"strings"
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
func TestBloomFilterCheckByteSlice(t *testing.T) {
	type BloomRecord struct {
		Bin   []byte `parquet:"name=bin, type=BYTE_ARRAY, bloomfilter=true"`
		Fixed []byte `parquet:"name=fixed, type=FIXED_LEN_BYTE_ARRAY, length=12, bloomfilter=true"`
		Ts    []byte `parquet:"name=ts, type=INT96, bloomfilter=true"`
	}
	value := func(i int) []byte { return fmt.Appendf(nil, "%012d", i) }

	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(context.Background(), &buf, new(BloomRecord), writer.WithNP(1))
	require.NoError(t, err)
	for i := range 100 {
		require.NoError(t, pw.WriteWithContext(context.Background(), BloomRecord{value(i), value(i), value(i)}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes()), new(BloomRecord), WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStopWithContext(context.Background()) }()

	for _, column := range []string{"bin", "fixed", "ts"} {
		for _, v := range []any{value(50), string(value(50))} {
			found, err := pr.BloomFilterCheckWithContext(context.Background(), column, 0, v)
			require.NoError(t, err, column)
			require.True(t, found, "%s %T", column, v)
		}
	}
	// A BYTE_ARRAY has no fixed width, so a value of any length is a valid probe.
	for _, v := range []any{value(50)[:11], string(value(50)) + "x"} {
		found, err := pr.BloomFilterCheckWithContext(context.Background(), "bin", 0, v)
		require.NoError(t, err)
		require.False(t, found, "%q", v)
	}
}

func TestBloomFilterCheckRejectsWrongWidth(t *testing.T) {
	type record struct {
		Fixed       []byte `parquet:"name=fixed, type=FIXED_LEN_BYTE_ARRAY, length=12, bloomfilter=true"`
		Ts          []byte `parquet:"name=ts, type=INT96, bloomfilter=true"`
		UUID        string `parquet:"name=uuid, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID, bloomfilter=true"`
		FixedNoBF   []byte `parquet:"name=fixed_no_bf, type=FIXED_LEN_BYTE_ARRAY, length=12"`
		TsNoBF      []byte `parquet:"name=ts_no_bf, type=INT96"`
		IntervalNoB string `parquet:"name=interval_no_bf, type=FIXED_LEN_BYTE_ARRAY, length=12, convertedtype=INTERVAL"`
	}
	v := []byte("abcdefghijkl")
	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(context.Background(), &buf, new(record), writer.WithNP(1))
	require.NoError(t, err)
	require.NoError(t, pw.WriteWithContext(context.Background(), record{v, v, "0123456789abcdef", v, v, string(v)}))
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes()), new(record), WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStopWithContext(context.Background()) }()

	for column, width := range map[string]int{"fixed": 12, "ts": 12, "uuid": 16, "fixed_no_bf": 12, "ts_no_bf": 12, "interval_no_bf": 12} {
		// A column without a filter answers true for any value of the right width; one of the
		// wrong width cannot be in the column whether or not a filter is there to ask.
		found, err := pr.BloomFilterCheckWithContext(context.Background(), column, 0, bytes.Repeat([]byte{'a'}, width))
		require.NoError(t, err, column)
		if strings.HasSuffix(column, "no_bf") {
			require.True(t, found, column)
		}
		for _, bad := range []any{bytes.Repeat([]byte{'a'}, width-1), strings.Repeat("a", width+1)} {
			_, err := pr.BloomFilterCheckWithContext(context.Background(), column, 0, bad)
			require.ErrorContains(t, err, "does not match column length", column)
		}
	}

	// A value that is not bytes at all has no width to check and fails in the hash instead.
	_, err = pr.BloomFilterCheckWithContext(context.Background(), "ts", 0, int64(1))
	require.ErrorContains(t, err, "is not a string or []byte")
}

func TestBloomFilterCheckUsesFileWidth(t *testing.T) {
	type stored struct {
		Code  string `parquet:"name=code, type=FIXED_LEN_BYTE_ARRAY, length=4, bloomfilter=true"`
		Label string `parquet:"name=label, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	type declared struct {
		Code  string `parquet:"name=code, type=FIXED_LEN_BYTE_ARRAY, length=8"`
		Label string `parquet:"name=label, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(context.Background(), &buf, new(stored), writer.WithNP(1))
	require.NoError(t, err)
	require.NoError(t, pw.WriteWithContext(context.Background(), stored{"abcd", "x"}))
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes()), new(declared), WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStopWithContext(context.Background()) }()

	// the filter was built from the file's 4-byte values, whatever width the destination declares
	found, err := pr.BloomFilterCheckWithContext(context.Background(), "code", 0, "abcd")
	require.NoError(t, err)
	require.True(t, found)
	_, err = pr.BloomFilterCheckWithContext(context.Background(), "code", 0, "abcdefgh")
	require.ErrorContains(t, err, "value of length 8 does not match column length 4")
}

func TestFileLeaf(t *testing.T) {
	footer := &parquet.FileMetaData{Schema: []*parquet.SchemaElement{
		{Name: "root", NumChildren: common.ToPtr(int32(2))},
		{Name: "a", TypeLength: common.ToPtr(int32(4))},
		{Name: "g", NumChildren: common.ToPtr(int32(1))},
		{Name: "b", TypeLength: common.ToPtr(int32(8))},
	}}
	require.Equal(t, "a", fileLeaf(footer, 0).GetName())
	require.Equal(t, "b", fileLeaf(footer, 1).GetName())
	require.Nil(t, fileLeaf(footer, 2))

	// an ordinal past what 16 bits hold still names its own leaf rather than wrapping to another
	wide := &parquet.FileMetaData{Schema: []*parquet.SchemaElement{{Name: "root"}}}
	for i := range 1<<16 + 1 {
		wide.Schema = append(wide.Schema, &parquet.SchemaElement{Name: fmt.Sprint(i)})
	}
	require.Equal(t, "65536", fileLeaf(wide, 1<<16).GetName())
}

func TestCheckFixedWidthInconsistentFooter(t *testing.T) {
	fixed := parquet.Type_FIXED_LEN_BYTE_ARRAY
	int32Type := parquet.Type_INT32
	pr := &ParquetReader{Footer: &parquet.FileMetaData{Schema: []*parquet.SchemaElement{
		{Name: "root", NumChildren: common.ToPtr(int32(2))},
		{Name: "code", Type: &fixed, TypeLength: common.ToPtr(int32(4))},
		{Name: "num", Type: &int32Type},
	}}}

	require.NoError(t, pr.checkFixedWidth("code", 0, fixed, "abcd"))
	require.ErrorContains(t, pr.checkFixedWidth("code", 0, fixed, "abc"), "does not match column length 4")
	// a row group naming a chunk the footer has no matching leaf for is malformed, not a width to skip
	for _, ordinal := range []int{1, 2} {
		err := pr.checkFixedWidth("other", ordinal, fixed, "abcd")
		require.ErrorContains(t, err, "column [other]: chunk "+fmt.Sprint(ordinal)+" has no FIXED_LEN_BYTE_ARRAY leaf in the file schema")
	}
}

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
				size, err := pr.BloomFilterSize(ctx, column, rgIndex)
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

		size, err := pr.BloomFilterSize(ctx, "id", 0)
		require.NoError(t, err)
		require.Equal(t, int32(bloomfilter.DefaultNumBytes), size)

		size, err = pr.BloomFilterSize(ctx, "id", 1)
		require.NoError(t, err)
		require.Equal(t, int32(65536), size)
	})

	t.Run("filter_only_outside_row_group_zero", func(t *testing.T) {
		ctx := context.Background()
		pr := openReader(t, writeMultiRowGroup(t, 2))

		// Simulate a writer that filtered "id" in the second row group only.
		rg0 := pr.Footer.RowGroups[0]
		rg0.Columns[columnIndex(t, rg0, "id")].MetaData.BloomFilterOffset = nil

		size, err := pr.BloomFilterSize(ctx, "id", 0)
		require.NoError(t, err)
		require.Zero(t, size)

		size, err = pr.BloomFilterSize(ctx, "id", 1)
		require.NoError(t, err)
		require.Equal(t, int32(bloomfilter.DefaultNumBytes), size)
	})

	t.Run("column_without_bloom_filter", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		size, err := pr.BloomFilterSize(context.Background(), "age", 0)
		require.NoError(t, err)
		require.Zero(t, size)
	})

	t.Run("invalid_row_group_index", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		_, err := pr.BloomFilterSize(context.Background(), "id", -1)
		require.ErrorContains(t, err, "out of range")

		_, err = pr.BloomFilterSize(context.Background(), "id", 5)
		require.ErrorContains(t, err, "out of range")
	})

	t.Run("invalid_column_path", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		_, err := pr.BloomFilterSize(context.Background(), "nonexistent", 0)
		require.ErrorContains(t, err, "not found")
	})

	t.Run("column_missing_from_row_group", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		// A chunk without metadata is skipped, leaving the column unresolvable.
		rg := pr.Footer.RowGroups[0]
		rg.Columns[columnIndex(t, rg, "id")].MetaData = nil

		_, err := pr.BloomFilterSize(context.Background(), "id", 0)
		require.ErrorContains(t, err, "not found")
	})

	t.Run("canceled_context", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := pr.BloomFilterSize(ctx, "id", 0)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("clone_error", func(t *testing.T) {
		data := writeMultiRowGroup(t, 1)
		pr := openReader(t, data)

		pr.PFile = &failCloneReader{ParquetFileReader: buffer.NewBufferReaderFromBytesNoAlloc(data)}
		_, err := pr.BloomFilterSize(context.Background(), "id", 0)
		require.ErrorContains(t, err, "clone file reader")
	})

	t.Run("reads_header_only", func(t *testing.T) {
		pr := openReader(t, writeMultiRowGroup(t, 1))

		pFile := pr.PFile
		defer func() { pr.PFile = pFile }()

		var sizeBytes int64
		pr.PFile = &countingReader{ParquetFileReader: pFile, bytesRead: &sizeBytes}
		size, err := pr.BloomFilterSize(context.Background(), "big", 0)
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

		_, err := pr.BloomFilterSize(context.Background(), "id", 0)
		require.ErrorContains(t, err, "read bloom filter")
	})
}
