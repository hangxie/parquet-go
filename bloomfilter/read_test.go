package bloomfilter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

// serializeBloomFilter creates the on-disk bytes for a bloom filter (header + bitset).
func serializeBloomFilter(f *Filter) ([]byte, error) {
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	headerBuf, err := ts.Write(context.TODO(), f.Header())
	if err != nil {
		return nil, err
	}
	return append(headerBuf, f.Bitset()...), nil
}

func TestReadBloomFilter(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		original := New(1024)
		original.Insert(42)
		original.Insert(99)

		data, err := serializeBloomFilter(original)
		require.NoError(t, err)

		r := bytes.NewReader(data)
		restored, err := ReadBloomFilter(r, 0)
		require.NoError(t, err)
		require.True(t, restored.Check(42))
		require.True(t, restored.Check(99))
		require.Equal(t, original.NumBytes(), restored.NumBytes())
	})

	t.Run("non-zero-offset", func(t *testing.T) {
		original := New(64)
		original.Insert(7)

		data, err := serializeBloomFilter(original)
		require.NoError(t, err)

		// Pad with garbage before the bloom filter data
		padding := make([]byte, 100)
		for i := range padding {
			padding[i] = 0xFF
		}
		full := append(padding, data...)

		r := bytes.NewReader(full)
		restored, err := ReadBloomFilter(r, 100)
		require.NoError(t, err)
		require.True(t, restored.Check(7))
	})

	t.Run("seek-error", func(t *testing.T) {
		r := bytes.NewReader([]byte{})
		// Offset beyond the reader's range; Seek itself doesn't error on bytes.Reader,
		// but reading will fail. Use a custom ReadSeeker that fails on Seek.
		_, err := ReadBloomFilter(&failSeeker{}, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "seek to bloom filter offset")
		_ = r
	})

	t.Run("invalid-header", func(t *testing.T) {
		// Garbage data that cannot be parsed as a Thrift-encoded BloomFilterHeader
		data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		r := bytes.NewReader(data)
		_, err := ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read bloom filter header")
	})

	t.Run("zero-num-bytes", func(t *testing.T) {
		// Serialize a header with NumBytes = 0
		header := &parquet.BloomFilterHeader{
			NumBytes: 0,
			Algorithm: &parquet.BloomFilterAlgorithm{
				BLOCK: parquet.NewSplitBlockAlgorithm(),
			},
			Hash: &parquet.BloomFilterHash{
				XXHASH: parquet.NewXxHash(),
			},
			Compression: &parquet.BloomFilterCompression{
				UNCOMPRESSED: parquet.NewUncompressed(),
			},
		}
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
		data, err := ts.Write(context.TODO(), header)
		require.NoError(t, err)

		r := bytes.NewReader(data)
		_, err = ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid bloom filter header: numBytes=0")
	})

	t.Run("negative-num-bytes", func(t *testing.T) {
		header := &parquet.BloomFilterHeader{
			NumBytes: -1,
			Algorithm: &parquet.BloomFilterAlgorithm{
				BLOCK: parquet.NewSplitBlockAlgorithm(),
			},
			Hash: &parquet.BloomFilterHash{
				XXHASH: parquet.NewXxHash(),
			},
			Compression: &parquet.BloomFilterCompression{
				UNCOMPRESSED: parquet.NewUncompressed(),
			},
		}
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
		data, err := ts.Write(context.TODO(), header)
		require.NoError(t, err)

		r := bytes.NewReader(data)
		_, err = ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid bloom filter header: numBytes=-1")
	})

	t.Run("truncated-bitset", func(t *testing.T) {
		// Create a valid header claiming 1024 bytes but provide only a few bitset bytes
		original := New(1024)
		data, err := serializeBloomFilter(original)
		require.NoError(t, err)

		// Truncate the data to include header + only 10 bytes of bitset
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
		headerBuf, err := ts.Write(context.TODO(), original.Header())
		require.NoError(t, err)
		truncated := data[:len(headerBuf)+10]

		r := bytes.NewReader(truncated)
		_, err = ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read bloom filter bitset")
	})

	t.Run("seek-to-bitset-error", func(t *testing.T) {
		// Use a reader that allows the initial seek and header read but fails on the second seek
		original := New(32)
		data, err := serializeBloomFilter(original)
		require.NoError(t, err)

		r := &failSecondSeek{Reader: bytes.NewReader(data)}
		_, err = ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "seek to bloom filter bitset")
	})
}

// TestReadBloomFilterInterop tests that our ReadBloomFilter and HashValue functions
// are compatible with bloom filters generated by parquet-mr (Java reference implementation).
// The bloom_filter.xxhash.bin file from apache/parquet-testing was generated by inserting
// the strings "hello", "parquet", "bloom", "filter".
func TestReadBloomFilterInterop(t *testing.T) {
	bloomBinURL := "https://github.com/apache/parquet-testing/raw/refs/heads/master/data/bloom_filter.xxhash.bin"

	resp, err := http.Get(bloomBinURL) //nolint:gosec // test URL is a constant
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	r := bytes.NewReader(data)
	filter, err := ReadBloomFilter(r, 0)
	require.NoError(t, err)
	require.Greater(t, filter.NumBytes(), int32(0))

	// Verify our hash function produces hashes compatible with parquet-mr.
	// These four strings were inserted into the bloom filter by parquet-mr.
	for _, val := range []string{"hello", "parquet", "bloom", "filter"} {
		hash, err := HashValue(val, parquet.Type_BYTE_ARRAY)
		require.NoError(t, err)
		require.True(t, filter.Check(hash))
	}
}

// failSeeker is a ReadSeeker that always fails on Seek.
type failSeeker struct{}

func (f *failSeeker) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (f *failSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, io.ErrClosedPipe
}

// failSecondSeek allows the first Seek but fails on the second.
type failSecondSeek struct {
	*bytes.Reader
	seekCount int
}

func (f *failSecondSeek) Seek(offset int64, whence int) (int64, error) {
	f.seekCount++
	if f.seekCount >= 2 {
		return 0, io.ErrClosedPipe
	}
	return f.Reader.Seek(offset, whence)
}
