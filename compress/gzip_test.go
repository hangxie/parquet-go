package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestGzipCompression(t *testing.T) {
	c := DefaultCompressor()
	input := []byte("test data")
	compressed, err := c.Compress(input, parquet.CompressionCodec_GZIP)
	require.NoError(t, err)
	output, err := c.Uncompress(compressed, parquet.CompressionCodec_GZIP)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func TestGzipCompressWithLevelError(t *testing.T) {
	// Call gzipCompressWithLevel directly with an invalid level to exercise
	// the error-return path that is unreachable through the public API.
	compress := gzipCompressWithLevel(100)
	_, err := compress([]byte("test data"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "gzip compress")
}

func TestGzipCompressionLevel(t *testing.T) {
	t.Run("valid level round-trip", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_GZIP, 1))
		require.NoError(t, err)

		input := []byte("test data for gzip level testing, needs enough data to compress")
		compressed, err := c.Compress(input, parquet.CompressionCodec_GZIP)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := c.Uncompress(compressed, parquet.CompressionCodec_GZIP)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})

	t.Run("invalid level returns error", func(t *testing.T) {
		_, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_GZIP, 100))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid compression level")
	})
}
