package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestCodec_BROTLI(t *testing.T) {
	c := DefaultCompressor()
	raw := []byte{1, 2, 3}
	compressed := []byte{0xb, 0x1, 0x80, 0x1, 0x2, 0x3, 0x3}

	actual, err := c.Compress(raw, parquet.CompressionCodec_BROTLI)
	require.NoError(t, err)
	require.Equal(t, compressed, actual)

	uncompressed, err := c.Uncompress(compressed, parquet.CompressionCodec_BROTLI)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = c.Uncompress([]byte{0xFF, 0xFF}, parquet.CompressionCodec_BROTLI)
	require.Error(t, err)
	require.Contains(t, err.Error(), "brotli:")
}

func TestBrotliCompressionLevel(t *testing.T) {
	t.Run("valid level round-trip", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_BROTLI, 1))
		require.NoError(t, err)

		input := []byte("test data for brotli level testing, needs enough data to compress")
		compressed, err := c.Compress(input, parquet.CompressionCodec_BROTLI)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := c.Uncompress(compressed, parquet.CompressionCodec_BROTLI)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})
}
