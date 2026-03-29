package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestCodec_ZSTD(t *testing.T) {
	c := DefaultCompressor()
	raw := []byte{1, 2, 3}
	compressed := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x4, 0x0, 0x19, 0x0, 0x0, 0x1, 0x2, 0x3, 0xa5, 0xe5, 0x4e, 0xc}

	actual, err := c.Compress(raw, parquet.CompressionCodec_ZSTD)
	require.NoError(t, err)
	require.Equal(t, compressed, actual)

	uncompressed, err := c.Uncompress(compressed, parquet.CompressionCodec_ZSTD)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = c.Uncompress([]byte{0}, parquet.CompressionCodec_ZSTD)
	require.Contains(t, err.Error(), "unexpected EOF")
}

func TestZstdCompressionLevel(t *testing.T) {
	t.Run("valid level round-trip", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_ZSTD, 10))
		require.NoError(t, err)

		input := []byte("test data for zstd level testing, needs enough data to compress")
		compressed, err := c.Compress(input, parquet.CompressionCodec_ZSTD)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := c.Uncompress(compressed, parquet.CompressionCodec_ZSTD)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})

	t.Run("different levels produce valid output", func(t *testing.T) {
		for _, level := range []int{1, 5, 15, 22} {
			c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_ZSTD, level))
			require.NoError(t, err)

			input := []byte("test data for zstd level testing, needs enough data to compress")
			compressed, err := c.Compress(input, parquet.CompressionCodec_ZSTD)
			require.NoError(t, err)

			output, err := c.Uncompress(compressed, parquet.CompressionCodec_ZSTD)
			require.NoError(t, err)
			require.Equal(t, input, output)
		}
	})
}
