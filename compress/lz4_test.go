package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestLz4CompressionLevel(t *testing.T) {
	t.Run("valid level round-trip", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, 4))
		require.NoError(t, err)

		input := []byte("test data for lz4 framed level testing, needs enough data to compress")
		compressed, err := c.Compress(input, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})
}

func TestCodec_LZ4(t *testing.T) {
	c := DefaultCompressor()
	raw := []byte{1, 2, 3}
	compressed := []byte{0x4, 0x22, 0x4d, 0x18, 0x64, 0x70, 0xb9, 0x3, 0x0, 0x0, 0x80, 0x1, 0x2, 0x3, 0x0, 0x0, 0x0, 0x0, 0xc4, 0x78, 0x9c, 0xf5}

	actual, err := c.Compress(raw, parquet.CompressionCodec_LZ4)
	require.NoError(t, err)
	require.Equal(t, compressed, actual)

	uncompressed, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = c.Uncompress([]byte{0}, parquet.CompressionCodec_LZ4)
	require.Contains(t, err.Error(), "unexpected EOF")
}
