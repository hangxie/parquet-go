package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestCodec_SNAPPY(t *testing.T) {
	c := DefaultCompressor()
	raw := []byte{1, 2, 3}
	compressed := []byte{0x3, 0x8, 0x1, 0x2, 0x3}

	actual, err := c.Compress(raw, parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.Equal(t, compressed, actual)

	uncompressed, err := c.Uncompress(compressed, parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = c.Uncompress([]byte{1}, parquet.CompressionCodec_SNAPPY)
	require.Contains(t, err.Error(), "corrupt input")
}
