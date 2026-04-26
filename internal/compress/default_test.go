package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestCodec_UNCOMPRESSED(t *testing.T) {
	c := DefaultCompressor()
	raw := []byte{1, 2, 3}
	compressed, err := c.Compress(raw, parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.Equal(t, raw, compressed)

	uncompressed, err := c.Uncompress(compressed, parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)
}
