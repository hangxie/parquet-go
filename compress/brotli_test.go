package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func TestCodec_BROTLI(t *testing.T) {
	raw := []byte{1, 2, 3}
	compressed := []byte{0xb, 0x1, 0x80, 0x1, 0x2, 0x3, 0x3}

	actual := compressors[parquet.CompressionCodec_BROTLI].Compress(raw)
	require.Equal(t, compressed, actual)

	uncompressed, err := compressors[parquet.CompressionCodec_BROTLI].Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = compressors[parquet.CompressionCodec_BROTLI].Uncompress([]byte{0xFF, 0xFF})
	require.Error(t, err)
}
