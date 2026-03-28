package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestCodec_UNCOMPRESSED(t *testing.T) {
	raw := []byte{1, 2, 3}
	compressed, err := compressors[parquet.CompressionCodec_UNCOMPRESSED].Compress(raw)
	require.NoError(t, err)
	require.Equal(t, raw, compressed)

	uncompressed, err := compressors[parquet.CompressionCodec_UNCOMPRESSED].Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)
}
