package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestGzipCompression(t *testing.T) {
	gzipCompressor := compressors[parquet.CompressionCodec_GZIP]
	input := []byte("test data")
	compressed, err := gzipCompressor.Compress(input)
	require.NoError(t, err)
	output, err := gzipCompressor.Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, input, output)
}
