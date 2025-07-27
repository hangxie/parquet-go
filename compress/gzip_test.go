package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_GzipCompression(t *testing.T) {
	gzipCompressor := compressors[parquet.CompressionCodec_GZIP]
	input := []byte("test data")
	compressed := gzipCompressor.Compress(input)
	output, err := gzipCompressor.Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, input, output)
}
