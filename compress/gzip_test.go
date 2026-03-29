package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestGzipCompression(t *testing.T) {
	gzipCompressor := compressors[parquet.CompressionCodec_GZIP]
	input := []byte("test data")
	compressed := gzipCompressor.Compress(input)
	output, err := gzipCompressor.Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func TestGzipCompressionLevel(t *testing.T) {
	saved := saveCompressor(parquet.CompressionCodec_GZIP)
	resetCompressionUsed()
	defer func() {
		restoreCompressor(parquet.CompressionCodec_GZIP, saved)
		resetCompressionUsed()
	}()

	t.Run("valid level round-trip", func(t *testing.T) {
		restoreCompressor(parquet.CompressionCodec_GZIP, saved)
		resetCompressionUsed()
		err := SetCompressionLevel(parquet.CompressionCodec_GZIP, 1)
		require.NoError(t, err)

		input := []byte("test data for gzip level testing, needs enough data to compress")
		compressed, err := CompressWithError(input, parquet.CompressionCodec_GZIP)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := Uncompress(compressed, parquet.CompressionCodec_GZIP)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})

	t.Run("invalid level returns error", func(t *testing.T) {
		restoreCompressor(parquet.CompressionCodec_GZIP, saved)
		resetCompressionUsed()
		err := SetCompressionLevel(parquet.CompressionCodec_GZIP, 100)
		require.Error(t, err)
	})
}
