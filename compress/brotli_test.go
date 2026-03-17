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

func TestBrotliCompressionLevel(t *testing.T) {
	saved := saveCompressor(parquet.CompressionCodec_BROTLI)
	resetCompressionUsed()
	defer func() {
		restoreCompressor(parquet.CompressionCodec_BROTLI, saved)
		resetCompressionUsed()
	}()

	t.Run("valid level round-trip", func(t *testing.T) {
		restoreCompressor(parquet.CompressionCodec_BROTLI, saved)
		resetCompressionUsed()
		err := SetCompressionLevel(parquet.CompressionCodec_BROTLI, 1)
		require.NoError(t, err)

		input := []byte("test data for brotli level testing, needs enough data to compress")
		compressed, err := CompressWithError(input, parquet.CompressionCodec_BROTLI)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := Uncompress(compressed, parquet.CompressionCodec_BROTLI)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})
}
