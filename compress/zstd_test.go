package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func TestCodec_ZSTD(t *testing.T) {
	raw := []byte{1, 2, 3}
	compressed := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x4, 0x0, 0x19, 0x0, 0x0, 0x1, 0x2, 0x3, 0xa5, 0xe5, 0x4e, 0xc}

	actual := compressors[parquet.CompressionCodec_ZSTD].Compress(raw)
	require.Equal(t, compressed, actual)

	uncompressed, err := compressors[parquet.CompressionCodec_ZSTD].Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = compressors[parquet.CompressionCodec_ZSTD].Uncompress([]byte{0})
	require.Contains(t, err.Error(), "unexpected EOF")
}

func TestZstdCompressionLevel(t *testing.T) {
	saved := saveCompressor(parquet.CompressionCodec_ZSTD)
	resetCompressionUsed()
	defer func() {
		restoreCompressor(parquet.CompressionCodec_ZSTD, saved)
		resetCompressionUsed()
	}()

	t.Run("valid level round-trip", func(t *testing.T) {
		restoreCompressor(parquet.CompressionCodec_ZSTD, saved)
		resetCompressionUsed()
		err := SetCompressionLevel(parquet.CompressionCodec_ZSTD, 10)
		require.NoError(t, err)

		input := []byte("test data for zstd level testing, needs enough data to compress")
		compressed, err := CompressWithError(input, parquet.CompressionCodec_ZSTD)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := Uncompress(compressed, parquet.CompressionCodec_ZSTD)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})

	t.Run("different levels produce valid output", func(t *testing.T) {
		for _, level := range []int{1, 5, 15, 22} {
			restoreCompressor(parquet.CompressionCodec_ZSTD, saved)
			resetCompressionUsed()
			err := SetCompressionLevel(parquet.CompressionCodec_ZSTD, level)
			require.NoError(t, err)

			input := []byte("test data for zstd level testing, needs enough data to compress")
			compressed, err := CompressWithError(input, parquet.CompressionCodec_ZSTD)
			require.NoError(t, err)

			output, err := Uncompress(compressed, parquet.CompressionCodec_ZSTD)
			require.NoError(t, err)
			require.Equal(t, input, output)
		}
	})
}
