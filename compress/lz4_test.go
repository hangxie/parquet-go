package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func TestLz4CompressionLevel(t *testing.T) {
	saved := saveCompressor(parquet.CompressionCodec_LZ4)
	resetCompressionUsed()
	defer func() {
		restoreCompressor(parquet.CompressionCodec_LZ4, saved)
		resetCompressionUsed()
	}()

	t.Run("valid level round-trip", func(t *testing.T) {
		restoreCompressor(parquet.CompressionCodec_LZ4, saved)
		resetCompressionUsed()
		err := SetCompressionLevel(parquet.CompressionCodec_LZ4, 4)
		require.NoError(t, err)

		input := []byte("test data for lz4 framed level testing, needs enough data to compress")
		compressed, err := CompressWithError(input, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := Uncompress(compressed, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})
}

func TestCodec_LZ4(t *testing.T) {
	raw := []byte{1, 2, 3}
	compressed := []byte{0x4, 0x22, 0x4d, 0x18, 0x64, 0x70, 0xb9, 0x3, 0x0, 0x0, 0x80, 0x1, 0x2, 0x3, 0x0, 0x0, 0x0, 0x0, 0xc4, 0x78, 0x9c, 0xf5}

	actual := compressors[parquet.CompressionCodec_LZ4].Compress(raw)
	require.Equal(t, compressed, actual)

	uncompressed, err := compressors[parquet.CompressionCodec_LZ4].Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = compressors[parquet.CompressionCodec_LZ4].Uncompress([]byte{0})
	require.Contains(t, err.Error(), "unexpected EOF")
}
