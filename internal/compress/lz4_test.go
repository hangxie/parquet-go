package compress

import (
	"sync"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestLz4CompressionLevel(t *testing.T) {
	t.Run("valid level round-trip", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, 4))
		require.NoError(t, err)

		input := []byte("test data for lz4 framed level testing, needs enough data to compress")
		compressed, err := c.Compress(input, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})

	t.Run("invalid level returns error", func(t *testing.T) {
		_, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, 10))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid lz4 compression level")
	})

	t.Run("pool creates writers with correct level", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, 4))
		require.NoError(t, err)

		input := []byte("repeating data for pool exercise repeating data for pool exercise")

		// Compress multiple times to force the pool to create new writers,
		// exercising the sync.Pool New callback that applies the compression level.
		for range 5 {
			compressed, err := c.Compress(input, parquet.CompressionCodec_LZ4)
			require.NoError(t, err)
			require.NotNil(t, compressed)

			output, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
			require.NoError(t, err)
			require.Equal(t, input, output)
		}
	})
}

func TestLz4CompressWithLevelApplyError(t *testing.T) {
	// Call lz4CompressWithLevel directly with an invalid level to exercise
	// the error-return path that is unreachable through the public API.
	pool := sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}
	invalidCL := lz4.CompressionLevel(128) // invalid level
	compress := lz4CompressWithLevel(&pool, invalidCL)
	_, err := compress([]byte("test data"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "lz4 compress")
}

func TestCodec_LZ4(t *testing.T) {
	c := DefaultCompressor()
	raw := []byte{1, 2, 3}
	compressed := []byte{0x4, 0x22, 0x4d, 0x18, 0x64, 0x70, 0xb9, 0x3, 0x0, 0x0, 0x80, 0x1, 0x2, 0x3, 0x0, 0x0, 0x0, 0x0, 0xc4, 0x78, 0x9c, 0xf5}

	actual, err := c.Compress(raw, parquet.CompressionCodec_LZ4)
	require.NoError(t, err)
	require.Equal(t, compressed, actual)

	uncompressed, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = c.Uncompress([]byte{0}, parquet.CompressionCodec_LZ4)
	require.Contains(t, err.Error(), "unexpected EOF")
}
