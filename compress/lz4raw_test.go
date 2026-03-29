package compress

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestLz4RawCompress(t *testing.T) {
	c := DefaultCompressor()
	input := []byte("Peter Parker")
	compressed := []byte{
		0xc0, 0x50, 0x65, 0x74, 0x65, 0x72, 0x20, 0x50, 0x61, 0x72, 0x6b, 0x65, 0x72,
	}

	// compression
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			output, err := c.Compress(input, parquet.CompressionCodec_LZ4_RAW)
			require.NoError(t, err)
			require.Equal(t, compressed, output)
		}()
	}
	wg.Wait()

	// uncompression
	output, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4_RAW)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func TestLz4RawUncompressLargeData(t *testing.T) {
	c := DefaultCompressor()

	// Test with larger data that requires buffer growth
	largeInput := make([]byte, 10000)
	for i := range largeInput {
		largeInput[i] = byte(i % 256)
	}

	compressed, err := c.Compress(largeInput, parquet.CompressionCodec_LZ4_RAW)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	output, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4_RAW)
	require.NoError(t, err)
	require.Equal(t, largeInput, output)
}

func TestLz4RawUncompressSizeLimit(t *testing.T) {
	c := DefaultCompressor()

	// Create compressible data
	input := make([]byte, 5000)
	for i := range input {
		input[i] = byte(i % 10) // Repeating pattern
	}

	compressed, err := c.Compress(input, parquet.CompressionCodec_LZ4_RAW)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	// Set a small limit that will be exceeded
	smallLimit, err := NewCompressor(WithMaxDecompressedSize(100))
	require.NoError(t, err)

	_, err = smallLimit.Uncompress(compressed, parquet.CompressionCodec_LZ4_RAW)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDecompressedSizeExceeded)
}

func TestLz4RawCompressionLevel(t *testing.T) {
	t.Run("valid level round-trip", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4_RAW, 4))
		require.NoError(t, err)

		input := []byte("test data for lz4 raw level testing, needs enough data to compress")
		compressed, err := c.Compress(input, parquet.CompressionCodec_LZ4_RAW)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4_RAW)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})

	t.Run("concurrent compression with custom level", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4_RAW, 4))
		require.NoError(t, err)

		input := []byte("Peter Parker")
		var wg sync.WaitGroup
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				compressed, compErr := c.Compress(input, parquet.CompressionCodec_LZ4_RAW)
				require.NoError(t, compErr)
				require.NotNil(t, compressed)
			}()
		}
		wg.Wait()
	})
}

func TestLz4RawUncompressInvalidData(t *testing.T) {
	c := DefaultCompressor()

	// Test with invalid/corrupt data
	invalidData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err := c.Uncompress(invalidData, parquet.CompressionCodec_LZ4_RAW)
	require.Error(t, err)
}
