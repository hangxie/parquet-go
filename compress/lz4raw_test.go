package compress

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestLz4RawCompress(t *testing.T) {
	lz4RawCompressor := compressors[parquet.CompressionCodec_LZ4_RAW]
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
			output, err := lz4RawCompressor.Compress(input)
			require.NoError(t, err)
			require.Equal(t, compressed, output)
		}()
	}
	wg.Wait()

	// uncompression
	output, err := lz4RawCompressor.Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func TestLz4RawUncompressLargeData(t *testing.T) {
	lz4RawCompressor := compressors[parquet.CompressionCodec_LZ4_RAW]

	// Test with larger data that requires buffer growth
	largeInput := make([]byte, 10000)
	for i := range largeInput {
		largeInput[i] = byte(i % 256)
	}

	compressed, err := lz4RawCompressor.Compress(largeInput)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	output, err := lz4RawCompressor.Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, largeInput, output)
}

func TestLz4RawUncompressInvalidData(t *testing.T) {
	lz4RawCompressor := compressors[parquet.CompressionCodec_LZ4_RAW]

	// Test with invalid/corrupt data
	invalidData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err := lz4RawCompressor.Uncompress(invalidData)
	require.Error(t, err)
}
