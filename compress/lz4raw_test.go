package compress

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
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
			output := lz4RawCompressor.Compress(input)
			require.Equal(t, compressed, output)
		}()
	}
	wg.Wait()

	// uncompression
	output, err := lz4RawCompressor.Uncompress(compressed)
	require.NoError(t, err)
	require.Equal(t, input, output)
}
