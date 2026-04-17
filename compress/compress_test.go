package compress

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestCompress(t *testing.T) {
	testCases := []struct {
		name         string
		codec        parquet.CompressionCodec
		rawData      []byte
		expectedData []byte
		errMsg       string
	}{
		{
			name:         "uncompressed-data",
			codec:        parquet.CompressionCodec_UNCOMPRESSED,
			rawData:      []byte{1, 2, 3, 4, 5},
			expectedData: []byte{1, 2, 3, 4, 5},
		},
		{
			name:         "uncompressed-empty",
			codec:        parquet.CompressionCodec_UNCOMPRESSED,
			rawData:      []byte{},
			expectedData: []byte{},
		},
		{
			name:         "uncompressed-nil",
			codec:        parquet.CompressionCodec_UNCOMPRESSED,
			rawData:      nil,
			expectedData: nil,
		},
		{
			name:         "snappy-compression",
			codec:        parquet.CompressionCodec_SNAPPY,
			rawData:      []byte{1, 2, 3, 4, 5},
			expectedData: nil, // Will be verified by round-trip test
		},
		{
			name:    "unsupported-codec",
			codec:   parquet.CompressionCodec(-1),
			rawData: []byte{1, 2, 3, 4, 5},
			errMsg:  "unsupported compress method",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actualCompressedData, err := CompressWithError(testCase.rawData, testCase.codec)

			if testCase.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), testCase.errMsg)
				require.Nil(t, actualCompressedData)
			} else if testCase.codec == parquet.CompressionCodec_UNCOMPRESSED {
				require.NoError(t, err)
				if testCase.name == "uncompressed-nil" {
					decompressed, err := Uncompress(actualCompressedData, testCase.codec)
					require.NoError(t, err)
					require.Equal(t, 0, len(decompressed))
				} else {
					require.Equal(t, testCase.expectedData, actualCompressedData)
				}
			} else {
				require.NoError(t, err)
				if actualCompressedData != nil {
					decompressed, err := Uncompress(actualCompressedData, testCase.codec)
					require.NoError(t, err)
					require.Equal(t, testCase.rawData, decompressed)
				}
			}
		})
	}
}

func TestCompressLargeData(t *testing.T) {
	// Test with larger data that should benefit from compression
	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = byte(i % 10) // Repeating pattern for better compression
	}

	compressed, err := CompressWithError(largeData, parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.NotNil(t, compressed)
	require.Less(t, len(compressed), len(largeData))

	decompressed, err := Uncompress(compressed, parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.Equal(t, largeData, decompressed)
}

func TestErrorHandling(t *testing.T) {
	// Test Uncompress with unsupported codec
	_, err := Uncompress([]byte{1, 2, 3}, parquet.CompressionCodec(999))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported compress method")

	// Test CompressWithError with unsupported codec returns error
	result, err := CompressWithError([]byte{1, 2, 3}, parquet.CompressionCodec(999))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported compress method")
	require.Nil(t, result)
}

func TestCompressWithError(t *testing.T) {
	testData := []byte{1, 2, 3, 4, 5}

	t.Run("supported codec", func(t *testing.T) {
		compressed, err := CompressWithError(testData, parquet.CompressionCodec_SNAPPY)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		// Verify round-trip
		decompressed, err := Uncompress(compressed, parquet.CompressionCodec_SNAPPY)
		require.NoError(t, err)
		require.Equal(t, testData, decompressed)
	})

	t.Run("uncompressed codec", func(t *testing.T) {
		compressed, err := CompressWithError(testData, parquet.CompressionCodec_UNCOMPRESSED)
		require.NoError(t, err)
		require.Equal(t, testData, compressed)
	})

	t.Run("unsupported codec returns error", func(t *testing.T) {
		result, err := CompressWithError(testData, parquet.CompressionCodec(999))
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported compress method")
		require.Nil(t, result)
	})

	t.Run("negative codec value", func(t *testing.T) {
		result, err := CompressWithError(testData, parquet.CompressionCodec(-1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported compress method")
		require.Nil(t, result)
	})
}

func TestUncompress(t *testing.T) {
	testCases := []struct {
		name             string
		codec            parquet.CompressionCodec
		compressedData   []byte
		expectedData     []byte
		expectedErrorMsg string
	}{
		{
			name:           "successful-snappy-decompression",
			codec:          parquet.CompressionCodec_SNAPPY,
			compressedData: []byte{3, 8, 1, 2, 3},
			expectedData:   []byte{1, 2, 3},
		},
		{
			name:             "corrupt-snappy-input",
			codec:            parquet.CompressionCodec_SNAPPY,
			compressedData:   []byte{1, 2, 3},
			expectedData:     nil,
			expectedErrorMsg: "corrupt input",
		},
		{
			name:             "unsupported-compression-codec",
			codec:            parquet.CompressionCodec(-1),
			compressedData:   []byte{1, 2, 3},
			expectedData:     nil,
			expectedErrorMsg: "unsupported compress method",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actualData, err := Uncompress(testCase.compressedData, testCase.codec)

			if testCase.expectedErrorMsg == "" {
				// Expecting success
				require.NoError(t, err)
				require.Equal(t, testCase.expectedData, actualData)
			} else {
				// Expecting error
				require.Error(t, err)
				require.Contains(t, err.Error(), testCase.expectedErrorMsg)
				require.Equal(t, testCase.expectedData, actualData)
			}
		})
	}
}

func TestDecompressionSizeLimit(t *testing.T) {
	// Create test data that will exceed a small limit
	testData := make([]byte, 1000)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Compress with snappy using default compressor
	compressed, err := CompressWithError(testData, parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	// Create compressor with small limit — decompression should fail
	smallLimit, err := NewCompressor(WithMaxDecompressedSize(100))
	require.NoError(t, err)

	_, err = smallLimit.Uncompress(compressed, parquet.CompressionCodec_SNAPPY)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDecompressedSizeExceeded)

	// Create compressor with larger limit — decompression should succeed
	largeLimit, err := NewCompressor(WithMaxDecompressedSize(10000))
	require.NoError(t, err)

	decompressed, err := largeLimit.Uncompress(compressed, parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)
}

func TestUncompressWithExpectedSize(t *testing.T) {
	testData := []byte("Hello, World! This is test data for compression.")
	compressed, err := CompressWithError(testData, parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	c := DefaultCompressor()

	// Test with correct expected size
	decompressed, err := c.UncompressWithExpectedSize(compressed, parquet.CompressionCodec_SNAPPY, int64(len(testData)))
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)

	// Test with wrong expected size
	_, err = c.UncompressWithExpectedSize(compressed, parquet.CompressionCodec_SNAPPY, int64(len(testData)+10))
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match expected size")

	// Test with expected size exceeding max
	smallLimit, err := NewCompressor(WithMaxDecompressedSize(10))
	require.NoError(t, err)
	_, err = smallLimit.UncompressWithExpectedSize(compressed, parquet.CompressionCodec_SNAPPY, int64(len(testData)))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDecompressedSizeExceeded)

	// Test with unsupported codec
	_, err = c.UncompressWithExpectedSize(compressed, parquet.CompressionCodec(-1), int64(len(testData)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported compress method")
}

func TestNewCompressor(t *testing.T) {
	t.Run("default settings", func(t *testing.T) {
		c, err := NewCompressor()
		require.NoError(t, err)
		require.Equal(t, int64(DefaultMaxDecompressedSize), c.MaxDecompressedSize())
	})

	t.Run("custom max size", func(t *testing.T) {
		c, err := NewCompressor(WithMaxDecompressedSize(1024))
		require.NoError(t, err)
		require.Equal(t, int64(1024), c.MaxDecompressedSize())
	})

	t.Run("zero max size disables limit", func(t *testing.T) {
		c, err := NewCompressor(WithMaxDecompressedSize(0))
		require.NoError(t, err)
		require.Equal(t, int64(0), c.MaxDecompressedSize())

		// Should decompress without limit
		testData := make([]byte, 1000)
		compressed, err := c.Compress(testData, parquet.CompressionCodec_SNAPPY)
		require.NoError(t, err)
		decompressed, err := c.Uncompress(compressed, parquet.CompressionCodec_SNAPPY)
		require.NoError(t, err)
		require.Equal(t, testData, decompressed)
	})

	t.Run("unsupported codec level", func(t *testing.T) {
		_, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_UNCOMPRESSED, 5))
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not support compression levels")
	})

	t.Run("codec without factory", func(t *testing.T) {
		_, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_SNAPPY, 5))
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not support compression levels")
	})
}

func TestCompressorCompressionLevelRoundTrip(t *testing.T) {
	testCases := []struct {
		name  string
		codec parquet.CompressionCodec
		level int
	}{
		{"gzip-level-1", parquet.CompressionCodec_GZIP, 1},
		{"gzip-level-9", parquet.CompressionCodec_GZIP, 9},
		{"zstd-level-1", parquet.CompressionCodec_ZSTD, 1},
		{"zstd-level-22", parquet.CompressionCodec_ZSTD, 22},
		{"brotli-level-0", parquet.CompressionCodec_BROTLI, 0},
		{"brotli-level-11", parquet.CompressionCodec_BROTLI, 11},
		{"lz4raw-level-1", parquet.CompressionCodec_LZ4_RAW, 1},
		{"lz4raw-level-9", parquet.CompressionCodec_LZ4_RAW, 9},
		{"lz4-level-1", parquet.CompressionCodec_LZ4, 1},
	}

	input := make([]byte, 1000)
	for i := range input {
		input[i] = byte(i % 10)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewCompressor(WithCompressionLevel(tc.codec, tc.level))
			require.NoError(t, err)

			compressed, err := c.Compress(input, tc.codec)
			require.NoError(t, err)
			require.NotNil(t, compressed)

			decompressed, err := c.Uncompress(compressed, tc.codec)
			require.NoError(t, err)
			require.Equal(t, input, decompressed)
		})
	}
}

func TestDefaultCompressor(t *testing.T) {
	c := DefaultCompressor()
	require.NotNil(t, c)
	require.Equal(t, int64(DefaultMaxDecompressedSize), c.MaxDecompressedSize())

	// Should be the same instance on repeated calls
	c2 := DefaultCompressor()
	require.Same(t, c, c2)
}

func TestErrDecompressedSizeExceeded(t *testing.T) {
	require.True(t, errors.Is(ErrDecompressedSizeExceeded, ErrDecompressedSizeExceeded))
}

func Test_limitedReadAll(t *testing.T) {
	t.Run("within limit", func(t *testing.T) {
		data := []byte("hello world")
		reader := bytes.NewReader(data)
		result, err := limitedReadAll(reader, 100)
		require.NoError(t, err)
		require.Equal(t, data, result)
	})

	t.Run("exceeds limit", func(t *testing.T) {
		data := []byte("hello world")
		reader := bytes.NewReader(data)
		_, err := limitedReadAll(reader, 5)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDecompressedSizeExceeded)
	})

	t.Run("zero limit means unlimited", func(t *testing.T) {
		data := []byte("hello world")
		reader := bytes.NewReader(data)
		result, err := limitedReadAll(reader, 0)
		require.NoError(t, err)
		require.Equal(t, data, result)
	})
}
