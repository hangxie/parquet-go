package compress

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func TestCompress(t *testing.T) {
	testCases := []struct {
		name         string
		codec        parquet.CompressionCodec
		rawData      []byte
		expectedData []byte
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
			expectedData: nil, // nil input may return nil for uncompressed codec
		},
		{
			name:         "snappy-compression",
			codec:        parquet.CompressionCodec_SNAPPY,
			rawData:      []byte{1, 2, 3, 4, 5},
			expectedData: nil, // Will be verified by round-trip test
		},
		{
			name:         "unsupported-codec",
			codec:        parquet.CompressionCodec(-1),
			rawData:      []byte{1, 2, 3, 4, 5},
			expectedData: nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actualCompressedData := Compress(testCase.rawData, testCase.codec)

			if testCase.codec == parquet.CompressionCodec_UNCOMPRESSED {
				if testCase.name == "uncompressed-nil" {
					// For nil input, test round-trip decompression
					decompressed, err := Uncompress(actualCompressedData, testCase.codec)
					require.NoError(t, err)
					require.Equal(t, 0, len(decompressed))
				} else {
					require.Equal(t, testCase.expectedData, actualCompressedData)
				}
			} else if testCase.codec == parquet.CompressionCodec(-1) {
				require.Nil(t, actualCompressedData)
			} else {
				// For real compression codecs, test round-trip
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

	compressed := Compress(largeData, parquet.CompressionCodec_SNAPPY)
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

	// Test Compress with unsupported codec returns nil
	result := Compress([]byte{1, 2, 3}, parquet.CompressionCodec(999))
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
	// Save original setting and restore after test
	originalMaxSize := GetMaxDecompressedSize()
	defer SetMaxDecompressedSize(originalMaxSize)

	// Create test data that will exceed a small limit
	testData := make([]byte, 1000)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Compress with snappy
	compressed := Compress(testData, parquet.CompressionCodec_SNAPPY)
	require.NotNil(t, compressed)

	// Set a very small limit
	SetMaxDecompressedSize(100)

	// Decompression should fail due to size limit
	_, err := Uncompress(compressed, parquet.CompressionCodec_SNAPPY)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")

	// Reset to a larger limit
	SetMaxDecompressedSize(10000)

	// Decompression should succeed now
	decompressed, err := Uncompress(compressed, parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)
}

func TestDecompressionRatioLimit(t *testing.T) {
	// Save original setting and restore after test
	originalMaxSize := GetMaxDecompressedSize()
	defer SetMaxDecompressedSize(originalMaxSize)

	// Disable size limit to test ratio limit
	SetMaxDecompressedSize(0)

	// Create highly compressible data (all zeros)
	// This creates a high compression ratio
	testData := make([]byte, 100000) // 100KB of zeros

	compressed := Compress(testData, parquet.CompressionCodec_SNAPPY)
	require.NotNil(t, compressed)

	// The ratio check in Uncompress should catch this if ratio > 1000:1
	// For realistic data, this should still pass
	decompressed, err := Uncompress(compressed, parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)
}

func TestUncompressWithExpectedSize(t *testing.T) {
	// Save original setting and restore after test
	originalMaxSize := GetMaxDecompressedSize()
	defer SetMaxDecompressedSize(originalMaxSize)
	SetMaxDecompressedSize(DefaultMaxDecompressedSize)

	testData := []byte("Hello, World! This is test data for compression.")
	compressed := Compress(testData, parquet.CompressionCodec_SNAPPY)
	require.NotNil(t, compressed)

	// Test with correct expected size
	decompressed, err := UncompressWithExpectedSize(compressed, parquet.CompressionCodec_SNAPPY, int64(len(testData)))
	require.NoError(t, err)
	require.Equal(t, testData, decompressed)

	// Test with wrong expected size
	_, err = UncompressWithExpectedSize(compressed, parquet.CompressionCodec_SNAPPY, int64(len(testData)+10))
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match expected size")

	// Test with expected size exceeding max
	SetMaxDecompressedSize(10)
	_, err = UncompressWithExpectedSize(compressed, parquet.CompressionCodec_SNAPPY, int64(len(testData)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum")

	// Test with unsupported codec
	SetMaxDecompressedSize(DefaultMaxDecompressedSize)
	_, err = UncompressWithExpectedSize(compressed, parquet.CompressionCodec(-1), int64(len(testData)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported compress method")

	// Test with expected ratio exceeding maximum
	SetMaxDecompressedSize(DefaultMaxDecompressedSize)
	smallData := []byte{1}
	_, err = UncompressWithExpectedSize(smallData, parquet.CompressionCodec_SNAPPY, int64(len(smallData))*MaxDecompressionRatio+1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum allowed ratio")
}

func TestSetGetMaxDecompressedSize(t *testing.T) {
	// Save original setting and restore after test
	originalMaxSize := GetMaxDecompressedSize()
	defer SetMaxDecompressedSize(originalMaxSize)

	// Test setting and getting
	SetMaxDecompressedSize(12345678)
	require.Equal(t, int64(12345678), GetMaxDecompressedSize())

	SetMaxDecompressedSize(0)
	require.Equal(t, int64(0), GetMaxDecompressedSize())

	SetMaxDecompressedSize(DefaultMaxDecompressedSize)
	require.Equal(t, int64(DefaultMaxDecompressedSize), GetMaxDecompressedSize())
}

func TestLimitedReadAll(t *testing.T) {
	t.Run("within limit", func(t *testing.T) {
		data := []byte("hello world")
		reader := bytes.NewReader(data)
		result, err := LimitedReadAll(reader, 100)
		require.NoError(t, err)
		require.Equal(t, data, result)
	})

	t.Run("exceeds limit", func(t *testing.T) {
		data := []byte("hello world")
		reader := bytes.NewReader(data)
		_, err := LimitedReadAll(reader, 5)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds maximum size")
	})

	t.Run("zero limit means unlimited", func(t *testing.T) {
		data := []byte("hello world")
		reader := bytes.NewReader(data)
		result, err := LimitedReadAll(reader, 0)
		require.NoError(t, err)
		require.Equal(t, data, result)
	})
}
