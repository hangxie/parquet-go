package compress

import (
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
