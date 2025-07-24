package compress

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_Uncompress(t *testing.T) {
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
				require.NoError(t, err, "Uncompress should succeed")
				require.Equal(t, testCase.expectedData, actualData, "Decompressed data should match expected")
			} else {
				// Expecting error
				require.Error(t, err, "Uncompress should fail")
				require.Contains(t, err.Error(), testCase.expectedErrorMsg, "Error message should contain expected text")
				require.Equal(t, testCase.expectedData, actualData, "Data should match expected (likely nil)")
			}
		})
	}
}

func Test_Compress(t *testing.T) {
	testCases := []struct {
		name         string
		codec        parquet.CompressionCodec
		rawData      []byte
		expectedData []byte
	}{
		// Note: Test cases are currently empty in the original implementation
		// This structure is prepared for future test cases
	}

	if len(testCases) == 0 {
		t.Skip("No test cases defined for Compress function yet")
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actualCompressedData := Compress(testCase.rawData, testCase.codec)
			require.Equal(t, testCase.expectedData, actualCompressedData,
				"Compressed data should match expected output")
		})
	}
}
