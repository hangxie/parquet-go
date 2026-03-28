package compress

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestDefaultCompressor(t *testing.T) {
	testCases := []struct {
		name  string
		codec parquet.CompressionCodec
		isNil bool
	}{
		{"uncompressed", parquet.CompressionCodec_UNCOMPRESSED, false},
		{"snappy", parquet.CompressionCodec_SNAPPY, false},
		{"gzip", parquet.CompressionCodec_GZIP, false},
		{"unknown", parquet.CompressionCodec(-1), true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := DefaultCompressor(tc.codec)
			if tc.isNil {
				require.Nil(t, c)
			} else {
				require.NotNil(t, c)
			}
		})
	}
}

func TestCompressDecompressRoundTrip(t *testing.T) {
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
			name:    "snappy-round-trip",
			codec:   parquet.CompressionCodec_SNAPPY,
			rawData: []byte{1, 2, 3, 4, 5},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := DefaultCompressor(tc.codec)
			require.NotNil(t, c)

			compressed, err := c.Compress(tc.rawData)
			require.NoError(t, err)

			if tc.expectedData != nil {
				require.Equal(t, tc.expectedData, compressed)
			}

			decompressed, err := Decompress(c, compressed, 0)
			require.NoError(t, err)
			require.Equal(t, tc.rawData, decompressed)
		})
	}
}

func TestCompressLargeData(t *testing.T) {
	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = byte(i % 10)
	}

	c := DefaultCompressor(parquet.CompressionCodec_SNAPPY)
	require.NotNil(t, c)

	compressed, err := c.Compress(largeData)
	require.NoError(t, err)
	require.NotNil(t, compressed)
	require.Less(t, len(compressed), len(largeData))

	decompressed, err := Decompress(c, compressed, 0)
	require.NoError(t, err)
	require.Equal(t, largeData, decompressed)
}

func TestDecompress(t *testing.T) {
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
			expectedErrorMsg: "corrupt input",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := DefaultCompressor(tc.codec)
			require.NotNil(t, c)

			actualData, err := Decompress(c, tc.compressedData, 0)
			if tc.expectedErrorMsg == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedData, actualData)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrorMsg)
			}
		})
	}
}

func TestDecompressNilCompressor(t *testing.T) {
	_, err := Decompress(nil, []byte{1, 2, 3}, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no compressor provided")
}

func TestDecompressWithSizeLimit(t *testing.T) {
	c := DefaultCompressor(parquet.CompressionCodec_SNAPPY)
	require.NotNil(t, c)

	data := []byte("test data for per-reader decompression limit checking")
	compressed, err := c.Compress(data)
	require.NoError(t, err)

	tests := []struct {
		name    string
		limit   int64
		wantErr bool
	}{
		{"zero-uses-default", 0, false},
		{"large-limit-ok", 1024 * 1024, false},
		{"small-limit-fails", 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Decompress(c, compressed, tc.limit)
			if tc.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrDecompressedSizeExceeded)
			} else {
				require.NoError(t, err)
				require.Equal(t, data, result)
			}
		})
	}
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

func TestNewCompressor(t *testing.T) {
	tests := []struct {
		name    string
		codec   parquet.CompressionCodec
		level   int
		wantErr error
	}{
		{"gzip-valid-level", parquet.CompressionCodec_GZIP, 6, nil},
		{"zstd-valid-level", parquet.CompressionCodec_ZSTD, 3, nil},
		{"brotli-valid-level", parquet.CompressionCodec_BROTLI, 4, nil},
		{"lz4-raw-valid-level", parquet.CompressionCodec_LZ4_RAW, 6, nil},
		{"lz4-valid-level", parquet.CompressionCodec_LZ4, 1, nil},
		{"snappy-unsupported", parquet.CompressionCodec_SNAPPY, 1, ErrUnsupportedCodec},
		{"uncompressed-unsupported", parquet.CompressionCodec_UNCOMPRESSED, 1, ErrUnsupportedCodec},
		{"unknown-codec", parquet.CompressionCodec(-99), 1, ErrUnsupportedCodec},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewCompressor(tc.codec, tc.level)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, c)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, c)
			require.NotNil(t, c.Compress)
			require.NotNil(t, c.Uncompress)

			// Round-trip test
			data := []byte("hello world, this is a test for per-writer compressor round-trip")
			compressed, err := c.Compress(data)
			require.NoError(t, err)
			decompressed, err := c.Uncompress(compressed)
			require.NoError(t, err)
			require.Equal(t, data, decompressed)
		})
	}
}

func TestNewCompressorInvalidLevels(t *testing.T) {
	tests := []struct {
		name  string
		codec parquet.CompressionCodec
		level int
	}{
		{"gzip-level-100", parquet.CompressionCodec_GZIP, 100},
		{"gzip-level-negative", parquet.CompressionCodec_GZIP, -10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewCompressor(tc.codec, tc.level)
			require.Error(t, err, "expected error for codec %v at level %d", tc.codec, tc.level)
			require.Nil(t, c)
		})
	}
}
