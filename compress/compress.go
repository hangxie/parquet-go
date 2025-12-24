package compress

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/hangxie/parquet-go/v2/parquet"
)

// LimitedReadAll reads from r until EOF or until maxSize bytes have been read.
// It returns an error if the data would exceed maxSize.
func LimitedReadAll(r io.Reader, maxSize int64) ([]byte, error) {
	if maxSize <= 0 {
		return io.ReadAll(r)
	}

	// Read up to maxSize+1 bytes to detect if we exceed the limit
	limited := io.LimitReader(r, maxSize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}

	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("decompressed data exceeds maximum size %d", maxSize)
	}

	return data, nil
}

// DefaultMaxDecompressedSize is the default maximum size for decompressed data (256 MB).
// This helps prevent decompression bomb attacks where a small compressed payload
// expands to exhaust available memory.
const DefaultMaxDecompressedSize = 256 * 1024 * 1024

// MaxDecompressionRatio is the maximum allowed ratio of decompressed to compressed size.
// A ratio of 1000:1 is generous for legitimate data but helps catch malicious payloads.
const MaxDecompressionRatio = 1000

// maxDecompressedSize is the configurable maximum decompressed size.
// Use SetMaxDecompressedSize to change this value.
var maxDecompressedSize int64 = DefaultMaxDecompressedSize

// SetMaxDecompressedSize sets the maximum allowed size for decompressed data.
// Set to 0 to disable the limit (not recommended for untrusted input).
func SetMaxDecompressedSize(size int64) {
	atomic.StoreInt64(&maxDecompressedSize, size)
}

// GetMaxDecompressedSize returns the current maximum decompressed size limit.
func GetMaxDecompressedSize() int64 {
	return atomic.LoadInt64(&maxDecompressedSize)
}

type Compressor struct {
	Compress   func(buf []byte) []byte
	Uncompress func(buf []byte) ([]byte, error)
}

var compressors = map[parquet.CompressionCodec]*Compressor{}

// Uncompress decompresses data using the specified compression method.
// It validates the output size against the configured maximum to prevent
// decompression bomb attacks.
func Uncompress(buf []byte, compressMethod parquet.CompressionCodec) ([]byte, error) {
	c, ok := compressors[compressMethod]
	if !ok {
		return nil, fmt.Errorf("unsupported compress method: %v", compressMethod)
	}

	result, err := c.Uncompress(buf)
	if err != nil {
		return nil, err
	}

	// Validate decompressed size
	maxSize := GetMaxDecompressedSize()
	if maxSize > 0 && int64(len(result)) > maxSize {
		return nil, fmt.Errorf("decompressed size %d exceeds maximum allowed size %d", len(result), maxSize)
	}

	// Validate decompression ratio to catch decompression bombs
	if len(buf) > 0 {
		ratio := int64(len(result)) / int64(len(buf))
		if ratio > MaxDecompressionRatio {
			return nil, fmt.Errorf("decompression ratio %d:1 exceeds maximum allowed ratio %d:1", ratio, MaxDecompressionRatio)
		}
	}

	return result, nil
}

// UncompressWithExpectedSize decompresses data and validates that the result
// matches the expected size. This is useful when the expected size is known
// from metadata (e.g., Parquet page headers).
func UncompressWithExpectedSize(buf []byte, compressMethod parquet.CompressionCodec, expectedSize int64) ([]byte, error) {
	c, ok := compressors[compressMethod]
	if !ok {
		return nil, fmt.Errorf("unsupported compress method: %v", compressMethod)
	}

	// Validate expected size against maximum before decompression
	maxSize := GetMaxDecompressedSize()
	if maxSize > 0 && expectedSize > maxSize {
		return nil, fmt.Errorf("expected decompressed size %d exceeds maximum allowed size %d", expectedSize, maxSize)
	}

	// Validate expected decompression ratio
	if len(buf) > 0 && expectedSize/int64(len(buf)) > MaxDecompressionRatio {
		return nil, fmt.Errorf("expected decompression ratio exceeds maximum allowed ratio %d:1", MaxDecompressionRatio)
	}

	result, err := c.Uncompress(buf)
	if err != nil {
		return nil, err
	}

	// Validate actual size matches expected
	if int64(len(result)) != expectedSize {
		return nil, fmt.Errorf("decompressed size %d does not match expected size %d", len(result), expectedSize)
	}

	return result, nil
}

func Compress(buf []byte, compressMethod parquet.CompressionCodec) []byte {
	c, ok := compressors[compressMethod]
	if !ok {
		return nil
	}
	return c.Compress(buf)
}
