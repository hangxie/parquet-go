package compress

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/hangxie/parquet-go/v3/parquet"
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

// ErrCompressionInUse is returned by SetCompressionLevel when compression has
// already been used. SetCompressionLevel must be called before any compression.
var ErrCompressionInUse = errors.New("compression already in use, SetCompressionLevel must be called before any compression")

// compressionUsed tracks whether any compression has occurred. Once set to true,
// SetCompressionLevel will refuse to modify compressors.
var compressionUsed atomic.Bool

// compressorFactories maps codecs to functions that create a Compressor at a given level.
// Each codec file registers its factory in init() if it supports levels.
var compressorFactories = map[parquet.CompressionCodec]func(level int) (*Compressor, error){}

// resetCompressionUsed resets the compressionUsed flag for testing purposes only.
func resetCompressionUsed() {
	compressionUsed.Store(false)
}

// saveCompressor returns the current compressor for a codec, for test cleanup.
func saveCompressor(codec parquet.CompressionCodec) *Compressor {
	return compressors[codec]
}

// restoreCompressor restores a previously saved compressor for a codec.
func restoreCompressor(codec parquet.CompressionCodec, c *Compressor) {
	compressors[codec] = c
}

// SetCompressionLevel sets the compression level for the specified codec.
// It must be called before any compression occurs. Returns ErrCompressionInUse
// if compression has already been used, or an error if the codec does not support
// levels or the level is invalid.
func SetCompressionLevel(codec parquet.CompressionCodec, level int) error {
	if compressionUsed.Load() {
		return ErrCompressionInUse
	}
	factory, ok := compressorFactories[codec]
	if !ok {
		return fmt.Errorf("codec %v does not support compression levels", codec)
	}
	c, err := factory(level)
	if err != nil {
		return fmt.Errorf("set compression level for %v: %w", codec, err)
	}
	compressors[codec] = c
	return nil
}

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

// CompressWithError compresses data using the specified compression method.
// Returns an error if the compression codec is not supported.
func CompressWithError(buf []byte, compressMethod parquet.CompressionCodec) ([]byte, error) {
	compressionUsed.Store(true)
	c, ok := compressors[compressMethod]
	if !ok {
		return nil, fmt.Errorf("unsupported compress method: %v", compressMethod)
	}
	return c.Compress(buf), nil
}

// Compress compresses data using the specified compression method.
// Deprecated: Use CompressWithError instead for proper error handling.
// This function returns nil if the compression codec is not supported.
func Compress(buf []byte, compressMethod parquet.CompressionCodec) []byte {
	result, _ := CompressWithError(buf, compressMethod)
	return result
}
