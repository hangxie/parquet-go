package compress

import (
	"errors"
	"fmt"
	"io"

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
		return nil, fmt.Errorf("decompressed data exceeds maximum size %d: %w", maxSize, ErrDecompressedSizeExceeded)
	}

	return data, nil
}

// MaxDecompressedSize is the default maximum size for decompressed data (256 MB).
// This helps prevent decompression bomb attacks where a small compressed payload
// expands to exhaust available memory.
const MaxDecompressedSize = 256 * 1024 * 1024

// Compressor holds compression and decompression functions for a codec.
// Both Compress and Uncompress must be safe for concurrent use by multiple
// goroutines, as a single writer/reader may invoke them from NP parallel
// goroutines.
type Compressor struct {
	Compress   func(buf []byte) ([]byte, error)
	Uncompress func(buf []byte) ([]byte, error)
}

var compressors = map[parquet.CompressionCodec]*Compressor{}

// ErrUnsupportedCodec is returned when a codec does not support compression levels.
var ErrUnsupportedCodec = errors.New("codec does not support compression levels")

// ErrDecompressedSizeExceeded is returned when decompressed data exceeds the configured limit.
var ErrDecompressedSizeExceeded = errors.New("decompressed size exceeds limit")

// compressorFactories maps codecs to functions that create a Compressor at a given level.
// Each codec file registers its factory in init() if it supports levels.
var compressorFactories = map[parquet.CompressionCodec]func(level int) (*Compressor, error){}

// NewCompressor creates a new Compressor for the given codec at the specified
// compression level. Returns ErrUnsupportedCodec if the codec does not support
// levels (e.g., Snappy, UNCOMPRESSED), or an error wrapping the codec-specific
// validation failure if the level is invalid. The returned Compressor is safe
// for concurrent use.
func NewCompressor(codec parquet.CompressionCodec, level int) (*Compressor, error) {
	factory, ok := compressorFactories[codec]
	if !ok {
		return nil, fmt.Errorf("codec %v: %w", codec, ErrUnsupportedCodec)
	}
	return factory(level)
}

// Decompress decompresses data using the provided compressor and validates
// against the given size limit. If limit is 0, falls back to MaxDecompressedSize.
func Decompress(c *Compressor, buf []byte, limit int64) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("no compressor provided")
	}

	result, err := c.Uncompress(buf)
	if err != nil {
		return nil, err
	}

	effectiveLimit := limit
	if effectiveLimit == 0 {
		effectiveLimit = MaxDecompressedSize
	}
	if effectiveLimit > 0 && int64(len(result)) > effectiveLimit {
		return nil, fmt.Errorf("decompressed size %d exceeds maximum allowed size %d: %w", len(result), effectiveLimit, ErrDecompressedSizeExceeded)
	}

	return result, nil
}

// DefaultCompressor returns the default compressor for the given codec.
// Returns nil if the codec is not registered.
func DefaultCompressor(codec parquet.CompressionCodec) *Compressor {
	return compressors[codec]
}
