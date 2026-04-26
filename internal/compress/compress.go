package compress

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// DefaultMaxDecompressedSize is the default maximum size for decompressed data (256 MB).
// This helps prevent decompression bomb attacks where a small compressed payload
// expands to exhaust available memory.
const DefaultMaxDecompressedSize = 256 * 1024 * 1024

// ErrDecompressedSizeExceeded is returned when decompressed data exceeds the configured limit.
var ErrDecompressedSizeExceeded = errors.New("decompressed data size exceeds configured limit")

// codec holds the compress/uncompress functions for a single codec.
type codec struct {
	compress   func(buf []byte) ([]byte, error)
	uncompress func(buf []byte, maxSize int64) ([]byte, error)
}

// defaultCodecs is populated by init() functions in codec-specific files.
var defaultCodecs = map[parquet.CompressionCodec]*codec{}

// codecFactories maps codecs to functions that create a codec at a given level.
// Each codec file registers its factory in init() if it supports levels.
var codecFactories = map[parquet.CompressionCodec]func(level int) (*codec, error){}

// Compressor provides per-instance compression and decompression for Parquet data.
// Create instances with NewCompressor. Use DefaultCompressor for default settings.
type Compressor struct {
	maxDecompressedSize int64
	codecs              map[parquet.CompressionCodec]*codec
}

// CompressorOption configures a Compressor.
type CompressorOption func(*compressorConfig) error

type compressorConfig struct {
	maxDecompressedSize int64
	levels              map[parquet.CompressionCodec]int
}

// WithMaxDecompressedSize sets the maximum allowed decompressed data size.
// A value of 0 disables the limit (not recommended for untrusted input).
func WithMaxDecompressedSize(size int64) CompressorOption {
	return func(cfg *compressorConfig) error {
		cfg.maxDecompressedSize = size
		return nil
	}
}

// WithCompressionLevel sets the compression level for a specific codec.
// Not all codecs support compression levels. Returns an error during
// NewCompressor if the codec does not support levels or the level is invalid.
func WithCompressionLevel(codec parquet.CompressionCodec, level int) CompressorOption {
	return func(cfg *compressorConfig) error {
		cfg.levels[codec] = level
		return nil
	}
}

// NewCompressor creates a new Compressor with the given options.
// Without options, it uses DefaultMaxDecompressedSize and default compression levels.
func NewCompressor(opts ...CompressorOption) (*Compressor, error) {
	cfg := &compressorConfig{
		maxDecompressedSize: DefaultMaxDecompressedSize,
		levels:              make(map[parquet.CompressionCodec]int),
	}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	codecs := make(map[parquet.CompressionCodec]*codec, len(defaultCodecs))
	for codec, impl := range defaultCodecs {
		codecs[codec] = impl
	}

	for codec, level := range cfg.levels {
		factory, ok := codecFactories[codec]
		if !ok {
			return nil, fmt.Errorf("codec %v does not support compression levels", codec)
		}
		impl, err := factory(level)
		if err != nil {
			return nil, fmt.Errorf("set compression level for %v: %w", codec, err)
		}
		codecs[codec] = impl
	}

	return &Compressor{
		maxDecompressedSize: cfg.maxDecompressedSize,
		codecs:              codecs,
	}, nil
}

var (
	defaultCompressorOnce sync.Once
	defaultCompressorInst *Compressor
)

// DefaultCompressor returns a Compressor with default settings
// (DefaultMaxDecompressedSize and default compression levels for all codecs).
func DefaultCompressor() *Compressor {
	defaultCompressorOnce.Do(func() {
		defaultCompressorInst = &Compressor{
			maxDecompressedSize: DefaultMaxDecompressedSize,
			codecs:              defaultCodecs,
		}
	})
	return defaultCompressorInst
}

// MaxDecompressedSize returns the configured maximum decompressed data size.
func (c *Compressor) MaxDecompressedSize() int64 {
	return c.maxDecompressedSize
}

// Compress compresses data using the specified codec.
func (c *Compressor) Compress(buf []byte, codec parquet.CompressionCodec) ([]byte, error) {
	if impl, ok := c.codecs[codec]; ok {
		return impl.compress(buf)
	}
	return nil, fmt.Errorf("unsupported compress method: %v", codec)
}

// Uncompress decompresses data using the specified codec.
// It validates the output size against the configured maximum.
func (c *Compressor) Uncompress(buf []byte, codec parquet.CompressionCodec) ([]byte, error) {
	if impl, ok := c.codecs[codec]; ok {
		return impl.uncompress(buf, c.maxDecompressedSize)
	}
	return nil, fmt.Errorf("unsupported compress method: %v", codec)
}

// UncompressWithExpectedSize decompresses data and validates that the result
// matches the expected size. This is useful when the expected size is known
// from metadata (e.g., Parquet page headers).
func (c *Compressor) UncompressWithExpectedSize(buf []byte, codec parquet.CompressionCodec, expectedSize int64) ([]byte, error) {
	if c.maxDecompressedSize > 0 && expectedSize > c.maxDecompressedSize {
		return nil, fmt.Errorf("expected decompressed size %d exceeds maximum allowed size %d: %w",
			expectedSize, c.maxDecompressedSize, ErrDecompressedSizeExceeded)
	}

	result, err := c.Uncompress(buf, codec)
	if err != nil {
		return nil, err
	}

	if int64(len(result)) != expectedSize {
		return nil, fmt.Errorf("decompressed size %d does not match expected size %d", len(result), expectedSize)
	}

	return result, nil
}

// CompressWithError compresses data using the specified compression method
// with the default compressor.
func CompressWithError(buf []byte, compressMethod parquet.CompressionCodec) ([]byte, error) {
	return DefaultCompressor().Compress(buf, compressMethod)
}

// Uncompress decompresses data using the specified compression method
// with the default compressor.
func Uncompress(buf []byte, compressMethod parquet.CompressionCodec) ([]byte, error) {
	return DefaultCompressor().Uncompress(buf, compressMethod)
}

// UncompressWithExpectedSize decompresses data and validates the result size
// with the default compressor.
func UncompressWithExpectedSize(buf []byte, compressMethod parquet.CompressionCodec, expectedSize int64) ([]byte, error) {
	return DefaultCompressor().UncompressWithExpectedSize(buf, compressMethod, expectedSize)
}

// limitedReadAll reads from r until EOF or until maxSize bytes have been read.
// It returns ErrDecompressedSizeExceeded if the data would exceed maxSize.
func limitedReadAll(r io.Reader, maxSize int64) ([]byte, error) {
	if maxSize <= 0 {
		return io.ReadAll(r)
	}

	limited := io.LimitReader(r, maxSize+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}

	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("decompressed data (%d bytes) exceeds maximum size %d: %w",
			len(data), maxSize, ErrDecompressedSizeExceeded)
	}

	return data, nil
}
