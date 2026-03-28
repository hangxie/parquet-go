//go:build !no_lz4raw
// +build !no_lz4raw

package compress

import (
	"fmt"
	"strings"

	"github.com/pierrec/lz4/v4"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func init() {
	compressorFactories[parquet.CompressionCodec_LZ4_RAW] = newLZ4RawCompressor

	compressors[parquet.CompressionCodec_LZ4_RAW] = &Compressor{
		Compress: func(buf []byte) ([]byte, error) {
			lz4hc := lz4.CompressorHC{
				Level: lz4.CompressionLevel(9),
			}
			res := make([]byte, lz4.CompressBlockBound(len(buf)))
			count, err := lz4hc.CompressBlock(buf, res)
			if err != nil {
				return nil, err
			}
			return res[:count], nil
		},
		Uncompress: lz4RawUncompress,
	}
}

func lz4RawUncompress(buf []byte) ([]byte, error) {
	// Safety cap prevents unbounded growth for corrupt/malicious data.
	// This is a defense-in-depth measure; the precise per-reader limit is
	// enforced by compress.Decompress() after decompression completes.
	safetyLimit := int64(MaxDecompressedSize)

	estimatedSize := int64(len(buf)) * 10
	if estimatedSize < 1024 {
		estimatedSize = 1024
	}
	if estimatedSize > safetyLimit {
		estimatedSize = safetyLimit
	}

	for {
		res := make([]byte, estimatedSize)
		count, err := lz4.UncompressBlock(buf, res)
		if err == nil && count >= 0 {
			return res[:count], nil
		}
		if err != nil && strings.Contains(err.Error(), "too short") {
			estimatedSize *= 2
			if estimatedSize > safetyLimit {
				return nil, fmt.Errorf("lz4 decompression would exceed maximum size %d: %w", safetyLimit, ErrDecompressedSizeExceeded)
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("lz4 decompression failed unexpectedly")
	}
}

func newLZ4RawCompressor(level int) (*Compressor, error) {
	cl := lz4.CompressionLevel(level)
	// Validate via test encode — CompressorHC does not validate level on construction
	testHC := lz4.CompressorHC{Level: cl}
	testSrc := []byte("test")
	testDst := make([]byte, lz4.CompressBlockBound(len(testSrc)))
	if _, err := testHC.CompressBlock(testSrc, testDst); err != nil {
		return nil, fmt.Errorf("invalid lz4 raw compression level %d: %w", level, err)
	}

	return &Compressor{
		Compress: func(buf []byte) ([]byte, error) {
			// CompressorHC is NOT thread-safe — must create per call
			lz4hc := lz4.CompressorHC{Level: cl}
			res := make([]byte, lz4.CompressBlockBound(len(buf)))
			count, err := lz4hc.CompressBlock(buf, res)
			if err != nil {
				return nil, err
			}
			return res[:count], nil
		},
		Uncompress: lz4RawUncompress,
	}, nil
}
