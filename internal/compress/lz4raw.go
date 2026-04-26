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
	codecFactories[parquet.CompressionCodec_LZ4_RAW] = newLZ4RawCompressor

	defaultCodecs[parquet.CompressionCodec_LZ4_RAW] = &codec{
		compress:   lz4RawCompress(lz4.CompressionLevel(9)),
		uncompress: lz4RawUncompress,
	}
}

func lz4RawCompress(level lz4.CompressionLevel) func([]byte) ([]byte, error) {
	return func(buf []byte) ([]byte, error) {
		// CompressorHC is NOT thread-safe — must create per call
		lz4hc := lz4.CompressorHC{Level: level}
		res := make([]byte, lz4.CompressBlockBound(len(buf)))
		count, err := lz4hc.CompressBlock(buf, res)
		if err != nil {
			return nil, fmt.Errorf("lz4 raw compress: %w", err)
		}
		return res[:count], nil
	}
}

func lz4RawUncompress(buf []byte, maxSize int64) ([]byte, error) {
	estimatedSize := max(int64(len(buf))*10, 1024)
	if maxSize > 0 && estimatedSize > maxSize {
		estimatedSize = maxSize
	}

	for {
		res := make([]byte, estimatedSize)
		count, err := lz4.UncompressBlock(buf, res)
		if err == nil && count >= 0 {
			return res[:count], nil
		}
		if err != nil && strings.Contains(err.Error(), "too short") {
			estimatedSize *= 2
			if maxSize > 0 && estimatedSize > maxSize {
				return nil, fmt.Errorf("lz4 decompression would exceed maximum size %d: %w",
					maxSize, ErrDecompressedSizeExceeded)
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("lz4 decompression failed unexpectedly")
	}
}

func newLZ4RawCompressor(level int) (*codec, error) {
	cl := lz4.CompressionLevel(level)
	// Validate via test encode — CompressorHC does not validate level on construction
	testHC := lz4.CompressorHC{Level: cl}
	testSrc := []byte("test")
	testDst := make([]byte, lz4.CompressBlockBound(len(testSrc)))
	if _, err := testHC.CompressBlock(testSrc, testDst); err != nil {
		return nil, fmt.Errorf("invalid lz4 raw compression level %d: %w", level, err)
	}

	return &codec{
		compress:   lz4RawCompress(cl),
		uncompress: lz4RawUncompress,
	}, nil
}
