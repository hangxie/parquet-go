//go:build !no_lz4raw
// +build !no_lz4raw

package compress

import (
	"fmt"
	"strings"

	"github.com/pierrec/lz4/v4"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func init() {
	compressors[parquet.CompressionCodec_LZ4_RAW] = &Compressor{
		Compress: func(buf []byte) []byte {
			lz4hc := lz4.CompressorHC{
				Level: lz4.CompressionLevel(9),
			}
			res := make([]byte, lz4.CompressBlockBound(len(buf)))
			count, _ := lz4hc.CompressBlock(buf, res)
			return res[:count]
		},
		Uncompress: func(buf []byte) ([]byte, error) {
			maxSize := GetMaxDecompressedSize()

			// Start with a reasonable estimate and grow if needed
			// LZ4 block format can achieve high compression ratios
			estimatedSize := int64(len(buf)) * 10
			if estimatedSize < 1024 {
				estimatedSize = 1024 // Minimum 1KB
			}

			// Cap initial estimate to max size if set
			if maxSize > 0 && estimatedSize > maxSize {
				estimatedSize = maxSize
			}

			// Try decompression with growing buffer
			for {
				res := make([]byte, estimatedSize)
				count, err := lz4.UncompressBlock(buf, res)
				if err == nil && count >= 0 {
					return res[:count], nil
				}

				// Check if it's a "buffer too short" error
				if err != nil && strings.Contains(err.Error(), "too short") {
					// Double buffer size and retry
					estimatedSize *= 2
					if maxSize > 0 && estimatedSize > maxSize {
						return nil, fmt.Errorf("lz4 decompression would exceed maximum size %d", maxSize)
					}
					continue
				}

				// Other error - return it
				if err != nil {
					return nil, err
				}

				// Unexpected state
				return nil, fmt.Errorf("lz4 decompression failed unexpectedly")
			}
		},
	}
}
