//go:build !no_snappy
// +build !no_snappy

package compress

import (
	"fmt"

	"github.com/klauspost/compress/snappy"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func init() {
	defaultCodecs[parquet.CompressionCodec_SNAPPY] = &codec{
		compress: func(buf []byte) ([]byte, error) {
			return snappy.Encode(nil, buf), nil
		},
		uncompress: func(buf []byte, maxSize int64) ([]byte, error) {
			if maxSize > 0 {
				n, err := snappy.DecodedLen(buf)
				if err != nil {
					return nil, fmt.Errorf("snappy decoded length: %w", err)
				}
				if int64(n) > maxSize {
					return nil, fmt.Errorf("snappy decoded length %d exceeds maximum allowed size %d: %w",
						n, maxSize, ErrDecompressedSizeExceeded)
				}
			}
			result, err := snappy.Decode(nil, buf)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
	}
}
