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
			result, err := snappy.Decode(nil, buf)
			if err != nil {
				return nil, err
			}
			if maxSize > 0 && int64(len(result)) > maxSize {
				return nil, fmt.Errorf("decompressed size %d exceeds maximum allowed size %d: %w",
					len(result), maxSize, ErrDecompressedSizeExceeded)
			}
			return result, nil
		},
	}
}
