//go:build !no_zstd
// +build !no_zstd

package compress

import (
	"fmt"

	"github.com/klauspost/compress/zstd"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func init() {
	enc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true))
	dec, _ := zstd.NewReader(nil)
	defaultCodecs[parquet.CompressionCodec_ZSTD] = &codec{
		compress: func(buf []byte) ([]byte, error) {
			return enc.EncodeAll(buf, nil), nil
		},
		uncompress: func(buf []byte, maxSize int64) ([]byte, error) {
			result, err := dec.DecodeAll(buf, nil)
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
	codecFactories[parquet.CompressionCodec_ZSTD] = newZSTDCompressor
}

func newZSTDCompressor(level int) (*codec, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
		zstd.WithZeroFrames(true),
	)
	if err != nil {
		return nil, err
	}
	dec, _ := zstd.NewReader(nil)
	return &codec{
		compress: func(buf []byte) ([]byte, error) {
			return enc.EncodeAll(buf, nil), nil
		},
		uncompress: func(buf []byte, maxSize int64) ([]byte, error) {
			result, err := dec.DecodeAll(buf, nil)
			if err != nil {
				return nil, err
			}
			if maxSize > 0 && int64(len(result)) > maxSize {
				return nil, fmt.Errorf("decompressed size %d exceeds maximum allowed size %d: %w",
					len(result), maxSize, ErrDecompressedSizeExceeded)
			}
			return result, nil
		},
	}, nil
}
