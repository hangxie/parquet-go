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
		uncompress: zstdUncompress(dec),
	}
	codecFactories[parquet.CompressionCodec_ZSTD] = newZSTDCompressor
}

func zstdUncompress(dec *zstd.Decoder) func([]byte, int64) ([]byte, error) {
	return func(buf []byte, maxSize int64) ([]byte, error) {
		if maxSize <= 0 {
			return dec.DecodeAll(buf, nil)
		}

		// Reject before decoding if frame header declares an oversized payload
		var h zstd.Header
		if err := h.Decode(buf); err == nil && h.HasFCS && h.FrameContentSize > uint64(maxSize) {
			return nil, fmt.Errorf("zstd frame content size %d exceeds maximum allowed size %d: %w",
				h.FrameContentSize, maxSize, ErrDecompressedSizeExceeded)
		}

		result, err := dec.DecodeAll(buf, nil)
		if err != nil {
			return nil, err
		}

		// Fallback for frames without content size in header (HasFCS=false)
		if int64(len(result)) > maxSize {
			return nil, fmt.Errorf("decompressed size %d exceeds maximum allowed size %d: %w",
				len(result), maxSize, ErrDecompressedSizeExceeded)
		}
		return result, nil
	}
}

func newZSTDCompressor(level *int) (*codec, error) {
	opts := []zstd.EOption{
		zstd.WithZeroFrames(true),
	}
	if level != nil {
		opts = append(opts, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(*level)))
	}
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return nil, err
	}
	dec, _ := zstd.NewReader(nil)
	return &codec{
		compress: func(buf []byte) ([]byte, error) {
			return enc.EncodeAll(buf, nil), nil
		},
		uncompress: zstdUncompress(dec),
	}, nil
}
