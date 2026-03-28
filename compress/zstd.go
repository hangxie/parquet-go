//go:build !no_zstd
// +build !no_zstd

package compress

import (
	"github.com/klauspost/compress/zstd"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func init() {
	// Create encoder/decoder with default parameters.
	enc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true))
	dec, _ := zstd.NewReader(nil)
	compressors[parquet.CompressionCodec_ZSTD] = &Compressor{
		Compress: func(buf []byte) ([]byte, error) {
			return enc.EncodeAll(buf, nil), nil
		},
		Uncompress: func(buf []byte) (bytes []byte, err error) {
			return dec.DecodeAll(buf, nil)
		},
	}
	compressorFactories[parquet.CompressionCodec_ZSTD] = newZSTDCompressor
}

func newZSTDCompressor(level int) (*Compressor, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
		zstd.WithZeroFrames(true),
	)
	if err != nil {
		return nil, err
	}
	dec, _ := zstd.NewReader(nil)
	return &Compressor{
		Compress: func(buf []byte) ([]byte, error) {
			return enc.EncodeAll(buf, nil), nil
		},
		Uncompress: func(buf []byte) ([]byte, error) {
			return dec.DecodeAll(buf, nil)
		},
	}, nil
}
