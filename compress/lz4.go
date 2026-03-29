//go:build !no_lz4
// +build !no_lz4

package compress

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/pierrec/lz4/v4"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func init() {
	compressorFactories[parquet.CompressionCodec_LZ4] = newLZ4Compressor

	lz4WriterPool := sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}
	compressors[parquet.CompressionCodec_LZ4] = &Compressor{
		Compress: func(buf []byte) []byte {
			lz4Writer := lz4WriterPool.Get().(*lz4.Writer)
			res := new(bytes.Buffer)
			lz4Writer.Reset(res)
			if _, err := lz4Writer.Write(buf); err != nil {
				return nil
			}
			_ = lz4Writer.Close()
			lz4Writer.Reset(nil)
			lz4WriterPool.Put(lz4Writer)
			return res.Bytes()
		},
		Uncompress: lz4Uncompress,
	}
}

func lz4Uncompress(buf []byte) ([]byte, error) {
	rbuf := bytes.NewReader(buf)
	lz4Reader := lz4.NewReader(rbuf)
	return LimitedReadAll(lz4Reader, GetMaxDecompressedSize())
}

func newLZ4Compressor(level int) (*Compressor, error) {
	cl := lz4.CompressionLevel(1 << (8 + level))

	// Validate by creating a writer and applying the option
	testWriter := lz4.NewWriter(nil)
	if err := testWriter.Apply(lz4.CompressionLevelOption(cl)); err != nil {
		return nil, fmt.Errorf("invalid lz4 compression level %d: %w", level, err)
	}

	writerPool := sync.Pool{
		New: func() any {
			w := lz4.NewWriter(nil)
			_ = w.Apply(lz4.CompressionLevelOption(cl))
			return w
		},
	}

	return &Compressor{
		Compress: func(buf []byte) []byte {
			lz4Writer := writerPool.Get().(*lz4.Writer)
			res := new(bytes.Buffer)
			lz4Writer.Reset(res)
			if _, err := lz4Writer.Write(buf); err != nil {
				return nil
			}
			_ = lz4Writer.Close()
			lz4Writer.Reset(nil)
			writerPool.Put(lz4Writer)
			return res.Bytes()
		},
		Uncompress: lz4Uncompress,
	}, nil
}
