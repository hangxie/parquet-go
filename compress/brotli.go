//go:build !no_brotli
// +build !no_brotli

package compress

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/andybalholm/brotli"

	"github.com/hangxie/parquet-go/v2/parquet"
)

var brotliWriterPool sync.Pool

func init() {
	brotliWriterPool = sync.Pool{
		New: func() any {
			return brotli.NewWriter(nil)
		},
	}

	compressorFactories[parquet.CompressionCodec_BROTLI] = newBrotliCompressor

	compressors[parquet.CompressionCodec_BROTLI] = &Compressor{
		Compress: func(buf []byte) []byte {
			res := new(bytes.Buffer)
			brotliWriter := brotliWriterPool.Get().(*brotli.Writer)
			brotliWriter.Reset(res)
			if _, err := brotliWriter.Write(buf); err != nil {
				return nil
			}
			_ = brotliWriter.Close()
			brotliWriterPool.Put(brotliWriter)
			return res.Bytes()
		},
		Uncompress: brotliUncompress,
	}
}

func brotliUncompress(buf []byte) ([]byte, error) {
	rbuf := bytes.NewReader(buf)
	brotliReader := brotli.NewReader(rbuf)
	return LimitedReadAll(brotliReader, GetMaxDecompressedSize())
}

func newBrotliCompressor(level int) (*Compressor, error) {
	// brotli.NewWriterLevel does not return an error, so validate via test encode
	w := brotli.NewWriterLevel(nil, level)
	var testBuf bytes.Buffer
	w.Reset(&testBuf)
	if _, err := w.Write([]byte("test")); err != nil {
		return nil, fmt.Errorf("invalid brotli compression level %d: %w", level, err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("invalid brotli compression level %d: %w", level, err)
	}

	writerPool := sync.Pool{
		New: func() any {
			return brotli.NewWriterLevel(nil, level)
		},
	}

	return &Compressor{
		Compress: func(buf []byte) []byte {
			res := new(bytes.Buffer)
			brotliWriter := writerPool.Get().(*brotli.Writer)
			brotliWriter.Reset(res)
			if _, err := brotliWriter.Write(buf); err != nil {
				return nil
			}
			_ = brotliWriter.Close()
			writerPool.Put(brotliWriter)
			return res.Bytes()
		},
		Uncompress: brotliUncompress,
	}, nil
}
