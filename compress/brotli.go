//go:build !no_brotli
// +build !no_brotli

package compress

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/andybalholm/brotli"

	"github.com/hangxie/parquet-go/v3/parquet"
)

var brotliWriterPool sync.Pool

func init() {
	brotliWriterPool = sync.Pool{
		New: func() any {
			return brotli.NewWriter(nil)
		},
	}

	codecFactories[parquet.CompressionCodec_BROTLI] = newBrotliCompressor
	defaultCodecs[parquet.CompressionCodec_BROTLI] = &codec{
		compress:   brotliCompress(&brotliWriterPool),
		uncompress: brotliUncompress,
	}
}

func brotliCompress(pool *sync.Pool) func([]byte) ([]byte, error) {
	return func(buf []byte) ([]byte, error) {
		res := new(bytes.Buffer)
		brotliWriter := pool.Get().(*brotli.Writer)
		defer pool.Put(brotliWriter)
		brotliWriter.Reset(res)
		if _, err := brotliWriter.Write(buf); err != nil {
			return nil, fmt.Errorf("brotli compress: %w", err)
		}
		if err := brotliWriter.Close(); err != nil {
			return nil, fmt.Errorf("brotli compress close: %w", err)
		}
		return res.Bytes(), nil
	}
}

func brotliUncompress(buf []byte, maxSize int64) ([]byte, error) {
	rbuf := bytes.NewReader(buf)
	brotliReader := brotli.NewReader(rbuf)
	return limitedReadAll(brotliReader, maxSize)
}

func newBrotliCompressor(level int) (*codec, error) {
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

	return &codec{
		compress:   brotliCompress(&writerPool),
		uncompress: brotliUncompress,
	}, nil
}
