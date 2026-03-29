//go:build !no_gzip
// +build !no_gzip

package compress

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/klauspost/compress/gzip"

	"github.com/hangxie/parquet-go/v3/parquet"
)

var gzipWriterPool sync.Pool

func init() {
	gzipWriterPool = sync.Pool{
		New: func() any {
			return gzip.NewWriter(nil)
		},
	}

	codecFactories[parquet.CompressionCodec_GZIP] = newGZIPCompressor

	defaultCodecs[parquet.CompressionCodec_GZIP] = &codec{
		compress:   gzipCompress(&gzipWriterPool),
		uncompress: gzipUncompress,
	}
}

func gzipCompress(pool *sync.Pool) func([]byte) ([]byte, error) {
	return func(buf []byte) ([]byte, error) {
		res := new(bytes.Buffer)
		gzipWriter := pool.Get().(*gzip.Writer)
		gzipWriter.Reset(res)
		if _, err := gzipWriter.Write(buf); err != nil {
			return nil, fmt.Errorf("gzip compress: %w", err)
		}
		if err := gzipWriter.Close(); err != nil {
			return nil, fmt.Errorf("gzip compress close: %w", err)
		}
		gzipWriter.Reset(nil)
		pool.Put(gzipWriter)
		return res.Bytes(), nil
	}
}

func gzipUncompress(buf []byte, maxSize int64) ([]byte, error) {
	rbuf := bytes.NewReader(buf)
	gzipReader, err := gzip.NewReader(rbuf)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = gzipReader.Close()
	}()
	return limitedReadAll(gzipReader, maxSize)
}

func newGZIPCompressor(level int) (*codec, error) {
	if _, err := gzip.NewWriterLevel(nil, level); err != nil {
		return nil, err
	}

	writerPool := sync.Pool{
		New: func() any {
			w, _ := gzip.NewWriterLevel(nil, level)
			return w
		},
	}

	return &codec{
		compress:   gzipCompress(&writerPool),
		uncompress: gzipUncompress,
	}, nil
}
