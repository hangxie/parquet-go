//go:build !no_gzip
// +build !no_gzip

package compress

import (
	"bytes"
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

	compressorFactories[parquet.CompressionCodec_GZIP] = newGZIPCompressor

	compressors[parquet.CompressionCodec_GZIP] = &Compressor{
		Compress: func(buf []byte) []byte {
			res := new(bytes.Buffer)
			gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
			gzipWriter.Reset(res)
			if _, err := gzipWriter.Write(buf); err != nil {
				return nil
			}
			_ = gzipWriter.Close()
			gzipWriter.Reset(nil)
			gzipWriterPool.Put(gzipWriter)
			return res.Bytes()
		},
		Uncompress: gzipUncompress,
	}
}

func gzipUncompress(buf []byte) ([]byte, error) {
	rbuf := bytes.NewReader(buf)
	gzipReader, err := gzip.NewReader(rbuf)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = gzipReader.Close()
	}()
	return LimitedReadAll(gzipReader, GetMaxDecompressedSize())
}

func newGZIPCompressor(level int) (*Compressor, error) {
	// Validate level by attempting to create a writer — library returns error if invalid
	if _, err := gzip.NewWriterLevel(nil, level); err != nil {
		return nil, err
	}

	writerPool := sync.Pool{
		New: func() any {
			w, _ := gzip.NewWriterLevel(nil, level)
			return w
		},
	}

	return &Compressor{
		Compress: func(buf []byte) []byte {
			res := new(bytes.Buffer)
			gzipWriter := writerPool.Get().(*gzip.Writer)
			gzipWriter.Reset(res)
			if _, err := gzipWriter.Write(buf); err != nil {
				return nil
			}
			_ = gzipWriter.Close()
			gzipWriter.Reset(nil)
			writerPool.Put(gzipWriter)
			return res.Bytes()
		},
		Uncompress: gzipUncompress,
	}, nil
}
