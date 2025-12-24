//go:build !no_brotli
// +build !no_brotli

package compress

import (
	"bytes"
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
		Uncompress: func(buf []byte) ([]byte, error) {
			rbuf := bytes.NewReader(buf)
			brotliReader := brotli.NewReader(rbuf)
			return LimitedReadAll(brotliReader, GetMaxDecompressedSize())
		},
	}
}
