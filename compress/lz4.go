//go:build !no_lz4
// +build !no_lz4

package compress

import (
	"bytes"
	"io"
	"sync"

	"github.com/pierrec/lz4/v4"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func init() {
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
		Uncompress: func(buf []byte) (i []byte, err error) {
			rbuf := bytes.NewReader(buf)
			lz4Reader := lz4.NewReader(rbuf)
			res, err := io.ReadAll(lz4Reader)
			return res, err
		},
	}
}
