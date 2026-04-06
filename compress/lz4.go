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
	codecFactories[parquet.CompressionCodec_LZ4] = newLZ4Compressor

	lz4WriterPool := sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}
	defaultCodecs[parquet.CompressionCodec_LZ4] = &codec{
		compress:   lz4Compress(&lz4WriterPool),
		uncompress: lz4Uncompress,
	}
}

func lz4Compress(pool *sync.Pool) func([]byte) ([]byte, error) {
	return func(buf []byte) ([]byte, error) {
		lz4Writer := pool.Get().(*lz4.Writer)
		res := new(bytes.Buffer)
		lz4Writer.Reset(res)
		if _, err := lz4Writer.Write(buf); err != nil {
			return nil, fmt.Errorf("lz4 compress: %w", err)
		}
		if err := lz4Writer.Close(); err != nil {
			return nil, fmt.Errorf("lz4 compress close: %w", err)
		}
		lz4Writer.Reset(nil)
		pool.Put(lz4Writer)
		return res.Bytes(), nil
	}
}

func lz4Uncompress(buf []byte, maxSize int64) ([]byte, error) {
	rbuf := bytes.NewReader(buf)
	lz4Reader := lz4.NewReader(rbuf)
	return limitedReadAll(lz4Reader, maxSize)
}

func lz4CompressWithLevel(pool *sync.Pool, cl lz4.CompressionLevel) func([]byte) ([]byte, error) {
	return func(buf []byte) ([]byte, error) {
		lz4Writer := pool.Get().(*lz4.Writer)
		if err := lz4Writer.Apply(lz4.CompressionLevelOption(cl)); err != nil {
			pool.Put(lz4Writer)
			return nil, fmt.Errorf("lz4 compress: %w", err)
		}
		res := new(bytes.Buffer)
		lz4Writer.Reset(res)
		if _, err := lz4Writer.Write(buf); err != nil {
			return nil, fmt.Errorf("lz4 compress: %w", err)
		}
		if err := lz4Writer.Close(); err != nil {
			return nil, fmt.Errorf("lz4 compress close: %w", err)
		}
		lz4Writer.Reset(nil)
		pool.Put(lz4Writer)
		return res.Bytes(), nil
	}
}

func newLZ4Compressor(level int) (*codec, error) {
	cl := lz4.CompressionLevel(1 << (8 + level))

	// Validate by creating a writer and applying the option
	testWriter := lz4.NewWriter(nil)
	if err := testWriter.Apply(lz4.CompressionLevelOption(cl)); err != nil {
		return nil, fmt.Errorf("invalid lz4 compression level %d: %w", level, err)
	}

	writerPool := sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}

	return &codec{
		compress:   lz4CompressWithLevel(&writerPool, cl),
		uncompress: lz4Uncompress,
	}, nil
}
