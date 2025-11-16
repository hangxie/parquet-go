package gocloud

import (
	"context"
	"fmt"

	"gocloud.dev/blob"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *blobFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*blobWriter)(nil)

type blobWriter struct {
	blobFile
	writer *blob.Writer
}

func NewBlobWriter(ctx context.Context, b *blob.Bucket, name string) (source.ParquetFileWriter, error) {
	bf := &blobWriter{
		blobFile: blobFile{
			ctx:    ctx,
			bucket: b,
		},
	}

	return bf.Create(name)
}

// Note that for blob storage, calling write on an existing blob overwrites that blob as opposed to appending to it.
// Additionally Write is not guaranteed to have succeeded unless Close() also succeeds
func (b *blobWriter) Write(p []byte) (n int, err error) {
	if b.writer == nil && b.key == "" {
		return 0, fmt.Errorf("writer not created or opened")
	}
	if b.writer == nil {
		if w, err := b.bucket.NewWriter(b.ctx, b.key, nil); err != nil {
			return 0, fmt.Errorf("create blob writer key=%s: %w", b.key, err)
		} else {
			b.writer = w
		}
	}

	n, err = b.writer.Write(p)
	b.size += int64(n)

	return n, err
}

func (b *blobWriter) Close() error {
	if b.writer != nil {
		return b.writer.Close()
	}

	return nil
}

func (b *blobWriter) Create(name string) (source.ParquetFileWriter, error) {
	if name == "" {
		return nil, fmt.Errorf("file name empty")
	}

	bf := &blobWriter{
		blobFile: blobFile{
			ctx:    b.ctx,
			bucket: b.bucket,
		},
	}

	w, err := bf.bucket.NewWriter(bf.ctx, name, nil)
	if err != nil {
		return nil, fmt.Errorf("create blob writer %s: %w", name, err)
	}

	bf.key = name
	bf.writer = w

	return bf, nil
}
