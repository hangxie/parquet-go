package gocloud

import (
	"context"
	"errors"

	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
)

var errBlobDriver = errors.New("blob driver error")

type errorBucket struct {
	driver.Bucket
}

func (b *errorBucket) ErrorCode(error) gcerrors.ErrorCode {
	return gcerrors.Unknown
}

func (b *errorBucket) As(any) bool {
	return false
}

func (b *errorBucket) ErrorAs(error, any) bool {
	return false
}

func (b *errorBucket) Attributes(context.Context, string) (*driver.Attributes, error) {
	return nil, errBlobDriver
}

func (b *errorBucket) ListPaged(context.Context, *driver.ListOptions) (*driver.ListPage, error) {
	return nil, errBlobDriver
}

func (b *errorBucket) NewRangeReader(context.Context, string, int64, int64, *driver.ReaderOptions) (driver.Reader, error) {
	return nil, errBlobDriver
}

func (b *errorBucket) NewTypedWriter(context.Context, string, string, *driver.WriterOptions) (driver.Writer, error) {
	return nil, errBlobDriver
}

func (b *errorBucket) Copy(context.Context, string, string, *driver.CopyOptions) error {
	return errBlobDriver
}

func (b *errorBucket) Delete(context.Context, string) error {
	return errBlobDriver
}

func (b *errorBucket) SignedURL(context.Context, string, *driver.SignedURLOptions) (string, error) {
	return "", errBlobDriver
}

func (b *errorBucket) Close() error {
	return nil
}

type attributesAfterExistsErrorBucket struct {
	errorBucket
	calls int
}

func (b *attributesAfterExistsErrorBucket) Attributes(context.Context, string) (*driver.Attributes, error) {
	b.calls++
	if b.calls == 1 {
		return &driver.Attributes{Size: 1}, nil
	}
	return nil, errBlobDriver
}
