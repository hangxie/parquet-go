package source

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
)

type ParquetFileReader interface {
	io.Seeker
	io.Reader
	io.Closer
	// Open returns a reader with an independent file handle for name. The new
	// reader may share an underlying client, but closing it must not invalidate
	// the receiver.
	Open(name string) (ParquetFileReader, error)
	Clone() (ParquetFileReader, error)
}

type ParquetFileWriter interface {
	io.Writer
	io.Closer
	Create(name string) (ParquetFileWriter, error)
}

// ContextReader optionally provides context-aware reads.
type ContextReader interface {
	ReadContext(context.Context, []byte) (int, error)
}

// ContextWriter optionally provides context-aware writes.
type ContextWriter interface {
	WriteContext(context.Context, []byte) (int, error)
}

// ContextSeeker optionally provides context-aware seeks.
type ContextSeeker interface {
	SeekContext(context.Context, int64, int) (int64, error)
}

// ContextOpener optionally provides context-aware opening of external column files.
type ContextOpener interface {
	OpenContext(context.Context, string) (ParquetFileReader, error)
}

// ContextCloner optionally provides context-aware reader cloning.
type ContextCloner interface {
	CloneContext(context.Context) (ParquetFileReader, error)
}

// ContextCreator optionally provides context-aware writer creation.
type ContextCreator interface {
	CreateContext(context.Context, string) (ParquetFileWriter, error)
}

// ContextCloser optionally provides context-aware closing.
type ContextCloser interface {
	CloseContext(context.Context) error
}

const bufferSize = 4096

// ConvertToThriftReader converts a file reader to a Thrift buffered transport.
// It seeks to the given offset before wrapping the reader.
//
// Deprecated: use ConvertToThriftReaderWithContext.
func ConvertToThriftReader(file ParquetFileReader, offset int64) (*thrift.TBufferedTransport, error) {
	return ConvertToThriftReaderWithContext(context.Background(), file, offset)
}

// ConvertToThriftReaderWithContext converts a file reader to a Thrift buffered
// transport and uses ctx for the initial seek and subsequent reads.
func ConvertToThriftReaderWithContext(ctx context.Context, file ParquetFileReader, offset int64) (*thrift.TBufferedTransport, error) {
	if file == nil {
		return nil, fmt.Errorf("file reader is nil")
	}
	if _, err := SeekWithContext(ctx, file, offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to offset %d: %w", offset, err)
	}
	thriftReader := thrift.NewStreamTransportR(ReaderWithContext{Ctx: ctx, Reader: file})
	bufferReader := thrift.NewTBufferedTransport(thriftReader, bufferSize)
	return bufferReader, nil
}

// ReaderWithContext wraps an io.Reader with a context.Context so that every
// Read checks ctx and calls the reader's ContextReader.ReadContext when
// available.
type ReaderWithContext struct {
	Ctx    context.Context
	Reader io.Reader
}

func (r ReaderWithContext) Read(p []byte) (int, error) {
	return ReadWithContext(r.Ctx, r.Reader, p)
}

// ReadSeekerWithContext wraps an io.ReadSeeker with a context.Context so that
// every Read and Seek checks ctx and calls the underlying type's context-aware
// methods when available.
type ReadSeekerWithContext struct {
	Ctx        context.Context
	ReadSeeker io.ReadSeeker
}

func (r ReadSeekerWithContext) Read(p []byte) (int, error) {
	return ReadWithContext(r.Ctx, r.ReadSeeker, p)
}

func (r ReadSeekerWithContext) Seek(offset int64, whence int) (int64, error) {
	return SeekWithContext(r.Ctx, r.ReadSeeker, offset, whence)
}

// ReadWithContext calls a reader's optional context-aware method, falling back
// to io.Reader after checking whether ctx is already done.
func ReadWithContext(ctx context.Context, reader io.Reader, p []byte) (int, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	if reader, ok := reader.(ContextReader); ok {
		return reader.ReadContext(ctx, p)
	}
	return reader.Read(p)
}

// ReadFullWithContext reads exactly len(p) bytes using ReadWithContext.
func ReadFullWithContext(ctx context.Context, reader io.Reader, p []byte) (int, error) {
	return io.ReadFull(ReaderWithContext{Ctx: ctx, Reader: reader}, p)
}

// WriteWithContext calls a writer's optional context-aware method, falling back
// to io.Writer after checking whether ctx is already done.
func WriteWithContext(ctx context.Context, writer io.Writer, p []byte) (int, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	if writer, ok := writer.(ContextWriter); ok {
		return writer.WriteContext(ctx, p)
	}
	return writer.Write(p)
}

// SeekWithContext calls a seeker's optional context-aware method, falling back
// to io.Seeker after checking whether ctx is already done.
func SeekWithContext(ctx context.Context, seeker io.Seeker, offset int64, whence int) (int64, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	if seeker, ok := seeker.(ContextSeeker); ok {
		return seeker.SeekContext(ctx, offset, whence)
	}
	return seeker.Seek(offset, whence)
}

// OpenWithContext calls a file reader's optional context-aware method, falling
// back to Open after checking whether ctx is already done.
func OpenWithContext(ctx context.Context, file ParquetFileReader, name string) (ParquetFileReader, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if file, ok := file.(ContextOpener); ok {
		return file.OpenContext(ctx, name)
	}
	return file.Open(name)
}

// CloneWithContext calls a file reader's optional context-aware method, falling
// back to Clone after checking whether ctx is already done.
func CloneWithContext(ctx context.Context, file ParquetFileReader) (ParquetFileReader, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if file, ok := file.(ContextCloner); ok {
		return file.CloneContext(ctx)
	}
	return file.Clone()
}

// CreateWithContext calls a file writer's optional context-aware method,
// falling back to Create after checking whether ctx is already done.
func CreateWithContext(ctx context.Context, file ParquetFileWriter, name string) (ParquetFileWriter, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if file, ok := file.(ContextCreator); ok {
		return file.CreateContext(ctx, name)
	}
	return file.Create(name)
}

// CloseWithContext calls a closer's optional context-aware method, falling back
// to io.Closer. Cancellation is never allowed to interrupt cleanup, but a
// cancellation that occurs during the close is still reported to the caller.
func CloseWithContext(ctx context.Context, closer io.Closer) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	var closeErr error
	if contextCloser, ok := closer.(ContextCloser); ok {
		closeErr = contextCloser.CloseContext(context.WithoutCancel(ctx))
	} else {
		closeErr = closer.Close()
	}
	return errors.Join(ctx.Err(), closeErr)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	return ctx.Err()
}
