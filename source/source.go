package source

import (
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
)

type ParquetFileReader interface {
	io.Seeker
	io.Reader
	io.Closer
	Open(name string) (ParquetFileReader, error)
	Clone() (ParquetFileReader, error)
}

// InPlaceReopener is an optional capability a ParquetFileReader may implement to
// declare that its Open method returns a reader sharing the receiver's
// underlying handle (for example, it reopens an internal handle and returns
// itself) rather than a fully independent reader. When a caller swaps in the
// reader returned by Open, it must not Close the previous handle if that handle
// reports ReopensInPlace() == true, because the previous and new readers are the
// same object; closing it would close the reader Open just returned.
//
// Readers whose Open yields an independent reader — the common case — should not
// implement this interface (or should return false); the caller owns the old
// handle and is responsible for closing it.
type InPlaceReopener interface {
	ReopensInPlace() bool
}

// ReopensInPlace reports whether r declares, via the InPlaceReopener capability,
// that Open returns a reader sharing its underlying handle. It is the safe way
// for callers to decide whether the previous handle must be closed after Open.
func ReopensInPlace(r ParquetFileReader) bool {
	o, ok := r.(InPlaceReopener)
	return ok && o.ReopensInPlace()
}

type ParquetFileWriter interface {
	io.Writer
	io.Closer
	Create(name string) (ParquetFileWriter, error)
}

const bufferSize = 4096

// ConvertToThriftReader converts a file reader to a Thrift buffered transport.
// It seeks to the given offset before wrapping the reader.
func ConvertToThriftReader(file ParquetFileReader, offset int64) (*thrift.TBufferedTransport, error) {
	if file == nil {
		return nil, fmt.Errorf("file reader is nil")
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to offset %d: %w", offset, err)
	}
	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, bufferSize)
	return bufferReader, nil
}
