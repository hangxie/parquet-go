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
