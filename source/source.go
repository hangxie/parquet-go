package source

import (
	"io"

	"github.com/apache/thrift/lib/go/thrift"
)

type ParquetFile interface {
	io.Seeker
	io.Reader
	io.Writer
	io.Closer
	Open(name string) (ParquetFile, error)
	Create(name string) (ParquetFile, error)
}

const bufferSize = 4096

// Convert a file reater to Thrift reader
func ConvertToThriftReader(file ParquetFile, offset int64) *thrift.TBufferedTransport {
	if _, err := file.Seek(offset, 0); err != nil {
		return nil
	}
	thriftReader := thrift.NewStreamTransportR(file)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, bufferSize)
	return bufferReader
}
