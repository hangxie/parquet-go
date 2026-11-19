package hdfs

import (
	"io"
	"os"

	"github.com/colinmarc/hdfs/v2"
)

// hdfsFileReaderIface abstracts *hdfs.FileReader to allow testing.
type hdfsFileReaderIface interface {
	io.ReadSeeker
	Close() error
}

// hdfsFileWriterIface abstracts *hdfs.FileWriter to allow testing.
type hdfsFileWriterIface interface {
	Write(b []byte) (n int, err error)
	Close() error
}

// hdfsClientIface abstracts the HDFS client to allow testing.
type hdfsClientIface interface {
	Stat(name string) (os.FileInfo, error)
	Open(name string) (hdfsFileReaderIface, error)
	Create(name string) (hdfsFileWriterIface, error)
	Close() error
}

// realHdfsClient wraps *hdfs.Client, adapting its concrete return types to satisfy hdfsClientIface.
type realHdfsClient struct {
	*hdfs.Client
}

func (c *realHdfsClient) Open(name string) (hdfsFileReaderIface, error) {
	return c.Client.Open(name)
}

func (c *realHdfsClient) Create(name string) (hdfsFileWriterIface, error) {
	return c.Client.Create(name)
}
