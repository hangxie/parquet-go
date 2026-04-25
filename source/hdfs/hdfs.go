package hdfs

import (
	"github.com/colinmarc/hdfs/v2"
)

// hdfsFileReaderIface abstracts *hdfs.FileReader to allow testing.
type hdfsFileReaderIface interface {
	Read(b []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
	Close() error
}

// hdfsFileWriterIface abstracts *hdfs.FileWriter to allow testing.
type hdfsFileWriterIface interface {
	Write(b []byte) (n int, err error)
	Close() error
}

// hdfsClientIface abstracts the HDFS client to allow testing.
type hdfsClientIface interface {
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

type hdfsFile struct {
	hosts    []string
	user     string
	filePath string
	client   hdfsClientIface
}
