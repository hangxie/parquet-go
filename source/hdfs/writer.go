package hdfs

import (
	"fmt"

	"github.com/colinmarc/hdfs/v2"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *hdfsFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*hdfsWriter)(nil)

type hdfsWriter struct {
	hdfsFile
	fileWriter *hdfs.FileWriter
}

func NewHdfsFileWriter(hosts []string, user, name string) (source.ParquetFileWriter, error) {
	res := &hdfsWriter{
		hdfsFile: hdfsFile{
			hosts:    hosts,
			user:     user,
			filePath: name,
		},
	}

	var err error
	res.client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hosts,
		User:      user,
	})
	if err != nil {
		return nil, fmt.Errorf("new hdfs client: %w", err)
	}
	w, err := res.Create(name)
	if err != nil {
		return nil, fmt.Errorf("create hdfs file: %w", err)
	}
	return w, nil
}

func (f *hdfsWriter) Create(name string) (source.ParquetFileWriter, error) {
	if f.client == nil {
		return nil, fmt.Errorf("client is nil")
	}
	var err error
	f.fileWriter, err = f.client.Create(name)
	if err != nil {
		return nil, fmt.Errorf("create hdfs file: %w", err)
	}
	return f, nil
}

func (f *hdfsWriter) Write(b []byte) (n int, err error) {
	if f.fileWriter == nil {
		return 0, fmt.Errorf("fileWriter is nil")
	}
	return f.fileWriter.Write(b)
}

func (f *hdfsWriter) Close() error {
	if f.fileWriter != nil {
		if err := f.fileWriter.Close(); err != nil {
			return fmt.Errorf("close HDFS file writer: %w", err)
		}
	}
	if f.client != nil {
		if err := f.client.Close(); err != nil {
			return fmt.Errorf("close HDFS client: %w", err)
		}
	}
	return nil
}
