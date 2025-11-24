package hdfs

import (
	"testing"

	"github.com/colinmarc/hdfs/v2"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source"
)

func TestHdfsFileInterfaceCompliance(t *testing.T) {
	var _ source.ParquetFileReader = (*hdfsReader)(nil)
	var _ source.ParquetFileWriter = (*hdfsWriter)(nil)
}

func TestHdfsFileStructure(t *testing.T) {
	file := hdfsFile{
		hosts:    []string{"localhost:9000"},
		user:     "test-user",
		filePath: "test.parquet",
		client:   &hdfs.Client{},
	}

	require.Equal(t, []string{"localhost:9000"}, file.hosts)
	require.Equal(t, "test-user", file.user)
	require.Equal(t, "test.parquet", file.filePath)
}

func TestHdfsReaderStructure(t *testing.T) {
	reader := &hdfsReader{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   &hdfs.Client{},
		},
		fileReader: nil,
	}

	require.Equal(t, []string{"localhost:9000"}, reader.hosts)
	require.Equal(t, "test-user", reader.user)
	require.Equal(t, "test.parquet", reader.filePath)
	require.Nil(t, reader.fileReader)
}

func TestHdfsReaderClose(t *testing.T) {
	reader := &hdfsReader{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
		},
		fileReader: nil,
	}

	err := reader.Close()
	require.NoError(t, err)
}

func TestNewHdfsFileReaderError(t *testing.T) {
	_, err := NewHdfsFileReader([]string{"nonexistent:9000"}, "test-user", "test.parquet")
	require.Error(t, err)
}

func TestHdfsReaderMethodDelegation(t *testing.T) {
	reader := &hdfsReader{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
		},
		fileReader: nil,
	}

	buf := make([]byte, 10)
	_, err := reader.Read(buf)
	require.Error(t, err)
}

func TestHdfsReaderSeekDelegation(t *testing.T) {
	reader := &hdfsReader{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
		},
		fileReader: nil,
	}

	_, err := reader.Seek(0, 0)
	require.Error(t, err)
}

func TestHdfsReader_Open(t *testing.T) {
	reader := &hdfsReader{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
		},
		fileReader: nil,
	}

	_, err := reader.Open("test.parquet")
	require.Error(t, err)
}

func TestHdfsReader_Clone(t *testing.T) {
	reader := &hdfsReader{
		hdfsFile: hdfsFile{
			hosts:    []string{"nonexistent:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
		},
		fileReader: nil,
	}

	_, err := reader.Clone()
	require.Error(t, err)
}
