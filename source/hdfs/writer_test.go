package hdfs

import (
	"testing"

	"github.com/colinmarc/hdfs/v2"
	"github.com/stretchr/testify/require"
)

func TestHdfsWriterStructure(t *testing.T) {
	writer := &hdfsWriter{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   &hdfs.Client{},
		},
		fileWriter: nil,
	}

	require.Equal(t, []string{"localhost:9000"}, writer.hosts)
	require.Equal(t, "test-user", writer.user)
	require.Equal(t, "test.parquet", writer.filePath)
	require.Nil(t, writer.fileWriter)
}

func TestHdfsWriterClose(t *testing.T) {
	writer := &hdfsWriter{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
		},
		fileWriter: nil,
	}

	err := writer.Close()
	require.NoError(t, err)
}

func TestNewHdfsFileWriterError(t *testing.T) {
	_, err := NewHdfsFileWriter([]string{"nonexistent:9000"}, "test-user", "test.parquet")
	require.Error(t, err)
}

func TestHdfsWriterWriteDelegation(t *testing.T) {
	writer := &hdfsWriter{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
		},
		fileWriter: nil,
	}

	testData := []byte("test")
	_, err := writer.Write(testData)
	require.Error(t, err)
}

func TestHdfsWriter_Create(t *testing.T) {
	writer := &hdfsWriter{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
		},
		fileWriter: nil,
	}

	_, err := writer.Create("test.parquet")
	require.Error(t, err)
}
