package hdfs

import (
	"testing"

	"github.com/colinmarc/hdfs/v2"
	"github.com/stretchr/testify/require"
)

func Test_HdfsWriterStructure(t *testing.T) {
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

func Test_HdfsWriterClose(t *testing.T) {
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

func Test_NewHdfsFileWriterError(t *testing.T) {
	_, err := NewHdfsFileWriter([]string{"nonexistent:9000"}, "test-user", "test.parquet")
	require.Error(t, err)
}

func Test_HdfsWriterWriteDelegation(t *testing.T) {
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
	require.Panics(t, func() {
		_, _ = writer.Write(testData)
	})
}

func Test_HdfsWriter_Create(t *testing.T) {
	writer := &hdfsWriter{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
		},
		fileWriter: nil,
	}

	require.Panics(t, func() {
		_, _ = writer.Create("test.parquet")
	})
}
