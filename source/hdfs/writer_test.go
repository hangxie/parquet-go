package hdfs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHdfsWriterStructure(t *testing.T) {
	writer := &hdfsWriter{
		hdfsFile: hdfsFile{
			hosts:    []string{"localhost:9000"},
			user:     "test-user",
			filePath: "test.parquet",
			client:   nil,
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
	require.Contains(t, err.Error(), "no available namenodes")
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
	require.Contains(t, err.Error(), "fileWriter is nil")
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
	require.Contains(t, err.Error(), "client is nil")
}

func TestHdfsWriter_CreateWithMockClient(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		writer := &hdfsWriter{
			hdfsFile: hdfsFile{
				client: &mockHdfsClient{createResult: &mockFileWriter{}},
			},
		}

		result, err := writer.Create("test.parquet")
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("create_error", func(t *testing.T) {
		writer := &hdfsWriter{
			hdfsFile: hdfsFile{
				client: &mockHdfsClient{createErr: errors.New("create failed")},
			},
		}

		_, err := writer.Create("test.parquet")
		require.Error(t, err)
		require.Contains(t, err.Error(), "create failed")
	})
}

func TestHdfsWriter_WriteWithMockFileWriter(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		data := []byte("hello parquet")
		writer := &hdfsWriter{
			hdfsFile:   hdfsFile{},
			fileWriter: &mockFileWriter{},
		}

		n, err := writer.Write(data)
		require.NoError(t, err)
		require.Equal(t, len(data), n)
	})

	t.Run("write_error", func(t *testing.T) {
		writer := &hdfsWriter{
			hdfsFile:   hdfsFile{},
			fileWriter: &mockFileWriter{writeErr: errors.New("write failed")},
		}

		_, err := writer.Write([]byte("data"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "write failed")
	})
}

func TestHdfsWriter_CloseWithMockComponents(t *testing.T) {
	t.Run("file_writer_close_error", func(t *testing.T) {
		writer := &hdfsWriter{
			hdfsFile:   hdfsFile{client: nil},
			fileWriter: &mockFileWriter{closeErr: errors.New("writer close error")},
		}
		err := writer.Close()
		require.Error(t, err)
		require.Contains(t, err.Error(), "writer close error")
	})

	t.Run("client_close_error", func(t *testing.T) {
		writer := &hdfsWriter{
			hdfsFile:   hdfsFile{client: &mockHdfsClient{closeErr: errors.New("client close error")}},
			fileWriter: &mockFileWriter{},
		}
		err := writer.Close()
		require.Error(t, err)
		require.Contains(t, err.Error(), "client close error")
	})

	t.Run("success", func(t *testing.T) {
		writer := &hdfsWriter{
			hdfsFile:   hdfsFile{client: &mockHdfsClient{}},
			fileWriter: &mockFileWriter{},
		}
		err := writer.Close()
		require.NoError(t, err)
	})
}
