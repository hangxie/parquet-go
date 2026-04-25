package hdfs

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

// mockFileReader implements hdfsFileReaderIface for testing.
type mockFileReader struct {
	data     []byte
	seekPos  int64
	readErr  error
	seekErr  error
	closeErr error
}

func (m *mockFileReader) Read(b []byte) (int, error) {
	if m.readErr != nil {
		return 0, m.readErr
	}
	n := copy(b, m.data)
	m.data = m.data[n:]
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (m *mockFileReader) Seek(offset int64, _ int) (int64, error) {
	if m.seekErr != nil {
		return 0, m.seekErr
	}
	m.seekPos = offset
	return m.seekPos, nil
}

func (m *mockFileReader) Close() error { return m.closeErr }

// mockFileWriter implements hdfsFileWriterIface for testing.
type mockFileWriter struct {
	written  []byte
	writeErr error
	closeErr error
}

func (m *mockFileWriter) Write(b []byte) (int, error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	m.written = append(m.written, b...)
	return len(b), nil
}

func (m *mockFileWriter) Close() error { return m.closeErr }

// mockHdfsClient implements hdfsClientIface for testing.
type mockHdfsClient struct {
	openResult   hdfsFileReaderIface
	openErr      error
	createResult hdfsFileWriterIface
	createErr    error
	closeErr     error
}

func (m *mockHdfsClient) Open(_ string) (hdfsFileReaderIface, error) {
	return m.openResult, m.openErr
}

func (m *mockHdfsClient) Create(_ string) (hdfsFileWriterIface, error) {
	return m.createResult, m.createErr
}

func (m *mockHdfsClient) Close() error { return m.closeErr }

func TestHdfsFileInterfaceCompliance(t *testing.T) {
	var _ source.ParquetFileReader = (*hdfsReader)(nil)
	var _ source.ParquetFileWriter = (*hdfsWriter)(nil)
}

func TestHdfsFileStructure(t *testing.T) {
	file := hdfsFile{
		hosts:    []string{"localhost:9000"},
		user:     "test-user",
		filePath: "test.parquet",
		client:   nil,
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
			client:   nil,
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
	require.Contains(t, err.Error(), "no available namenodes")
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
	require.Contains(t, err.Error(), "fileReader is nil")
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
	require.Contains(t, err.Error(), "fileReader is nil")
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
	require.Contains(t, err.Error(), "client is nil")
}

func TestHdfsReader_Clone(t *testing.T) {
	t.Run("clone_with_nil_client_returns_error", func(t *testing.T) {
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
		require.Contains(t, err.Error(), "client is nil")
	})

	t.Run("clone_preserves_reader_metadata", func(t *testing.T) {
		reader := &hdfsReader{
			hdfsFile: hdfsFile{
				hosts:    []string{"localhost:9000", "localhost:9001"},
				user:     "production-user",
				filePath: "data/table.parquet",
				client:   nil,
			},
			fileReader: nil,
		}

		_, err := reader.Clone()
		require.Error(t, err)
		require.Contains(t, err.Error(), "client is nil")
	})
}

func TestHdfsReader_OpenWithMockClient(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		reader := &hdfsReader{
			hdfsFile: hdfsFile{
				client: &mockHdfsClient{openResult: &mockFileReader{}},
			},
		}

		result, err := reader.Open("test.parquet")
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("open_error", func(t *testing.T) {
		reader := &hdfsReader{
			hdfsFile: hdfsFile{
				client: &mockHdfsClient{openErr: errors.New("connection refused")},
			},
		}

		_, err := reader.Open("test.parquet")
		require.Error(t, err)
		require.Contains(t, err.Error(), "connection refused")
	})
}

func TestHdfsReader_CloneWithMockClient(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		reader := &hdfsReader{
			hdfsFile: hdfsFile{
				hosts:    []string{"localhost:9000"},
				user:     "test-user",
				filePath: "test.parquet",
				client:   &mockHdfsClient{openResult: &mockFileReader{}},
			},
			fileReader: &mockFileReader{},
		}

		clone, err := reader.Clone()
		require.NoError(t, err)
		require.NotNil(t, clone)
	})

	t.Run("open_error", func(t *testing.T) {
		reader := &hdfsReader{
			hdfsFile: hdfsFile{
				client: &mockHdfsClient{openErr: errors.New("clone open error")},
			},
		}

		_, err := reader.Clone()
		require.Error(t, err)
		require.Contains(t, err.Error(), "clone open error")
	})
}

func TestHdfsReader_ReadWithMockFileReader(t *testing.T) {
	data := []byte("hello parquet")
	reader := &hdfsReader{
		hdfsFile:   hdfsFile{},
		fileReader: &mockFileReader{data: data},
	}

	buf := make([]byte, len(data))
	n, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)
}

func TestHdfsReader_SeekWithMockFileReader(t *testing.T) {
	reader := &hdfsReader{
		hdfsFile:   hdfsFile{},
		fileReader: &mockFileReader{seekPos: 42},
	}

	pos, err := reader.Seek(42, 0)
	require.NoError(t, err)
	require.Equal(t, int64(42), pos)
}

func TestHdfsReader_CloseWithMockComponents(t *testing.T) {
	t.Run("file_reader_close_error", func(t *testing.T) {
		reader := &hdfsReader{
			hdfsFile:   hdfsFile{client: nil},
			fileReader: &mockFileReader{closeErr: errors.New("reader close error")},
		}
		err := reader.Close()
		require.Error(t, err)
		require.Contains(t, err.Error(), "reader close error")
	})

	t.Run("client_close_error", func(t *testing.T) {
		reader := &hdfsReader{
			hdfsFile:   hdfsFile{client: &mockHdfsClient{closeErr: errors.New("client close error")}},
			fileReader: &mockFileReader{},
		}
		err := reader.Close()
		require.Error(t, err)
		require.Contains(t, err.Error(), "client close error")
	})

	t.Run("success", func(t *testing.T) {
		reader := &hdfsReader{
			hdfsFile:   hdfsFile{client: &mockHdfsClient{}},
			fileReader: &mockFileReader{},
		}
		err := reader.Close()
		require.NoError(t, err)
	})
}
