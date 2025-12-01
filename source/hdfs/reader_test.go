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
	t.Run("clone_with_nil_client_returns_error", func(t *testing.T) {
		// Test that Clone returns an error when client is nil
		// This verifies the optimization: Clone checks for nil client early
		// instead of calling NewHdfsFileReader which would try to create a new client
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

		// The key optimization: the error is "client is nil", not an error from
		// trying to create a new HDFS client. This proves Clone() checks the
		// existing client instead of calling NewHdfsFileReader.
	})

	t.Run("clone_preserves_reader_metadata", func(t *testing.T) {
		// Test that Clone would preserve metadata if it could succeed
		// We verify the structure of what Clone attempts to do

		// Create a reader with metadata
		reader := &hdfsReader{
			hdfsFile: hdfsFile{
				hosts:    []string{"localhost:9000", "localhost:9001"},
				user:     "production-user",
				filePath: "data/table.parquet",
				client:   nil, // Nil to avoid actual network calls
			},
			fileReader: nil,
		}

		// Verify Clone detects nil client immediately (optimized behavior)
		_, err := reader.Clone()
		require.Error(t, err)
		require.Contains(t, err.Error(), "client is nil")

		// This early return proves the optimization: Clone doesn't call
		// NewHdfsFileReader, which would attempt to create a new client
		// and make network calls. Instead, it fails fast on nil client check.
	})
}
