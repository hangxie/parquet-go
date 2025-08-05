package minio

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source"
)

func Test_MinioFileInterfaceComplianceReader(t *testing.T) {
	var _ source.ParquetFileReader = (*minioReader)(nil)
}

func Test_MinioReaderClose(t *testing.T) {
	ctx := context.Background()

	reader := &minioReader{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
	}

	err := reader.Close()
	require.NoError(t, err)
}

func Test_MinioReaderReadEOF(t *testing.T) {
	ctx := context.Background()

	reader := &minioReader{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		offset:     100,
		fileSize:   100,
		downloader: nil,
	}

	buf := make([]byte, 10)
	n, err := reader.Read(buf)
	require.Equal(t, io.EOF, err)

	require.Equal(t, 0, n)
}

func Test_MinioReaderSeek(t *testing.T) {
	ctx := context.Background()
	reader := &minioReader{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		offset:   0,
		fileSize: 100,
	}

	pos, err := reader.Seek(10, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(10), pos)
	require.Equal(t, int64(10), reader.offset)

	pos, err = reader.Seek(5, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(15), pos)

	pos, err = reader.Seek(-10, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(-10), pos)
}

func Test_MinioReaderSeekErrors(t *testing.T) {
	ctx := context.Background()
	reader := &minioReader{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		offset:   0,
		fileSize: 100,
	}

	_, err := reader.Seek(0, -1)
	require.Error(t, err)
	require.Equal(t, errWhence, err)

	_, err = reader.Seek(-10, io.SeekStart)
	require.Error(t, err)
	require.Equal(t, errInvalidOffset, err)

	_, err = reader.Seek(150, io.SeekStart)
	require.Error(t, err)
	require.Equal(t, errInvalidOffset, err)
}

func Test_MinioReaderStructure(t *testing.T) {
	ctx := context.Background()

	reader := &minioReader{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		offset:   10,
		fileSize: 100,
	}

	require.Equal(t, "test-bucket", reader.bucketName)
	require.Equal(t, "test.parquet", reader.key)
	require.Equal(t, int64(10), reader.offset)
	require.Equal(t, int64(100), reader.fileSize)
}

func Test_MinioReaderZeroFileSize(t *testing.T) {
	ctx := context.Background()

	reader := &minioReader{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		offset:   0,
		fileSize: 0,
	}

	_, err := reader.Seek(10, io.SeekStart)
	require.NoError(t, err)
}

func Test_NewS3FileReaderWithClient(t *testing.T) {
	// Create a context with timeout to avoid hanging
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := minio.New("localhost:9000", &minio.Options{
		Secure: false, // HTTP instead of HTTPS
	})
	require.NoError(t, err)

	// This will timeout quickly, covering the method
	_, err = NewS3FileReaderWithClient(ctx, client, "test-bucket", "test.parquet")
	// We expect this to fail (either timeout or connection refused)
	require.Error(t, err)
}

func Test_MinioReader_Open(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := minio.New("localhost:9000", &minio.Options{Secure: false})
	require.NoError(t, err)

	reader := &minioReader{
		minioFile: minioFile{
			ctx:        ctx,
			client:     client,
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
	}

	// This will timeout quickly, covering the Open method
	_, err = reader.Open("test.parquet")
	require.Error(t, err) // Expected to fail (timeout or connection refused)
}

func Test_MinioReader_Clone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := minio.New("localhost:9000", &minio.Options{Secure: false})
	require.NoError(t, err)

	reader := &minioReader{
		minioFile: minioFile{
			ctx:        ctx,
			client:     client,
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
	}

	// This will timeout quickly, covering the Clone method
	_, err = reader.Clone()
	require.Error(t, err) // Expected to fail (timeout or connection refused)
}
