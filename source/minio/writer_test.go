package minio

import (
	"context"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source"
)

func Test_MinioErrorConstants(t *testing.T) {
	require.Equal(t, "Seek: invalid whence", errWhence.Error())
	require.Equal(t, "Seek: invalid offset", errInvalidOffset.Error())
}

func Test_MinioFileInterfaceComplianceWriter(t *testing.T) {
	var _ source.ParquetFileWriter = (*minioWriter)(nil)
}

func Test_MinioFileStructure(t *testing.T) {
	ctx := context.Background()

	file := minioFile{
		ctx:        ctx,
		client:     &minio.Client{},
		bucketName: "test-bucket",
		key:        "test.parquet",
	}

	require.Equal(t, "test-bucket", file.bucketName)
	require.Equal(t, "test.parquet", file.key)
}

func Test_MinioWriterClose(t *testing.T) {
	ctx := context.Background()

	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		pipeReader: pr,
		pipeWriter: pw,
	}

	err := writer.Close()
	require.NoError(t, err)

	writer.pipeWriter = nil
	err = writer.Close()
	require.NoError(t, err)
}

func Test_MinioWriterCloseWithNilPipeWriter(t *testing.T) {
	ctx := context.Background()

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		pipeWriter: nil,
	}

	err := writer.Close()
	require.NoError(t, err)
}

func Test_MinioWriterCloseWithValidPipe(t *testing.T) {
	ctx := context.Background()

	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		pipeReader: pr,
		pipeWriter: pw,
	}

	err := writer.Close()
	require.NoError(t, err)
}

func Test_MinioWriterCreateFunctionality(t *testing.T) {
	ctx := context.Background()
	client := &minio.Client{}

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     client,
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
	}

	require.Panics(t, func() {
		_, _ = writer.Create("new-file.parquet")
	})
}

func Test_MinioWriterErrorField(t *testing.T) {
	ctx := context.Background()

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
	}

	// Initially no error
	require.Nil(t, writer.err)

	// Test setting error through write failure
	pr, pw := io.Pipe()
	expectedErr := io.ErrClosedPipe
	pw.CloseWithError(expectedErr)

	writer.pipeReader = pr
	writer.pipeWriter = pw

	// This should set the error field
	_, err := writer.Write([]byte("test"))
	require.Error(t, err)

	// Check that error field was set
	require.NotNil(t, writer.err)

	_ = pr.Close()
}

func Test_MinioWriterMultipleWrites(t *testing.T) {
	ctx := context.Background()

	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()
	defer func() { _ = pw.Close() }()

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		pipeReader: pr,
		pipeWriter: pw,
	}

	// Start a goroutine to read from the pipe
	readComplete := make(chan []byte)
	go func() {
		var allData []byte
		buf := make([]byte, 1024)
		for {
			n, err := pr.Read(buf)
			if n > 0 {
				allData = append(allData, buf[:n]...)
			}
			if err != nil {
				break
			}
		}
		readComplete <- allData
	}()

	// Write multiple chunks
	chunks := [][]byte{
		[]byte("chunk1"),
		[]byte("chunk2"),
		[]byte("chunk3"),
	}

	totalExpected := 0
	for _, chunk := range chunks {
		n, err := writer.Write(chunk)
		require.NoError(t, err)
		require.Equal(t, len(chunk), n)
		totalExpected += len(chunk)
	}

	// Close to signal EOF
	_ = pw.Close()

	// Check all data was received
	allReadData := <-readComplete
	expectedData := "chunk1chunk2chunk3"

	require.Equal(t, expectedData, string(allReadData))

	require.Equal(t, totalExpected, len(allReadData))
}

func Test_MinioWriterStructFieldAccess(t *testing.T) {
	ctx := context.Background()
	client := &minio.Client{}

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     client,
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
	}

	// Test access to embedded fields
	require.Equal(t, ctx, writer.ctx)
	require.Equal(t, client, writer.client)
	require.Equal(t, "test-bucket", writer.bucketName)
	require.Equal(t, "test.parquet", writer.key)
}

func Test_MinioWriterStructure(t *testing.T) {
	ctx := context.Background()

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
	}

	require.Equal(t, "test-bucket", writer.bucketName)
	require.Equal(t, "test.parquet", writer.key)
}

func Test_MinioWriterWriteErrorHandling(t *testing.T) {
	ctx := context.Background()

	pr, pw := io.Pipe()

	// Close with error to simulate write failure
	expectedErr := io.ErrClosedPipe
	pw.CloseWithError(expectedErr)

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		pipeReader: pr,
		pipeWriter: pw,
	}

	testData := []byte("test data")
	n, err := writer.Write(testData)

	require.Error(t, err)

	require.Equal(t, 0, n)

	require.NotNil(t, writer.err)

	_ = pr.Close()
}

func Test_MinioWriterWriteWithClosedPipe(t *testing.T) {
	ctx := context.Background()

	pr, pw := io.Pipe()
	_ = pr.Close()
	_ = pw.Close()

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		pipeReader: pr,
		pipeWriter: pw,
	}

	testData := []byte("test data")
	_, err := writer.Write(testData)
	require.Error(t, err)

	// Verify that error is stored in writer
	require.NotNil(t, writer.err)
}

func Test_MinioWriterWriteWithValidPipe(t *testing.T) {
	ctx := context.Background()

	// Create a proper pipe for testing
	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()
	defer func() { _ = pw.Close() }()

	writer := &minioWriter{
		minioFile: minioFile{
			ctx:        ctx,
			client:     &minio.Client{},
			bucketName: "test-bucket",
			key:        "test.parquet",
		},
		pipeReader: pr,
		pipeWriter: pw,
	}

	testData := []byte("test data to write")

	// Start a goroutine to read from the pipe to prevent blocking
	readComplete := make(chan bool)
	var readData []byte
	go func() {
		defer close(readComplete)
		buf := make([]byte, len(testData))
		n, err := pr.Read(buf)
		if err != nil && err != io.EOF {
			require.NoError(t, err)
			return
		}
		readData = buf[:n]
	}()

	// Test Write method
	n, err := writer.Write(testData)
	require.NoError(t, err)

	require.Equal(t, len(testData), n)

	// Close the writer to signal EOF
	_ = pw.Close()

	// Wait for read to complete
	<-readComplete

	require.Equal(t, string(testData), string(readData))
}

func Test_NewS3FileWriterWithClientFunctionality(t *testing.T) {
	ctx := context.Background()
	client := &minio.Client{}

	require.Panics(t, func() {
		_, _ = NewS3FileWriterWithClient(ctx, client, "test-bucket", "test.parquet")
	})
}
