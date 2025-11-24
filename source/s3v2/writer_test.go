package s3v2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source"
)

// Mock S3 Write Client
type mockS3WriteClient struct {
	uploads     map[string][]byte
	uploadError error
	callCounts  map[string]int
	mu          sync.Mutex
}

func newMockS3WriteClient() *mockS3WriteClient {
	return &mockS3WriteClient{
		uploads:    make(map[string][]byte),
		callCounts: make(map[string]int),
	}
}

func (m *mockS3WriteClient) setUploadError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadError = err
}

func (m *mockS3WriteClient) getUploadedData(bucket, key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.uploads[bucket+"/"+key]
}

func (m *mockS3WriteClient) PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["PutObject"]++

	if m.uploadError != nil {
		return nil, m.uploadError
	}

	// Read the body data
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	objKey := *input.Bucket + "/" + *input.Key
	m.uploads[objKey] = data

	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3WriteClient) UploadPart(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{}, nil
}

func (m *mockS3WriteClient) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{}, nil
}

func (m *mockS3WriteClient) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (m *mockS3WriteClient) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

// Test data
var testDataWriter = []byte("Hello, this is test data for S3v2 parquet file reader and writer testing with comprehensive coverage")

func TestNewS3FileWriterWithClient_Success(t *testing.T) {
	ctx := context.Background()
	client := newMockS3WriteClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	writer, err := NewS3FileWriterWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Test that it implements the interface
	var _ source.ParquetFileWriter = writer
}

func TestS3Writer_CloseWithoutWrite(t *testing.T) {
	ctx := context.Background()
	client := newMockS3WriteClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	writer, err := NewS3FileWriterWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Close without writing anything
	err = writer.Close()
	require.NoError(t, err)
}

func TestS3Writer_Create(t *testing.T) {
	ctx := context.Background()
	client := newMockS3WriteClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	writer, err := NewS3FileWriterWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Test Create method
	newWriter, err := writer.Create("new-file.parquet")
	require.NoError(t, err)
	require.NotNil(t, newWriter)

	// Verify it's a different instance
	require.NotEqual(t, writer, newWriter)
}

func TestS3Writer_UploadError(t *testing.T) {
	ctx := context.Background()
	client := newMockS3WriteClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	// Set upload error
	client.setUploadError(errors.New("upload failed"))

	writer, err := NewS3FileWriterWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Write some data
	_, err = writer.Write(testDataWriter)
	require.NoError(t, err)

	// Close should return the upload error
	err = writer.Close()
	require.Error(t, err)

	expectedError := "upload failed"
	require.Contains(t, err.Error(), expectedError)
}

func TestS3Writer_WithPutObjectInputOptions(t *testing.T) {
	ctx := context.Background()
	client := newMockS3WriteClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	// Create put object input options
	putObjectOptions := []func(*s3.PutObjectInput){
		func(input *s3.PutObjectInput) {
			input.ContentType = aws.String("application/octet-stream")
		},
	}

	writer, err := NewS3FileWriterWithClient(ctx, client, bucket, key, nil, putObjectOptions...)
	require.NoError(t, err)
	require.NotNil(t, writer)
}

func TestS3Writer_WithUploaderOptions(t *testing.T) {
	ctx := context.Background()
	client := newMockS3WriteClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	// Create uploader options
	uploaderOptions := []func(*manager.Uploader){
		func(u *manager.Uploader) {
			u.PartSize = 1024 * 1024 // 1MB parts
		},
	}

	writer, err := NewS3FileWriterWithClient(ctx, client, bucket, key, uploaderOptions)
	require.NoError(t, err)
	require.NotNil(t, writer)
}

func TestS3Writer_WriteAfterError(t *testing.T) {
	ctx := context.Background()
	client := newMockS3WriteClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	writer, err := NewS3FileWriterWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Simulate error by setting upload error after creation
	client.setUploadError(errors.New("upload failed"))

	// First write should succeed (error happens in background)
	_, err = writer.Write(testDataWriter[:10])
	require.NoError(t, err)

	// Give background goroutine time to detect error
	time.Sleep(10 * time.Millisecond)

	// Second write should fail due to background error
	_, err = writer.Write(testDataWriter[10:])
	if err == nil {
		// If no immediate error, close should still return error
		err = writer.Close()
		require.Error(t, err)
	}
}

func TestS3Writer_Write_Multiple(t *testing.T) {
	ctx := context.Background()
	client := newMockS3WriteClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	writer, err := NewS3FileWriterWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Write in multiple chunks
	chunk1 := testDataWriter[:30]
	chunk2 := testDataWriter[30:]

	n1, err := writer.Write(chunk1)
	require.NoError(t, err)
	require.Equal(t, len(chunk1), n1)

	n2, err := writer.Write(chunk2)
	require.NoError(t, err)
	require.Equal(t, len(chunk2), n2)

	// Close to complete upload
	err = writer.Close()
	require.NoError(t, err)

	// Verify combined data was uploaded
	uploadedData := client.getUploadedData(bucket, key)
	require.True(t, bytes.Equal(uploadedData, testDataWriter))
}

func TestS3Writer_Write_Success(t *testing.T) {
	ctx := context.Background()
	client := newMockS3WriteClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	writer, err := NewS3FileWriterWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Write test data
	n, err := writer.Write(testDataWriter)
	require.NoError(t, err)

	require.Equal(t, len(testDataWriter), n)

	// Close to complete upload
	err = writer.Close()
	require.NoError(t, err)

	// Verify data was uploaded
	uploadedData := client.getUploadedData(bucket, key)
	require.True(t, bytes.Equal(uploadedData, testDataWriter))
}
