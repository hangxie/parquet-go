package s3v2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source"
)

// Mock S3 Read Client
type mockS3ReadClient struct {
	objects       map[string][]byte
	objectHeaders map[string]s3.HeadObjectOutput
	getObjectErr  error
	headObjectErr error
	callCounts    map[string]int
	mu            sync.Mutex
}

func newMockS3ReadClient() *mockS3ReadClient {
	return &mockS3ReadClient{
		objects:       make(map[string][]byte),
		objectHeaders: make(map[string]s3.HeadObjectOutput),
		callCounts:    make(map[string]int),
	}
}

func (m *mockS3ReadClient) setObject(bucket, key string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	objKey := bucket + "/" + key
	m.objects[objKey] = data
	contentLength := int64(len(data))
	m.objectHeaders[objKey] = s3.HeadObjectOutput{
		ContentLength: &contentLength,
	}
}

func (m *mockS3ReadClient) setGetObjectError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getObjectErr = err
}

func (m *mockS3ReadClient) setHeadObjectError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.headObjectErr = err
}

func (m *mockS3ReadClient) GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["GetObject"]++

	if m.getObjectErr != nil {
		return nil, m.getObjectErr
	}

	objKey := *input.Bucket + "/" + *input.Key
	data, exists := m.objects[objKey]
	if !exists {
		return nil, errors.New("object not found")
	}

	// Handle range requests
	var responseData []byte
	if input.Range != nil {
		rangeStr := *input.Range
		// Parse range header like "bytes=0-99" or "bytes=100"
		if strings.HasPrefix(rangeStr, "bytes=") {
			rangeStr = strings.TrimPrefix(rangeStr, "bytes=")
			parts := strings.Split(rangeStr, "-")

			if len(parts) == 2 {
				// Range like "0-99"
				var start, end int
				if parts[0] != "" {
					start = parseInt(parts[0])
				}
				if parts[1] != "" {
					end = parseInt(parts[1])
				} else {
					end = len(data) - 1
				}

				if start >= 0 && start < len(data) && end >= start && end < len(data) {
					responseData = data[start : end+1]
				}
			} else if len(parts) == 1 {
				// Range like "100" (suffix)
				start := parseInt(parts[0])
				if start >= 0 && start < len(data) {
					responseData = data[start:]
				}
			}
		}
	} else {
		responseData = data
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(responseData)),
	}, nil
}

func (m *mockS3ReadClient) HeadObject(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callCounts["HeadObject"]++

	if m.headObjectErr != nil {
		return nil, m.headObjectErr
	}

	objKey := *input.Bucket + "/" + *input.Key
	header, exists := m.objectHeaders[objKey]
	if !exists {
		return nil, errors.New("object not found")
	}

	return &header, nil
}

// Helper function to parse integer from string (simplified)
func parseInt(s string) int {
	result := 0
	for _, char := range s {
		if char >= '0' && char <= '9' {
			result = result*10 + int(char-'0')
		}
	}
	return result
}

// Test data
var testData = []byte("Hello, this is test data for S3v2 parquet file reader and writer testing with comprehensive coverage")

func Test_GetBytesRange_KnownFileSize(t *testing.T) {
	reader := &s3Reader{
		offset:   10,
		fileSize: 1000,
		whence:   io.SeekStart,
	}

	rangeStr := reader.getBytesRange(100)
	expected := "bytes=10-109"
	require.Equal(t, expected, rangeStr)

	// Test range beyond file size
	reader.offset = 950
	rangeStr = reader.getBytesRange(100)
	expected = "bytes=950-999" // Should clamp to file size
	require.Equal(t, expected, rangeStr)
}

func Test_GetBytesRange_NegativeBegin(t *testing.T) {
	reader := &s3Reader{
		offset:   -10,
		fileSize: 1000,
		whence:   io.SeekEnd,
	}

	rangeStr := reader.getBytesRange(100)
	expected := "bytes=990-999"
	require.Equal(t, expected, rangeStr)
}

func Test_GetBytesRange_SeekEnd(t *testing.T) {
	reader := &s3Reader{
		offset:   -50,
		fileSize: 1000,
		whence:   io.SeekEnd,
	}

	rangeStr := reader.getBytesRange(100)
	expected := "bytes=950-999" // fileSize + offset = 1000 + (-50) = 950
	require.Equal(t, expected, rangeStr)
}

func Test_GetBytesRange_UnknownFileSize(t *testing.T) {
	reader := &s3Reader{
		offset:   10,
		fileSize: 0, // Unknown file size
		whence:   io.SeekStart,
	}

	rangeStr := reader.getBytesRange(100)
	expected := "bytes=10-109"
	require.Equal(t, expected, rangeStr)

	// Test SeekEnd with unknown file size
	reader.whence = io.SeekEnd
	reader.offset = -50
	rangeStr = reader.getBytesRange(100)
	expected = "bytes=-50"
	require.Equal(t, expected, rangeStr)
}

func Test_NewS3FileReaderWithClient_Success(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)
	require.NotNil(t, reader)

	// Test that it implements the interface
	var _ source.ParquetFileReader = reader
}

func Test_NewS3FileReaderWithClient_WithVersion(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"
	version := "version123"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, &version)
	require.NoError(t, err)
	require.NotNil(t, reader)
}

func Test_NewS3FileReaderWithParams_DefaultMinRequestSize(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	params := S3FileReaderParams{
		Bucket:   bucket,
		Key:      key,
		S3Client: client,
		// MinRequestSize not set, should use default
	}

	reader, err := NewS3FileReaderWithParams(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, reader)
}

func Test_NewS3FileReaderWithParams_Success(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	params := S3FileReaderParams{
		Bucket:         bucket,
		Key:            key,
		S3Client:       client,
		MinRequestSize: 1024,
	}

	reader, err := NewS3FileReaderWithParams(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, reader)
}

func Test_S3Reader_Clone(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Test Clone
	clonedReader, err := reader.Clone()
	require.NoError(t, err)
	require.NotNil(t, clonedReader)

	// Verify clone is independent
	require.NotSame(t, reader, clonedReader)
}

func Test_S3Reader_Close(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Test Close
	err = reader.Close()
	require.NoError(t, err)
}

func Test_S3Reader_GetObjectError(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Set error for GetObject
	client.setGetObjectError(errors.New("network error"))

	buffer := make([]byte, 10)
	_, err = reader.Read(buffer)
	require.Error(t, err)

	expectedError := "network error"
	require.Contains(t, err.Error(), expectedError)
}

func Test_S3Reader_HeadObjectError(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setHeadObjectError(errors.New("access denied"))

	_, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.Error(t, err)

	expectedError := "access denied"
	require.Contains(t, err.Error(), expectedError)
}

func Test_S3Reader_Open(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Test Open with new name
	newReader, err := reader.Open("new-file.parquet")
	require.NoError(t, err)
	require.NotNil(t, newReader)

	// Test Open with empty name (should use original key)
	emptyNameReader, err := reader.Open("")
	require.NoError(t, err)
	require.NotNil(t, emptyNameReader)
}

func Test_S3Reader_Read_BeyondEOF(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Seek to end of file
	_, err = reader.Seek(int64(len(testData)), io.SeekStart)
	require.NoError(t, err)

	// Try to read beyond EOF
	buffer := make([]byte, 10)
	n, err := reader.Read(buffer)

	require.Equal(t, io.EOF, err)

	require.Equal(t, 0, n)
}

func Test_S3Reader_Read_MultipleChunks(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Read in two chunks
	firstChunk := make([]byte, 30)
	n1, err := reader.Read(firstChunk)
	require.NoError(t, err)
	require.Equal(t, 30, n1)

	remainingSize := len(testData) - 30
	secondChunk := make([]byte, remainingSize)
	n2, err := reader.Read(secondChunk)
	require.NoError(t, err)
	require.Equal(t, remainingSize, n2)

	// Verify combined data
	combined := make([]byte, 0, len(testData))
	combined = append(combined, firstChunk...)
	combined = append(combined, secondChunk...)

	require.True(t, bytes.Equal(combined, testData))
}

func Test_S3Reader_Read_Success(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Read first 20 bytes
	buffer := make([]byte, 20)
	n, err := reader.Read(buffer)
	require.NoError(t, err)

	require.Equal(t, 20, n)

	expected := testData[:20]
	require.True(t, bytes.Equal(buffer, expected))
}

func Test_S3Reader_Seek_InvalidOffset(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Try negative offset from start
	_, err = reader.Seek(-10, io.SeekStart)
	require.Equal(t, errInvalidOffset, err)

	// Try offset beyond file size
	_, err = reader.Seek(int64(len(testData))+100, io.SeekStart)
	require.Equal(t, errInvalidOffset, err)
}

func Test_S3Reader_Seek_InvalidWhence(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Try invalid whence
	_, err = reader.Seek(0, 999)
	require.Equal(t, errWhence, err)
}

func Test_S3Reader_Seek_SeekCurrent(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// First seek to position 10
	_, err = reader.Seek(10, io.SeekStart)
	require.NoError(t, err)

	// Seek 5 bytes forward from current position
	offset, err := reader.Seek(5, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(15), offset)
}

func Test_S3Reader_Seek_SeekEnd(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Seek to 10 bytes before end
	offset, err := reader.Seek(-10, io.SeekEnd)
	require.NoError(t, err)
	// The seek implementation stores the offset as-is for SeekEnd
	expectedOffset := int64(-10)
	require.Equal(t, expectedOffset, offset)

	// Seek to exact end
	offset, err = reader.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	expectedOffset = int64(len(testData))
	require.Equal(t, expectedOffset, offset)
}

func Test_S3Reader_Seek_SeekStart(t *testing.T) {
	ctx := context.Background()
	client := newMockS3ReadClient()
	bucket := "test-bucket"
	key := "test-file.parquet"

	client.setObject(bucket, key, testData)

	reader, err := NewS3FileReaderWithClient(ctx, client, bucket, key, nil)
	require.NoError(t, err)

	// Seek to position 20
	offset, err := reader.Seek(20, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(20), offset)

	// Read from new position
	buffer := make([]byte, 10)
	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 10, n)

	expected := testData[20:30]
	require.True(t, bytes.Equal(buffer, expected))
}
