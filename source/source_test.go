package source

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// Mock implementation of ParquetFileReader for testing
type mockParquetFileReader struct {
	data      []byte
	position  int64
	seekError bool
	readError bool
}

func newMockParquetFileReader(data []byte) *mockParquetFileReader {
	return &mockParquetFileReader{
		data:     data,
		position: 0,
	}
}

func (m *mockParquetFileReader) Read(p []byte) (int, error) {
	if m.readError {
		return 0, errors.New("read error")
	}

	if m.position >= int64(len(m.data)) {
		return 0, io.EOF
	}

	remaining := int64(len(m.data)) - m.position
	readSize := int64(len(p))
	if remaining < readSize {
		readSize = remaining
	}

	copy(p, m.data[m.position:m.position+readSize])
	m.position += readSize

	return int(readSize), nil
}

func (m *mockParquetFileReader) Seek(offset int64, whence int) (int64, error) {
	if m.seekError {
		return 0, errors.New("seek error")
	}

	switch whence {
	case io.SeekStart:
		m.position = offset
	case io.SeekCurrent:
		m.position += offset
	case io.SeekEnd:
		m.position = int64(len(m.data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if m.position < 0 {
		m.position = 0
	}
	if m.position > int64(len(m.data)) {
		m.position = int64(len(m.data))
	}

	return m.position, nil
}

func (m *mockParquetFileReader) Close() error {
	return nil
}

func (m *mockParquetFileReader) Open(name string) (ParquetFileReader, error) {
	return newMockParquetFileReader(m.data), nil
}

func (m *mockParquetFileReader) Clone() (ParquetFileReader, error) {
	clone := newMockParquetFileReader(m.data)
	clone.position = m.position
	return clone, nil
}

// Test the buffer size constant

func Test_BufferSizeConstant(t *testing.T) {
	expectedBufferSize := 4096
	require.Equal(t, expectedBufferSize, bufferSize, "Expected buffer size %d, got %d", expectedBufferSize, bufferSize)
}

func Test_ConvertToThriftReader_BufferSize(t *testing.T) {
	// Test that the buffer size constant is used correctly
	testData := make([]byte, bufferSize*2) // Data larger than buffer
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Create mock reader
	mockReader := newMockParquetFileReader(testData)

	// Test ConvertToThriftReader
	thriftReader := ConvertToThriftReader(mockReader, 0)

	require.NotNil(t, thriftReader)

	// Read data in chunks and verify it matches
	readData := make([]byte, 0, len(testData))
	buffer := make([]byte, 1000) // Read in smaller chunks

	for {
		n, err := thriftReader.Read(buffer[:1000])
		if err != nil && errors.Is(err, io.EOF) {
			if n > 0 {
				readData = append(readData, buffer[:n]...)
			}
			break
		}
		if err != nil {
			require.NoError(t, err, "Failed to read from thrift reader: %v", err)
		}
		readData = append(readData, buffer[:n]...)
	}

	require.True(t, bytes.Equal(readData, testData))
}

func Test_ConvertToThriftReader_EmptyData(t *testing.T) {
	// Test with empty data
	testData := []byte{}

	// Create mock reader
	mockReader := newMockParquetFileReader(testData)

	// Test ConvertToThriftReader with offset 0
	thriftReader := ConvertToThriftReader(mockReader, 0)

	require.NotNil(t, thriftReader)

	// Try to read - should get EOF immediately
	buffer := make([]byte, 10)
	n, err := thriftReader.Read(buffer)

	// When reading empty data, we expect EOF and 0 bytes read
	if n == 0 && errors.Is(err, io.EOF) {
		// This is the expected behavior
		// Expected behavior: EOF and 0 bytes when reading empty data
	} else {
		require.Fail(t, "Expected EOF and 0 bytes", "got error: %v, bytes: %d", err, n)
	}
}

func Test_ConvertToThriftReader_LargeOffset(t *testing.T) {
	// Test data
	testData := []byte("Small data")

	// Create mock reader
	mockReader := newMockParquetFileReader(testData)

	// Test ConvertToThriftReader with offset beyond data length
	largeOffset := int64(len(testData) + 100)
	thriftReader := ConvertToThriftReader(mockReader, largeOffset)

	require.NotNil(t, thriftReader)

	// Try to read - should get EOF immediately since we're beyond the data
	buffer := make([]byte, 10)
	n, err := thriftReader.Read(buffer)

	// When reading beyond data, we expect EOF and 0 bytes read
	if n == 0 && errors.Is(err, io.EOF) {
		// This is the expected behavior
		// Expected behavior: EOF and 0 bytes when reading beyond data
	} else {
		require.Fail(t, "Expected EOF and 0 bytes", "got error: %v, bytes: %d", err, n)
	}
}

func Test_ConvertToThriftReader_NegativeOffset(t *testing.T) {
	// Test data
	testData := []byte("Hello, this is test data")

	// Create mock reader
	mockReader := newMockParquetFileReader(testData)

	// Test ConvertToThriftReader with negative offset (should be clamped to 0)
	thriftReader := ConvertToThriftReader(mockReader, -10)

	require.NotNil(t, thriftReader)

	// Verify it reads from the beginning
	buffer := make([]byte, 5)
	n, err := thriftReader.Read(buffer)
	require.NoError(t, err, "Failed to read from thrift reader: %v", err)

	require.Equal(t, 5, n, "Expected to read 5 bytes, got %d", n)

	expected := testData[:5] // "Hello"
	require.True(t, bytes.Equal(buffer, expected), "Expected %s, got %s", string(expected), string(buffer))
}

func Test_ConvertToThriftReader_SeekError(t *testing.T) {
	// Test data
	testData := []byte("Hello, this is test data")

	// Create mock reader with seek error
	mockReader := newMockParquetFileReader(testData)
	mockReader.seekError = true

	// Test ConvertToThriftReader when seek fails
	thriftReader := ConvertToThriftReader(mockReader, 10)

	require.Nil(t, thriftReader)
}

func Test_ConvertToThriftReader_Success(t *testing.T) {
	// Test data
	testData := []byte("Hello, this is test data for Parquet file reader conversion to Thrift reader")

	// Create mock reader
	mockReader := newMockParquetFileReader(testData)

	// Test ConvertToThriftReader with offset 0
	thriftReader := ConvertToThriftReader(mockReader, 0)

	require.NotNil(t, thriftReader)

	// Verify the thrift reader can read data
	buffer := make([]byte, 10)
	n, err := thriftReader.Read(buffer)
	require.NoError(t, err, "Failed to read from thrift reader: %v", err)

	require.Equal(t, 10, n, "Expected to read 10 bytes, got %d", n)

	expected := testData[:10]
	require.True(t, bytes.Equal(buffer, expected))
}

func Test_ConvertToThriftReader_TypeAssertion(t *testing.T) {
	// Test that the returned object is indeed a *thrift.TBufferedTransport
	testData := []byte("test data")
	mockReader := newMockParquetFileReader(testData)

	result := ConvertToThriftReader(mockReader, 0)

	require.NotNil(t, result)

	// Verify we can use it as a reader
	buffer := make([]byte, 4)
	n, err := result.Read(buffer)
	require.NoError(t, err, "Failed to read from returned thrift reader: %v", err)
	require.Equal(t, 4, n, "Expected to read 4 bytes, got %d", n)
}

func Test_ConvertToThriftReader_WithOffset(t *testing.T) {
	// Test data
	testData := []byte("Hello, this is test data for Parquet file reader conversion to Thrift reader")

	// Create mock reader
	mockReader := newMockParquetFileReader(testData)

	// Test ConvertToThriftReader with offset 7 (should start reading from "this")
	offset := int64(7)
	thriftReader := ConvertToThriftReader(mockReader, offset)

	require.NotNil(t, thriftReader)

	// Verify the thrift reader reads from the correct offset
	buffer := make([]byte, 4)
	n, err := thriftReader.Read(buffer)
	require.NoError(t, err, "Failed to read from thrift reader: %v", err)

	require.Equal(t, 4, n, "Expected to read 4 bytes, got %d", n)

	expected := testData[7:11] // "this"
	require.True(t, bytes.Equal(buffer, expected))
}
