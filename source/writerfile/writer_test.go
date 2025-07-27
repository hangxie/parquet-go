package writerfile

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockWriter struct {
	buffer []byte
	err    error
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	if m.err != nil {
		return 0, m.err
	}
	m.buffer = append(m.buffer, p...)
	return len(p), nil
}

func Test_NewWriterFile(t *testing.T) {
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	require.NotNil(t, writerFile)

	// We can't check internal state since writerFile is not exported
	// Just verify it's not nil and works
	testData := []byte("test")
	n, err := writerFile.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)
	require.Equal(t, "test", buffer.String())
}

func Test_WriterFile_Close(t *testing.T) {
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	err := writerFile.Close()
	require.NoError(t, err)

	// Should be able to write after close (since it's a no-op)
	testData := []byte("after close")
	n, err := writerFile.Write(testData)
	require.NoError(t, err, "Write after close failed: %v", err)
	require.Equal(t, len(testData), n, "Expected to write %d bytes after close, got %d", len(testData), n)
}

func Test_WriterFile_Create(t *testing.T) {
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	created, err := writerFile.Create("dummy_name")
	require.NoError(t, err, "Create should not return error: %v", err)
	require.Equal(t, writerFile, created)
}

func Test_WriterFile_WithBytesBuffer(t *testing.T) {
	// Test with the most common use case - bytes.Buffer
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	// Write multiple times
	writes := []string{"First", " Second", " Third"}
	for _, write := range writes {
		n, err := writerFile.Write([]byte(write))
		require.NoError(t, err)
		require.Equal(t, len(write), n)
	}

	expected := "First Second Third"
	require.Equal(t, expected, buffer.String())

	// Test that buffer contents persist after close
	err := writerFile.Close()
	require.NoError(t, err)
	require.Equal(t, expected, buffer.String())
}

func Test_WriterFile_Write(t *testing.T) {
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	testData := []byte("Hello, WriterFile!")
	n, err := writerFile.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	// Check that data was written to the underlying writer
	require.Equal(t, string(testData), buffer.String())
}

func Test_WriterFile_WriteError(t *testing.T) {
	expectedError := errors.New("mock write error")
	mockWriter := &mockWriter{err: expectedError}
	writerFile := NewWriterFile(mockWriter)

	testData := []byte("test data")
	n, err := writerFile.Write(testData)
	require.Equal(t, expectedError, err)
	require.Equal(t, 0, n)
}

func Test_WriterFile_WriteMultiple(t *testing.T) {
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	chunks := [][]byte{
		[]byte("Hello, "),
		[]byte("Writer"),
		[]byte("File!"),
	}

	var totalWritten int
	for _, chunk := range chunks {
		n, err := writerFile.Write(chunk)
		require.NoError(t, err)
		totalWritten += n
	}

	expected := "Hello, WriterFile!"
	require.Equal(t, expected, buffer.String())
	require.Equal(t, len(expected), totalWritten)
}

func Test_WriterFile_WriteWithMockWriter(t *testing.T) {
	mockWriter := &mockWriter{}
	writerFile := NewWriterFile(mockWriter)

	testData := []byte("mock writer test")
	n, err := writerFile.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	// Check that data was written to the mock writer
	require.Equal(t, string(testData), string(mockWriter.buffer))
}
