package writerfile

import (
	"bytes"
	"errors"
	"testing"
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

	if writerFile == nil {
		t.Fatal("Expected non-nil writerFile")
	}

	// We can't check internal state since writerFile is not exported
	// Just verify it's not nil and works
	testData := []byte("test")
	n, err := writerFile.Write(testData)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, got %d", len(testData), n)
	}
	if buffer.String() != "test" {
		t.Errorf("Expected 'test' in buffer, got %q", buffer.String())
	}
}

func Test_WriterFile_Create(t *testing.T) {
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	created, err := writerFile.Create("dummy_name")
	if err != nil {
		t.Errorf("Create should not return error: %v", err)
	}
	if created != writerFile {
		t.Error("Create should return the same instance")
	}
}

func Test_WriterFile_Write(t *testing.T) {
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	testData := []byte("Hello, WriterFile!")
	n, err := writerFile.Write(testData)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, got %d", len(testData), n)
	}

	// Check that data was written to the underlying writer
	if buffer.String() != string(testData) {
		t.Errorf("Expected %q in buffer, got %q", string(testData), buffer.String())
	}
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
		if err != nil {
			t.Errorf("Write failed for chunk: %v", err)
		}
		totalWritten += n
	}

	expected := "Hello, WriterFile!"
	if buffer.String() != expected {
		t.Errorf("Expected %q in buffer, got %q", expected, buffer.String())
	}
	if totalWritten != len(expected) {
		t.Errorf("Expected to write %d bytes total, got %d", len(expected), totalWritten)
	}
}

func Test_WriterFile_WriteError(t *testing.T) {
	expectedError := errors.New("mock write error")
	mockWriter := &mockWriter{err: expectedError}
	writerFile := NewWriterFile(mockWriter)

	testData := []byte("test data")
	n, err := writerFile.Write(testData)
	if err != expectedError {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}
	if n != 0 {
		t.Errorf("Expected to write 0 bytes on error, got %d", n)
	}
}

func Test_WriterFile_WriteWithMockWriter(t *testing.T) {
	mockWriter := &mockWriter{}
	writerFile := NewWriterFile(mockWriter)

	testData := []byte("mock writer test")
	n, err := writerFile.Write(testData)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, got %d", len(testData), n)
	}

	// Check that data was written to the mock writer
	if string(mockWriter.buffer) != string(testData) {
		t.Errorf("Expected %q in mock buffer, got %q", string(testData), string(mockWriter.buffer))
	}
}

func Test_WriterFile_Close(t *testing.T) {
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	err := writerFile.Close()
	if err != nil {
		t.Errorf("Close should not return error: %v", err)
	}

	// Should be able to write after close (since it's a no-op)
	testData := []byte("after close")
	n, err := writerFile.Write(testData)
	if err != nil {
		t.Errorf("Write after close failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes after close, got %d", len(testData), n)
	}
}

func Test_WriterFile_WithBytesBuffer(t *testing.T) {
	// Test with the most common use case - bytes.Buffer
	buffer := &bytes.Buffer{}
	writerFile := NewWriterFile(buffer)

	// Write multiple times
	writes := []string{"First", " Second", " Third"}
	for _, write := range writes {
		n, err := writerFile.Write([]byte(write))
		if err != nil {
			t.Errorf("Write failed: %v", err)
		}
		if n != len(write) {
			t.Errorf("Expected to write %d bytes, got %d", len(write), n)
		}
	}

	expected := "First Second Third"
	if buffer.String() != expected {
		t.Errorf("Expected %q, got %q", expected, buffer.String())
	}

	// Test that buffer contents persist after close
	err := writerFile.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}
	if buffer.String() != expected {
		t.Errorf("Buffer contents should persist after close, got %q", buffer.String())
	}
}
