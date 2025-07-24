package buffer

import (
	"io"
	"testing"
)

func Test_BufferFile_Bytes(t *testing.T) {
	testData := []byte("Hello, Buffer!")
	bf := bufferFile{buff: testData}

	bytes := bf.Bytes()
	if len(bytes) != len(testData) {
		t.Errorf("Expected %d bytes, got %d", len(testData), len(bytes))
	}
	if string(bytes) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(bytes))
	}
}

func Test_NewBufferReaderFromBytes(t *testing.T) {
	testData := []byte("Hello, Reader!")
	reader := NewBufferReaderFromBytes(testData)

	if reader == nil {
		t.Fatal("Expected non-nil reader")
	}

	// Should have copied the data
	readerBytes := reader.Bytes()
	if len(readerBytes) != len(testData) {
		t.Errorf("Expected %d bytes, got %d", len(testData), len(readerBytes))
	}
	if string(readerBytes) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(readerBytes))
	}

	// Modifying original should not affect reader
	testData[0] = 'X'
	if readerBytes[0] == 'X' {
		t.Error("Reader should have independent copy of data")
	}
}

func Test_NewBufferReaderFromBytesNoAlloc(t *testing.T) {
	testData := []byte("Hello, Reader!")
	reader := NewBufferReaderFromBytesNoAlloc(testData)

	if reader == nil {
		t.Fatal("Expected non-nil reader")
	}

	// Should use the same slice
	readerBytes := reader.Bytes()
	if &readerBytes[0] != &testData[0] {
		t.Error("Reader should use the same underlying slice")
	}
}

func Test_BufferReader_Open(t *testing.T) {
	testData := []byte("Hello, Reader!")
	reader := NewBufferReaderFromBytes(testData)

	opened, err := reader.Open("dummy")
	if err != nil {
		t.Errorf("Open should not return error: %v", err)
	}
	if opened == nil {
		t.Fatal("Expected non-nil opened reader")
	}

	// Should be a new instance with copied data
	openedBytes := opened.(*bufferReader).Bytes()
	if string(openedBytes) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(openedBytes))
	}
}

func Test_BufferReader_Clone(t *testing.T) {
	testData := []byte("Hello, Reader!")
	reader := NewBufferReaderFromBytes(testData)

	cloned, err := reader.Clone()
	if err != nil {
		t.Errorf("Clone should not return error: %v", err)
	}
	if cloned == nil {
		t.Fatal("Expected non-nil cloned reader")
	}

	// Should share the same underlying data (no allocation)
	clonedBytes := cloned.(*bufferReader).Bytes()
	if &clonedBytes[0] != &reader.buff[0] {
		t.Error("Cloned reader should share the same underlying slice")
	}
}

func Test_BufferReader_Read(t *testing.T) {
	testData := []byte("Hello, World!")
	reader := NewBufferReaderFromBytes(testData)

	// Test reading part of the data
	buffer := make([]byte, 5)
	n, err := reader.Read(buffer)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("Expected to read 5 bytes, got %d", n)
	}
	if string(buffer) != "Hello" {
		t.Errorf("Expected 'Hello', got %q", string(buffer))
	}

	// Test reading the rest
	buffer2 := make([]byte, 20)
	n2, err := reader.Read(buffer2)
	if err != io.EOF {
		t.Errorf("Expected EOF error, got %v", err)
	}
	if n2 != 8 {
		t.Errorf("Expected to read 8 bytes, got %d", n2)
	}
	if string(buffer2[:n2]) != ", World!" {
		t.Errorf("Expected ', World!', got %q", string(buffer2[:n2]))
	}
}

func Test_BufferReader_Seek(t *testing.T) {
	testData := []byte("Hello, World!")
	reader := NewBufferReaderFromBytes(testData)

	// Test SeekStart
	pos, err := reader.Seek(7, io.SeekStart)
	if err != nil {
		t.Errorf("Seek failed: %v", err)
	}
	if pos != 7 {
		t.Errorf("Expected position 7, got %d", pos)
	}

	// Read from seeked position
	buffer := make([]byte, 6)
	n, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		t.Errorf("Read after seek failed: %v", err)
	}
	if string(buffer[:n]) != "World!" {
		t.Errorf("Expected 'World!', got %q", string(buffer[:n]))
	}

	// Test SeekCurrent
	pos, err = reader.Seek(-6, io.SeekCurrent)
	if err != nil {
		t.Errorf("SeekCurrent failed: %v", err)
	}
	if pos != 7 {
		t.Errorf("Expected position 7, got %d", pos)
	}

	// Test SeekEnd
	pos, err = reader.Seek(-6, io.SeekEnd)
	if err != nil {
		t.Errorf("SeekEnd failed: %v", err)
	}
	if pos != 7 {
		t.Errorf("Expected position 7, got %d", pos)
	}
}

func Test_BufferReader_Seek_errors(t *testing.T) {
	testData := []byte("Hello")
	reader := NewBufferReaderFromBytes(testData)

	// Test seek to negative position
	_, err := reader.Seek(-10, io.SeekStart)
	if err == nil {
		t.Error("Expected error for negative seek")
	}

	// Test seek beyond end
	pos, err := reader.Seek(100, io.SeekStart)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if pos != int64(len(testData)) {
		t.Errorf("Expected position %d, got %d", len(testData), pos)
	}
}

func Test_BufferReader_Close(t *testing.T) {
	reader := NewBufferReaderFromBytes([]byte("test"))
	err := reader.Close()
	if err != nil {
		t.Errorf("Close should not return error: %v", err)
	}
}

func Test_NewBufferWriter(t *testing.T) {
	writer := NewBufferWriter()
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	if cap(writer.buff) != DefaultCapacity {
		t.Errorf("Expected capacity %d, got %d", DefaultCapacity, cap(writer.buff))
	}
}

func Test_NewBufferWriterCapacity(t *testing.T) {
	capacity := 1024
	writer := NewBufferWriterCapacity(capacity)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	if cap(writer.buff) != capacity {
		t.Errorf("Expected capacity %d, got %d", capacity, cap(writer.buff))
	}
}

func Test_NewBufferWriterFromBytesNoAlloc(t *testing.T) {
	testData := []byte("Hello, Writer!")
	writer := NewBufferWriterFromBytesNoAlloc(testData)
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}
	if &writer.buff[0] != &testData[0] {
		t.Error("Writer should use the same underlying slice")
	}
}

func Test_BufferWriter_Create(t *testing.T) {
	writer := NewBufferWriter()
	created, err := writer.Create("dummy")
	if err != nil {
		t.Errorf("Create should not return error: %v", err)
	}
	if created == nil {
		t.Fatal("Expected non-nil created writer")
	}
}

func Test_BufferWriter_Write(t *testing.T) {
	writer := NewBufferWriter()
	testData := []byte("Hello, Writer!")

	n, err := writer.Write(testData)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, got %d", len(testData), n)
	}

	writtenData := writer.Bytes()
	if string(writtenData) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(writtenData))
	}
}

func Test_BufferWriter_Write_multiple(t *testing.T) {
	writer := NewBufferWriter()

	chunks := [][]byte{
		[]byte("Hello, "),
		[]byte("Buffer "),
		[]byte("Writer!"),
	}

	for _, chunk := range chunks {
		n, err := writer.Write(chunk)
		if err != nil {
			t.Errorf("Write failed: %v", err)
		}
		if n != len(chunk) {
			t.Errorf("Expected to write %d bytes, got %d", len(chunk), n)
		}
	}

	expected := "Hello, Buffer Writer!"
	writtenData := writer.Bytes()
	if string(writtenData) != expected {
		t.Errorf("Expected %q, got %q", expected, string(writtenData))
	}
}

func Test_BufferWriter_Write_expansion(t *testing.T) {
	// Create writer with small capacity
	writer := NewBufferWriterCapacity(5)

	// Write data that exceeds initial capacity
	largeData := []byte("This is a much longer string that exceeds the initial capacity")
	n, err := writer.Write(largeData)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != len(largeData) {
		t.Errorf("Expected to write %d bytes, got %d", len(largeData), n)
	}

	writtenData := writer.Bytes()
	if string(writtenData) != string(largeData) {
		t.Errorf("Expected %q, got %q", string(largeData), string(writtenData))
	}

	// Check that capacity was expanded
	if cap(writer.buff) <= 5 {
		t.Errorf("Expected capacity to be expanded beyond 5, got %d", cap(writer.buff))
	}
}

func Test_BufferWriter_Close(t *testing.T) {
	writer := NewBufferWriter()
	err := writer.Close()
	if err != nil {
		t.Errorf("Close should not return error: %v", err)
	}
}
