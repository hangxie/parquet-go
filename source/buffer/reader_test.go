package buffer

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferReader_Clone(t *testing.T) {
	testData := []byte("Hello, Reader!")
	reader := NewBufferReaderFromBytes(testData)

	cloned, err := reader.Clone()
	require.NoError(t, err)
	require.NotNil(t, cloned)

	// Should share the same underlying data (no allocation)
	clonedBytes := cloned.(*bufferReader).Bytes()
	require.Equal(t, &reader.buff[0], &clonedBytes[0])
}

func TestBufferReader_Close(t *testing.T) {
	reader := NewBufferReaderFromBytes([]byte("test"))
	err := reader.Close()
	require.NoError(t, err)
}

func TestBufferReader_Open(t *testing.T) {
	testData := []byte("Hello, Reader!")
	reader := NewBufferReaderFromBytes(testData)

	opened, err := reader.Open("dummy")
	require.NoError(t, err)
	require.NotNil(t, opened)

	// Should be a new instance with copied data
	openedBytes := opened.(*bufferReader).Bytes()
	require.Equal(t, string(testData), string(openedBytes))
}

func TestBufferReader_Read(t *testing.T) {
	testData := []byte("Hello, World!")
	reader := NewBufferReaderFromBytes(testData)

	// Test reading part of the data
	buffer := make([]byte, 5)
	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "Hello", string(buffer))

	// Test reading the rest
	buffer2 := make([]byte, 20)
	n2, err := reader.Read(buffer2)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 8, n2)
	require.Equal(t, ", World!", string(buffer2[:n2]))
}

func TestBufferReader_Seek(t *testing.T) {
	testData := []byte("Hello, World!")
	reader := NewBufferReaderFromBytes(testData)

	// Test SeekStart
	pos, err := reader.Seek(7, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(7), pos)

	// Read from seeked position
	buffer := make([]byte, 6)
	n, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, "World!", string(buffer[:n]))

	// Test SeekCurrent
	pos, err = reader.Seek(-6, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(7), pos)

	// Test SeekEnd
	pos, err = reader.Seek(-6, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(7), pos)
}

func TestBufferReader_Seek_errors(t *testing.T) {
	testData := []byte("Hello")
	reader := NewBufferReaderFromBytes(testData)

	// Test seek to negative position
	_, err := reader.Seek(-10, io.SeekStart)
	require.Error(t, err)

	// Test seek beyond end
	pos, err := reader.Seek(100, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(len(testData)), pos)
}

func TestNewBufferReaderFromBytes(t *testing.T) {
	testData := []byte("Hello, Reader!")
	reader := NewBufferReaderFromBytes(testData)

	require.NotNil(t, reader)

	// Should have copied the data
	readerBytes := reader.Bytes()
	require.Equal(t, len(testData), len(readerBytes))
	require.Equal(t, string(testData), string(readerBytes))

	// Modifying original should not affect reader
	testData[0] = 'X'
	require.NotEqual(t, byte('X'), readerBytes[0])
}

func TestNewBufferReaderFromBytesNoAlloc(t *testing.T) {
	testData := []byte("Hello, Reader!")
	reader := NewBufferReaderFromBytesNoAlloc(testData)

	require.NotNil(t, reader)

	// Should use the same slice
	readerBytes := reader.Bytes()
	require.Equal(t, &testData[0], &readerBytes[0])
}
