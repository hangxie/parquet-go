package buffer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_BufferFile_Bytes(t *testing.T) {
	testData := []byte("Hello, Buffer!")
	bf := bufferFile{buff: testData}

	bytes := bf.Bytes()
	require.Len(t, bytes, len(testData))
	require.Equal(t, string(testData), string(bytes))
}

func Test_BufferWriter_Close(t *testing.T) {
	writer := NewBufferWriter()
	err := writer.Close()
	require.NoError(t, err)
}

func Test_BufferWriter_Create(t *testing.T) {
	writer := NewBufferWriter()
	created, err := writer.Create("dummy")
	require.NoError(t, err)
	require.NotNil(t, created)
}

func Test_BufferWriter_Write(t *testing.T) {
	writer := NewBufferWriter()
	testData := []byte("Hello, Writer!")

	n, err := writer.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	writtenData := writer.Bytes()
	require.Equal(t, string(testData), string(writtenData))
}

func Test_BufferWriter_Write_expansion(t *testing.T) {
	// Create writer with small capacity
	writer := NewBufferWriterCapacity(5)

	// Write data that exceeds initial capacity
	largeData := []byte("This is a much longer string that exceeds the initial capacity")
	n, err := writer.Write(largeData)
	require.NoError(t, err)
	require.Equal(t, len(largeData), n)

	writtenData := writer.Bytes()
	require.Equal(t, string(largeData), string(writtenData))

	// Check that capacity was expanded
	require.Greater(t, cap(writer.buff), 5)
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
		require.NoError(t, err)
		require.Equal(t, len(chunk), n)
	}

	expected := "Hello, Buffer Writer!"
	writtenData := writer.Bytes()
	require.Equal(t, expected, string(writtenData))
}

func Test_NewBufferWriter(t *testing.T) {
	writer := NewBufferWriter()
	require.NotNil(t, writer)
	require.Equal(t, DefaultCapacity, cap(writer.buff))
}

func Test_NewBufferWriterCapacity(t *testing.T) {
	capacity := 1024
	writer := NewBufferWriterCapacity(capacity)
	require.NotNil(t, writer)
	require.Equal(t, capacity, cap(writer.buff))
}

func Test_NewBufferWriterFromBytesNoAlloc(t *testing.T) {
	testData := []byte("Hello, Writer!")
	writer := NewBufferWriterFromBytesNoAlloc(testData)
	require.NotNil(t, writer)
	require.Equal(t, &testData[0], &writer.buff[0])
}
