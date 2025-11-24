package mem

import (
	"errors"
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestMemWriter_CloseWithoutOnCloseFunc(t *testing.T) {
	memFs = afero.NewMemMapFs()

	writer := &memWriter{}
	created, err := writer.Create("no_onclose.parquet")
	require.NoError(t, err)

	// Write some data
	testData := []byte("No onClose test")
	_, err = created.Write(testData)
	require.NoError(t, err)

	// Close without onClose function - should not error
	err = created.Close()
	require.NoError(t, err)
}

func TestMemWriter_Create(t *testing.T) {
	memFs = afero.NewMemMapFs()

	writer := &memWriter{}
	created, err := writer.Create("created.parquet")
	require.NoError(t, err)
	require.NotNil(t, created)

	// Cast back to memWriter to check internal state
	memWriter := created.(*memWriter)
	require.Equal(t, "created.parquet", memWriter.filePath)
	require.NotNil(t, memWriter.file)

	err = created.Close()
	require.NoError(t, err)
}

func TestMemWriter_OnCloseError(t *testing.T) {
	memFs = afero.NewMemMapFs()

	onCloseError := func(filename string, reader io.Reader) error {
		return io.ErrUnexpectedEOF
	}

	writer, err := NewMemFileWriter("error_test.parquet", onCloseError)
	require.NoError(t, err)

	_, err = writer.Write([]byte("test"))
	require.NoError(t, err)

	// Close should return the error from onClose (wrapped)
	err = writer.Close()
	require.True(t, errors.Is(err, io.ErrUnexpectedEOF))
}

func TestMemWriter_WithSubdirectoryPath(t *testing.T) {
	memFs = afero.NewMemMapFs()

	onCloseCalled := false
	var closedFilename string

	onCloseFunc := func(filename string, reader io.Reader) error {
		onCloseCalled = true
		closedFilename = filename
		return nil
	}

	// Create writer with subdirectory path
	writer, err := NewMemFileWriter("subdir/test.parquet", onCloseFunc)
	require.NoError(t, err)

	_, err = writer.Write([]byte("subdir test"))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Check that onClose was called with just the base filename
	require.True(t, onCloseCalled)
	require.Equal(t, "test.parquet", closedFilename)
}

func TestMemWriter_Write(t *testing.T) {
	memFs = afero.NewMemMapFs()

	writer := &memWriter{}
	created, err := writer.Create("write_test.parquet")
	require.NoError(t, err)
	defer func() {
		if err := created.Close(); err != nil {
			require.NoError(t, err)
		}
	}()

	testData := []byte("Test write data")
	n, err := created.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	// Close and verify the data was written
	err = created.Close()
	require.NoError(t, err)

	// Read back from the file system
	file, err := memFs.Open("write_test.parquet")
	require.NoError(t, err)
	defer func() {
		if err := file.Close(); err != nil {
			require.NoError(t, err)
		}
	}()

	content, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, string(testData), string(content))
}

func TestNewMemFileWriter(t *testing.T) {
	// Reset memFs to nil to test initialization
	memFs = nil

	onCloseCalled := false
	var closedFilename string
	var closedReader io.Reader

	onCloseFunc := func(filename string, reader io.Reader) error {
		onCloseCalled = true
		closedFilename = filename
		closedReader = reader
		return nil
	}

	writer, err := NewMemFileWriter("test.parquet", onCloseFunc)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Test that memFs was initialized
	require.NotNil(t, memFs)

	// Write some data
	testData := []byte("Hello, Mem Writer!")
	n, err := writer.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	// Close the writer
	err = writer.Close()
	require.NoError(t, err)

	// Check that onClose was called
	require.True(t, onCloseCalled)
	require.Equal(t, "test.parquet", closedFilename)
	require.NotNil(t, closedReader)

	// Read the data from the closed reader to verify
	if closedReader != nil {
		content, err := io.ReadAll(closedReader)
		require.NoError(t, err)
		require.Equal(t, string(testData), string(content))
	}
}

func TestNewMemFileWriterWithExistingFs(t *testing.T) {
	// Set up an existing file system
	existingFs := afero.NewMemMapFs()
	memFs = existingFs

	writer, err := NewMemFileWriter("test2.parquet", nil)
	require.NoError(t, err)

	// Should use the existing file system, not create a new one
	require.Equal(t, existingFs, memFs)

	err = writer.Close()
	require.NoError(t, err)
}

func TestSetAndGetInMemFileFs(t *testing.T) {
	// Reset memFs to nil first
	memFs = nil

	// Create a new file system
	newFs := afero.NewMemMapFs()
	SetInMemFileFs(&newFs)

	// Test that it was set correctly
	retrievedFs := GetMemFileFs()
	require.NotNil(t, retrievedFs)

	// Test that it's the same file system by creating a file in one and reading from the other
	testFile, err := newFs.Create("test.txt")
	require.NoError(t, err)
	_, err = testFile.WriteString("test content")
	require.NoError(t, err)
	err = testFile.Close()
	require.NoError(t, err)

	// Check if we can read from retrieved fs
	readFile, err := retrievedFs.Open("test.txt")
	require.NoError(t, err)
	content, err := io.ReadAll(readFile)
	require.NoError(t, err)
	require.Equal(t, "test content", string(content))
	err = readFile.Close()
	require.NoError(t, err)
}
