package mem

import (
	"errors"
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestMemWriter_CloseWithoutOnCloseFunc(t *testing.T) {
	fs := afero.NewMemMapFs()

	writer, err := NewMemFileWriterWithFs("no_onclose.parquet", fs, nil)
	require.NoError(t, err)

	// Write some data
	testData := []byte("No onClose test")
	_, err = writer.Write(testData)
	require.NoError(t, err)

	// Close without onClose function - should not error
	err = writer.Close()
	require.NoError(t, err)
}

func TestMemWriter_Create(t *testing.T) {
	fs := afero.NewMemMapFs()

	writer, err := NewMemFileWriterWithFs("created.parquet", fs, nil)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Cast back to memWriter to check internal state
	mw := writer.(*memWriter)
	require.Equal(t, "created.parquet", mw.filePath)
	require.NotNil(t, mw.file)

	err = writer.Close()
	require.NoError(t, err)
}

func TestMemWriter_OnCloseError(t *testing.T) {
	fs := afero.NewMemMapFs()

	onCloseError := func(filename string, reader io.Reader) error {
		return io.ErrUnexpectedEOF
	}

	writer, err := NewMemFileWriterWithFs("error_test.parquet", fs, onCloseError)
	require.NoError(t, err)

	_, err = writer.Write([]byte("test"))
	require.NoError(t, err)

	// Close should return the error from onClose (wrapped)
	err = writer.Close()
	require.True(t, errors.Is(err, io.ErrUnexpectedEOF))
}

func TestMemWriter_WithSubdirectoryPath(t *testing.T) {
	fs := afero.NewMemMapFs()

	onCloseCalled := false
	var closedFilename string

	onCloseFunc := func(filename string, reader io.Reader) error {
		onCloseCalled = true
		closedFilename = filename
		return nil
	}

	// Create writer with subdirectory path
	writer, err := NewMemFileWriterWithFs("subdir/test.parquet", fs, onCloseFunc)
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
	fs := afero.NewMemMapFs()

	writer, err := NewMemFileWriterWithFs("write_test.parquet", fs, nil)
	require.NoError(t, err)

	testData := []byte("Test write data")
	n, err := writer.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	// Close and verify the data was written
	err = writer.Close()
	require.NoError(t, err)

	// Read back from the file system
	file, err := fs.Open("write_test.parquet")
	require.NoError(t, err)
	defer func() {
		_ = file.Close()
	}()

	content, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, string(testData), string(content))
}

func TestNewMemFileWriter(t *testing.T) {
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

func TestNewMemFileWriterWithFs(t *testing.T) {
	fs := afero.NewMemMapFs()

	writer, err := NewMemFileWriterWithFs("test.parquet", fs, nil)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Verify data is written to the provided fs
	_, err = writer.Write([]byte("data"))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	file, err := fs.Open("test.parquet")
	require.NoError(t, err)
	content, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, "data", string(content))
	_ = file.Close()
}

func TestNewMemFileWriterWithFs_NilFs(t *testing.T) {
	_, err := NewMemFileWriterWithFs("test.parquet", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "filesystem must not be nil")
}

func TestNewMemFileWriterWithFs_CreateError(t *testing.T) {
	// Use a read-only filesystem to trigger a Create error
	fs := afero.NewReadOnlyFs(afero.NewMemMapFs())

	_, err := NewMemFileWriterWithFs("test.parquet", fs, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "create mem file")
}

func TestMemWriterWithFs_IsolatedFilesystems(t *testing.T) {
	fs1 := afero.NewMemMapFs()
	fs2 := afero.NewMemMapFs()

	w1, err := NewMemFileWriterWithFs("file.parquet", fs1, nil)
	require.NoError(t, err)
	_, err = w1.Write([]byte("fs1 data"))
	require.NoError(t, err)
	require.NoError(t, w1.Close())

	w2, err := NewMemFileWriterWithFs("file.parquet", fs2, nil)
	require.NoError(t, err)
	_, err = w2.Write([]byte("fs2 data"))
	require.NoError(t, err)
	require.NoError(t, w2.Close())

	// Verify each fs has its own data
	f1, err := fs1.Open("file.parquet")
	require.NoError(t, err)
	c1, _ := io.ReadAll(f1)
	require.Equal(t, "fs1 data", string(c1))
	_ = f1.Close()

	f2, err := fs2.Open("file.parquet")
	require.NoError(t, err)
	c2, _ := io.ReadAll(f2)
	require.Equal(t, "fs2 data", string(c2))
	_ = f2.Close()
}
