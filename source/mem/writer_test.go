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

	writer := &memWriter{fs: fs}
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
	fs := afero.NewMemMapFs()

	writer := &memWriter{fs: fs}
	created, err := writer.Create("created.parquet")
	require.NoError(t, err)
	require.NotNil(t, created)

	// Cast back to memWriter to check internal state
	mw := created.(*memWriter)
	require.Equal(t, "created.parquet", mw.filePath)
	require.NotNil(t, mw.file)

	err = created.Close()
	require.NoError(t, err)
}

func TestMemWriter_OnCloseError(t *testing.T) {
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
	fs := afero.NewMemMapFs()

	writer := &memWriter{fs: fs}
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
	file, err := fs.Open("write_test.parquet")
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

func TestNewMemFileWriter_CreateError(t *testing.T) {
	// Use NewMemFileWriterWithFs with a read-only filesystem to trigger a Create error
	fs := afero.NewReadOnlyFs(afero.NewMemMapFs())
	_, err := NewMemFileWriterWithFs("test.parquet", nil, fs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "create mem file")
}

func TestNewMemFileWriterWithFs(t *testing.T) {
	fs1 := afero.NewMemMapFs()
	fs2 := afero.NewMemMapFs()

	// Write to fs1
	w1, err := NewMemFileWriterWithFs("test1.parquet", nil, fs1)
	require.NoError(t, err)
	_, err = w1.Write([]byte("data1"))
	require.NoError(t, err)
	require.NoError(t, w1.Close())

	// Write to fs2
	w2, err := NewMemFileWriterWithFs("test2.parquet", nil, fs2)
	require.NoError(t, err)
	_, err = w2.Write([]byte("data2"))
	require.NoError(t, err)
	require.NoError(t, w2.Close())

	// Verify isolation
	exists1, _ := afero.Exists(fs1, "test1.parquet")
	exists2, _ := afero.Exists(fs1, "test2.parquet")
	require.True(t, exists1)
	require.False(t, exists2)

	exists3, _ := afero.Exists(fs2, "test2.parquet")
	exists4, _ := afero.Exists(fs2, "test1.parquet")
	require.True(t, exists3)
	require.False(t, exists4)
}

func TestNewMemFileWriterWithFs_NilFs(t *testing.T) {
	_, err := NewMemFileWriterWithFs("test.parquet", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "filesystem must not be nil")
}

func TestNewMemFileWriterWithFs_OnClose(t *testing.T) {
	fs := afero.NewMemMapFs()
	var closedName string
	onClose := func(name string, r io.Reader) error {
		closedName = name
		return nil
	}

	w, err := NewMemFileWriterWithFs("dir/test.parquet", onClose, fs)
	require.NoError(t, err)
	_, err = w.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.Equal(t, "test.parquet", closedName)
}
