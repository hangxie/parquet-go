package writerfile

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile-time check: *writerFile satisfies source.ParquetFileWriter.
var _ source.ParquetFileWriter = (*writerFile)(nil)

func TestWriterFile_InterfaceCompliance(t *testing.T) {
	w := NewWriterFile(&bytes.Buffer{})
	require.NotNil(t, w)
}

func TestWriterFile_CreateReturnsSelf(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewWriterFile(buf)
	created, err := w.Create("any-name")
	require.NoError(t, err)
	require.Equal(t, w, created)
}

func TestWriterFile_WritePassesThrough(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewWriterFile(buf)
	n, err := w.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "hello", buf.String())
}

func TestWriterFile_CloseIsNoOp(t *testing.T) {
	w := NewWriterFile(&bytes.Buffer{})
	require.NoError(t, w.Close())
	// Close a second time should also succeed
	require.NoError(t, w.Close())
}
