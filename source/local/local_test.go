package local

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile-time checks for interface compliance.
var (
	_ source.ParquetFileReader = (*localReader)(nil)
	_ source.ParquetFileWriter = (*localWriter)(nil)
)

func TestLocalFile_FieldsAreSet(t *testing.T) {
	f := &localFile{
		filePath: "/tmp/test.parquet",
		file:     nil,
	}
	require.Equal(t, "/tmp/test.parquet", f.filePath)
	require.Nil(t, f.file)
}

func TestLocalWriter_CloseFile(t *testing.T) {
	f, err := os.CreateTemp("", "parquet-local-test-*.parquet")
	require.NoError(t, err)
	path := f.Name()
	defer func() { _ = os.Remove(path) }()

	w := &localWriter{
		localFile: localFile{filePath: path, file: f},
	}
	require.NoError(t, w.Close())
}
