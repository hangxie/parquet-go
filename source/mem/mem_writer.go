package mem

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/hangxie/parquet-go/v3/source"
)

// OnCloseFunc function type, handles what to do
// after converted file is closed in-memory.
// Close() will pass the filename string and data as io.Reader
type OnCloseFunc func(string, io.Reader) error

type memFile struct {
	filePath string
	file     afero.File
	onClose  OnCloseFunc
	fs       afero.Fs
}

// Compile time check that *memWriter implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*memWriter)(nil)

// memWriter - ParquetFileWriter type for in-memory file operations
type memWriter struct {
	memFile
}

// NewMemFileWriterWithFs creates an in-memory ParquetFileWriter using the provided filesystem.
func NewMemFileWriterWithFs(name string, fs afero.Fs, f OnCloseFunc) (source.ParquetFileWriter, error) {
	if fs == nil {
		return nil, fmt.Errorf("filesystem must not be nil")
	}

	file, err := fs.Create(name)
	if err != nil {
		return nil, fmt.Errorf("create mem file: %w", err)
	}

	return &memWriter{
		memFile: memFile{
			filePath: name,
			file:     file,
			onClose:  f,
			fs:       fs,
		},
	}, nil
}

// NewMemFileWriter creates an in-memory ParquetFileWriter with a new in-memory filesystem.
func NewMemFileWriter(name string, f OnCloseFunc) (source.ParquetFileWriter, error) {
	return NewMemFileWriterWithFs(name, afero.NewMemMapFs(), f)
}

// Create creates a new in-memory file.
func (fs *memWriter) Create(name string) (source.ParquetFileWriter, error) {
	file, err := fs.fs.Create(name)
	if err != nil {
		return fs, fmt.Errorf("memfs create: %w", err)
	}

	fs.file = file
	fs.filePath = name
	return fs, nil
}

// Write writes data to the in-memory file.
func (fs *memWriter) Write(b []byte) (n int, err error) {
	return fs.file.Write(b)
}

// Close closes the file and executes the OnCloseFunc callback.
func (fs *memWriter) Close() error {
	if err := fs.file.Close(); err != nil {
		return fmt.Errorf("close mem file: %w", err)
	}
	if fs.onClose != nil {
		f, _ := fs.fs.Open(fs.filePath)
		if err := fs.onClose(filepath.Base(fs.filePath), f); err != nil {
			return fmt.Errorf("execute onClose callback: %w", err)
		}
	}
	return nil
}
