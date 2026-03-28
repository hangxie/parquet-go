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
// Close() will pass the filename string and data as io.reader
type OnCloseFunc func(string, io.Reader) error

type memFile struct {
	filePath string
	file     afero.File
	onClose  OnCloseFunc
}

// Compile time check that *memFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*memWriter)(nil)

// memWriter - ParquetFileWriter type for in-memory file operations
type memWriter struct {
	memFile
	fs afero.Fs
}

// NewMemFileWriter initiates and creates an instance of MemFiles.
// NOTE: there is no NewMemFileReader as this particular type was written
// to handle in-memory conversions and offloading. The results of
// conversion can then be stored and read via HDFS, LocalFS, etc without
// the need for loading the file back into memory directly
func NewMemFileWriter(name string, f OnCloseFunc) (source.ParquetFileWriter, error) {
	fs := afero.NewMemMapFs()

	var m memWriter
	m.onClose = f
	m.fs = fs
	w, err := m.Create(name)
	if err != nil {
		return nil, fmt.Errorf("create mem file: %w", err)
	}
	return w, nil
}

// NewMemFileWriterWithFs creates an in-memory file writer using the provided
// filesystem instance, enabling isolation between writers.
func NewMemFileWriterWithFs(name string, f OnCloseFunc, fs afero.Fs) (source.ParquetFileWriter, error) {
	if fs == nil {
		return nil, fmt.Errorf("filesystem must not be nil")
	}
	var m memWriter
	m.onClose = f
	m.fs = fs
	w, err := m.Create(name)
	if err != nil {
		return nil, fmt.Errorf("create mem file: %w", err)
	}
	return w, nil
}

// Create - create in-memory file
func (w *memWriter) Create(name string) (source.ParquetFileWriter, error) {
	file, err := w.fs.Create(name)
	if err != nil {
		return w, fmt.Errorf("memfs create: %w", err)
	}

	w.file = file
	w.filePath = name
	return w, nil
}

// Write - write file in-memory
func (w *memWriter) Write(b []byte) (n int, err error) {
	return w.file.Write(b)
}

// Close - close file and execute OnCloseFunc
func (w *memWriter) Close() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close mem file: %w", err)
	}
	if w.onClose != nil {
		f, err := w.fs.Open(w.filePath)
		if err != nil {
			return fmt.Errorf("reopen mem file for onClose: %w", err)
		}
		if err := w.onClose(filepath.Base(w.filePath), f); err != nil {
			return fmt.Errorf("execute onClose callback: %w", err)
		}
	}
	return nil
}
