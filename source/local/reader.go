package local

import (
	"os"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile-time check that *localReader implements source.ParquetFileReader.
var _ source.ParquetFileReader = (*localReader)(nil)

type localReader struct {
	cursor *fileCursor
}

func NewLocalFileReader(name string) (source.ParquetFileReader, error) {
	return (&localReader{}).Open(name)
}

func (f *localReader) Open(name string) (source.ParquetFileReader, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	return &localReader{cursor: newFileCursor(file)}, nil
}

func (f localReader) Clone() (source.ParquetFileReader, error) {
	cursor, err := f.cursor.Clone()
	if err != nil {
		return nil, err
	}
	return &localReader{cursor: cursor}, nil
}

func (f *localReader) Seek(offset int64, pos int) (int64, error) {
	return f.cursor.Seek(offset, pos)
}

func (f *localReader) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = f.cursor.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	return cnt, err
}

func (f *localReader) Close() error {
	return f.cursor.Close()
}
