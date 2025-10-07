package swiftsource

import (
	"fmt"

	"github.com/ncw/swift"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *swiftFile implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*swiftReader)(nil)

type swiftReader struct {
	swiftFile
	fileReader *swift.ObjectOpenFile
}

func NewSwiftFileReader(container, filePath string, conn *swift.Connection) (source.ParquetFileReader, error) {
	res := swiftReader{
		swiftFile: swiftFile{
			connection: conn,
			container:  container,
			filePath:   filePath,
		},
	}
	r, err := res.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open swift file: %w", err)
	}
	return r, nil
}

func (file *swiftReader) Open(name string) (source.ParquetFileReader, error) {
	fr, _, err := file.connection.ObjectOpen(file.container, name, false, nil)
	if err != nil {
		return nil, fmt.Errorf("swift object open: %w", err)
	}

	res := &swiftReader{
		swiftFile: swiftFile{
			connection: file.connection,
			container:  file.container,
			filePath:   name,
		},
		fileReader: fr,
	}

	return res, nil
}

func (file swiftReader) Clone() (source.ParquetFileReader, error) {
	return NewSwiftFileReader(file.container, file.filePath, file.connection)
}

func (file *swiftReader) Read(b []byte) (n int, err error) {
	if file.fileReader == nil {
		return 0, fmt.Errorf("fileReader is nil")
	}
	return file.fileReader.Read(b)
}

func (file *swiftReader) Seek(offset int64, whence int) (int64, error) {
	if file.fileReader == nil {
		return 0, fmt.Errorf("fileReader is nil")
	}
	return file.fileReader.Seek(offset, whence)
}

func (file *swiftReader) Close() error {
	if file.fileReader != nil {
		if err := file.fileReader.Close(); err != nil {
			return fmt.Errorf("failed to close Swift reader: %w", err)
		}
	}
	return nil
}
