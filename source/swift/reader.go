package swiftsource

import (
	"fmt"

	"github.com/ncw/swift"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile-time check that *swiftReader implements source.ParquetFileReader.
var _ source.ParquetFileReader = (*swiftReader)(nil)

type swiftReader struct {
	swiftFile
	fileReader *streamCursor
}

// NewSwiftFileReader creates a Swift reader with lazy independent streams.
func NewSwiftFileReader(container, filePath string, conn *swift.Connection) (source.ParquetFileReader, error) {
	cursor, err := newStreamCursor(newReaderPool(conn, container), filePath)
	if err != nil {
		return nil, fmt.Errorf("open swift file: %w", err)
	}
	return &swiftReader{swiftFile: swiftFile{connection: conn, container: container, filePath: filePath}, fileReader: cursor}, nil
}

func (file *swiftReader) Open(name string) (source.ParquetFileReader, error) {
	var pool *readerPool
	if file.fileReader != nil {
		pool = file.fileReader.pool
	} else {
		pool = newReaderPool(file.connection, file.container)
	}
	fr, err := newStreamCursor(pool, name)
	if err != nil {
		return nil, err
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
	if file.fileReader == nil {
		return file.Open(file.filePath)
	}
	fr, err := file.fileReader.Clone()
	if err != nil {
		return nil, err
	}

	return &swiftReader{
		swiftFile: swiftFile{
			connection: file.connection,
			container:  file.container,
			filePath:   file.filePath,
		},
		fileReader: fr,
	}, nil
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
			return fmt.Errorf("close Swift reader: %w", err)
		}
	}
	return nil
}
