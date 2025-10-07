package swiftsource

import (
	"fmt"

	"github.com/ncw/swift"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *swiftFile implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*swiftWriter)(nil)

type swiftWriter struct {
	swiftFile
	fileWriter *swift.ObjectCreateFile
}

func NewSwiftFileWriter(container, filePath string, conn *swift.Connection) (source.ParquetFileWriter, error) {
	res := swiftWriter{
		swiftFile: swiftFile{
			connection: conn,
			container:  container,
			filePath:   filePath,
		},
	}

	w, err := res.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("create swift file: %w", err)
	}
	return w, nil
}

func (file *swiftWriter) Create(name string) (source.ParquetFileWriter, error) {
	fw, err := file.connection.ObjectCreate(file.container, name, false, "", "", nil)
	if err != nil {
		return nil, fmt.Errorf("swift object create: %w", err)
	}

	res := &swiftWriter{
		swiftFile: swiftFile{
			connection: file.connection,
			container:  file.container,
			filePath:   name,
		},
		fileWriter: fw,
	}

	return res, nil
}

func (file *swiftWriter) Write(p []byte) (n int, err error) {
	if file.fileWriter == nil {
		return 0, fmt.Errorf("fileWriter is nil")
	}
	return file.fileWriter.Write(p)
}

func (file *swiftWriter) Close() error {
	if file.fileWriter != nil {
		if err := file.fileWriter.Close(); err != nil {
			return fmt.Errorf("failed to close Swift writer: %w", err)
		}
	}
	return nil
}
