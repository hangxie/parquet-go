package writer

import (
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

// ArrowWriter is a writer for Arrow record batches to parquet files.
type ArrowWriter struct {
	ParquetWriter
}

// NewArrowWriterFromWriter creates an ArrowWriter from an io.Writer.
func NewArrowWriterFromWriter(arrowSchema *arrow.Schema, w io.Writer, opts ...WriterOption) (*ArrowWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewArrowWriter(arrowSchema, wf, opts...)
}

// NewArrowWriter creates a parquet writer from an Arrow schema.
// The default compression for Arrow writers is GZIP (unlike SNAPPY for other writers).
// Use WithCompressionType to override.
func NewArrowWriter(arrowSchema *arrow.Schema, pfile source.ParquetFileWriter, opts ...WriterOption) (*ArrowWriter, error) {
	res := new(ArrowWriter)
	// ArrowWriter defaults to GZIP; user opts come after and can override.
	allOpts := append([]WriterOption{WithCompressionType(parquet.CompressionCodec_GZIP)}, opts...)
	if err := res.initBase(pfile, allOpts...); err != nil {
		return nil, err
	}

	var err error
	res.SchemaHandler, err = schema.NewSchemaHandlerFromArrow(arrowSchema)
	if err != nil {
		return nil, fmt.Errorf("create schema from arrow definition: %w", err)
	}
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.marshalFunc = marshal.MarshalArrow
	res.initBloomFilters()

	res.stopped = false
	return res, nil
}

// WriteArrow writes an Arrow RecordBatch to the parquet file.
// It transposes columnar Arrow data into row-oriented format for parquet-go.
func (w *ArrowWriter) WriteArrow(batch arrow.RecordBatch) error {
	table := make([][]any, 0)
	for i, column := range batch.Columns() {
		columnFromRecord, err := common.ArrowColToParquetCol(
			batch.Schema().Field(i), column)
		if err != nil {
			return fmt.Errorf("arrow column conversion: %w", err)
		}

		if len(columnFromRecord) > 0 {
			table = append(table, columnFromRecord)
		}
	}
	transposedTable := common.TransposeTable(table)
	for _, row := range transposedTable {
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	return nil
}
