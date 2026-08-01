package writer

import (
	"context"
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

// ArrowWriter writes Arrow record batches to parquet files.
//
// An ArrowWriter must not be used by multiple goroutines concurrently. Callers
// must serialize all operations on an instance. WithNP controls internal
// parallelism and does not make concurrent method calls safe.
type ArrowWriter struct {
	ParquetWriter
}

// NewArrowWriterFromWriter creates an ArrowWriter from an io.Writer.
//
// Deprecated: use NewArrowWriterFromWriterWithContext.
func NewArrowWriterFromWriter(arrowSchema *arrow.Schema, w io.Writer, opts ...WriterOption) (*ArrowWriter, error) {
	return NewArrowWriterFromWriterWithContext(context.Background(), arrowSchema, w, opts...)
}

// NewArrowWriterFromWriterWithContext creates an ArrowWriter using ctx.
func NewArrowWriterFromWriterWithContext(ctx context.Context, arrowSchema *arrow.Schema, w io.Writer, opts ...WriterOption) (*ArrowWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewArrowWriterWithContext(ctx, arrowSchema, wf, opts...)
}

// NewArrowWriter creates a parquet writer from an Arrow schema.
// The default compression for Arrow writers is GZIP (unlike SNAPPY for other writers).
// Use WithCompressionCodec to override.
//
// Deprecated: use NewArrowWriterWithContext.
func NewArrowWriter(arrowSchema *arrow.Schema, pfile source.ParquetFileWriter, opts ...WriterOption) (*ArrowWriter, error) {
	return NewArrowWriterWithContext(context.Background(), arrowSchema, pfile, opts...)
}

// NewArrowWriterWithContext creates an ArrowWriter using ctx.
func NewArrowWriterWithContext(ctx context.Context, arrowSchema *arrow.Schema, pfile source.ParquetFileWriter, opts ...WriterOption) (*ArrowWriter, error) {
	res := new(ArrowWriter)
	// ArrowWriter defaults to GZIP; user opts come after and can override.
	allOpts := append([]WriterOption{WithCompressionCodec(parquet.CompressionCodec_GZIP)}, opts...)
	if err := res.initBase(ctx, pfile, allOpts...); err != nil {
		return nil, fmt.Errorf("init arrow writer base: %w", err)
	}

	var err error
	res.SchemaHandler, err = schema.NewSchemaHandlerFromArrow(arrowSchema)
	if err != nil {
		return nil, fmt.Errorf("create schema from arrow definition: %w", err)
	}
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.marshalFunc = marshal.MarshalArrow
	if err = res.validateSortingColumns(); err != nil {
		return nil, fmt.Errorf("validate sorting columns: %w", err)
	}
	if err = res.initBloomFilters(); err != nil {
		return nil, fmt.Errorf("init bloom filters: %w", err)
	}
	if err = res.validateEncryptionColumnKeys(); err != nil {
		return nil, fmt.Errorf("validate encryption column keys: %w", err)
	}
	if err = res.writeMagicHeader(); err != nil {
		return nil, fmt.Errorf("write magic header: %w", err)
	}

	res.stopped = false
	return res, nil
}

// WriteArrow writes an Arrow RecordBatch to the parquet file.
// It transposes columnar Arrow data into row-oriented format for parquet-go.
//
// Deprecated: use WriteArrowWithContext.
func (w *ArrowWriter) WriteArrow(batch arrow.RecordBatch) error {
	return w.WriteArrowWithContext(w.defaultContext(), batch)
}

// WriteArrowWithContext writes an Arrow record batch using ctx.
func (w *ArrowWriter) WriteArrowWithContext(ctx context.Context, batch arrow.RecordBatch) error {
	if err := w.setContext(ctx); err != nil {
		return err
	}
	table := make([][]any, 0)
	for i, column := range batch.Columns() {
		columnFromRecord, err := common.ArrowColToParquetCol(
			batch.Schema().Field(i), column,
		)
		if err != nil {
			return fmt.Errorf("arrow column conversion: %w", err)
		}

		if len(columnFromRecord) > 0 {
			table = append(table, columnFromRecord)
		}
	}
	transposedTable := common.TransposeTable(table)
	for _, row := range transposedTable {
		if err := w.WriteWithContext(ctx, row); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	return nil
}
