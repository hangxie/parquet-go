package writer

import (
	"context"
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/types"
)

// CSVWriter writes CSV-style data to parquet files.
//
// A CSVWriter must not be used by multiple goroutines concurrently. Callers
// must serialize all operations on an instance. WithNP controls internal
// parallelism and does not make concurrent method calls safe.
type CSVWriter struct {
	ParquetWriter
}

// NewCSVWriterFromWriter creates a CSVWriter from an io.Writer.
//
// Deprecated: use NewCSVWriterFromWriterWithContext.
func NewCSVWriterFromWriter(md []string, w io.Writer, opts ...WriterOption) (*CSVWriter, error) {
	return NewCSVWriterFromWriterWithContext(context.Background(), md, w, opts...)
}

// NewCSVWriterFromWriterWithContext creates a CSVWriter using ctx.
func NewCSVWriterFromWriterWithContext(ctx context.Context, md []string, w io.Writer, opts ...WriterOption) (*CSVWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewCSVWriterWithContext(ctx, md, wf, opts...)
}

// NewCSVWriter creates a CSVWriter from a schema metadata list and a ParquetFileWriter.
//
// Deprecated: use NewCSVWriterWithContext.
func NewCSVWriter(md []string, pfile source.ParquetFileWriter, opts ...WriterOption) (*CSVWriter, error) {
	return NewCSVWriterWithContext(context.Background(), md, pfile, opts...)
}

// NewCSVWriterWithContext creates a CSVWriter using ctx.
func NewCSVWriterWithContext(ctx context.Context, md []string, pfile source.ParquetFileWriter, opts ...WriterOption) (*CSVWriter, error) {
	res := new(CSVWriter)
	if err := res.initBase(ctx, pfile, opts...); err != nil {
		return nil, fmt.Errorf("init CSV writer base: %w", err)
	}

	var err error
	res.SchemaHandler, err = schema.NewSchemaHandlerFromMetadata(md)
	if err != nil {
		return nil, fmt.Errorf("create schema from metadata: %w", err)
	}
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.marshalFunc = marshal.MarshalCSV
	if err = res.initBloomFilters(); err != nil {
		return nil, fmt.Errorf("init bloom filters: %w", err)
	}
	if err = res.validateEncryptionColumnKeys(); err != nil {
		return nil, fmt.Errorf("validate encryption column keys: %w", err)
	}

	res.stopped = false
	return res, nil
}

// WriteString writes string values to parquet file.
//
// Deprecated: use WriteStringWithContext.
func (w *CSVWriter) WriteString(recsi any) error {
	return w.WriteStringWithContext(w.defaultContext(), recsi)
}

// WriteStringWithContext writes string values using ctx.
func (w *CSVWriter) WriteStringWithContext(ctx context.Context, recsi any) error {
	if err := w.setContext(ctx); err != nil {
		return err
	}
	var err error
	recs, ok := recsi.([]*string)
	if !ok {
		return fmt.Errorf("WriteString: expected []*string, got %T", recsi)
	}
	lr := len(recs)
	rec := make([]any, lr)
	for i := range lr {
		rec[i] = nil
		if recs[i] != nil {
			rec[i], err = types.StrToParquetTypeWithLogical(
				*recs[i],
				w.SchemaHandler.SchemaElements[i+1].Type,
				w.SchemaHandler.SchemaElements[i+1].ConvertedType,
				w.SchemaHandler.SchemaElements[i+1].LogicalType,
				int(w.SchemaHandler.SchemaElements[i+1].GetTypeLength()),
				int(w.SchemaHandler.SchemaElements[i+1].GetScale()),
			)
			if err != nil {
				return fmt.Errorf("convert string to parquet type: %w", err)
			}
		}
	}

	if err := w.WriteWithContext(ctx, rec); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	return nil
}
