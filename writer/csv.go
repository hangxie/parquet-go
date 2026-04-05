package writer

import (
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/types"
)

// CSVWriter is a writer for CSV-style data to parquet files.
type CSVWriter struct {
	ParquetWriter
}

// NewCSVWriterFromWriter creates a CSVWriter from an io.Writer.
func NewCSVWriterFromWriter(md []string, w io.Writer, opts ...WriterOption) (*CSVWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewCSVWriter(md, wf, opts...)
}

// NewCSVWriter creates a CSVWriter from a schema metadata list and a ParquetFileWriter.
func NewCSVWriter(md []string, pfile source.ParquetFileWriter, opts ...WriterOption) (*CSVWriter, error) {
	res := new(CSVWriter)
	if err := res.initBase(pfile, opts...); err != nil {
		return nil, err
	}

	var err error
	res.SchemaHandler, err = schema.NewSchemaHandlerFromMetadata(md)
	if err != nil {
		return nil, fmt.Errorf("create schema from metadata: %w", err)
	}
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.marshalFunc = marshal.MarshalCSV
	res.initBloomFilters()

	res.stopped = false
	return res, nil
}

// WriteString writes string values to parquet file.
func (w *CSVWriter) WriteString(recsi any) error {
	var err error
	recs := recsi.([]*string)
	lr := len(recs)
	rec := make([]any, lr)
	for i := range lr {
		rec[i] = nil
		if recs[i] != nil {
			rec[i], err = types.StrToParquetTypeWithLogical(*recs[i],
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

	if err := w.Write(rec); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	return nil
}
