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

// JSONWriter is a writer for JSON-schema-defined data to parquet files.
type JSONWriter struct {
	ParquetWriter
}

// NewJSONWriterFromWriter creates a JSONWriter from an io.Writer.
func NewJSONWriterFromWriter(jsonSchema string, w io.Writer, opts ...WriterOption) (*JSONWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewJSONWriter(jsonSchema, wf, opts...)
}

// NewJSONWriter creates a JSONWriter from a JSON schema string and a ParquetFileWriter.
func NewJSONWriter(jsonSchema string, pfile source.ParquetFileWriter, opts ...WriterOption) (*JSONWriter, error) {
	res := new(JSONWriter)
	if err := res.initBase(pfile, opts...); err != nil {
		return nil, err
	}

	var err error
	res.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("create schema from JSON: %w", err)
	}
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.marshalFunc = marshal.MarshalJSON
	res.initBloomFilters()

	res.stopped = false
	return res, nil
}

// WriteString writes string values to parquet file.
// Note: this switches the internal marshal function to MarshalCSV because the
// data is passed as []any (not a JSON string). Once WriteString is called,
// subsequent Write calls on this writer will also use MarshalCSV. Do not mix
// Write (JSON) and WriteString (CSV-style) on the same JSONWriter.
func (w *JSONWriter) WriteString(recsi any) error {
	w.marshalFunc = marshal.MarshalCSV

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
