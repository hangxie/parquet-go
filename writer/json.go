package writer

import (
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
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
