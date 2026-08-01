package writer

import (
	"context"
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

// JSONWriter writes JSON-schema-defined data to parquet files.
//
// A JSONWriter must not be used by multiple goroutines concurrently. Callers
// must serialize all operations on an instance. WithNP controls internal
// parallelism and does not make concurrent method calls safe.
type JSONWriter struct {
	ParquetWriter
}

// NewJSONWriterFromWriter creates a JSONWriter from an io.Writer.
//
// Deprecated: use NewJSONWriterFromWriterWithContext.
func NewJSONWriterFromWriter(jsonSchema string, w io.Writer, opts ...WriterOption) (*JSONWriter, error) {
	return NewJSONWriterFromWriterWithContext(context.Background(), jsonSchema, w, opts...)
}

// NewJSONWriterFromWriterWithContext creates a JSONWriter using ctx.
func NewJSONWriterFromWriterWithContext(ctx context.Context, jsonSchema string, w io.Writer, opts ...WriterOption) (*JSONWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewJSONWriterWithContext(ctx, jsonSchema, wf, opts...)
}

// NewJSONWriter creates a JSONWriter from a JSON schema string and a ParquetFileWriter.
//
// Deprecated: use NewJSONWriterWithContext.
func NewJSONWriter(jsonSchema string, pfile source.ParquetFileWriter, opts ...WriterOption) (*JSONWriter, error) {
	return NewJSONWriterWithContext(context.Background(), jsonSchema, pfile, opts...)
}

// NewJSONWriterWithContext creates a JSONWriter using ctx.
func NewJSONWriterWithContext(ctx context.Context, jsonSchema string, pfile source.ParquetFileWriter, opts ...WriterOption) (*JSONWriter, error) {
	res := new(JSONWriter)
	if err := res.initBase(ctx, pfile, opts...); err != nil {
		return nil, fmt.Errorf("init JSON writer base: %w", err)
	}

	var err error
	res.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("create schema from JSON: %w", err)
	}
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.marshalFunc = marshal.MarshalJSON
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
