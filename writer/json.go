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

type JSONWriter struct {
	ParquetWriter
}

func NewJSONWriterFromWriter(jsonSchema string, w io.Writer, opts ...WriterOption) (*JSONWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewJSONWriter(jsonSchema, wf, opts...)
}

// NewJSONWriter creates a JSON writer with functional options.
func NewJSONWriter(jsonSchema string, pfile source.ParquetFileWriter, opts ...WriterOption) (*JSONWriter, error) {
	cfg := defaultWriterConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, fmt.Errorf("apply writer option: %w", err)
		}
	}
	if cfg.rowGroupSize < cfg.pageSize {
		return nil, fmt.Errorf("row group size (%d) must be >= page size (%d)", cfg.rowGroupSize, cfg.pageSize)
	}

	res := new(JSONWriter)
	sh, err := schema.NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("create schema from JSON: %w", err)
	}
	res.SchemaHandler = sh

	applyWriterConfig(&res.ParquetWriter, cfg)
	res.PFile = pfile
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)

	if err := res.createCompressors(cfg); err != nil {
		return nil, err
	}

	res.MarshalFunc = marshal.MarshalJSON

	if err := res.SchemaHandler.ValidateEncodingsForDataPageVersion(res.dataPageVersion); err != nil {
		return nil, fmt.Errorf("encoding validation: %w", err)
	}

	if _, err := res.PFile.Write([]byte("PAR1")); err != nil {
		return nil, fmt.Errorf("write magic header: %w", err)
	}
	res.initBloomFilters()
	res.headerWritten = true
	res.stopped = false
	return res, nil
}

// WriteString writes string values to parquet file
func (w *JSONWriter) WriteString(recsi any) error {
	// WriteString uses MarshalCSV since we're passing []any, not JSON
	w.MarshalFunc = marshal.MarshalCSV

	var err error
	recs := recsi.([]*string)
	lr := len(recs)
	rec := make([]any, lr)
	for i := range lr {
		rec[i] = nil
		if recs[i] != nil {
			rec[i], err = types.StrToParquetType(*recs[i],
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
