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

type CSVWriter struct {
	ParquetWriter
}

func NewCSVWriterFromWriter(md []string, w io.Writer, opts ...WriterOption) (*CSVWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewCSVWriter(md, wf, opts...)
}

// NewCSVWriter creates a CSV writer with functional options.
func NewCSVWriter(md []string, pfile source.ParquetFileWriter, opts ...WriterOption) (*CSVWriter, error) {
	cfg := defaultWriterConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, fmt.Errorf("apply writer option: %w", err)
		}
	}
	if cfg.rowGroupSize < cfg.pageSize {
		return nil, fmt.Errorf("row group size (%d) must be >= page size (%d)", cfg.rowGroupSize, cfg.pageSize)
	}

	res := new(CSVWriter)
	sh, err := schema.NewSchemaHandlerFromMetadata(md)
	if err != nil {
		return nil, fmt.Errorf("create schema from metadata: %w", err)
	}
	res.SchemaHandler = sh

	applyWriterConfig(&res.ParquetWriter, cfg)
	res.PFile = pfile
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)

	if err := res.createCompressors(cfg); err != nil {
		return nil, err
	}

	res.MarshalFunc = marshal.MarshalCSV

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

// Write string values to parquet file
func (w *CSVWriter) WriteString(recsi any) error {
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
