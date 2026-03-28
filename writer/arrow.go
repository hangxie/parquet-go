package writer

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
)

// ArrowWriter extending the base ParquetWriter
type ArrowWriter struct {
	ParquetWriter
}

// NewArrowWriter creates arrow schema parquet writer given the native
// arrow schema, parquet file writer which contains the parquet file in
// which we will write the record.
func NewArrowWriter(arrowSchema *arrow.Schema, pfile source.ParquetFileWriter, opts ...WriterOption) (*ArrowWriter, error) {
	cfg := defaultWriterConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, fmt.Errorf("apply writer option: %w", err)
		}
	}
	if cfg.rowGroupSize < cfg.pageSize {
		return nil, fmt.Errorf("row group size (%d) must be >= page size (%d)", cfg.rowGroupSize, cfg.pageSize)
	}

	res := new(ArrowWriter)
	sh, err := schema.NewSchemaHandlerFromArrow(arrowSchema)
	if err != nil {
		return nil, fmt.Errorf("create schema from arrow definition: %w", err)
	}
	res.SchemaHandler = sh

	applyWriterConfig(&res.ParquetWriter, cfg)
	res.PFile = pfile
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)

	if err := res.createCompressors(cfg); err != nil {
		return nil, err
	}

	res.MarshalFunc = marshal.MarshalArrow

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

// WriteArrow wraps the base Write function provided by writer.ParquetWriter.
// The function transforms the data from the record, which the go arrow library
// gives as array of columns, to array of rows which the parquet-go library
// can understand as it does not accepts data by columns, but rather by rows.
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
