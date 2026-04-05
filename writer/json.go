package writer

import (
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/types"
)

type JSONWriter struct {
	ParquetWriter
}

func NewJSONWriterFromWriter(jsonSchema string, w io.Writer, np int64) (*JSONWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewJSONWriter(jsonSchema, wf, np)
}

// Create JSON writer
func NewJSONWriter(jsonSchema string, pfile source.ParquetFileWriter, np int64) (*JSONWriter, error) {
	var err error
	res := new(JSONWriter)
	res.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema)
	if err != nil {
		return res, err
	}

	res.PFile = pfile
	res.pageSize = common.DefaultPageSize         // 8K
	res.rowGroupSize = common.DefaultRowGroupSize // 128M
	res.compressionType = parquet.CompressionCodec_SNAPPY
	res.pagesMapBuf = make(map[string][]*layout.Page)
	res.np = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.offset = 4
	_, err = res.PFile.Write([]byte("PAR1"))
	res.marshalFunc = marshal.MarshalJSON
	res.initBloomFilters()
	return res, err
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
