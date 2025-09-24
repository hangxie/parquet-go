package writer

import (
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/marshal"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
	"github.com/hangxie/parquet-go/v2/types"
)

type CSVWriter struct {
	ParquetWriter
}

func NewCSVWriterFromWriter(md []string, w io.Writer, np int64) (*CSVWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewCSVWriter(md, wf, np)
}

// Create CSV writer
func NewCSVWriter(md []string, pfile source.ParquetFileWriter, np int64) (*CSVWriter, error) {
	var err error
	res := new(CSVWriter)
	res.SchemaHandler, err = schema.NewSchemaHandlerFromMetadata(md)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema from metadata: %s", err.Error())
	}
	res.PFile = pfile
	res.PageSize = common.DefaultPageSize         // 8K
	res.RowGroupSize = common.DefaultRowGroupSize // 128M
	res.CompressionType = parquet.CompressionCodec_SNAPPY
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.Offset = 4
	_, err = res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = marshal.MarshalCSV
	return res, err
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
				int(w.SchemaHandler.SchemaElements[i+1].GetTypeLength()),
				int(w.SchemaHandler.SchemaElements[i+1].GetScale()),
			)
			if err != nil {
				return err
			}
		}
	}

	return w.Write(rec)
}
