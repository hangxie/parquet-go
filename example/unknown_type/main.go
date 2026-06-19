//go:build example

package main

import (
	"fmt"
	"log"

	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Row contains an always-null column using the UNKNOWN logical type.
// NullCol must be *int32 (OPTIONAL INT32) and must always be written as nil.
type Row struct {
	ID      int32  `parquet:"name=id, type=INT32"`
	NullCol *int32 `parquet:"name=null_col, type=INT32, logicaltype=UNKNOWN, repetitiontype=OPTIONAL"`
}

func main() {
	const path = "/tmp/unknown-type.parquet"

	// Write
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		log.Fatal(err)
	}
	pw, err := writer.NewParquetWriter(fw, new(Row), writer.WithNP(1))
	if err != nil {
		log.Fatal(err)
	}
	for i := range 3 {
		if err := pw.Write(Row{ID: int32(i + 1), NullCol: nil}); err != nil {
			log.Fatal(err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		log.Fatal(err)
	}
	_ = fw.Close()

	// Read
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Fatal(err)
	}
	pr, err := reader.NewParquetReader(fr, new(Row), reader.WithNP(1))
	if err != nil {
		log.Fatal(err)
	}
	defer pr.ReadStop()

	rows := make([]Row, pr.GetNumRows())
	if err := pr.Read(&rows); err != nil {
		log.Fatal(err)
	}
	for _, r := range rows {
		fmt.Printf("ID=%d NullCol=%v\n", r.ID, r.NullCol)
	}
}
