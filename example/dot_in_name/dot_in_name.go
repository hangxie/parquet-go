//go:build example

package main

import (
	"log"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type A struct {
	V1 int32 `parquet:"name=b.c, type=INT32, encoding=PLAIN"`
	V2 B     `parquet:"name=b"`
	V3 int32 `parquet:"name=c, type=INT32, encoding=PLAIN"`
}

type B struct {
	C int32 `parquet:"name=c, type=INT32, encoding=PLAIN"`
}

func main() {
	var err error
	fw, err := local.NewLocalFileWriter("/tmp/a.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	// write
	pw, err := writer.NewParquetWriter(
		fw, new(A),
		writer.WithRowGroupSize(common.DefaultRowGroupSize),
		writer.WithPageSize(common.DefaultPageSize),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	num := 10
	for range num {
		o := A{
			V1: 1,
			V2: B{
				C: 2,
			},
			V3: 3,
		}
		if err = pw.Write(o); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	_ = fw.Close()

	///read all
	fr, err := local.NewLocalFileReader("/tmp/a.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, new(A), reader.WithNP(4))
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	os := make([]A, num)

	if err = pr.Read(&os); err != nil {
		log.Println("Read error", err)
	}
	log.Println(os)

	_ = pr.ReadStop()
	_ = fr.Close()

	///read column by path
	fr, err = local.NewLocalFileReader("/tmp/a.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err = reader.NewParquetReader(fr, new(A), reader.WithNP(4))
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	cn := pr.GetNumRows()
	// "." is an ordinary character in a name, so "b.c" is a single component:
	// this addresses the column literally named b.c, distinct from the nested b -> c.
	v1, _, _, _ := pr.ReadColumnByPath(common.PathToStr([]string{"parquet_go_root", "b.c"}), cn)
	v2, _, _, _ := pr.ReadColumnByPath(common.PathToStr([]string{"parquet_go_root", "b", "c"}), cn)
	v3, _, _, _ := pr.ReadColumnByPath(common.PathToStr([]string{"parquet_go_root", "c"}), cn)
	log.Println(v1, v2, v3)

	_ = pr.ReadStop()
	_ = fr.Close()
}
