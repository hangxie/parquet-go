//go:build example

package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

type Shoe struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	var err error

	fw, err := local.NewLocalFileWriter("shoes.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Shoe), 2)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = common.DefaultRowGroupSize // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	csvFile, _ := os.Open("shoes.csv")
	reader := csv.NewReader(bufio.NewReader(csvFile))

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		shoe := Shoe{
			ShoeBrand: line[0],
			ShoeName:  line[1],
		}
		if err = pw.Write(shoe); err != nil {
			log.Println("Write error", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}

	log.Println("Write Finished")
	_ = fw.Close()
}
