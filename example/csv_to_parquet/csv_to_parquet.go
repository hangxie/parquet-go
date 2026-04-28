//go:build example

package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

type Shoe struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	var err error

	fw, err := local.NewLocalFileWriter("/tmp/shoes.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(Shoe),
		writer.WithNP(2),
		writer.WithRowGroupSize(common.DefaultRowGroupSize),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	csvFile, err := os.Open("shoes.csv")
	if err != nil {
		log.Println("Can't open CSV file", err)
		return
	}
	defer csvFile.Close()
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
