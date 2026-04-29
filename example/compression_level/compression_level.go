//go:build example

package main

import (
	"log"
	"os"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/writer"
)

type Data struct {
	// Name uses the default compression (defined at writer level)
	Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	// Description explicitly uses GZIP compression via tags
	Description string `parquet:"name=description, type=BYTE_ARRAY, convertedtype=UTF8, compression=GZIP"`
	// Data explicitly uses ZSTD compression via tags
	Content string `parquet:"name=content, type=BYTE_ARRAY, convertedtype=UTF8, compression=ZSTD"`
}

func main() {
	f, err := os.Create("compression_example.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Create a writer with:
	// 1. Default compression set to SNAPPY
	// 2. GZIP compression level set to 9 (will be used by 'Description' column)
	// 3. ZSTD compression level set to 10 (will be used by 'Content' column)
	pw, err := writer.NewParquetWriterFromWriter(f, new(Data),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		writer.WithCompressionLevel(parquet.CompressionCodec_GZIP, 9),
		writer.WithCompressionLevel(parquet.CompressionCodec_ZSTD, 10),
	)
	if err != nil {
		log.Fatal(err)
	}

	data := []Data{
		{Name: "File 1", Description: "Highly compressed description", Content: "ZSTD compressed content"},
		{Name: "File 2", Description: "Another description", Content: "More content"},
	}

	for _, d := range data {
		if err := pw.Write(d); err != nil {
			log.Fatal(err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		log.Fatal(err)
	}

	log.Println("Successfully wrote parquet file with mixed compression and custom levels")
}
