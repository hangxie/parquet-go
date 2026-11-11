//go:build example

// Writes a parquet file to an in-memory buffer and reads it back.
package main

import (
	"context"
	"log"
	"slices"

	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/writer"

	"github.com/hangxie/parquet-go/v3/source/buffer"
)

type Student struct {
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING"`
	Age    int32   `parquet:"name=age, type=INT32"`
	ID     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
	ctx := context.Background()

	students := make([]Student, 100)
	for i := range students {
		students[i] = Student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			ID:     int64(i),
			Weight: 50.0 + float32(i)*0.1,
			Sex:    i%2 == 0,
		}
	}

	// write
	fw := buffer.NewBufferWriter()
	pw, err := writer.NewParquetWriterWithContext(ctx, fw, new(Student))
	if err != nil {
		log.Fatal("Can't create parquet writer: ", err)
	}
	for _, stu := range students {
		if err = pw.WriteWithContext(ctx, stu); err != nil {
			log.Fatal("Write error: ", err)
		}
	}
	// write the parquet file footer
	if err = pw.WriteStopWithContext(ctx); err != nil {
		log.Fatal("WriteStop error: ", err)
	}
	if err = fw.Close(); err != nil {
		log.Fatal("Can't close buffer writer: ", err)
	}
	log.Printf("Wrote %d rows to a %d byte buffer", len(students), len(fw.Bytes()))

	// read: the bytes written above are a complete parquet file
	fr := buffer.NewBufferReaderFromBytes(fw.Bytes())
	pr, err := reader.NewParquetReaderWithContext(ctx, fr, new(Student), reader.WithNP(4))
	if err != nil {
		log.Fatal("Can't create parquet reader: ", err)
	}
	got := make([]Student, pr.GetNumRows())
	if err = pr.ReadWithContext(ctx, &got); err != nil {
		log.Fatal("Read error: ", err)
	}
	if err = pr.ReadStopWithContext(ctx); err != nil {
		log.Fatal("ReadStop error: ", err)
	}
	if err = fr.Close(); err != nil {
		log.Fatal("Can't close buffer reader: ", err)
	}

	if !slices.Equal(students, got) {
		log.Fatalf("Read %d rows that differ from the %d rows written", len(got), len(students))
	}
	log.Printf("Read %d rows back, identical to what was written", len(got))
}
