//go:build example

package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/writer"

	"github.com/hangxie/parquet-go/v2/source/s3v2"
)

type Student struct {
	Name   string  `parquet:"name=name, type=UTF8"`
	Age    int32   `parquet:"name=age, type=INT32"`
	ID     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
	ctx := context.Background()
	bucket := "my-bucket"
	key := "test/foobar.parquet"
	num := 100

	// create new S3 file writer
	s3client := s3.NewFromConfig(aws.Config{})
	fw, err := s3v2.NewS3FileWriterWithClient(ctx, s3client, bucket, key, nil)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	// create new parquet file writer
	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	// write 100 student records to the parquet file
	for i := range num {
		stu := Student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			ID:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Sex:    bool(i%2 == 0),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	// write parquet file footer
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop err", err)
	}

	err = fw.Close()
	if err != nil {
		log.Println("Error closing S3 file writer")
	}
	log.Println("Write Finished")

	// read the written parquet file
	// create new S3 file reader
	fr, err := s3v2.NewS3FileReaderWithClient(ctx, s3client, bucket, key, nil)
	if err != nil {
		log.Println("Can't open file")
		return
	}

	// create new parquet file reader
	pr, err := reader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	// read the student rows and print
	num = int(pr.GetNumRows())
	for i := range num / 10 {
		if i%2 == 0 {
			_ = pr.SkipRows(10) // skip 10 rows
			continue
		}
		stus := make([]Student, 10) // read 10 rows
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}

	// close the parquet file
	pr.ReadStop()
	err = fr.Close()
	if err != nil {
		log.Println("Error closing S3 file reader")
	}
	log.Println("Read Finished")
}
