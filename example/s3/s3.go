//go:build example

// Writes a parquet file to S3 and reads it back.
//
// Usage:
//
//	AWS_PROFILE=aws-profile-name AWS_REGION=us-east-1 TEST_S3_BUCKET=test-bucket-you-can-read-write go run -tags example ./example/s3
//
// AWS_REGION must be the bucket's region; AWS_PROFILE and AWS_REGION can be omitted when the
// default AWS configuration already supplies them.
//
// Every run overwrites parquet-go-example/s3.parquet in the bucket, so use a bucket kept for testing.
package main

import (
	"context"
	"log"
	"os"
	"slices"

	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/writer"

	"github.com/hangxie/parquet-go/v3/source/s3v2"
)

type Student struct {
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING"`
	Age    int32   `parquet:"name=age, type=INT32"`
	ID     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
	bucket, key := os.Getenv("TEST_S3_BUCKET"), "parquet-go-example/s3.parquet"
	if bucket == "" {
		log.Fatal("TEST_S3_BUCKET is not set")
	}
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

	// write: use s3v2.NewS3FileWriterWithClient to supply your own S3 client
	fw, err := s3v2.NewS3FileWriter(ctx, bucket, key, nil)
	if err != nil {
		log.Fatal("Can't create S3 file writer: ", err)
	}
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
	// Close blocks until the upload completes
	if err = fw.Close(); err != nil {
		log.Fatal("Can't close S3 file writer: ", err)
	}
	log.Printf("Wrote %d rows to s3://%s/%s", len(students), bucket, key)

	// read: a nil version reads the latest version of the object
	fr, err := s3v2.NewS3FileReader(ctx, bucket, key, nil)
	if err != nil {
		log.Fatal("Can't create S3 file reader: ", err)
	}
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
		log.Fatal("Can't close S3 file reader: ", err)
	}

	if !slices.Equal(students, got) {
		log.Fatalf("Read %d rows that differ from the %d rows written", len(got), len(students))
	}
	log.Printf("Read %d rows back, identical to what was written", len(got))
}
