//go:build example

// Writes a parquet file to Google Cloud Storage and reads it back.
//
// Usage:
//
//	TEST_GCS_BUCKET=test-bucket-you-can-read-write go run -tags example ./example/gcs
//
// Credentials come from application default credentials; GOOGLE_CLOUD_PROJECT is optional.
//
// Every run overwrites parquet-go-example/gcs.parquet in the bucket, so use a bucket kept for testing.
package main

import (
	"context"
	"log"
	"os"
	"slices"

	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/writer"

	"github.com/hangxie/parquet-go/v3/source/gcs"
)

type Student struct {
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING"`
	Age    int32   `parquet:"name=age, type=INT32"`
	ID     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
	bucket, object := os.Getenv("TEST_GCS_BUCKET"), "parquet-go-example/gcs.parquet"
	if bucket == "" {
		log.Fatal("TEST_GCS_BUCKET is not set")
	}
	// the source only records the project, object access does not depend on it
	project := os.Getenv("GOOGLE_CLOUD_PROJECT")
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

	// write: use gcs.NewGcsFileWriterWithClient to supply your own storage client
	fw, err := gcs.NewGcsFileWriter(ctx, project, bucket, object)
	if err != nil {
		log.Fatal("Can't create GCS file writer: ", err)
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
	// Close commits the upload, the object does not exist before it returns
	if err = fw.Close(); err != nil {
		log.Fatal("Can't close GCS file writer: ", err)
	}
	log.Printf("Wrote %d rows to gs://%s/%s", len(students), bucket, object)

	// read: generation -1 reads the latest generation of the object
	fr, err := gcs.NewGcsFileReader(ctx, project, bucket, object, -1)
	if err != nil {
		log.Fatal("Can't create GCS file reader: ", err)
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
		log.Fatal("Can't close GCS file reader: ", err)
	}

	if !slices.Equal(students, got) {
		log.Fatalf("Read %d rows that differ from the %d rows written", len(got), len(students))
	}
	log.Printf("Read %d rows back, identical to what was written", len(got))
}
