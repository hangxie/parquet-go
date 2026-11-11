//go:build example

// Writes a parquet file to a Go CDK blob bucket and reads it back.
//
// Usage, with an in-memory bucket and then with real storage through each provider's default credentials:
//
//	go run -tags example ./example/gocloud
//	AWS_PROFILE=aws-profile-name TEST_GOCLOUD_URL="s3://test-bucket-you-can-read-write?region=us-east-1" go run -tags example ./example/gocloud
//	TEST_GOCLOUD_URL=gs://test-bucket-you-can-read-write go run -tags example ./example/gocloud
//	AZURE_STORAGE_ACCOUNT=storage-account-name TEST_GOCLOUD_URL=azblob://test-container-you-can-read-write go run -tags example ./example/gocloud
//
// Every run overwrites parquet-go-example/gocloud.parquet in the bucket, so use a bucket kept for testing.
package main

import (
	"context"
	"log"
	"os"
	"slices"

	"gocloud.dev/blob"
	// each driver registers its URL scheme with blob.OpenBucket
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/writer"

	"github.com/hangxie/parquet-go/v3/source/gocloud"
)

type Student struct {
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING"`
	Age    int32   `parquet:"name=age, type=INT32"`
	ID     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
	bucketURL, key := "mem://", "parquet-go-example/gocloud.parquet"
	if v := os.Getenv("TEST_GOCLOUD_URL"); v != "" {
		bucketURL = v
	}
	ctx := context.Background()

	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		log.Fatal("Can't open bucket: ", err)
	}
	defer func() { _ = bucket.Close() }()

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
	fw, err := gocloud.NewBlobWriter(ctx, bucket, key)
	if err != nil {
		log.Fatal("Can't create blob writer: ", err)
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
	// Close commits the blob, it does not exist before it returns
	if err = fw.Close(); err != nil {
		log.Fatal("Can't close blob writer: ", err)
	}
	log.Printf("Wrote %d rows to %s in %s", len(students), key, bucketURL)

	// read
	fr, err := gocloud.NewBlobReader(ctx, bucket, key)
	if err != nil {
		log.Fatal("Can't create blob reader: ", err)
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
		log.Fatal("Can't close blob reader: ", err)
	}

	if !slices.Equal(students, got) {
		log.Fatalf("Read %d rows that differ from the %d rows written", len(got), len(students))
	}
	log.Printf("Read %d rows back, identical to what was written", len(got))
}
