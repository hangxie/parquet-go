//go:build example

// Writes a parquet file to Azure Blob Storage and reads it back.
//
// Usage:
//
//	AZURE_STORAGE_ACCOUNT=storage-account-name TEST_AZURE_CONTAINER=test-container-you-can-read-write go run -tags example ./example/azblob
//
// Credentials come from the default Azure credential chain, such as an az login session.
//
// Every run overwrites parquet-go-example/azblob.parquet in the container, so use a container kept for testing.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/writer"

	"github.com/hangxie/parquet-go/v3/source/azblob"
)

type Student struct {
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING"`
	Age    int32   `parquet:"name=age, type=INT32"`
	ID     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
	account, container := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("TEST_AZURE_CONTAINER")
	if account == "" || container == "" {
		log.Fatal("AZURE_STORAGE_ACCOUNT and TEST_AZURE_CONTAINER must both be set")
	}
	blobName := "parquet-go-example/azblob.parquet"
	ctx := context.Background()

	// a *blob.SharedKeyCredential built from the account key works in place of the token credential
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatal("Can't create Azure credential: ", err)
	}
	url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", account, container, blobName)

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

	// write: use azblob.NewAzBlobFileWriterWithClient to supply your own block blob client
	fw, err := azblob.NewAzBlobFileWriter(ctx, url, credential, blockblob.ClientOptions{})
	if err != nil {
		log.Fatal("Can't create Azure Blob file writer: ", err)
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
		log.Fatal("Can't close Azure Blob file writer: ", err)
	}
	log.Printf("Wrote %d rows to %s", len(students), url)

	// read
	fr, err := azblob.NewAzBlobFileReader(ctx, url, credential, blockblob.ClientOptions{})
	if err != nil {
		log.Fatal("Can't create Azure Blob file reader: ", err)
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
		log.Fatal("Can't close Azure Blob file reader: ", err)
	}

	if !slices.Equal(students, got) {
		log.Fatalf("Read %d rows that differ from the %d rows written", len(got), len(students))
	}
	log.Printf("Read %d rows back, identical to what was written", len(got))
}
