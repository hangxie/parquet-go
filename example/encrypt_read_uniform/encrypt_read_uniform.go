//go:build example

package main

import (
	"fmt"
	"log"

	"github.com/hangxie/parquet-go/v3/reader"
	sourcehttp "github.com/hangxie/parquet-go/v3/source/http"
)

const fileURL = "https://raw.githubusercontent.com/apache/parquet-testing/master/data/uniform_encryption.parquet.encrypted"

// footerKey is the single key used for both the footer and all column pages.
// In uniform encryption there are no per-column keys, so WithFooterKey is sufficient.
var footerKey = []byte("0123456789012345")

// kmsKeys simulates a KMS that maps the key ID embedded in the file to the actual key.
// For uniform encryption the file stores one key ID ("kf") for the footer and reuses
// it for every column, so the retriever is called once regardless of column count.
var kmsKeys = map[string][]byte{
	"kf": footerKey,
}

func keyRetriever(keyMetadata []byte) ([]byte, error) {
	key, ok := kmsKeys[string(keyMetadata)]
	if !ok {
		return nil, fmt.Errorf("unknown key ID: %q", string(keyMetadata))
	}
	return key, nil
}

func main() {
	// Approach 1: supply the footer key directly — one key decrypts everything.
	readWithFooterKey()

	// Approach 2: use a key retriever — the reader passes the embedded key ID to
	// the retriever, which looks up the actual key (e.g. from a KMS).
	readWithKeyRetriever()
}

func readWithFooterKey() {
	fr, err := sourcehttp.NewHttpReader(fileURL, false, false, map[string]string{})
	if err != nil {
		log.Fatal("open HTTP reader: ", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, reader.WithFooterKey(footerKey))
	if err != nil {
		log.Fatal("create parquet reader: ", err)
	}
	defer pr.ReadStop()

	rows, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		log.Fatal("read rows: ", err)
	}
	log.Printf("WithFooterKey: %d rows", len(rows))
}

func readWithKeyRetriever() {
	fr, err := sourcehttp.NewHttpReader(fileURL, false, false, map[string]string{})
	if err != nil {
		log.Fatal("open HTTP reader: ", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, reader.WithKeyRetriever(keyRetriever))
	if err != nil {
		log.Fatal("create parquet reader: ", err)
	}
	defer pr.ReadStop()

	rows, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		log.Fatal("read rows: ", err)
	}
	log.Printf("WithKeyRetriever: %d rows", len(rows))
}
