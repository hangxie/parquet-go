//go:build example

package main

import (
	"fmt"
	"log"

	"github.com/hangxie/parquet-go/v3/reader"
	sourcehttp "github.com/hangxie/parquet-go/v3/source/http"
)

const fileURL = "https://raw.githubusercontent.com/apache/parquet-testing/master/data/encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted"

var keys = map[string][]byte{
	"kf":  []byte("0123456789012345"),
	"kc1": []byte("1234567890123450"),
	"kc2": []byte("1234567890123451"),
}

func keyRetriever(keyMetadata []byte) ([]byte, error) {
	id := string(keyMetadata)
	if id == "" {
		id = "kf"
	}
	key, ok := keys[id]
	if !ok {
		return nil, fmt.Errorf("unknown key ID: %q", id)
	}
	return key, nil
}

func main() {
	fr, err := sourcehttp.NewHttpReader(fileURL, false, false, map[string]string{})
	if err != nil {
		log.Fatal("open HTTP reader: ", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(
		fr, nil,
		reader.WithKeyRetriever(keyRetriever),
		reader.WithAADPrefix([]byte("tester")),
	)
	if err != nil {
		log.Fatal("create parquet reader: ", err)
	}
	defer pr.ReadStop()

	rows, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		log.Fatal("read rows: ", err)
	}
	log.Printf("%d rows", len(rows))
	for _, row := range rows {
		log.Println(row)
	}
}
