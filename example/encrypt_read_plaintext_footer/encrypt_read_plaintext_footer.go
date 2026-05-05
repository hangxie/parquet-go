//go:build example

package main

import (
	"log"

	"github.com/hangxie/parquet-go/v3/reader"
	sourcehttp "github.com/hangxie/parquet-go/v3/source/http"
)

const fileURL = "https://raw.githubusercontent.com/apache/parquet-testing/master/data/encrypt_columns_plaintext_footer.parquet.encrypted"

var (
	footerKey = []byte("0123456789012345")
	doubleKey = []byte("1234567890123450")
	floatKey  = []byte("1234567890123451")
)

func main() {
	fr, err := sourcehttp.NewHttpReader(fileURL, false, false, map[string]string{})
	if err != nil {
		log.Fatal("open HTTP reader: ", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(
		fr, nil,
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("double_field", doubleKey),
		reader.WithColumnKey("float_field", floatKey),
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
