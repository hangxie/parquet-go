//go:build example

// Demonstrates how string and []byte relate on byte-backed columns: which Go
// representations a typed struct may use, what a schema-inferred read produces, and
// how marshal.ConvertToJSONFriendly renders each column.
package main

import (
	"encoding/json"
	"log"

	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

// Named types defined from string and []byte are written and read like the types they
// are defined from.
type (
	Label  string
	Digest []byte
)

// Every field below sits on a BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY column. The Go type is
// the caller's choice; the column decides what the value means and how it renders.
type Record struct {
	// Text: annotated UTF8, so the bytes are the column's text.
	Text  string `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8"`
	Bytes []byte `parquet:"name=bytes, type=BYTE_ARRAY, convertedtype=UTF8"`
	Named Label  `parquet:"name=named, type=BYTE_ARRAY, convertedtype=UTF8"`

	// Binary: unannotated, so the bytes are opaque and render as base64.
	Blob   []byte `parquet:"name=blob, type=BYTE_ARRAY"`
	Digest Digest `parquet:"name=digest, type=BYTE_ARRAY"`

	// A UUID is a fixed 16 bytes, supplied either way and rendered in its dashed form.
	UUID []byte `parquet:"name=uuid, type=FIXED_LEN_BYTE_ARRAY, logicaltype=UUID, length=16"`

	// A byte-backed map key must be a comparable Go type, so it is a string: Go forbids
	// a slice as a map key.
	Tags map[string]string `parquet:"name=tags, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`

	// A LIST is written as a slice of its element type, so a []byte cannot supply one.
	Lines []string `parquet:"name=lines, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

func main() {
	path := "/tmp/byte_representation.parquet"
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, new(Record), writer.WithNP(1))
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	uuid := []byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	record := Record{
		Text:   "hello",
		Bytes:  []byte("hello"),
		Named:  Label("hello"),
		Blob:   []byte{0x01, 0x02, 0x03},
		Digest: Digest{0x01, 0x02, 0x03},
		UUID:   uuid,
		Tags:   map[string]string{"env": "prod"},
		Lines:  []string{"first", "second"},
	}
	if err = pw.Write(record); err != nil {
		log.Println("Write error", err)
		return
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()

	// Read back into the same struct: each field keeps the representation it was declared
	// with, and the three text fields hold the same bytes.
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pr, err := reader.NewParquetReader(fr, new(Record), reader.WithNP(1))
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	records := make([]Record, 1)
	if err = pr.Read(&records); err != nil {
		log.Println("Read error", err)
		return
	}
	log.Printf("typed read: text=%q bytes=%q named=%q blob=%v digest=%v",
		records[0].Text, string(records[0].Bytes), string(records[0].Named), records[0].Blob, records[0].Digest)

	// JSON-friendly conversion renders by the column, not by the Go type: text columns as
	// strings, unannotated byte-backed ones as base64, the UUID in its dashed form.
	friendly, err := marshal.ConvertToJSONFriendly(records, pr.SchemaHandler)
	if err != nil {
		log.Println("ConvertToJSONFriendly error", err)
		return
	}
	encoded, err := json.MarshalIndent(friendly, "", "  ")
	if err != nil {
		log.Println("Marshal error", err)
		return
	}
	log.Printf("JSON-friendly:\n%s", encoded)
	_ = pr.ReadStop()
	_ = fr.Close()

	// A reader given no struct infers the schema, and every byte-backed column comes back
	// as a string whatever was written to it.
	fr, err = local.NewLocalFileReader(path)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	ir, err := reader.NewParquetReader(fr, nil, reader.WithNP(1))
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	inferred, err := ir.ReadByNumber(1)
	if err != nil {
		log.Println("ReadByNumber error", err)
		return
	}
	log.Printf("inferred read: %+v", inferred[0])
	_ = ir.ReadStop()
	_ = fr.Close()
}
