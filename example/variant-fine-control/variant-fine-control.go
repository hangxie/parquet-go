//go:build example

package main

import (
	"log"

	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

// VariantRaw allows fine-grained control over the underlying physical columns
// of a VARIANT logical type.
//
// While 'any' is sufficient for most use cases, defining a struct allows you
// to specify compression codecs and encodings for the 'metadata' and 'value'
// columns independently.
type VariantRaw struct {
	// Use ZSTD for metadata as it often compresses dictionaries well
	Metadata []byte `parquet:"name=metadata, type=BYTE_ARRAY, compression=ZSTD"`
	// Use SNAPPY for values for faster access
	Value []byte `parquet:"name=value, type=BYTE_ARRAY, compression=SNAPPY"`
}

type Record struct {
	ID int64 `parquet:"name=id, type=INT64"`
	// Use logicaltype=VARIANT to identify this as a VARIANT type to readers
	Variant VariantRaw `parquet:"name=variant, type=VARIANT, logicaltype=VARIANT"`
}

func main() {
	fw, err := local.NewLocalFileWriter("/tmp/variant_fine_control.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer fw.Close()

	// Create a record with raw VARIANT data
	// In a real application, you would construct the metadata and value bytes
	// according to the Parquet VARIANT specification.
	rec := Record{
		ID: 1,
		Variant: VariantRaw{
			Metadata: []byte{0x01, 0x02, 0x03},
			Value:    []byte{0x04, 0x05, 0x06},
		},
	}

	pw, err := writer.NewParquetWriter(fw, new(Record), 1)
	if err != nil {
		log.Fatal(err)
	}

	if err := pw.Write(rec); err != nil {
		log.Fatal(err)
	}
	if err := pw.WriteStop(); err != nil {
		log.Fatal(err)
	}
	log.Println("Successfully wrote variant_fine_control.parquet")
}
