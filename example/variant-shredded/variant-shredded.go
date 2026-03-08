//go:build example

package main

import (
	"fmt"
	"log"
	"reflect"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/types"
	"github.com/hangxie/parquet-go/v2/writer"
)

// ShreddedVariant demonstrates a shredded VARIANT layout.
// The typed_value column stores int64 values directly for better compression
// and query performance. When the value doesn't match int64, it falls back
// to the binary value column.
type ShreddedVariant struct {
	Metadata   string  `parquet:"name=metadata, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
	Value      *string `parquet:"name=value, type=BYTE_ARRAY, repetitiontype=OPTIONAL"`
	TypedValue *int64  `parquet:"name=typed_value, type=INT64, repetitiontype=OPTIONAL"`
}

// Record is the top-level struct written to the Parquet file.
type Record struct {
	ID   int64           `parquet:"name=id, type=INT64"`
	Data ShreddedVariant `parquet:"name=data, type=VARIANT"`
}

func main() {
	const path = "/tmp/variant_shredded.parquet"

	// Derive a ShredSchema from the struct's parquet tags
	schema, err := types.ShredSchemaFromStruct(reflect.TypeOf(ShreddedVariant{}))
	if err != nil {
		log.Fatal(err)
	}

	// Shred values and populate structs
	records := []Record{
		makeRecord(1, int64(42), schema),   // int64 → typed_value
		makeRecord(2, int64(100), schema),  // int64 → typed_value
		makeRecord(3, "hello", schema),     // string → value blob (doesn't match int64)
		makeRecord(4, nil, schema),         // null variant
		makeRecord(5, int64(-999), schema), // int64 → typed_value
	}

	// Write
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		log.Fatal(err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(Record), 1)
	if err != nil {
		log.Fatal(err)
	}
	for _, rec := range records {
		if err := pw.Write(rec); err != nil {
			log.Fatal(err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		log.Fatal(err)
	}

	// Read back
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Fatal(err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(Record), 1)
	if err != nil {
		log.Fatal(err)
	}

	readRecords := make([]Record, pr.GetNumRows())
	if err := pr.Read(&readRecords); err != nil {
		log.Fatal(err)
	}
	_ = pr.ReadStopWithError()

	for _, rec := range readRecords {
		tv := "<nil>"
		if rec.Data.TypedValue != nil {
			tv = fmt.Sprintf("%d", *rec.Data.TypedValue)
		}
		v := "<nil>"
		if rec.Data.Value != nil {
			v = fmt.Sprintf("[%d bytes]", len(*rec.Data.Value))
		}
		fmt.Printf("ID=%d  typed_value=%s  value=%s\n", rec.ID, tv, v)
	}
}

func makeRecord(id int64, value any, schema *types.ShredSchema) Record {
	result, err := types.ShredVariant(value, schema)
	if err != nil {
		log.Fatalf("shred value for id=%d: %v", id, err)
	}
	var sv ShreddedVariant
	if err := types.FillShreddedStruct(result, &sv); err != nil {
		log.Fatalf("fill struct for id=%d: %v", id, err)
	}
	return Record{ID: id, Data: sv}
}
