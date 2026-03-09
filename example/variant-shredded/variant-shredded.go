//go:build example

// This example demonstrates VARIANT shredding — decomposing semi-structured
// data into typed Parquet columns for better compression and query performance.
//
// Without shredding, a VARIANT stores all data as opaque binary blobs in two
// columns (metadata + value). With shredding, frequently-accessed values are
// stored in a native typed column (typed_value), so query engines can read them
// without deserializing the variant binary.
//
// Two approaches are shown:
//
//  1. Automatic shredding — use types.ShredVariant() to decompose any Go value,
//     then types.FillShreddedStruct() to populate the struct for writing.
//
//  2. Manual shredding — fill the struct fields directly when you already know
//     the types and want full control.
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

// ShreddedVariant defines the physical layout for a shredded VARIANT column.
//
// Parquet schema produced:
//
//	optional group data (VARIANT) {
//	    required binary metadata;
//	    optional binary value;     -- fallback for values that don't match typed_value
//	    optional int64 typed_value; -- fast path for int64 values
//	}
//
// Key rules:
//   - metadata is always REQUIRED (carries the variant dictionary)
//   - value is OPTIONAL (nil when the value is fully shredded into typed_value)
//   - typed_value is OPTIONAL (nil when the value doesn't match the typed column's type)
type ShreddedVariant struct {
	Metadata   string  `parquet:"name=metadata, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
	Value      *string `parquet:"name=value, type=BYTE_ARRAY, repetitiontype=OPTIONAL"`
	TypedValue *int64  `parquet:"name=typed_value, type=INT64, repetitiontype=OPTIONAL"`
}

// Record is the top-level row.
type Record struct {
	ID   int64           `parquet:"name=id, type=INT64"`
	Data ShreddedVariant `parquet:"name=data, type=VARIANT"`
}

func main() {
	const path = "/tmp/variant_shredded.parquet"

	writeFile(path)
	readFile(path)
}

func writeFile(path string) {
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		log.Fatal(err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(Record), 1)
	if err != nil {
		log.Fatal(err)
	}

	// ---------------------------------------------------------------
	// Approach 1: Automatic shredding
	//
	// Use ShredVariant() to decide whether a Go value should go into
	// typed_value (fast path) or value (binary fallback), then
	// FillShreddedStruct() to populate the struct.
	// ---------------------------------------------------------------
	shredSchema, err := types.ShredSchemaFromStruct(reflect.TypeOf(ShreddedVariant{}))
	if err != nil {
		log.Fatal(err)
	}

	autoRecords := []struct {
		id  int64
		val any
	}{
		{1, int64(42)},   // int64 matches typed_value → shredded
		{2, int64(100)},  // int64 matches typed_value → shredded
		{3, "hello"},     // string does NOT match int64 → falls back to value blob
		{4, nil},         // null variant → both value and typed_value are nil
		{5, int64(-999)}, // int64 matches typed_value → shredded
	}

	fmt.Println("=== Writing with automatic shredding ===")
	for _, r := range autoRecords {
		result, err := types.ShredVariant(r.val, shredSchema)
		if err != nil {
			log.Fatalf("shred id=%d: %v", r.id, err)
		}

		var sv ShreddedVariant
		if err := types.FillShreddedStruct(result, &sv); err != nil {
			log.Fatalf("fill id=%d: %v", r.id, err)
		}

		rec := Record{ID: r.id, Data: sv}
		if err := pw.Write(rec); err != nil {
			log.Fatal(err)
		}
		printRecord(rec)
	}

	// ---------------------------------------------------------------
	// Approach 2: Manual shredding
	//
	// When you already know the value types, fill the struct directly.
	// This avoids reflection and gives you full control.
	// ---------------------------------------------------------------
	fmt.Println("\n=== Writing with manual shredding ===")

	// Row 6: known int64 value → put in typed_value, leave value nil
	tv := int64(777)
	rec6 := Record{
		ID: 6,
		Data: ShreddedVariant{
			Metadata:   string(types.EncodeVariantMetadata(nil)),
			Value:      nil,
			TypedValue: &tv,
		},
	}
	if err := pw.Write(rec6); err != nil {
		log.Fatal(err)
	}
	printRecord(rec6)

	// Row 7: value type unknown/mixed → put in value blob, leave typed_value nil
	valBlob := string(types.EncodeVariantString("manual"))
	rec7 := Record{
		ID: 7,
		Data: ShreddedVariant{
			Metadata:   string(types.EncodeVariantMetadata(nil)),
			Value:      &valBlob,
			TypedValue: nil,
		},
	}
	if err := pw.Write(rec7); err != nil {
		log.Fatal(err)
	}
	printRecord(rec7)

	if err := pw.WriteStop(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nWrote %d rows to %s\n", 7, path)
}

func readFile(path string) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Fatal(err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(Record), 1)
	if err != nil {
		log.Fatal(err)
	}

	rows := make([]Record, pr.GetNumRows())
	if err := pr.Read(&rows); err != nil {
		log.Fatal(err)
	}
	_ = pr.ReadStopWithError()

	fmt.Printf("\n=== Read back %d rows ===\n", len(rows))
	for _, rec := range rows {
		printRecord(rec)
	}
}

func printRecord(rec Record) {
	tv := "<nil>"
	if rec.Data.TypedValue != nil {
		tv = fmt.Sprintf("%d", *rec.Data.TypedValue)
	}
	v := "<nil>"
	if rec.Data.Value != nil {
		v = fmt.Sprintf("[%d bytes]", len(*rec.Data.Value))
	}
	fmt.Printf("  id=%-3d  typed_value=%-6s  value=%s\n", rec.ID, tv, v)
}
