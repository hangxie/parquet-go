//go:build example

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/writer"
)

// Student represents our data model with parquet tags.
// Use `bloomfilter=true` to enable a Bloom filter for a specific column.
// Optional: `bloomfiltersize=2048` to specify the size in bytes (rounded to power of 2).
type Student struct {
	ID   int64  `parquet:"name=id, type=INT64, bloomfilter=true"`
	Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true, bloomfiltersize=4096"`
	Age  int32  `parquet:"name=age, type=INT32"` // No bloom filter for Age
}

func main() {
	filename := "student_bloom.parquet"

	// 1. Setup Writer and Write Data
	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		log.Fatalf("Failed to create local file writer: %v", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Fatalf("Failed to create parquet writer: %v", err)
	}

	numRows := 1000
	for i := 0; i < numRows; i++ {
		stu := Student{
			ID:   int64(i),
			Name: fmt.Sprintf("student-%d", i),
			Age:  int32(18 + (i % 10)),
		}
		if err := pw.Write(stu); err != nil {
			log.Fatalf("Write error: %v", err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		log.Fatalf("WriteStop error: %v", err)
	}
	_ = fw.Close()
	fmt.Printf("Successfully wrote %d rows to %s with Bloom filters\n", numRows, filename)

	// 2. Detect Schema and Verify Bloom Filter Attributes
	// NewParquetReader with nil schema automatically detects schema and Bloom filters.
	fr, err := local.NewLocalFileReader(filename)
	if err != nil {
		log.Fatalf("Failed to create local file reader: %v", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Fatalf("Failed to create parquet reader: %v", err)
	}
	defer pr.ReadStop()

	fmt.Println("\nDetected Schema Attributes:")
	for _, info := range pr.SchemaHandler.Infos {
		if info.InName == "Parquet_go_root" {
			continue // Skip root element
		}
		bfStatus := "Disabled"
		if info.BloomFilter {
			// Note: BloomFilterSize detected from file includes Thrift header overhead
			bfStatus = fmt.Sprintf("Enabled (Size estimate: %d bytes)", info.BloomFilterSize)
		}
		fmt.Printf("Column: %-10s | BloomFilter: %s\n", info.InName, bfStatus)
	}

	// 3. Use BloomFilterCheck to efficiently check for values.
	fmt.Println("\nBloom Filter Query Results:")
	queryIDs := []int64{500, 1500}
	for _, id := range queryIDs {
		found, err := pr.BloomFilterCheck("id", 0, id)
		if err != nil {
			log.Fatalf("BloomFilterCheck error for ID %d: %v", id, err)
		}
		status := "DEFINITELY DOES NOT exist"
		if found {
			status = "MIGHT exist"
		}
		fmt.Printf("ID %d: %s\n", id, status)
	}

	queryNames := []string{"student-100", "unknown-student"}
	for _, name := range queryNames {
		found, err := pr.BloomFilterCheck("name", 0, name)
		if err != nil {
			log.Fatalf("BloomFilterCheck error for Name %s: %v", name, err)
		}
		status := "DEFINITELY DOES NOT exist"
		if found {
			status = "MIGHT exist"
		}
		fmt.Printf("Name %q: %s\n", name, status)
	}

	// Dump full schema as JSON for inspection
	fmt.Println("\nFull Schema JSON (first 2 columns):")
	if len(pr.SchemaHandler.Infos) > 3 {
		schemaJSON, _ := json.MarshalIndent(pr.SchemaHandler.Infos[:3], "", "  ")
		fmt.Println(string(schemaJSON))
	} else {
		schemaJSON, _ := json.MarshalIndent(pr.SchemaHandler.Infos, "", "  ")
		fmt.Println(string(schemaJSON))
	}

	// Clean up
	_ = os.Remove(filename)
}
