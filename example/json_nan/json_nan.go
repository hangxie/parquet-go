//go:build example

// This example writes JSON records containing non-finite floating point values
// (NaN, Infinity, -Infinity) into a parquet file, reads them back as JSON, then feeds
// that JSON straight back into the writer to show the representation is symmetric:
// what the reader emits is what the writer accepts.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

const schema = `
{
    "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
    "Fields": [
        {"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
        {"Tag": "name=float_value, type=FLOAT, repetitiontype=REQUIRED"},
        {"Tag": "name=double_value, type=DOUBLE, repetitiontype=OPTIONAL"},
        {"Tag": "name=float16_value, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16, repetitiontype=REQUIRED"},
        {"Tag": "name=samples, type=LIST, repetitiontype=REQUIRED",
         "Fields": [
             {"Tag": "name=element, type=DOUBLE, repetitiontype=REQUIRED"}
         ]
        },
        {"Tag": "name=metrics, type=MAP, repetitiontype=REQUIRED",
         "Fields": [
             {"Tag": "name=key, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
             {"Tag": "name=value, type=DOUBLE, repetitiontype=REQUIRED"}
         ]
        }
    ]
}
`

// Non-finite values have no JSON number form, so they are written as quoted strings.
// The writer accepts NaN, Inf, Infinity and their signed forms case-insensitively; the
// reader always renders the portable spellings "NaN", "Infinity", and "-Infinity".
var records = []string{
	`{
        "name": "finite",
        "float_value": 1.5,
        "double_value": -2.25,
        "float16_value": "0.5",
        "samples": [1, 2.5, -3],
        "metrics": {"mean": 0.5}
    }`,
	`{
        "name": "not-a-number",
        "float_value": "NaN",
        "double_value": "nan",
        "float16_value": "NaN",
        "samples": ["NaN", 0],
        "metrics": {"mean": "NaN"}
    }`,
	`{
        "name": "positive-infinity",
        "float_value": "+Inf",
        "double_value": "Infinity",
        "float16_value": "Inf",
        "samples": ["inf", 1e308],
        "metrics": {"mean": "+Inf"}
    }`,
	`{
        "name": "negative-infinity",
        "float_value": "-Inf",
        "double_value": "-infinity",
        "float16_value": "-Inf",
        "samples": ["-Inf", -1e308],
        "metrics": {"mean": "-Inf"}
    }`,
	`{
        "name": "null-double",
        "float_value": "NaN",
        "double_value": null,
        "float16_value": "-0",
        "samples": [],
        "metrics": {}
    }`,
}

func main() {
	dir := os.TempDir()

	first := filepath.Join(dir, "json-nan.parquet")
	if err := write(first, records); err != nil {
		log.Fatal(err)
	}
	out, err := read(first)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(out))

	// Feed the emitted JSON back in: the second file must render identically.
	rewritten, err := rowsAsJSON(out)
	if err != nil {
		log.Fatal(err)
	}
	second := filepath.Join(dir, "json-nan-roundtrip.parquet")
	if err := write(second, rewritten); err != nil {
		log.Fatal(err)
	}
	out2, err := read(second)
	if err != nil {
		log.Fatal(err)
	}
	if !bytes.Equal(out, out2) {
		log.Fatalf("round trip changed the data:\n%s\n%s", out, out2)
	}
	fmt.Println("round trip stable")
}

// rowsAsJSON splits a marshalled row array back into per-row JSON documents.
func rowsAsJSON(buf []byte) ([]string, error) {
	var rows []json.RawMessage
	if err := json.Unmarshal(buf, &rows); err != nil {
		return nil, fmt.Errorf("split rows: %w", err)
	}
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = string(row)
	}
	return out, nil
}

func write(path string, recs []string) error {
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer func() { _ = fw.Close() }()

	pw, err := writer.NewJSONWriter(schema, fw, writer.WithNP(1))
	if err != nil {
		return fmt.Errorf("create JSON writer: %w", err)
	}
	for _, rec := range recs {
		if err := pw.Write(rec); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("finalize file: %w", err)
	}
	return nil
}

func read(path string) ([]byte, error) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = fr.Close() }()

	pr, err := reader.NewParquetReader(fr, nil, reader.WithNP(1))
	if err != nil {
		return nil, fmt.Errorf("create parquet reader: %w", err)
	}
	defer func() { _ = pr.ReadStop() }()

	rows, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}

	// Without this step the rows still hold native NaN/Inf floats, which
	// encoding/json refuses to marshal.
	friendly, err := marshal.ConvertToJSONFriendly(rows, pr.SchemaHandler)
	if err != nil {
		return nil, fmt.Errorf("convert to JSON friendly: %w", err)
	}
	return json.MarshalIndent(friendly, "", "  ")
}
