# parquet-go/v3

[![](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hangxie/parquet-go/blob/main/LICENSE)
[![](https://img.shields.io/github/v/tag/hangxie/parquet-go.svg?color=brightgreen&label=version&sort=semver)](https://github.com/hangxie/parquet-go/releases)
[![[parquet-go]](https://github.com/hangxie/parquet-go/actions/workflows/build.yml/badge.svg)](https://github.com/hangxie/parquet-go/actions/workflows/build.yml)
[![](https://hangxie.github.io/parquet-go/coverage.svg)](https://hangxie.github.io/parquet-go/coverage-history.html)

parquet-go is a pure-Go library for reading and writing Apache Parquet files.

## Highlights

- Read and write flat and nested Parquet data.
- Use Go struct tags, JSON schema, CSV metadata, or Arrow schemas.
- Work with local files, memory buffers, cloud object stores, HDFS, HTTP, and GoCloud CDK blobs.
- Configure readers and writers with per-instance functional options.
- Use modern Parquet features including Data Page V2, CRC page checksums, modular encryption, bloom filters, and newer logical types.
- Convert geospatial logical types with configurable GeoJSON, hex, base64, or hybrid JSON output.
- Choose how the JSON and CSV writers read logical values, as canonical text or as the raw physical value.

## Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Write a File](#write-a-file)
  - [Read a File](#read-a-file)
- [Configuration](#configuration)
- [Schema Definition](#schema-definition)
- [Type System](#type-system)
  - [Reading Values](#reading-values)
  - [Value Modes](#value-modes)
  - [BSON Values](#bson-values)
  - [Geospatial Values](#geospatial-values)
  - [TIMESTAMP Values](#timestamp-values)
- [Encoding Support](#encoding-support)
- [Compression Support](#compression-support)
- [Readers and Writers](#readers-and-writers)
- [File Sources](#file-sources)
- [Advanced Features](#advanced-features)
  - [CRC Page Checksums](#crc-page-checksums)
  - [Bloom Filters](#bloom-filters)
  - [Encryption](#encryption)
  - [Non-finite Floating Point JSON Representation](#non-finite-floating-point-json-representation)
  - [GeoParquet](#geoparquet)
  - [Concurrency](#concurrency)
- [Examples](#examples)
- [Local Development](#local-development)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Installation

```sh
go get github.com/hangxie/parquet-go/v3
```

parquet-go builds with the latest Go toolchain and is guaranteed to stay compatible two releases back, so `go.mod` never requires anything newer than the current Go release minus two. CI enforces that floor and runs the test suite against it and the two releases above it. See [go.mod](go.mod) for the exact minimum, currently Go 1.25.

## Quick Start

### Write a File

```go
package main

import (
    "log"

    "github.com/hangxie/parquet-go/v3/source/local"
    "github.com/hangxie/parquet-go/v3/writer"
)

type Student struct {
    Name   string  `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING, encoding=PLAIN_DICTIONARY"`
    Age    int32   `parquet:"name=age, type=INT32"`
    ID     int64   `parquet:"name=id, type=INT64"`
    Weight float32 `parquet:"name=weight, type=FLOAT"`
    Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
    fw, err := local.NewLocalFileWriter("output.parquet")
    if err != nil {
        log.Fatal("can't create file: ", err)
    }
    defer fw.Close()

    pw, err := writer.NewParquetWriter(fw, new(Student))
    if err != nil {
        log.Fatal("can't create parquet writer: ", err)
    }

    for i := 0; i < 10; i++ {
        stu := Student{
            Name:   "StudentName",
            Age:    int32(20 + i%5),
            ID:     int64(i),
            Weight: float32(50.0 + float32(i)*0.1),
            Sex:    i%2 == 0,
        }
        if err = pw.Write(stu); err != nil {
            log.Fatal("write error: ", err)
        }
    }

    if err = pw.WriteStop(); err != nil {
        log.Fatal("writestop error: ", err)
    }
}
```

`WriteStop` is idempotent after successful finalization. If finalization fails, the writer remains stopped and later `WriteStop` calls return an error stating that the file is incomplete instead of retrying non-idempotent footer writes.

### Read a File

```go
package main

import (
    "log"

    "github.com/hangxie/parquet-go/v3/reader"
    "github.com/hangxie/parquet-go/v3/source/local"
)

type Student struct {
    Name   string  `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING"`
    Age    int32   `parquet:"name=age, type=INT32"`
    ID     int64   `parquet:"name=id, type=INT64"`
    Weight float32 `parquet:"name=weight, type=FLOAT"`
    Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
    fr, err := local.NewLocalFileReader("output.parquet")
    if err != nil {
        log.Fatal("can't open file: ", err)
    }
    defer fr.Close()

    pr, err := reader.NewParquetReader(fr, new(Student), reader.WithNP(4))
    if err != nil {
        log.Fatal("can't create parquet reader: ", err)
    }
    defer func() { _ = pr.ReadStop() }()

    students := make([]Student, int(pr.GetNumRows()))
    if err = pr.Read(&students); err != nil {
        log.Fatal("read error: ", err)
    }

    for _, stu := range students {
        log.Printf("%+v\n", stu)
    }
}
```

## Configuration

Readers and writers are configured per instance by passing functional options to their constructors. This keeps independent readers and writers safe to use with different settings in the same process. Options are construction inputs, not a public API for mutating an already-created reader or writer.

Common writer options:

- `writer.WithNP`
- `writer.WithPageSize`
- `writer.WithRowGroupSize`
- `writer.WithMaxDictionarySize`
- `writer.WithBinaryMinMaxTruncateLength`
- `writer.WithCompressionCodec`
- `writer.WithCompressionLevel`
- `writer.WithDataPageVersion`
- `writer.WithWriteCRC`
- `writer.WithSortingColumns`

Common reader options:

- `reader.WithNP`
- `reader.WithCaseInsensitive`
- `reader.WithCRCMode`

Encryption-related options are covered in [Encryption](#encryption).

Reader footers expose schema and column paths as stored in the Parquet file. Use `ParquetReader.InternalFooter()` when a tool needs a converted footer with internal Go schema names.

Use `writer.WithSortingColumns` to declare that the caller supplies rows in a known order. Each `parquet.SortingColumn.ColumnIdx` is a zero-based leaf-column ordinal; the option applies the same declaration to every row group, and the writer records but does not enforce or produce that ordering. A writer using this option must be constructed with a schema so the column ordinals can be validated.

```go
pw, err := writer.NewParquetWriter(fw, new(Student), writer.WithSortingColumns(
    &parquet.SortingColumn{ColumnIdx: 2, Descending: false, NullsFirst: false},
))
```

Readers can inspect a row group's declaration without mutating the loaded footer:

```go
sortingColumns, err := pr.RowGroupSortingColumns(0)
```

## Schema Definition

Only fields included in the schema are written. Struct fields must be exported.

### Go Struct Tags

```go
type Student struct {
    Name   string  `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING, encoding=PLAIN_DICTIONARY"`
    Age    int32   `parquet:"name=age, type=INT32, encoding=PLAIN"`
    ID     int64   `parquet:"name=id, type=INT64"`
    Weight float32 `parquet:"name=weight, type=FLOAT"`
    Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}
```

### JSON Schema

```go
jsonSchema := `{
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
  "Fields": [
    {"Tag": "name=name, type=BYTE_ARRAY, logicaltype=STRING, repetitiontype=REQUIRED"},
    {"Tag": "name=age, type=INT32, repetitiontype=REQUIRED"}
  ]
}`
```

### CSV Metadata

```go
md := []string{
    "name=Name, type=BYTE_ARRAY, logicaltype=STRING, encoding=PLAIN_DICTIONARY",
    "name=Age, type=INT32",
}
```

### Arrow Schema

```go
schema := arrow.NewSchema(
    []arrow.Field{
        {Name: "int64", Type: arrow.PrimitiveTypes.Int64},
        {Name: "float64", Type: arrow.PrimitiveTypes.Float64},
        {Name: "float16", Type: arrow.FixedWidthTypes.Float16},
    },
    nil,
)
```

Schema notes:

- `InName` is the Go field name. `ExName` is the Parquet field name.
- Avoid field names that differ only by first-letter case.
- `PARGO_PREFIX_` is reserved and should not be used as a field prefix.
- Column paths separate their components with `common.ParGoPathDelimiter` (`\x01`); build a path with `common.PathToStr`. `.` is an ordinary character in a field name, never a separator, so a name may contain `.` (it stays a single path component). This applies to every path-taking API, including `ParquetReader.ReadPartial`, `ReadColumnByPath`, `SkipRowsByPath`, `BloomFilterCheck`, `BloomFilterSize`, `reader.WithColumnKey`, and `writer.WithColumnEncrypted`.
- Arrow `Float16` fields are written as `FIXED_LEN_BYTE_ARRAY` with `length=2` and `logicaltype=FLOAT16`; generic Parquet reads expose them as raw two-byte strings. Use `types.ConvertFloat16LogicalValue` when a `float32` value is needed. FLOAT16 statistics and column indexes use the Parquet FLOAT16 total ordering.
- `UNKNOWN` columns represent always-null columns per the Parquet spec. The Go field must be `*int32` with `repetitiontype=OPTIONAL`. The writer rejects any non-nil value with an error. For a well-formed file every read returns `nil`; a malformed file that stores a non-null INT32 value in an UNKNOWN column will have that value returned as-is.

## Type System

### Primitive Types

| Primitive Type | Go Type |
| --- | --- |
| `BOOLEAN` | `bool` |
| `INT32` | `int32` |
| `INT64` | `int64` |
| `INT96` ([deprecated](https://github.com/xitongsys/parquet-go/issues/420)) | `string` |
| `FLOAT` | `float32` |
| `DOUBLE` | `float64` |
| `BYTE_ARRAY` | `string` |
| `FIXED_LEN_BYTE_ARRAY` | `string` |

### Logical Types

| Logical Type | Primitive Type | Go Type |
| --- | --- | --- |
| `UTF8` | `BYTE_ARRAY` | `string` |
| `INT_8` | `INT32` | `int32` |
| `INT_16` | `INT32` | `int32` |
| `INT_32` | `INT32` | `int32` |
| `INT_64` | `INT64` | `int64` |
| `UINT_8` | `INT32` | `int32` |
| `UINT_16` | `INT32` | `int32` |
| `UINT_32` | `INT32` | `int32` |
| `UINT_64` | `INT64` | `int64` |
| `DATE` | `INT32` | `int32` |
| `TIME_MILLIS` | `INT32` | `int32` |
| `TIME_MICROS` | `INT64` | `int64` |
| `TIMESTAMP_MILLIS` | `INT64` | `int64` |
| `TIMESTAMP_MICROS` | `INT64` | `int64` |
| `INTERVAL` | `FIXED_LEN_BYTE_ARRAY` | `string` |
| `DECIMAL` | `INT32`, `INT64`, `FIXED_LEN_BYTE_ARRAY`, `BYTE_ARRAY` | `int32`, `int64`, `string`, `string` |
| `UUID` | `FIXED_LEN_BYTE_ARRAY` | `string` |
| `FLOAT16` | `FIXED_LEN_BYTE_ARRAY` | `string` |
| `UNKNOWN` | `INT32` | `*int32` (always `nil`) |
| `GEOMETRY` | `BYTE_ARRAY` | `string` |
| `GEOGRAPHY` | `BYTE_ARRAY` | `string` |
| `JSON` | `BYTE_ARRAY` | `string` |
| `BSON` | `BYTE_ARRAY` | `string` |
| `LIST` | - | slice |
| `MAP` | - | map |

`UUID`, `FLOAT16`, and `INTERVAL` have the column width fixed by the specification at 16, 2, and 12 bytes. A struct tag, CSV metadata entry, or JSON schema that annotates one of them may leave `length` out and get the fixed width filled in; declaring any other width is an error, including an explicit `length=0` in a written-out tag. Code that builds a `common.Tag` directly cannot express an explicit zero, since an unset `Length` and a deliberate `0` are the same value there, and both get the fixed width. `types.StrToParquetTypeWithLogical` and `types.JSONTypeToParquetTypeWithLogical` require the same width in their `length` argument for `UUID` and `FLOAT16`; releases up to v3.8.2 ignored that argument, so a direct caller that passed `0` has to pass the column width.

A `FIXED_LEN_BYTE_ARRAY` column must declare a positive `length`. The declared width is what values are encoded and decoded with, so a zero-width column is unusable in either direction: everything written to it comes back empty, and reading an existing file through such a declaration decodes every value as empty. A tag, CSV metadata entry, or JSON schema that omits `length` or sets `length=0` is now rejected when the schema is built, which includes building a reader over a struct whose tag left it out; a raw `[]*parquet.SchemaElement` or `*schema.SchemaHandler`, which never passes through tag validation, is rejected when the writer is created.

Every value written to such a column must be exactly that wide, annotated (`DECIMAL`, `INTERVAL`, `FLOAT16`, `UUID`) or not. A value of any other width is rejected while the page is built, by `Write`, `Flush`, or `WriteStop` depending on when the row group is flushed; up to v3.8.3 it was written verbatim, so a longer value came back truncated and a shorter one left the column chunk unreadable. The Go zero value is not exempt: an empty string in a `FIXED_LEN_BYTE_ARRAY` field is an error, and a column that has to carry no value needs `repetitiontype=OPTIONAL` and a `nil` pointer.

Writers that take string input read an unannotated `FIXED_LEN_BYTE_ARRAY` value as base64 and check the decoded width against the column, rejecting anything that is not base64 or does not decode to the declared width, naming the string that was supplied. `CSVWriter` converts each row as it is handed over, so that error returns from `WriteString`; `JSONWriter` converts while the row group is flushed, so its error arrives from `Write`, `Flush`, or `WriteStop` like the width check above. Up to v3.8.3 the reading was guessed per value, base64 first and the literal string as a fallback, so `0123456789abcdef` for an `FLBA(16)` column was stored as the 12 bytes it decodes to while `abc bcd` for an `FLBA(7)` column was stored as its own seven characters. See [Value Modes](#value-modes) for the rule that replaced the guess.

Type aliases are supported, for example `type MyString string`, when the base type follows the table. Conversion utilities are available in [types/converter.go](types/converter.go).

### Reading Values

`types.ConvertValue` renders a column value for output, taking the schema element and the same options the write path does:

```go
rendered, err := types.ConvertValue(v, schemaElement)
```

A value the column cannot render is reported rather than replaced, each error wrapping `types.ErrUnrenderable`: BSON that does not parse or is empty, bytes the requested geospatial rendering cannot be produced from, and a `UUID`, `FLOAT16`, `INTERVAL` or `INT96` whose width is not the one the format fixes. Only the geospatial renderings that read the bytes can fail that way, `GeospatialModeGeoJSON` and `GeospatialModeHybrid`, since `GeospatialModeHex` and `GeospatialModeBase64` render any bytes; a value that is not bytes at all is reported whichever of them is selected. The WKB reading is 2D, so an ISO geometry carrying Z or M ordinates is among the bytes those two cannot read. An integer outside the range its annotation declares is reported in both modes: raw mode cannot write it back, and interpreted mode renders it by wrapping it into the annotation's width, so a `UINT_8` column holding 256 reads as 0. Raw mode reports one more, since it promises the value writes back as it was read: a `FIXED_LEN_BYTE_ARRAY` whose width is not the schema element's `type_length`, where the interpreted path renders base64 or a decimal and says nothing. A column annotated as text is carried verbatim in both directions and measured in neither. A number-backed column given a value of the wrong Go type still passes it through, since only a corrupt reader produces one.

A call the column cannot answer is reported separately: `types.ErrInvalidSchemaElement` for a nil schema element, or one carrying no physical type where the rendering needs one (raw mode, which is a reading of that type and nothing else, and the `DECIMAL` converter, the one interpreted converter that reads it), and `types.ErrUnsupportedValueMode` for a mode outside the two defined ones. A caller can tell the three apart with `errors.Is` rather than by matching on strings.

On error the returned value is a rendering rather than nil, so a caller that wants to carry on with one can: the substitute `ConvertToJSONType` produces in interpreted mode, and in raw mode the base64 of the bytes for a byte-backed column of the wrong width, the value itself otherwise. `ConvertValue` also honours `WithValueMode`, which is how a value is read back in the raw form it was written; see [Value Modes](#value-modes).

`types.ConvertVariantValue` follows the same two-result convention. Invalid metadata or value bytes return a map containing the base64 of both byte arrays alongside an error wrapping `types.ErrUnrenderable`. For compatibility, a zero-length value retains its historical nil result without validating metadata; the Parquet VARIANT encoding of null is the one byte `0x00`, not an empty value.

`types.ConvertToJSONType` is deprecated in favour of it. That function returns a value for anything, substituting where it cannot render — base64 for unparsable BSON, a `wkb_hex` map for unparsable WKB, the raw bytes for a mis-sized `UUID` — and a caller cannot tell those from a value that really rendered. Its substitutions are unchanged apart from two fixes, each of which replaced an output no caller could have used: an `INT96` is checked for the exact twelve bytes the format fixes, so a longer value comes back as itself rather than as the timestamp its first twelve bytes spell, and one passed as `[]byte` renders rather than passing through; and a `DECIMAL` whose unscaled digits equal its scale keeps its leading zero, so a `DECIMAL(9,2)` holding 92 renders as `0.92` rather than as a `.92` that `json.Marshal` refuses. A third moved in v3.9.0, before this: a geometry carrying Z or M ordinates returns the `wkb_hex` substitute rather than coordinates its bytes do not hold, described under [GeoParquet](#geoparquet).

UTF-8 validation is opt-in. `types.WithEnforceUTF8(true)` makes `ConvertValue`, `StrToParquetTypeWithLogical`, and `JSONTypeToParquetTypeWithLogical` reject invalid UTF-8 in columns annotated as `STRING`/`UTF8`, `JSON`, or `ENUM`, using either logical or converted annotations. It defaults to false, applies in both interpreted and raw modes, and reports value-conversion errors wrapping `types.ErrUnrenderable`; the error identifies the first invalid byte and includes at most eight bytes of hex context. Read conversion checks `string`, `[]byte`, and types defined from either one; nil values remain allowed, while values of any other Go type are reported as unrenderable. The check validates UTF-8 only, not JSON syntax, and does not change unannotated binary or number-backed columns. The deprecated `ConvertToJSONType` cannot report errors and ignores this option.

```go
rendered, err := types.ConvertValue(v, schemaElement, types.WithEnforceUTF8(true))
friendly, err := marshal.ConvertToJSONFriendly(rows, schemaHandler, marshal.WithEnforceUTF8(true))
```

`writer.WithEnforceUTF8(true)` forwards the same check to `CSVWriter` string conversion and `JSONWriter` decoded value conversion. With enforcement enabled, `JSONWriter` deliberately validates the complete input document before schema projection, including object names and fields the schema does not select; it rejects invalid UTF-8 and unpaired Unicode escapes. Both JSON decoding paths reject a leading UTF-8 BOM. JSON numbers retain their exact text, and the decoder reuses its buffers and string cache within each batch. Typed `ParquetWriter` input is unaffected because its values bypass text conversion; callers that require this guarantee must validate strings before `Write`. Go 1.27 uses standard-library jsonv2; Go 1.25 and 1.26 use a pinned, verified upstream commit compatible with Go 1.25 without requiring `GOEXPERIMENT`. Validation cannot recover bytes already replaced by an upstream JSON encoder. With enforcement off, existing conversion behavior is unchanged: malformed text bytes can survive conversion but are replaced by U+FFFD when `json.Marshal` encodes the string. Raw mode also keeps text columns as text and does not prevent that loss; neither mode silently switches malformed text to base64.

### Value Modes

`JSONWriter` and `CSVWriter` take values as text, so every logical column has two readings: the canonical text of the logical type, and the physical value the column actually stores. Up to v3.8.3 the reading was guessed per value, by attempting a base64 decode and keeping the literal string when that failed. `writer.WithValueMode` replaces the guess with a choice the caller makes:

```go
jw, err := writer.NewJSONWriterWithContext(ctx, jsonSchema, fw,
    writer.WithValueMode(types.ValueModeRaw),
)
```

| Mode | Input grammar |
| --- | --- |
| `types.ValueModeInterpreted` (default) | The canonical text of the logical type: `550e8400-e29b-41d4-a716-446655440000` for `UUID`, `2023-12-25` for `DATE`, `2 mon 3 day 4.500 sec` for `INTERVAL`, Extended JSON for `BSON`. `DATE`, `TIMESTAMP`, `TIME`, `INT96` and `INTERVAL` also still accept the bare number their column stores, which predates the mode. For `INT96` that number is the whole 96-bit value, not a day: its low eight bytes are nanoseconds within the day and its high four the Julian day, so `12345` is 12,345 nanoseconds on Julian day 0. |
| `types.ValueModeRaw` | The physical value: base64 for every byte-backed column, the underlying number for `INT32` and `INT64` backed ones. |

Three rules hold in both modes. A column annotated `UTF8`, `ENUM`, or `JSON` is text and is stored verbatim, whether the annotation is the converted type or the logical type, since there is nothing to interpret; an unannotated `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` is base64, matching what the read path renders for it; and `INT96` keeps its own timestamp form in interpreted mode and is base64 in raw mode.

`GEOMETRY` and `GEOGRAPHY` take the object their `GeospatialJSONMode` renders, described under [Geospatial Values](#geospatial-values). Up to v3.8.3 a value for one of those columns fell through to the byte-array guess, so `POINT (1 2)` was stored as the eleven characters of that string claiming to be WKB; WKT is still not a form any mode renders, so it is still refused, now with an error naming the form the column does take.

Numbers are checked against the column rather than cast through it, in both modes. A value takes the whole field, surrounding whitespace aside, and anything else is reported:

| Column | Rule | Rejected up to v3.8.3 as |
| --- | --- | --- |
| `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE` | The physical type's range | `2023-12-25` into `INT32` stored the year `2023` |
| `INT_8`…`INT_64`, `UINT_8`…`UINT_64` | The annotation's width and signedness | `256` into `UINT_8` stored `0`, `-1` stored `255`, `"123abc"` stored `123` |
| `INTEGER` logical type | The annotation's width and signedness | already strict for text, but the number `1000` into `INTEGER(8)` stored `1000` |
| `DATE`, `TIMESTAMP`, under either spelling | The physical type's range, for the bare day or tick count these also accept | `"19723abc"` stored day `19723`; a logical `TIMESTAMP` stored `"123abc"` as `123` |

The rule holds however the value arrives. An integer column is read the same way whether the value is text from `CSVWriter`, a `json.Number` from `JSONWriter`, or a Go number handed to `types.JSONTypeToParquetTypeWithLogical` by a caller who decoded JSON into `any` without `UseNumber`; all three go through one scanner. Up to v3.8.3 the number form was cast through the column instead, so the same document could be stored differently depending on how it had been decoded.

Both spellings of an integer annotation behave identically, which matters because a schema built from a tag carries both while a schema built from raw `parquet.SchemaElement` values may carry only the converted type. An annotation the physical type cannot hold, such as `INT_8` on an `INT64` column, is not applied at all: the column's own range decides, since that is what the read path renders.

An 8- or 16-bit annotation is checked in raw mode as well, since every value it can hold fits the physical `INT32` and the two ranges therefore cannot disagree. `UINT_32` and `UINT_64` are the exception: raw mode carries the upper half of their range as a negative physical value, so the physical number is the whole story and there is nothing further to check.

A column whose value travels as text taken at face value — base64 for a byte-backed column with nothing to interpret, the string itself for one annotated as text — requires an actual JSON string. A number or boolean is rejected rather than rendered to text and read as if it had been one: `true` is valid base64, so a JSON `true` used to be stored as the three bytes `b6 bb 9e`, and a number under `UseNumber` went the same way because `json.Number` is a string to Go. This is the change most likely to reach existing JSON data: `{"zip": 94110}` into a `UTF8` column now fails, where it used to be stored as `"94110"`. Quote such a value, or declare the column as the type it holds. `INT96` is not in that set: it reads its timestamp form and falls back to the bare 96-bit integer the column stores, so a number is a value it holds.

The mode reaches the conversion helpers as `types.ValueOption`, which `types.StrToParquetTypeWithLogical` and `types.JSONTypeToParquetTypeWithLogical` accept as trailing arguments. `types.ValueConfig` and `types.ValueOption` are the former `types.JSONTypeConfig` and `types.JSONTypeOption`, which remain as deprecated aliases. `ParquetWriter` over structs and maps is unaffected: its values are already typed, so there is no text to read either way.

`types.ConvertValue` takes the mode too, so a value reads back in the form it was written: raw rendering emits base64 for a byte-backed column and the underlying number otherwise, which is exactly what the raw write path takes.

`types.ConvertToJSONType` renders the interpreted form whatever mode it is given, which is what it did before the mode existed; use `ConvertValue` for raw.

### BSON Values

A `BSON` column is a `BYTE_ARRAY` holding a BSON document, and its Go representation is a `string` of those raw bytes. Its interpreted form on both sides is [canonical MongoDB Extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/): `types.ConvertValue` renders a document as Extended JSON text, and `JSONWriter`, `CSVWriter`, and the `types.StrToParquetType` helpers read that text back. Canonical rather than relaxed, because the `{"$numberInt": "1"}` wrappers are what distinguish an `int32` from an `int64` and an `ObjectID` from a string; relaxed and hand-written JSON such as `{"i": 1}` is accepted on input as well.

The text is a JSON string in JSON output, not a nested object, so the document's own field order survives: a Go map would have reordered the fields and written back a different document. A JSON object for a `BSON` column is therefore an error rather than an alternative spelling. Text that is not exactly one JSON document fails the write with a `parse BSON` error, trailing text included.

Extended JSON is MongoDB's format and this library renders it as specified, including where the specification cannot express what BSON holds. A caller that needs the bytes themselves reads the column in raw mode, which is base64 of the document and lossless for everything below; a caller working in Extended JSON decides for itself what these mean:

| Document | What Extended JSON does with it |
| --- | --- |
| A nested document whose keys spell a wrapper, such as `{"x": {"$numberInt": "1"}}` where `$numberInt` is an ordinary field | Indistinguishable from the `int32` 1; the specification requires it to be read as the type it names, and offers no escape |
| A `double` holding a NaN whose sign or payload is not the driver's own | Every NaN is spelled `"NaN"`, so both are gone |
| A key or string that is not valid UTF-8, which BSON requires both to be | Rendered as U+FFFD, as in any JSON |
| A regular expression whose option letters are not in alphabetical order, which BSON also requires | Comes back sorted: the same expression, different bytes |
| A malformed binary of the deprecated `0x02` subtype, missing the length prefix its payload carries | Gains that prefix on every pass through the rendering, rather than settling after one |
| A document nested more than 200 levels deep | The driver's Extended JSON parser stops there, though its BSON decoder does not, so the text it renders will not parse again |

Two of these fail louder than the rest, and a pipeline that reads a column and writes it back should expect it: where a wrapper-shaped document's value does not fit the type its key names — `{"$numberInt": "A"}`, or an `$oid` that is not hex — and where a document is nested past 200 levels, the rendering does not parse at all. Feeding such a rendering to a writer, or `marshal.ConvertToJSONFriendly` output straight into one, fails with a `parse BSON` error rather than storing a changed document.

The write path has the same shape: the Extended JSON parser accepts a raw byte in a key that the rendering then spells U+FFFD, so text written and text read back can disagree. So a read-modify-write of one of these documents through the interpreted path changes it, without an error. That is a property of Extended JSON rather than of this library, and raw mode is the way around it. Rendering costs about 1.25 to 1.4 times the `map[string]any` conversion it replaces, and raw mode roughly 30 to 40 times less than either; `BenchmarkBSON` measures all three.

Releases up to v3.8.3 rendered a document as a `map[string]any`, which cannot express `ObjectID`, `Decimal128`, dates, binary subtypes or integer widths, and only `JSONWriter` could write a valid document at all — it base64-decoded the string it was given through the byte-array guess, while `CSVWriter` stored the characters verbatim, so a `BSON` column written from CSV never held BSON and the reader base64-encoded that ASCII a second time on the way out. That rendering had ambiguities of its own, which this one removes: an `ObjectID` was indistinguishable from a string holding its hex, an `int32` from an `int64`, and field order was lost to Go's map. Removing the byte-array guess earlier in v3.9.0 left interpreted writes refused outright until this form arrived. Raw mode is unchanged throughout: base64 of the document bytes in both directions.

### Geospatial Values

A `GEOMETRY` or `GEOGRAPHY` column is a `BYTE_ARRAY` holding WKB. Unlike every other logical type, its interpreted form is not one shape but four, chosen by `types.GeospatialJSONMode`, and the write path takes whichever one the same config renders. Symmetry is by mode rather than by shape: no sniffing, and nothing to guess.

| Mode | Form, read and written | Round trips exactly |
| --- | --- | --- |
| `GeospatialModeHex` (the `GEOMETRY` default) | `{"wkb_hex": "0101…", "crs": "OGC:CRS84"}` | yes |
| `GeospatialModeBase64` | `{"wkb_b64": "AQEA…", "crs": "OGC:CRS84"}` | yes |
| `GeospatialModeHybrid` | both, plus `geojson`; the write path reads the WKB half | yes |
| `GeospatialModeGeoJSON` (the `GEOGRAPHY` default) | a GeoJSON geometry, wrapped in a Feature unless `WithGeospatialGeoJSONAsFeature(false)` | no, see below |

`JSONWriter` is handed the object itself, which is the one case where a primitive column takes a JSON object rather than a scalar. `CSVWriter` has no way to carry an object, so a geospatial cell is the JSON text of that same object.

GeoJSON cannot always carry the bytes back, and every reason is a property of that form rather than of the column:

| Limit | Consequence |
| --- | --- |
| `CoordPrecision` defaults to 6, so coordinates are rounded before they are rendered | `WithGeospatialCoordinatePrecision(-1)` turns rounding off and restores the exact round trip |
| GeoJSON has no M ordinate, and Z is not read yet (#439) | a geometry carrying either is refused on read in GeoJSON and hybrid mode, so it never reaches the write path; hex and base64 carry it untouched in both directions |
| `GeospatialReprojector` transforms to CRS84 one way, with no inverse | a reprojected rendering cannot be written back to its original CRS |

So a read-modify-write in GeoJSON mode can move a coordinate, quietly, at the default precision. The three WKB-carrying modes have no such gap, and raw mode — base64 of the WKB, unchanged in both directions — costs a third to a fifth of what the others do, `BenchmarkGeospatial` measuring all five.

Two edits the write path silently ignores, in any mode. The `crs` and `algorithm` fields a rendering carries are decoration on the way in: the column's own annotation decides them, so changing either and writing the value back does nothing and reports nothing. And in hybrid mode the WKB half is what is stored, so an edited `geojson` beside an unedited `wkb_hex` writes the original geometry — edit one or the other, not both.

For geometries containing only the seven basic types, every mode checks that the bytes it ends up with are one whole WKB geometry and nothing more. That measurement reads structure rather than coordinates, so it covers Z, M and ZM as well as 2D: a `POINT Z` is carried through untouched, and a truncated one is refused. It also holds a container to the ISO rule that its members share its own dimension — a `MultiPointZ` holds `PointZ`, a plain `MultiPoint` holds plain `Point` — and to the OGC rule that a `MultiPoint` holds points and nothing else, which a `GeometryCollection` alone is exempt from. A standardized code above `GeometryCollection`, such as `CircularString` or `TIN`, has no body reader here. When encountered at the root or inside a nested collection, its header is checked and its bytes are preserved; validation stops there because the remaining members and trailing bytes cannot be located.

The check is also the one way the reader can hand you a value it will not take back. Hex and base64 render bytes without parsing them, so a column holding something the check refuses reads back cleanly as `{"wkb_hex": …}` and then fails on the way in: a corrupt value, one written with EWKB's SRID flag, or a geometry whose members disagree with their container on dimension or type — well formed by every other measure, and malformed by the one above. Raw mode moves any of those without inspecting them in either direction.

### UUID Values

A `UUID` column is a `FIXED_LEN_BYTE_ARRAY(16)`, and its Go representation is a `string` holding the 16 raw bytes. Writers that take Go values directly, such as `ParquetWriter` over structs or maps, write that string as-is: convert to the raw 16 bytes before handing a value to those writers, since a string of any other width is rejected when the page is built.

Writers that take string input parse the value instead. `JSONWriter`, `CSVWriter`, and the `types.StrToParquetTypeWithLogical` helper accept these textual forms:

| Form | Example |
| --- | --- |
| Canonical dashed | `550e8400-e29b-41d4-a716-446655440000` |
| Undashed hex | `550e8400e29b41d4a716446655440000` |
| Braced | `{550e8400-e29b-41d4-a716-446655440000}` |
| URN | `urn:uuid:550e8400-e29b-41d4-a716-446655440000` |

The writers pass the column's declared width to those helpers themselves; a direct caller has to pass `16`, as noted under [Logical Types](#logical-types).

Any other input fails the write with a `parse UUID` error, including a 38-byte string in some wrapper other than braces such as `[550e8400-e29b-41d4-a716-446655440000]`. Releases up to v3.8.2 silently wrote unparsable input as the raw bytes of the string itself, padding or truncating it to 16 bytes and corrupting the column value, and a `UUID` column declared with a width other than 16 truncated every value it stored. That fallback also let a 16-byte binary string through unchanged; convert such values to one of the forms above first, for example with `uuid.FromBytes([]byte(v))` and `String()`.

JSON output renders UUID columns as canonical dashed strings, so values read that way can be written back without conversion.

### DECIMAL Values

A `DECIMAL` column stores an unscaled integer and a fixed `scale`. Every conversion here carries that integer as a `big.Int`, so a `DECIMAL(38, 2)` stays exact end to end.

`JSONWriter`, `CSVWriter`, and the `types.StrToParquetType` helpers parse decimal text at arbitrary precision. Plain (`123.45`) and exponent (`1.2345e2`) forms are both accepted. Digits below the column's scale are rounded half away from zero, the rule SQL engines use when casting to a narrower `DECIMAL`. Bad input fails the write rather than being stored as a different number:

| Input | Error |
| --- | --- |
| More digits than the declared `precision` | `exceeds precision` |
| Too large for the physical type or the column width | `does not fit` |
| Not decimal text, such as `1/2`, `0x10`, or `NaN` | `parse DECIMAL` |

The precision check needs the `DECIMAL` logical type. Every schema built from a struct tag, JSON schema, or CSV metadata entry carries one, so it is skipped only for a caller that passes a bare `convertedtype=DECIMAL` to the helpers. `ParquetWriter` over structs and maps stores the unscaled integer as given, checking neither precision nor width, the same way it handles `UUID`.

JSON output is the exact decimal text, trailing zeros included, carried in a `json.Number`. `encoding/json` writes that as a bare number, so a `DECIMAL(38, 2)` reads back as `123456789012345678901234.56` and can be written back unchanged. Decoding into a Go `float64` still yields one, with the rounding that implies.

Releases up to v3.8.3 routed DECIMAL through `float64`, silently rounding anything past about 16 digits. `9999999999999999.99` was stored as `1000000000000000000`, and the value above read back as `1.2345678901234569e+23`.

### TIME Values

A `TIME` column holds elapsed time since midnight, so the only legal values are `[0, 24h)`: for `TIME_MILLIS` that is `0` through `86399999`, and `86400000` is already out of range.

`JSONWriter`, `CSVWriter`, and the `types.StrToParquetType` helpers accept either a clock string (`23:59:59.999`, `12:34:56.789012345`, or `12:34:56` with no fraction) or a bare count of the column's unit since midnight. A value past the end of the day, or a negative one, fails the write with an `outside [0, 24h)` error, and input that is neither form, `12abc` included, fails with a `parse TIME_MILLIS` error rather than being stored as the digits it happens to start with. `ParquetWriter` over structs and maps stores the integer as given, checking nothing, the same way it handles `UUID` and `DECIMAL`.

`types.JSONTypeToParquetTypeWithLogical` also takes a TIME as a Go number rather than text, and checks it the same way: a fractional or non-finite value fails with a `not a whole number of ticks` error instead of being truncated into the column.

JSON output renders a `TIME` as `HH:MM:SS` with the column's fractional width. An out-of-range value can now only come from another writer; the sign applies to the whole value and the hour field grows past 24, so `-1000` reads as `-00:00:01.000` and 25 hours as `25:00:00.000`, neither of which can be written back as a different value. Releases up to v3.8.3 signed each component separately, rendering `-1000` as `00:00:-1.000` and rewriting that string as `0`, and rewrote their own `25:00:00.000` as `25`.

### TIMESTAMP Values

A `TIMESTAMP` column stores a count of milliseconds, microseconds, or nanoseconds since the Unix epoch, and `JSONWriter`, `CSVWriter`, and `types.StrToParquetTypeWithLogical` accept either an RFC 3339 timestamp or that bare count. Both spellings of the annotation, the `TIMESTAMP` logical type and the legacy `TIMESTAMP_MILLIS`/`TIMESTAMP_MICROS` converted types, are read identically; the count is scanned over the whole field, so `"123abc"` fails with a `parse TIMESTAMP_MICROS` error rather than being stored as `123`.

Milliseconds and microseconds reach well beyond the year 3000, and a timestamp is now scaled at the column's own unit so that range is usable. Up to v3.8.3 every unit was scaled from a nanosecond count, which only spans 1678 through 2262 and wraps outside it, so `2300-01-01T00:00:00Z` in a `TIMESTAMP_MILLIS` column was stored as a negative count that reads back as 1715. The same arithmetic truncated toward zero rather than down, so a pre-epoch value with sub-unit precision lost a tick: `1969-12-31T23:59:59.9995Z` stored `0` instead of `-1`. `TIMESTAMP_NANOS` genuinely cannot hold a date outside that window and now reports it instead of wrapping.

A `TIMESTAMP` annotation that names no unit, including one whose unit field is set to a member this release does not know, describes nothing about the value, so the column's own `INT64` scan reads it.

### INT96 Values

An `INT96` column stores a nanosecond within the day followed by a Julian day, spanning 4713 BC to year 5874898. Both halves are carried whole: JSON output renders the value as an ISO 8601 timestamp with all nine fractional digits, and `JSONWriter`, `CSVWriter`, and `types.ParseINT96String` read that form back, including years that are negative or wider than four digits.

A timestamp the column cannot hold fails the write rather than being stored as a different one: a day past either end of the range gives an `outside the range an INT96 can hold` error, and a day that does not exist in the year supplied, such as `10001-02-29`, gives an `out of range for February` error. Input that is neither a timestamp nor a bare integer fails with a `parse INT96 timestamp` error; up to v3.8.3 it fell through to the integer form, so a rejected timestamp was stored as whatever digits it started with, and `5874898-06-04T00:00:00Z` became the number 5874898.

Releases up to v3.8.3 converted through a nanosecond offset from the Unix epoch, which only covers 1678 through 2262. A day outside that window wrapped onto an unrelated date, so Julian day 0 read as `1717-12-28T19:20:10.805067776Z` and writing that string back stored a different value again; sub-microsecond digits were dropped in both directions, so an `INT96` from another writer lost its last three digits on any read-modify-write.

### Repetition Types

| Repetition Type | Go Declaration | Description |
| --- | --- | --- |
| `REQUIRED` | `V1 int32` with tag `parquet:"name=v1, type=INT32"` | Standard required field |
| `OPTIONAL` | `V1 *int32` with tag `parquet:"name=v1, type=INT32"` | Use a pointer for optional fields |
| `REPEATED` | `V1 []int32` with tag `parquet:"name=v1, type=INT32, repetitiontype=REPEATED"` | Use a slice with `repetitiontype=REPEATED` |

LIST and REPEATED are different in the Parquet format. Prefer LIST for list data. Standard and non-standard LIST/MAP layouts are both supported.

## Encoding Support

| Encoding | Types | Read | Write |
| --- | --- | --- | --- |
| `PLAIN` | All types | Y | Y |
| `PLAIN_DICTIONARY` | All types | Y | Y |
| `RLE_DICTIONARY` | All types | Y | Y |
| `DELTA_BINARY_PACKED` | Integer types | Y | Y |
| `DELTA_BYTE_ARRAY` | `BYTE_ARRAY`, `UTF8` | Y | Y |
| `DELTA_LENGTH_BYTE_ARRAY` | `BYTE_ARRAY`, `UTF8` | Y | Y |
| `BYTE_STREAM_SPLIT` | `INT32`, `INT64`, `FIXED_LEN_BYTE_ARRAY` | Y | Y |
| `BIT_PACKED` | Boolean, integer | Y | Y |

Encoding notes:

- For maximum compatibility, use `PLAIN` and `PLAIN_DICTIONARY`.
- Dictionary indices use the minimum bit width required by the completed row-group dictionary. Encoded dictionary value bytes are capped at 1 MiB by default, after which the writer uses `PLAIN` encoding for subsequent pages; tune the cap with `writer.WithMaxDictionarySize`.
- `writer.WithDataPageVersion(2)` applies to both dictionary-encoded and plain data pages.
- Use `omitstats=true` in a field tag to skip statistics for large array fields.
- Whenever min/max statistics are available, the current `min_value`/`max_value` fields are written. The deprecated `min`/`max` fields (PARQUET-251) are limited to signed sort orders; for unsigned-ordered columns (e.g. `BYTE_ARRAY`/UTF8 and unsigned integer logical types) they are omitted so legacy readers do not misinterpret them.
- `SizeStatistics.unencoded_byte_array_data_bytes` counts the bytes of each `BYTE_ARRAY` value present at the column's maximum definition level, excluding the four-byte length prefixes and counting a repeated value once per occurrence, on dictionary-encoded and plain pages alike. Up to v3.8.3 a dictionary-encoded column reported zero instead of its true total, which a reader sizing a decompression buffer from it would believe.
- A column chunk whose data pages are all dictionary encoded carries an exact `distinct_count` statistic taken from its dictionary, which holds one entry per distinct non-null value, and covers that row group alone. It is omitted for columns tagged `omitstats=true`, and once the dictionary reaches `writer.WithMaxDictionarySize` and the remaining pages fall back to `PLAIN`, because the dictionary then covers only part of the chunk. It is omitted again wherever a dictionary entry does not correspond one-to-one with a distinct logical value: for `FLOAT`/`DOUBLE` columns holding a NaN, whose entries do not deduplicate; for `DECIMAL` backed by `BYTE_ARRAY`, whose variable width admits several two's-complement encodings of the same unscaled value (`FIXED_LEN_BYTE_ARRAY` decimals keep the statistic, since their width pins one encoding per value); for `FLOAT16`, where `-0.0` and `+0.0` occupy separate entries; and for `GEOMETRY`/`GEOGRAPHY`, whose WKB payloads carry a byte-order flag and so encode one geometry two ways.
- A column whose annotation has no defined sort order carries no `min`/`max` statistics: the `INTERVAL` converted type, whose ordering the specification leaves undefined, and `GEOMETRY`/`GEOGRAPHY`, for which `GeospatialStatistics` carries the bounds instead. Up to v3.8.3 a dictionary-encoded column of either kind wrote them anyway, from the raw bytes. A chunk with no min/max carries no `ColumnIndex` at all, since the index requires bounds, so its per-page null counts and level histograms go with them.
- Column indexes advertise `ASCENDING` or `DESCENDING` boundary order when both page-level minimum and maximum bounds are monotonic under the column's Parquet sort order. Null-only pages are ignored when determining the order; non-monotonic bound sequences are marked `UNORDERED`. NaN never appears as a bound, so it cannot make a sequence non-monotonic.

### Binary statistics bound truncation

Binary footer statistics and column-index bounds are not truncated by default, preserving the writer's historical behavior. Enable truncation and set its target byte length with `writer.WithBinaryMinMaxTruncateLength`.

The maximum length applies only to these column types:

- Unannotated `BYTE_ARRAY` and unannotated `FIXED_LEN_BYTE_ARRAY`: bounds are truncated as raw bytes. The configured length is a target rather than a hard cap; if an all-`0xFF` maximum prefix cannot be incremented, the original exact maximum is retained.
- `BYTE_ARRAY` annotated with logical type `STRING` or converted type `UTF8`: minimum bounds are shortened at a UTF-8 character boundary and maximum bounds are rounded up to a valid UTF-8 upper bound. The configured length is a target rather than a hard cap for these columns; for example, with a target of 64 bytes, a stored bound may exceed 64 bytes when retaining the original value is necessary to keep a valid UTF-8 bound.

The maximum length does not apply to any other logical or converted type, including annotated `FIXED_LEN_BYTE_ARRAY`, `ENUM`, `JSON`, `BSON`, `UUID`, `DECIMAL`, `FLOAT16`, `INTERVAL`, `GEOMETRY`, and `GEOGRAPHY`. Compact Parquet bounds must remain valid values of their logical type, which arbitrary prefix truncation cannot guarantee for those annotations.

When reading files from non-conforming writers, invalid footer minimum and maximum bounds are independently treated as absent. A column index containing an invalid bound is ignored in full so malformed metadata cannot be used for page pruning. Bounds with unsupported logical ordering or validation, including `GEOMETRY` and `GEOGRAPHY`, are also treated as absent. Compact bounds for unannotated `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` columns remain valid raw byte bounds.

## Compression Support

| Compression | Supported | Default Level | Library |
| --- | --- | --- | --- |
| `UNCOMPRESSED` | Y | N/A | N/A |
| `SNAPPY` | Y | N/A | `klauspost/compress/snappy` |
| `GZIP` | Y | 6 | `klauspost/compress/gzip` |
| `LZO` | N | N/A | N/A |
| `BROTLI` | Y | 6 | `andybalholm/brotli` |
| `LZ4` | Y | Fast (0) | `pierrec/lz4/v4` |
| `LZ4_RAW` | Y | 9 | `pierrec/lz4/v4` |
| `ZSTD` | Y | 3 | `klauspost/compress/zstd` |

Compression notes:

- Standard writers default to `SNAPPY`. `NewArrowWriter` defaults to `GZIP`.
- Set a file-level codec with `writer.WithCompressionCodec`.
- Set a per-column codec with a struct tag such as `parquet:"name=col, compression=GZIP"`.
- Set codec-level compression levels with `writer.WithCompressionLevel(codec, level)`. All columns using that codec share the same level.
- `LZ4` is deprecated and ambiguous: files in the wild carry either the Hadoop block framing that parquet-mr writes or the framed LZ4 format. Reads accept both, detected from the payload. Writes emit the Hadoop block framing, so a file written with `LZ4` is readable by parquet-mr and Arrow. Releases up to v3.7.4 wrote the framed format under this codec, and those files still read. Use `LZ4_RAW`, which uses raw LZ4 blocks and is the preferred LZ4 variant in the Parquet specification.
- Compression codecs enforce decompressed size limits, defaulting to 256 MB, via `compress.WithMaxDecompressedSize`.

## Readers and Writers

Writer types:

| Writer | Use |
| --- | --- |
| `ParquetWriter` | Write Go structs |
| `JSONWriter` | Convert JSON rows to Parquet |
| `CSVWriter` | Write flat CSV-like data |
| `ArrowWriter` | Write data using Arrow schemas |

Reader types:

| Reader | Use |
| --- | --- |
| `ParquetReader` | Read rows into Go structs or inferred schemas |
| `ColumnReader` | Read raw column values with repetition and definition levels |

For large files, read in chunks rather than loading all rows at once.

```go
pw, err := writer.NewParquetWriter(fw, new(MyStruct),
    writer.WithRowGroupSize(common.DefaultRowGroupSize), // default 128M
    writer.WithPageSize(common.DefaultPageSize),         // default 8K
)
```

`ParquetReader.SkipRows` jumps ahead using the file's own positional metadata: it skips whole row groups by their declared row counts and, when a column offset index is present, seeks straight to the target data page instead of decoding every page along the way. Both are taken on trust, the same way the reader already trusts row group row counts everywhere else, so a corrupted file could in theory point a skip at the wrong row. That is really a broken file rather than a reader bug, and parquet-go just does its best with what the file declares: if an offset index is missing or structurally unusable it quietly falls back to a plain sequential skip, which reads the real page boundaries.

## File Sources

File sources implement separate reader and writer interfaces.

```go
type ParquetFileReader interface {
    io.Seeker
    io.Reader
    io.Closer
    Open(name string) (ParquetFileReader, error)
    Clone() (ParquetFileReader, error)
}

type ParquetFileWriter interface {
    io.Writer
    io.Closer
    Create(name string) (ParquetFileWriter, error)
}
```

Supported sources:

- Local filesystem
- HDFS
- S3 (AWS SDK v2)
- Google Cloud Storage
- Azure Blob Storage
- HTTP (read-only)
- Memory buffer
- GoCloud CDK generic blob storage
- OpenStack Swift

See [source/README.md](source/README.md) for source-specific details.

## Advanced Features

### CRC Page Checksums

The Parquet format defines an optional CRC32 field in `PageHeader`. The checksum covers the compressed page data, not the page header itself.

Reader validation is controlled with `reader.WithCRCMode`.

| Scenario | Strict | Auto | Ignore (default) |
| --- | --- | --- | --- |
| CRC absent in header | Fail | Pass | Pass |
| CRC present and valid | Pass | Pass | Pass |
| CRC present and invalid | Fail | Fail | Pass |

Mode behavior:

- `common.CRCIgnore` skips validation and preserves backward-compatible behavior.
- `common.CRCAuto` validates CRC when present and passes when absent.
- `common.CRCStrict` requires CRC on every page and validates it.

Writer CRC computation is controlled with `writer.WithWriteCRC`.

```go
pw, err := writer.NewParquetWriter(pFile, obj, writer.WithWriteCRC(true))
```

CRC is computed for data pages, dictionary pages, and dictionary-encoded data pages.

### Bloom Filters

Write a bloom filter for a column with the `bloomfilter=true` struct tag, optionally sized with `bloomfiltersize` (bytes, rounded up to a power of two). Every row group of that column gets a filter of the configured size. When a schema is built programmatically rather than from struct tags, `common.Tag.SetBloomFilter(enabled, numBytes)` configures the same thing and `common.Tag.BloomFilterConfig()` reports it back. The same pair sits on `Tag.Key` and `Tag.Value`, covering the key and value columns of a map or list the way the `keybloomfilter` and `valuebloomfilter` tags do.

Reads are per row group. `ParquetReader.BloomFilterCheckWithContext(ctx, columnPath, rowGroupIndex, value)` probes membership and returns true when the column has no filter, so it answers "might contain" rather than "has a filter". `ParquetReader.BloomFilterSize(ctx, columnPath, rowGroupIndex)` returns the bitset size in bytes for that row group's filter, or 0 when the column chunk has no filter; it reads the filter header only, never the bitset, which matters because a bitset may be up to 128MB.

Presence and on-disk size are also readable straight from the footer, without touching the file body: `ColumnMetaData.IsSetBloomFilterOffset` reports presence, and `ColumnMetaData.GetBloomFilterLength`, when the writer sets the optional field, gives the stored length of the Thrift header plus the bitset. Prefer these for whole-file inventories.

Bloom filter state is never reported through the schema, because a filter belongs to a column chunk in one row group rather than to the column. Opening a file therefore reads no bloom filter data at all. Earlier v3 releases exposed `BloomFilter` and `BloomFilterSize` on `SchemaHandler.Infos`, populated from row group 0 alone: replace reads of those fields with `ColumnMetaData.IsSetBloomFilterOffset` for presence and `ParquetReader.BloomFilterSize` for size, and replace writes with `common.Tag.SetBloomFilter`.

### Encryption

The reader and writer support Apache Parquet modular encryption for encrypted footers (`PARE`) and plaintext footers signed with AES-GCM (`PAR1`). Page headers, data pages, dictionary pages, column metadata, column indexes, offset indexes, and bloom filter headers/bitsets are encrypted and decrypted when encryption metadata and the required keys are available.

Footer mode and column classification interact as follows:

| Footer mode | Plaintext column | Footer-key column | Column-key column |
| --- | --- | --- | --- |
| Encrypted footer (`PARE`) | Page/index/bloom modules are plaintext. The footer key is required to open the encrypted file metadata, regardless of column classification. | Page/index/bloom modules use the footer key; column metadata stays in the encrypted footer. | Page/index/bloom modules use the column key; encrypted column metadata is stored for readers with only the column key. |
| Signed plaintext footer (`PAR1`) | Page/index/bloom modules and column statistics are plaintext; readers without keys can read projected plaintext columns. | Page/index/bloom modules use the footer key; plaintext footer metadata is present but statistics are stripped. | Page/index/bloom modules use the column key; plaintext footer metadata is present but statistics are stripped. |

Writer column classification is selected with `writer.WithColumnEncrypted(path, opts...)`, where `path` is the rootless leaf path in the file schema (without the root element). Path components are separated by `common.ParGoPathDelimiter` (`\x01`); build the value with `common.PathToStr` — `.` is an ordinary character in a name, not a separator. The path is matched against external Parquet names (the `name=` value in the struct tag); Go struct field names are not accepted, so the writer and `reader.WithColumnKey` apply the same path-resolution rules. Once the writer has a schema, it prepends the external schema root internally for validation and lookup. Root names are not stripped from option values: `common.PathToStr([]string{"parquet_go_root", "ssn"})` will not match a rootless `ssn` column, but it can match a nested column whose first path component is actually named `parquet_go_root`.

```go
// Omitted path: column is plaintext. Only the footer is encrypted.
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
)
```

```go
// Footer-key column; sibling unkeyed columns remain plaintext.
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithColumnEncrypted("name", writer.ColumnFooterKey()),
)
```

```go
// Literal column key.
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithColumnEncrypted("ssn", writer.ColumnKey(ssnKey)),
)
```

```go
// Literal column key plus stored key metadata for downstream KMS readers.
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithColumnEncrypted("ssn", writer.ColumnKey(ssnKey, []byte("kms://prod/ssn"))),
)
```

```go
// Writer resolves the column key from metadata through its KeyRetriever.
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithColumnEncrypted("ssn", writer.ColumnKeyByMetadata([]byte("ssn-key"))),
    writer.WithKeyRetriever(keyRetriever),
)
```

For callers that build `EncryptionConfig` literally, `ColumnKeys[p] = writer.EncryptionColumnKey{}` is equivalent to `WithColumnEncrypted(p)` and produces `ENCRYPTION_WITH_FOOTER_KEY`. To retrieve by metadata, set non-empty `KeyMetadata` and configure `KeyRetriever`; to use the footer key, keep the zero value or use `ColumnFooterKey()`.

| `ColumnKeys[p]` state | How to produce | Resolved behavior |
| --- | --- | --- |
| not in map | omit `WithColumnEncrypted(p, ...)` | plaintext |
| `{}` | `WithColumnEncrypted(p)` or `WithColumnEncrypted(p, ColumnFooterKey())` | footer-key, no `KeyMetadata` stored |
| `{Key: bytes}` | `WithColumnEncrypted(p, ColumnKey(key))` | column-key, no `KeyMetadata` stored |
| `{Key: bytes, KeyMetadata: md}` | `WithColumnEncrypted(p, ColumnKey(key, md))` | column-key plus `KeyMetadata` stored in file |
| `{Key: nil, KeyMetadata: md}` plus writer `KeyRetriever` | `WithColumnEncrypted(p, ColumnKeyByMetadata(md))` | retriever called at write time; empty result is an error |
| `{Key: nil, KeyMetadata: md}` with no writer `KeyRetriever` | same call without retriever | construction error |

Repeated column options follow standard Go map semantics: the last call wins and no conflict detection runs. Applications that compose options dynamically should keep a single owner for each column path.

```go
writer.WithColumnEncrypted("ssn", writer.ColumnKey(oldKey))
writer.WithColumnEncrypted("ssn", writer.ColumnFooterKey()) // final state
```

Reader key options, including `reader.WithColumnKey`, supply key bytes directly. The writer API uses the structured `WithColumnEncrypted` to express column treatment — key metadata, the `ColumnKeyByMetadata` retriever path, and explicit `ColumnFooterKey` selection — none of which have a reader-side analogue.

Encrypted footer with one column-key column and the rest plaintext:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithColumnEncrypted("ssn", writer.ColumnKey(ssnKey)),
)
```

Signed plaintext footer with one column-key column and the rest plaintext:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithPlaintextFooter(true),
    writer.WithColumnEncrypted("ssn", writer.ColumnKey(ssnKey)),
)
```

Encrypted footer with a footer-key column and plaintext siblings:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithColumnEncrypted("name"),
)
```

`WithColumnEncrypted(p, ColumnFooterKey())` is the explicit selector that keeps `p` encrypted with the footer key while sibling columns omitted from `ColumnKeys` are plaintext.

Three-way mix in one file:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithColumnEncrypted("name"),
    writer.WithColumnEncrypted("ssn", writer.ColumnKey(ssnKey)),
)
```

All columns plaintext with an encrypted footer:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
)
```

KMS pattern where the writer already has the key:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithColumnEncrypted("ssn", writer.ColumnKey(ssnKey, []byte("ssn-key-id"))),
)
```

Pure retriever pattern:

```go
keyRetriever := func(keyMetadata []byte) ([]byte, error) {
    return lookupKey(keyMetadata)
}

pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKeyMetadata([]byte("footer-key-id")),
    writer.WithColumnEncrypted("ssn", writer.ColumnKeyByMetadata([]byte("ssn-key-id"))),
    writer.WithKeyRetriever(keyRetriever),
)
```

`WithAADPrefix` supplies the file AAD prefix. If `WithSupplyAADPrefix(true)` is set, readers must pass the same value with `reader.WithAADPrefix`. `EncryptionAESGCMV1` encrypts all modules with AES-GCM; `EncryptionAESGCMCTRV1` uses AES-CTR for page bodies and AES-GCM for metadata modules.

Security guidance:

- Plaintext columns expose values, statistics, indexes, and bloom filters in the clear. Use `ColumnKey(...)` or `ColumnFooterKey()` for sensitive fields.
- Encrypted columns under plaintext-footer mode have `Statistics`, `SizeStatistics`, and `GeospatialStatistics` stripped from the plaintext `ColumnMetaData` and stored only in authenticated `EncryptedColumnMetadata`.
- Plaintext-footer encrypted columns still expose page counts, encodings, value counts, offsets, compressed sizes, key metadata, and column names through the plaintext footer. Use encrypted footer mode when those metadata are sensitive.
- Each file should use a unique `(AADPrefix, AADFileUnique)` pair. Reusing the same pair with the same key weakens module-swap protection.
- `AES_GCM_CTR_V1` does not authenticate page bodies; use `AES_GCM_V1` when page-data tamper detection is required.

Reader behavior is driven by the file's per-column `CryptoMetadata`: nil means plaintext, `ENCRYPTION_WITH_FOOTER_KEY` means footer-key column, and `ENCRYPTION_WITH_COLUMN_KEY` means column-key column. `reader.WithColumnKey(path, key)` supplies a direct key for a rootless leaf path in the file schema, matched against the file's `PathInSchema` (external Parquet names); `WithCaseInsensitive(true)` accepts case-only differences, but Go struct field names with different spelling are not accepted. `reader.WithKeyRetriever` resolves keys from stored `key_metadata`. Once the reader has a schema, it prepends the external schema root internally for validation and lookup. Like writer column options, reader column-key paths must omit the schema root; a root name in the option value is treated as an ordinary path component. If a writer intentionally stores `ENCRYPTION_WITH_COLUMN_KEY` while using bytes equal to the footer key, downstream readers can decrypt it either with `reader.WithColumnKey(path, footerKey)` or with a retriever that returns `footerKey` for that column metadata.

Writer metadata-based keys are strict: `ColumnKeyByMetadata(md)` requires the writer's `KeyRetriever` to return a non-empty AES key at construction time. The reader API still treats missing column-key material as a read-time decryption failure because `reader.WithColumnKey` carries only key bytes, not expected key metadata.

Compatibility: `apache/parquet-testing` includes mixed plaintext/encrypted plaintext-footer fixtures generated through Parquet C++/parquet-mr test paths, and this repository reads them in interop tests. It does not currently include parquet-cpp 1.x mixed writer fixtures, so compatibility with older readers on writer-produced mixed files is based on Parquet spec compliance rather than direct fixture coverage. Files produced by this library should interoperate with spec-compliant readers when the same keys and AAD configuration are used.

Spec references:

- Modular encryption: https://parquet.apache.org/docs/file-format/data-pages/encryption/
- Bloom filter encryption: https://parquet.apache.org/docs/file-format/bloomfilter/

### Non-finite Floating Point JSON Representation

`NaN` and infinite `FLOAT`, `DOUBLE`, and `FLOAT16` values have no JSON number form, so `marshal.ConvertToJSONFriendly` and `types.ConvertToJSONType` render them as the quoted strings `"NaN"`, `"Infinity"`, and `"-Infinity"`, in struct fields, list elements, map values, and legacy `REPEATED` columns alike. `JSONWriter` accepts those strings on input, along with `Inf`, `+Inf`, and `-Inf`, case-insensitive. Finite values are unchanged. See [example/json_nan](example/json_nan) for a round trip.

Quoted strings are used rather than the bare `NaN` and `Infinity` literals that Python and DuckDB emit, because those are not valid JSON and Go's `encoding/json` refuses to produce them; a quoted string is ordinary JSON that every parser accepts.

The infinity spelling is `"Infinity"` rather than Go's native `"+Inf"` because the output has to survive being read somewhere else. Both round trip through this library, and `"NaN"` is recovered as a float everywhere, but `"+Inf"` and `"-Inf"` are not consistently accepted across ecosystems. Go, Python, and DuckDB parse them as infinities, while Java's `Double.parseDouble` and Jackson reject them and JavaScript's `Number()` silently returns `NaN`, turning an infinity into a different value with no error; Spark's JSON reader is documented as handling quoted non-numeric tokens inconsistently (SPARK-38060). `"Infinity"` and `"-Infinity"` are recovered correctly by all of them. This differs from Apache Arrow's Go implementation, which emits `"+Inf"`.

`marshal.ConvertToJSONFriendly` reports any value its column cannot render, wrapping `types.ErrUnrenderable`, rather than returning a substitute. That covers every case listed under [Reading Values](#reading-values): BSON that does not parse or is empty, bytes a geospatial rendering cannot be produced from, a `UUID`, `FLOAT16`, `INTERVAL` or `INT96` whose width is not the one the format fixes, an integer outside the range its annotation declares, a value that is not bytes where bytes are required, and malformed VARIANT metadata or value bytes. Up to v3.8.3 the non-VARIANT cases came back as substitutes indistinguishable from values that rendered; malformed VARIANT data also returned its base64 map with a nil error until the same rule was applied to it. The conversion stops at the first and returns no output, so one such value is reported for the whole batch rather than hidden in it. Direct callers of `types.ConvertValue` or `types.ConvertVariantValue` still receive the substitute alongside the error if they explicitly want to carry on with it; the deprecated `types.ConvertToJSONType` remains best-effort for a single non-VARIANT value.

Logical type conversion in `marshal.ConvertToJSONFriendly` follows the column's own schema path, so it covers legacy `REPEATED` columns, which repeat a value in place rather than wrapping it in a three-level `LIST` group. Those columns get the same treatment as `LIST` elements: `DATE`, `TIMESTAMP`, `DECIMAL`, and non-finite floats all render in their JSON form.

### Non-finite Floating Point Statistics

The Parquet specification treats the two kinds of non-finite value differently in `min`/`max` statistics. Infinities are ordinary values under the column's sort order and are stored as bounds like any other. `NaN` has no position in that ordering, so it is excluded: bounds are computed from non-NaN values only, and a column chunk or page whose non-null values are all `NaN` gets no bounds written at all. A page with no bounds also suppresses the `ColumnIndex` for its column chunk, since `min_values` and `max_values` are required there. This applies to `FLOAT`, `DOUBLE`, and `FLOAT16` columns.

### GeoParquet

parquet-go supports Apache Parquet geospatial logical types and configurable JSON output through `marshal.ConvertToJSONFriendly`.

Overview:

- `GEOMETRY` stores planar coordinates with optional CRS.
- `GEOGRAPHY` stores spherical coordinates with optional CRS and edge interpolation algorithm.
- Physical storage is WKB in `BYTE_ARRAY` fields.
- CRS defaults to `OGC:CRS84` when not provided.
- GEOGRAPHY algorithms include `SPHERICAL`, `VINCENTY`, `THOMAS`, `ANDOYER`, and `KARNEY`.

Z and M ordinates: the WKB reading here is 2D. From v3.9.0 an ISO geometry carrying Z or M ordinates is refused by the GeoJSON and hybrid modes and comes back as the `wkb_hex` substitute, rather than as coordinates the value does not hold. Up to v3.8.3 the extra ordinates were read as though the value were 2D. The same header check refuses two malformed shapes that used to render as 2D GeoJSON: a byte order byte other than 0 or 1, and a dimension above ZM, which is what an EWKB flag word such as PostGIS's `0x20000001` sets. The hex and base64 modes render any bytes and are unaffected.

Geometry types with no reader here: the standardized WKB/SQL-MM type-code space extends through `Triangle` (17), and this library reads the seven basic geometries, `Point` through `GeometryCollection`. A value carrying another recognized code, such as `CircularString`, `PolyhedralSurface` or `TIN`, is treated like a Z or M geometry: it renders as the `wkb_hex` substitute and contributes no bounding box, while its declared type is retained, so a column chunk holding one still reports it in `geospatial_types`. Such a code is valid, but `geospatial_types` is borrowed from GeoParquet's `geometry_types`, which covers only the seven basic geometries, so a reader may have no branch for it. The write path preserves these bytes unchanged, including when the unsupported type occurs inside nested `GeometryCollection` members. It validates structure up to the unsupported body, then skips the remaining body and trailing-byte checks because their boundaries cannot be measured, so these geometries still round trip through the hex and base64 modes.

Mixed byte orders: each member of a `MultiPoint`, `MultiLineString` or `MultiPolygon` carries its own byte order byte, which up to v3.8.3 was read but then ignored, the member's type being read with the outer geometry's byte order instead. A legal value mixing the two therefore failed to parse. From v3.9.0 it reads correctly, so such a value renders as GeoJSON and contributes to the bounding box rather than withdrawing it.

`types.BoundingBoxCalculator` follows the same rule as the statistics below: from v3.9.0 `GetBounds` reports no bounds once a value has been added whose coordinates could not be read, where it used to return the bounds of the values it did read. The new `BoundsUnknown` tells that apart from having been given nothing to measure.

Column chunk statistics: the writer reads each value's WKB to build the `GeospatialStatistics` bounding box and geometry type list, on dictionary-encoded and plain pages alike, so a column's statistics do not depend on its encoding. This library's reader is 2D, so a value it cannot read leaves both halves incomplete, and an incomplete statistic is worse than none: a bounding box smaller than the data makes a reader pushing a spatial filter down to it skip rows that match, and a short type list tells a reader the chunk holds only the types it could read.

What each unreadable value costs the chunk, from v3.9.0:

| Value | Bounding box | Type list |
| --- | --- | --- |
| An ISO geometry carrying Z or M ordinates | withheld | kept, from its header |
| A truncated body whose header reads, such as a short point | withheld | kept, from its header |
| Bytes with no readable WKB header | withheld | withheld |
| An empty value | withheld | withheld |
| A value that is not bytes at all | withheld | withheld |
| A null, or a page holding no geometries | unaffected | unaffected |

The type list is written in ascending order, so it is identical on every run. Up to v3.8.3 it followed Go's map iteration order, which made a column holding more than one geometry type produce byte-different files from identical input.

An empty value is not an absent one: parquet spells absence with a null, which is skipped, while WKB has no zero-byte form, so an empty value is one the column holds that neither half can read.

Up to v3.8.3 a Z or M value was read as 2D, so the box was built from coordinates the value does not hold, and a value that could not be read was skipped, so the box covered the rest of the chunk as though it were all of it.

JSON output modes:

| Mode | Output |
| --- | --- |
| Hex | WKB data as hexadecimal strings |
| Base64 | WKB data as base64 strings |
| GeoJSON | RFC 7946 compliant GeoJSON output |
| Hybrid | GeoJSON plus raw WKB |

Defaults are GeoJSON for `GEOGRAPHY` and hex for `GEOMETRY`.

The geospatial conversion helper `JSONTypeToParquetTypeWithLogical` accepts `ConvertValue` output directly, including native typed coordinate and geometry slices in GeoJSON mode; JSON marshaling and unmarshaling are not required before writing it back.

```go
cfg := types.NewGeospatialConfig(
    types.WithGeographyJSONMode(types.GeospatialModeGeoJSON),
    types.WithGeometryJSONMode(types.GeospatialModeHex),
    types.WithGeospatialHybridRawBase64(true),
    types.WithGeospatialGeoJSONAsFeature(false),
    types.WithGeospatialCoordinatePrecision(6),
    types.WithGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
        return nil, false
    }),
)

result := types.ConvertGeographyLogicalValue(wkbBytes, geogType, cfg)
```

The built-in converter reads and writes the seven basic geometries — `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon` and `GeometryCollection`, the last nesting to any depth — in their 2D form. A geometry carrying Z or M ordinates, or a type outside those seven, is not converted: it falls back to raw WKB, with CRS or algorithm metadata where applicable, as does any value whose WKB does not parse. The hex and base64 modes never parse at all, so they carry every one of those untouched. See [Geospatial Values](#geospatial-values) for the write side.

### Concurrency

Use `WithNP(n)` to set the number of parallel goroutines. The default is 4.

`WithNP` controls parallel work performed inside a single operation. It does not make concurrent method calls on one reader or writer safe. Callers must serialize all operations on each `ParquetReader`, `ParquetWriter`, `CSVWriter`, `JSONWriter`, or `ArrowWriter` instance. In particular, writes must not overlap other writes, flushes, or finalization, and reads must not overlap other reads, skips, inspection operations, resets, or closing.

Separate reader or writer instances do not share mutable library state and may be used concurrently when each has an independent file handle. A `ParquetFileReader` implementation must provide independent cursors from `Clone` and `Open`, as required by the source interface; any shared backend client must support the concurrency performed by those independent handles.

`Clone` creates another low-level reader with an independent cursor. `NewParquetReader` also clones its supplied file reader internally for column-level work controlled by `WithNP`, but those internal handles belong to one high-level reader and do not make concurrent calls on that `ParquetReader` safe. To read the same file concurrently, clone the file reader and construct a separate `ParquetReader` for each goroutine:

```go
file1, err := source.CloneWithContext(ctx, file)
if err != nil {
    return err
}
file2, err := source.CloneWithContext(ctx, file)
if err != nil {
    _ = file1.Close()
    return err
}

reader1, err := reader.NewParquetReaderWithContext(ctx, file1, new(Row))
if err != nil {
    _ = file1.Close()
    _ = file2.Close()
    return err
}
reader2, err := reader.NewParquetReaderWithContext(ctx, file2, new(Row))
if err != nil {
    _ = reader1.ReadStopWithContext(context.WithoutCancel(ctx))
    _ = file1.Close()
    _ = file2.Close()
    return err
}

// reader1 and reader2 may now be used by separate goroutines.
```

Each `ParquetReader` has its own logical position; reads through one do not advance the other. `ReadStop` closes the reader's internal column handles, while the cloned file handles passed to the constructors remain the caller's responsibility to close.

```go
func NewParquetReader(pFile source.ParquetFileReader, obj any, opts ...ReaderOption) (*ParquetReader, error)
func NewParquetWriter(pFile source.ParquetFileWriter, obj any, opts ...WriterOption) (*ParquetWriter, error)
func NewJSONWriter(jsonSchema string, pfile source.ParquetFileWriter, opts ...WriterOption) (*JSONWriter, error)
func NewCSVWriter(md []string, pfile source.ParquetFileWriter, opts ...WriterOption) (*CSVWriter, error)
func NewArrowWriter(arrowSchema *arrow.Schema, pfile source.ParquetFileWriter, opts ...WriterOption) (*ArrowWriter, error)
```

### Cancellation

Context-aware constructors and operations are additive, and all existing APIs retain their signatures for backward compatibility. Context-free entry points that have a direct `WithContext` replacement are deprecated. Legacy constructors use `context.Background()`; plain methods use the context supplied to their constructor, so their behavior is unchanged when constructed through a legacy API. Use `reader.NewParquetReaderWithContext`, `ReadWithContext`, `writer.NewParquetWriterWithContext`, `WriteWithContext`, `FlushWithContext`, and `WriteStopWithContext` when reads or writes need cancellation or deadlines. Column reads, index and bloom-filter inspection, dictionary-page inspection, and the CSV, JSON, and Arrow writer constructors also provide `WithContext` variants. Cleanup and finalization still release resources and produce a valid footer after cancellation, then report the cancellation error.

> **Note:** `CloseWithContext`, `ReadStopWithContext`, `ResetWithContext`, and `WriteStopWithContext` always detach cancellation from the underlying close operations. This ensures resources are released and the file is never left in a corrupt or partially-written state, but it also means an application-level timeout or deadline that expires during cleanup will not abort it. If your backend I/O can hang indefinitely on close, consider adding a separate transport-level timeout on the file source rather than relying on the context given to these methods.

Source compatibility is unchanged. A source may implement the optional context capabilities in the `source` package to cancel in-flight operations; otherwise parquet-go checks the context before calling the legacy method.

## Examples

Build examples with the `example` build tag.

```bash
go build -tags example ./example/local_flat
go build -tags example ./example/local_nested
go build -tags example ./example/json_write
go build -tags example ./example/csv_write
go build -tags example ./example/new_logical
go build -tags example ./example/geospatial
go build -tags example ./example/bloom_filter
go build -tags example ./example/encrypt_write
go build -tags example ./example/all_types
```

| Example | Description |
| --- | --- |
| [local_flat](example/local_flat) | Write/read flat parquet file |
| [local_nested](example/local_nested) | Write/read nested structures |
| [read_partial](example/read_partial) | Read partial fields |
| [read_partial2](example/read_partial2) | Read sub-structs |
| [read_without_schema_predefined](example/read_without_schema_predefined) | Read without predefined schema |
| [read_partial_without_schema_predefined](example/read_partial_without_schema_predefined) | Read partial without predefined schema |
| [json_schema](example/json_schema) | Define schema with JSON |
| [json_write](example/json_write) | Convert JSON to Parquet |
| [convert_to_json](example/convert_to_json) | Convert Parquet to JSON |
| [json_nan](example/json_nan) | Round trip NaN and infinite values through JSON and inspect their bounds |
| [csv_write](example/csv_write) | CSV writer |
| [csv_to_parquet](example/csv_to_parquet) | CSV file to Parquet |
| [column_read](example/column_read) | Read raw column data |
| [type](example/type) | Type examples |
| [type_alias](example/type_alias) | Type alias examples |
| [new_logical](example/new_logical) | New logical types including FLOAT16 and INTEGER |
| [unknown_type](example/unknown_type) | UNKNOWN logical type (always-null columns) |
| [geospatial](example/geospatial) | GEOMETRY and GEOGRAPHY examples |
| [bloom_filter](example/bloom_filter) | Bloom filter |
| [encrypt_write](example/encrypt_write) | Write and read back encrypted Parquet files |
| [encrypt_read](example/encrypt_read) | Read encrypted Parquet file |
| [encrypt_read_aad](example/encrypt_read_aad) | Read encrypted Parquet file with external AAD prefix |
| [encrypt_read_plaintext_footer](example/encrypt_read_plaintext_footer) | Read encrypted Parquet file with plaintext footer |
| [encrypt_read_uniform](example/encrypt_read_uniform) | Read uniformly encrypted Parquet file |
| [datapagev2](example/datapagev2) | Data Page V2 |
| [date](example/date) | Date type |
| [all_types](example/all_types) | Comprehensive type support |
| [arrow_to_parquet](example/arrow_to_parquet) | Arrow schema to Parquet |
| [variant-fine-control](example/variant-fine-control) | VARIANT type fine control |
| [dot_in_name](example/dot_in_name) | Dot in field name |
| [keyvalue_metadata](example/keyvalue_metadata) | Key-value metadata |
| [writer](example/writer) | ParquetWriter from `io.Writer` |
| [writer_file](example/writer_file) | WriterFile example |
| [mem](example/mem) | In-memory file system |

## Local Development

### Generating GitHub Pages

`make pages` generates the project's GitHub Pages content locally to `build/pages/`; it currently runs the single `make pages-coverage` target.

`make pages-coverage` collects coverage data and generates the chart. It checks out each day's latest commit, runs `go test`, and appends results to `build/coverage.csv` (sorted chronologically). Days with no commits carry forward the previous day's coverage, and days before the first commit with non-zero coverage are skipped. It also writes the per-package HTML coverage report to `build/pages/coverage.html` and the README coverage badge to `build/pages/coverage.svg`.

```bash
make pages-coverage                                                     # last 7 days (default)
make pages-coverage COLLECT_ARGS="--start 2021-05-01"                   # full history from a date
make pages-coverage COLLECT_ARGS="--start 2024-01-01 --end 2024-06-01"  # explicit range
```

The HTML chart needs no third-party modules. The companion PNG additionally requires Python's `matplotlib`, which is **not** installed automatically; without it that one file is skipped with a warning and the rest of the run still succeeds. Install it with whichever tool fits your environment:

```bash
# apt (Debian/Ubuntu)
sudo apt install python3-matplotlib

# Homebrew (macOS)
brew install python3 && pip3 install matplotlib

# virtualenv, any platform
python3 -m venv .venv && .venv/bin/pip install matplotlib && export PYTHON=$PWD/.venv/bin/python
```

The `github-pages` workflow runs the same target weekly and publishes the result to <https://hangxie.github.io/parquet-go/>. It seeds `build/coverage.csv` from the previously published copy so history accumulates across runs rather than living in the repository, falling back to [`coverage.csv` in the wiki](https://raw.githubusercontent.com/wiki/hangxie/parquet-go/coverage.csv) when nothing has been published yet. That wiki copy holds the history collected before the first deploy; once Pages has a copy it always wins, leaving the wiki file as a static backup.

## Documentation

- [v1 README](READMEv1.md): original v1 documentation.
- [v2 README](READMEv2.md): v2 documentation.
- [source/README.md](source/README.md): file source implementations.

## Contributing

Contributions are welcome. Please submit issues or pull requests.

## License

Apache License 2.0
