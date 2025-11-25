# parquet-go/v2

[![](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hangxie/parquet-go/blob/main/LICENSE)
[![](https://img.shields.io/github/v/tag/hangxie/parquet-go.svg?color=brightgreen&label=version&sort=semver)](https://github.com/hangxie/parquet-go/releases)
[![[parquet-go]](https://github.com/hangxie/parquet-go/actions/workflows/build.yml/badge.svg)](https://github.com/hangxie/parquet-go/actions/workflows/build.yml)
[![](https://github.com/hangxie/parquet-go/wiki/coverage.svg)](https://github.com/hangxie/parquet-go/wiki/Coverage-Report)

parquet-go is a pure-go implementation of reading and writing the parquet format file.

* Support Read/Write Nested/Flat Parquet File
* Simple to use
* High performance
* Comprehensive encoding support
* New logical types including geospatial types

## Installation

```sh
go get github.com/hangxie/parquet-go
```

## What's New in v2

This repo was forked from https://github.com/xitongsys/parquet-go and merged https://github.com/xitongsys/parquet-go-source. v2 introduces significant improvements and new features:

### Major Improvements

1. **Better Error Handling**: Most functions now return errors instead of using panic/recover style code
2. **Performance Enhancements**:
   - Optimized `SkipRows()` for faster data skipping
   - Optimized schema reading performance by eliminating redundant tree traversals
   - Reduced lock contention using `sync.Map` in critical paths
   - Improved memory usage efficiency
3. **Enhanced Type Support**: Proper interpretation of logical types and converted types
4. **Apache Parquet Format 2.12.0**: Updated to the latest parquet format specification

### New Features Since v1

#### Encoding Support
- **BYTE_STREAM_SPLIT**: Full support for INT32/INT64/FIXED_LEN_BYTE_ARRAY types
- **BIT_PACKED**: Read support for BIT_PACKED encoding
- **Data Page V2**: Complete support for Data Page V2 format
- Proper validation of encoding/type compatibility at schema stage

#### New Logical Types
- **FLOAT16**: Half-precision floating point numbers stored as FIXED[2], decoded to float32
- **INTEGER**: Enhanced integer types with proper bitWidth and signedness mapping
  - 8-bit → int8/uint8
  - 16-bit → int16/uint16
  - 32-bit → int32/uint32
  - 64-bit → int64/uint64
- **UUID**: 16-byte values automatically converted to canonical UUID strings
- **GEOMETRY**: Planar geospatial coordinates with optional CRS
- **GEOGRAPHY**: Spherical geospatial coordinates with optional CRS and edge interpolation algorithm
- **VARIANT**: Dynamic type support (limited tooling compatibility)

#### Geospatial Support (GeoParquet)

Comprehensive support for geospatial data with configurable JSON output modes:

1. **Hex Mode**: WKB data as hexadecimal strings
2. **Base64 Mode**: WKB data as base64-encoded strings
3. **GeoJSON Mode**: RFC 7946 compliant GeoJSON output (default for GEOGRAPHY)
4. **Hybrid Mode**: Both GeoJSON and raw WKB together

Features:
- Configurable coordinate precision
- Optional CRS reprojection to CRS84
- Support for Point, LineString, and Polygon geometries
- Proper handling of CRS and algorithm metadata

See [geoparquet.md](geoparquet.md) for detailed documentation.

#### API Enhancements
- `Reset()`: Reset reader to beginning of file
- `ReadStopWithError()`: Handle errors during ReadStop operations
- `SkipRowsByIndexWithError()`: Handle errors during SkipRowsByIndex operations
- `Clone()`: Clone ParquetFileReader interface for concurrent access
- Page manipulation functions for advanced use cases

#### Better JSON/BSON Support
- Proper BSON data decoding in JSON output
- Improved DATE type output in ISO 8601 format
- TIME values output as human-readable strings
- INTERVAL type with proper millisecond precision

#### Additional Data Sources
- HTTP reader support for reading parquet files over HTTP
- Enhanced S3v2 support with versioned object access
- Improved Azure Blob storage support
- GoCloud CDK integration for generic blob storage

### Bug Fixes Since v1

- Fixed race conditions in:
  - source/http
  - writer/writer.go (flush operations)
  - lz4_raw compression
- Fixed panic issues:
  - Handling corrupted parquet files
  - Old-style LIST format compatibility
  - Zero-value unmarshal operations
  - Empty files with zero records
  - Out of bound index errors
- Fixed encoding issues:
  - Hardcoded encoding bug in column chunks
  - PLAIN_DICTIONARY encoding compatibility
  - Proper encoding validation
- Fixed data handling:
  - Empty slice handling in decimal comparison
  - Negative decimal values between (-1, 1)
  - Optional scalar field handling
  - Default root name assumptions
- Fixed metadata:
  - Format version in footer
  - create_by field format
  - Statistics for INTERVAL and geospatial data
- Fixed GeoJSON output format for multi-geometries

### Breaking Changes from v1

Please refer to [v1 README.md](READMEv1.md) for v1 documentation. Key breaking changes:

1. Many functions now return errors instead of panicking
2. Separated reader and writer interfaces for ParquetFile sources
3. Updated to use github.com/hangxie/parquet-go module path

## Quick Start

### Writing Parquet Files

```go
package main

import (
    "log"
    "github.com/hangxie/parquet-go/parquet"
    "github.com/hangxie/parquet-go/writer"
    "github.com/hangxie/parquet-go/source"
)

type Student struct {
    Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
    Age    int32   `parquet:"name=age, type=INT32"`
    ID     int64   `parquet:"name=id, type=INT64"`
    Weight float32 `parquet:"name=weight, type=FLOAT"`
    Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
    fw, err := source.NewLocalFileWriter("output.parquet")
    if err != nil {
        log.Fatal("Can't create file", err)
    }
    defer fw.Close()

    pw, err := writer.NewParquetWriter(fw, new(Student), 4)
    if err != nil {
        log.Fatal("Can't create parquet writer", err)
    }

    num := 10
    for i := 0; i < num; i++ {
        stu := Student{
            Name:   "StudentName",
            Age:    int32(20 + i%5),
            ID:     int64(i),
            Weight: float32(50.0 + float32(i)*0.1),
            Sex:    i%2 == 0,
        }
        if err = pw.Write(stu); err != nil {
            log.Fatal("Write error", err)
        }
    }

    if err = pw.WriteStop(); err != nil {
        log.Fatal("WriteStop error", err)
    }
}
```

### Reading Parquet Files

```go
package main

import (
    "log"
    "github.com/hangxie/parquet-go/reader"
    "github.com/hangxie/parquet-go/source"
)

type Student struct {
    Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
    Age    int32   `parquet:"name=age, type=INT32"`
    ID     int64   `parquet:"name=id, type=INT64"`
    Weight float32 `parquet:"name=weight, type=FLOAT"`
    Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

func main() {
    fr, err := source.NewLocalFileReader("output.parquet")
    if err != nil {
        log.Fatal("Can't open file", err)
    }
    defer fr.Close()

    pr, err := reader.NewParquetReader(fr, new(Student), 4)
    if err != nil {
        log.Fatal("Can't create parquet reader", err)
    }
    defer pr.ReadStop()

    num := int(pr.GetNumRows())
    students := make([]Student, num)
    if err = pr.Read(&students); err != nil {
        log.Fatal("Read error", err)
    }

    for _, stu := range students {
        log.Printf("%+v\n", stu)
    }
}
```

## Type System

### Primitive Types

|Primitive Type|Go Type|
|-|-|
|BOOLEAN|bool|
|INT32|int32|
|INT64|int64|
|INT96 ([deprecated](https://github.com/xitongsys/parquet-go/issues/420))|string|
|FLOAT|float32|
|DOUBLE|float64|
|BYTE_ARRAY|string|
|FIXED_LEN_BYTE_ARRAY|string|

### Logical Types

|Logical Type|Primitive Type|Go Type|
|-|-|-|
|UTF8|BYTE_ARRAY|string|
|INT_8|INT32|int32|
|INT_16|INT32|int32|
|INT_32|INT32|int32|
|INT_64|INT64|int64|
|UINT_8|INT32|int32|
|UINT_16|INT32|int32|
|UINT_32|INT32|int32|
|UINT_64|INT64|int64|
|DATE|INT32|int32|
|TIME_MILLIS|INT32|int32|
|TIME_MICROS|INT64|int64|
|TIMESTAMP_MILLIS|INT64|int64|
|TIMESTAMP_MICROS|INT64|int64|
|INTERVAL|FIXED_LEN_BYTE_ARRAY|string|
|DECIMAL|INT32,INT64,FIXED_LEN_BYTE_ARRAY,BYTE_ARRAY|int32,int64,string,string|
|UUID|FIXED_LEN_BYTE_ARRAY|string|
|FLOAT16|FIXED_LEN_BYTE_ARRAY|string|
|GEOMETRY|BYTE_ARRAY|string|
|GEOGRAPHY|BYTE_ARRAY|string|
|JSON|BYTE_ARRAY|string|
|BSON|BYTE_ARRAY|string|
|LIST|-|slice|
|MAP|-|map|

### Type Notes

* Type aliases are supported (e.g., `type MyString string`), but the base type must follow the table
* Use [converter.go](https://github.com/hangxie/parquet-go/blob/master/types/converter.go) for type conversion utilities

## Encoding Support

### Supported Encodings

|Encoding|Types|Read|Write|
|-|-|-|-|
|PLAIN|All types|✓|✓|
|PLAIN_DICTIONARY|All types|✓|✓|
|RLE_DICTIONARY|All types|✓|✓|
|DELTA_BINARY_PACKED|Integer types|✓|✓|
|DELTA_BYTE_ARRAY|BYTE_ARRAY, UTF8|✓|✓|
|DELTA_LENGTH_BYTE_ARRAY|BYTE_ARRAY, UTF8|✓|✓|
|BYTE_STREAM_SPLIT|INT32, INT64, FIXED_LEN_BYTE_ARRAY|✓|✓|
|BIT_PACKED|Boolean, Integer|✓|✓|

### Encoding Notes

* For maximum compatibility, use PLAIN and PLAIN_DICTIONARY encodings
* Avoid PLAIN_DICTIONARY for high-cardinality fields to prevent excessive memory usage
* Use `omitstats=true` tag to skip statistics for large array fields

## Compression Support

|Compression|Supported|
|-|-|
|UNCOMPRESSED|✓|
|SNAPPY|✓|
|GZIP|✓|
|LZO|✗|
|BROTLI|✓|
|LZ4|✓|
|LZ4_RAW|✓|
|ZSTD|✓|

## Repetition Types

|Repetition Type|Go Declaration|Description|
|-|-|-|
|REQUIRED|`V1 int32` with tag `parquet:"name=v1, type=INT32"`|Standard required field|
|OPTIONAL|`V1 *int32` with tag `parquet:"name=v1, type=INT32"`|Use pointer for optional fields|
|REPEATED|`V1 []int32` with tag `parquet:"name=v1, type=INT32, repetitiontype=REPEATED"`|Use slice with repetitiontype tag|

### Repetition Notes

* LIST and REPEATED are different in the parquet format - prefer LIST
* Standard and non-standard LIST/MAP formats are both supported

## Schema Definition

Four methods to define schema:

### 1. Go Struct Tags

```go
type Student struct {
    Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
    Age    int32   `parquet:"name=age, type=INT32, encoding=PLAIN"`
    ID     int64   `parquet:"name=id, type=INT64"`
    Weight float32 `parquet:"name=weight, type=FLOAT"`
    Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}
```

### 2. JSON Schema

```go
jsonSchema := `{
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
  "Fields": [
    {"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"},
    {"Tag": "name=age, type=INT32, repetitiontype=REQUIRED"}
  ]
}`
```

### 3. CSV Metadata

```go
md := []string{
    "name=Name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY",
    "name=Age, type=INT32",
}
```

### 4. Arrow Schema

```go
schema := arrow.NewSchema(
    []arrow.Field{
        {Name: "int64", Type: arrow.PrimitiveTypes.Int64},
        {Name: "float64", Type: arrow.PrimitiveTypes.Float64},
    },
    nil,
)
```

### Schema Notes

* All struct fields must be exported (start with uppercase letter)
* `InName` (Go field name) and `ExName` (Parquet field name) are distinct
* Avoid field names differing only by first letter case
* `PARGO_PREFIX_` is reserved - don't use as field prefix
* Use `\x01` as delimiter to support `.` in field names

## Writers

Four writer types are available:

1. **ParquetWriter**: Write Go structs - [example](example/local_flat.go)
2. **JSONWriter**: Convert JSON to Parquet - [example](example/json_write.go)
3. **CSVWriter**: Write CSV-like data - [example](example/csv_write.go)
4. **ArrowWriter**: Write using Arrow schemas - [example](example/arrow_to_parquet.go)

## Readers

Two reader types:

1. **ParquetReader**: Read into Go structs - [example](example/local_nested.go)
2. **ColumnReader**: Read raw column data with repetition/definition levels - [example](example/column_read.go)

### Reader Notes

* For large files, read in chunks to avoid OOM
* Configure `RowGroupSize` and `PageSize` in writer:
```go
pw.RowGroupSize = common.DefaultRowGroupSize // default 128M
pw.PageSize = common.DefaultPageSize         // default 8K
```

## ParquetFile Interface

All file sources must implement:

```go
type ParquetFile interface {
    io.Seeker
    io.Reader
    io.Writer
    io.Closer
    Open(name string) (ParquetFile, error)
    Create(name string) (ParquetFile, error)
}
```

### Supported Sources

* Local filesystem
* HDFS
* S3 (AWS SDK v1 and v2)
* Google Cloud Storage
* Azure Blob Storage
* HTTP (read-only)
* Memory buffer
* GoCloud CDK (generic blob storage)
* OpenStack Swift

See [source/README.md](source/README.md) for details.

## Concurrency

Optimize performance with parallel marshaling/unmarshaling:

```go
func NewParquetReader(pFile ParquetFile.ParquetFile, obj interface{}, np int64) (*ParquetReader, error)
func NewParquetWriter(pFile ParquetFile.ParquetFile, obj interface{}, np int64) (*ParquetWriter, error)
func NewJSONWriter(jsonSchema string, pfile ParquetFile.ParquetFile, np int64) (*JSONWriter, error)
func NewCSVWriter(md []string, pfile ParquetFile.ParquetFile, np int64) (*CSVWriter, error)
```

Set `np` parameter to control the number of parallel goroutines.

## Examples

Build examples with the `example` build tag:

```bash
go build -tags example ./example/local_flat        # Basic flat structure
go build -tags example ./example/local_nested      # Nested structures
go build -tags example ./example/json_write        # JSON to Parquet
go build -tags example ./example/csv_write         # CSV to Parquet
go build -tags example ./example/new_logical       # FLOAT16 + INTEGER
go build -tags example ./example/geospatial        # GEOMETRY + GEOGRAPHY
go build -tags example ./example/all_types         # Comprehensive sample
```

|Example|Description|
|-|-|
|[local_flat.go](example/local_flat.go)|Write/read flat parquet file|
|[local_nested.go](example/local_nested.go)|Write/read nested structures|
|[read_partial.go](example/read_partial.go)|Read partial fields|
|[read_partial2.go](example/read_partial2.go)|Read sub-structs|
|[read_without_schema_predefined.go](example/read_without_schema_predefined.go)|Read without predefined schema|
|[json_schema.go](example/json_schema.go)|Define schema with JSON|
|[json_write.go](example/json_write.go)|Convert JSON to Parquet|
|[convert_to_json.go](example/convert_to_json.go)|Convert Parquet to JSON|
|[csv_write.go](example/csv_write.go)|CSV writer|
|[column_read.go](example/column_read.go)|Read raw column data|
|[type.go](example/type.go)|Type examples|
|[type_alias.go](example/type_alias.go)|Type alias examples|
|[new_logical.go](example/new_logical.go)|New logical types|
|[geospatial.go](example/geospatial.go)|Geospatial types|
|[all_types.go](example/all_types.go)|All type support|

## Documentation

* [v1 README](READMEv1.md) - Original v1 documentation
* [source/README.md](source/README.md) - File source implementations
* [geoparquet.md](geoparquet.md) - Detailed geospatial support documentation

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

Apache License 2.0
