# parquet-go/v3

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
go get github.com/hangxie/parquet-go/v3
```

## Configuration

All configuration is per-instance via functional options, enabling safe concurrent use of multiple readers/writers with independent settings in the same process.

### Writer Options

`WithNP`, `WithPageSize`, `WithRowGroupSize`, `WithCompressionCodec`, `WithCompressionLevel`, `WithDataPageVersion`, `WithWriteCRC`.

### Reader Options

`WithNP`, `WithCaseInsensitive`, `WithCRCMode`.

## Quick Start

### Writing Parquet Files

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
        log.Fatal("Can't create file", err)
    }
    defer fw.Close()

    pw, err := writer.NewParquetWriter(fw, new(Student))
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
        log.Fatal("Can't open file", err)
    }
    defer fr.Close()

    pr, err := reader.NewParquetReader(fr, new(Student), reader.WithNP(4))
    if err != nil {
        log.Fatal("Can't create parquet reader", err)
    }
    defer func() { _ = pr.ReadStop() }()

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
* Use [converter.go](types/converter.go) for type conversion utilities

## Encoding Support

### Supported Encodings

|Encoding|Types|Read|Write|
|-|-|-|-|
|PLAIN|All types|Y|Y|
|PLAIN_DICTIONARY|All types|Y|Y|
|RLE_DICTIONARY|All types|Y|Y|
|DELTA_BINARY_PACKED|Integer types|Y|Y|
|DELTA_BYTE_ARRAY|BYTE_ARRAY, UTF8|Y|Y|
|DELTA_LENGTH_BYTE_ARRAY|BYTE_ARRAY, UTF8|Y|Y|
|BYTE_STREAM_SPLIT|INT32, INT64, FIXED_LEN_BYTE_ARRAY|Y|Y|
|BIT_PACKED|Boolean, Integer|Y|Y|

### Encoding Notes

* For maximum compatibility, use PLAIN and PLAIN_DICTIONARY encodings
* Avoid PLAIN_DICTIONARY for high-cardinality fields to prevent excessive memory usage
* Use `omitstats=true` tag to skip statistics for large array fields

## Compression Support

| Compression | Supported | Default Level | Library |
|-------------|-----------|---------------|---------|
| UNCOMPRESSED| Y         | N/A           | N/A     |
| SNAPPY      | Y         | N/A           | klauspost/compress/snappy |
| GZIP        | Y         | 6             | klauspost/compress/gzip |
| LZO         | N         | N/A           | N/A     |
| BROTLI      | Y         | 6             | andybalholm/brotli |
| LZ4         | Y         | Fast (0)      | pierrec/lz4/v4 |
| LZ4_RAW     | Y         | 9             | pierrec/lz4/v4 |
| ZSTD        | Y         | 3             | klauspost/compress/zstd |

### Compression Notes

* **Default Codec**: For standard writers, the default compression is `SNAPPY`. `NewArrowWriter` defaults to `GZIP`.
* **Per-Column Codec**: You can specify a compression codec for a specific column using the `compression` tag in your Go struct (e.g., `parquet:"name=col, compression=GZIP"`). If not specified, the file-level default is used.
* **Levels**: Codecs that support compression levels can be configured on writers using `writer.WithCompressionLevel(codec, level)`. Note that levels are set **per codec at the writer level**; all columns using a specific codec will share the same compression level. Per-column compression levels are not yet supported.
* **LZ4**: Uses the standard LZ4 frame format with frame headers. This is the legacy Parquet compression type. User-facing levels are mapped via `1 << (8 + level)` to lz4 `CompressionLevel` constants.
* **LZ4_RAW**: Uses raw LZ4 block compression without framing. This is the preferred LZ4 variant per the Parquet specification. User-facing levels are passed directly to `lz4.CompressorHC`. Level 0 is a valid compression level and is distinct from the default level (9).
* **Decompression Safety**: All compression codecs enforce decompressed size limits (default 256MB) to prevent decompression bombs. Configure this via `compress.WithMaxDecompressedSize`.

## CRC Checksum Handling

The Parquet format defines an optional CRC32 field in `PageHeader` (polynomial 0x04C11DB7, same as GZip). The checksum covers the compressed page data (not the page header itself).

### Reader: CRC Validation Modes

CRC validation on read is controlled by `reader.WithCRCMode()`, applied globally to all columns.

| Scenario               | Strict     | Auto           | Ignore (default) |
|------------------------|------------|----------------|-------------------|
| CRC absent in header   | **Fail**   | Pass           | Pass              |
| CRC present, valid     | Pass       | Pass           | Pass              |
| CRC present, invalid   | **Fail**   | **Fail**       | Pass              |

- **Ignore** -- Default mode. Skips all CRC validation regardless of whether a checksum is present in the page header. This preserves backward-compatible behavior.
- **Auto** -- Validates CRC when present, silently passes when absent.
- **Strict** -- Requires CRC on every page and validates it. Returns an error if CRC is missing or if the computed checksum does not match.

The flag applies to all columns uniformly; there is no per-column CRC setting.

### Writer: CRC Computation

CRC computation on write is controlled by `writer.WithWriteCRC()`. When enabled, the writer computes CRC32 (IEEE polynomial) over the compressed page data and sets `PageHeader.Crc` before serialization. The default is `false` (no CRC written).

```go
pw, _ := writer.NewParquetWriter(pFile, obj, writer.WithWriteCRC(true))
```

CRC is computed on all page types: data pages (v1 and v2), dictionary pages, and dictionary-encoded data pages. The checksum covers the same bytes that the reader validates -- the full `CompressedPageSize` payload after the page header.

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
    Name   string  `parquet:"name=name, type=BYTE_ARRAY, logicaltype=STRING, encoding=PLAIN_DICTIONARY"`
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
    {"Tag": "name=name, type=BYTE_ARRAY, logicaltype=STRING, repetitiontype=REQUIRED"},
    {"Tag": "name=age, type=INT32, repetitiontype=REQUIRED"}
  ]
}`
```

### 3. CSV Metadata

```go
md := []string{
    "name=Name, type=BYTE_ARRAY, logicaltype=STRING, encoding=PLAIN_DICTIONARY",
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

1. **ParquetWriter**: Write Go structs - [example](example/local_flat)
2. **JSONWriter**: Convert JSON to Parquet - [example](example/json_write)
3. **CSVWriter**: Write CSV-like data - [example](example/csv_write)
4. **ArrowWriter**: Write using Arrow schemas - [example](example/arrow_to_parquet)

## Readers

Two reader types:

1. **ParquetReader**: Read into Go structs - [example](example/local_nested)
2. **ColumnReader**: Read raw column data with repetition/definition levels - [example](example/column_read)

### Reader Notes

* For large files, read in chunks to avoid OOM
* Configure `RowGroupSize` and `PageSize` via writer options:
```go
pw, err := writer.NewParquetWriter(fw, new(MyStruct),
    writer.WithRowGroupSize(common.DefaultRowGroupSize), // default 128M
    writer.WithPageSize(common.DefaultPageSize),         // default 8K
)
```

## ParquetFile Interfaces

File sources implement separate reader and writer interfaces:

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

### Supported Sources

* Local filesystem
* HDFS
* S3 (AWS SDK v2)
* Google Cloud Storage
* Azure Blob Storage
* HTTP (read-only)
* Memory buffer
* GoCloud CDK (generic blob storage)
* OpenStack Swift

See [source/README.md](source/README.md) for details.

## GeoParquet: Geospatial Logical Types

parquet-go supports Apache Parquet's geospatial logical types and provides configurable JSON output when converting data to a "JSON-friendly" form (via `marshal.ConvertToJSONFriendly`).

### Overview

- Physical storage: WKB (Well-Known Binary) in `BYTE_ARRAY` fields
- Logical types:
  - `GEOMETRY`: planar coordinates with an optional `crs`
  - `GEOGRAPHY`: spherical coordinates with an optional `crs` and an `algorithm` for edge interpolation
- CRS: defaults to `"OGC:CRS84"` when not provided
- Algorithm (GEOGRAPHY): `SPHERICAL` (default), `VINCENTY`, `THOMAS`, `ANDOYER`, `KARNEY`

### JSON Output Modes

You can choose how geospatial values are emitted during JSON conversion:

1. **Hex**: WKB data as hexadecimal strings
2. **Base64**: WKB data as base64-encoded strings
3. **GeoJSON**: RFC 7946 compliant GeoJSON output (default for GEOGRAPHY)
4. **Hybrid**: Both GeoJSON and raw WKB together

Defaults: `GEOGRAPHY` -> GeoJSON, `GEOMETRY` -> Hex.

### Configuration API

Geospatial JSON rendering is configured per-instance via `types.GeospatialConfig` using functional options:

```go
cfg := types.NewGeospatialConfig(
    // Select per-type JSON output mode
    types.WithGeographyJSONMode(types.GeospatialModeGeoJSON), // or Hex, Base64, Hybrid
    types.WithGeometryJSONMode(types.GeospatialModeHex),

    // In Hybrid mode, choose raw encoding: false -> hex (default), true -> base64
    types.WithGeospatialHybridRawBase64(true),

    // Optional: emit GeoJSON geometry object instead of Feature in GeoJSON mode
    types.WithGeospatialGeoJSONAsFeature(false), // default is true

    // Optional: round coordinates to a fixed number of decimals in GeoJSON
    // (RFC 7946 S11.2 discusses precision considerations)
    types.WithGeospatialCoordinatePrecision(6), // default is 6; set -1 to disable

    // Optional reprojection to CRS84
    types.WithGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
        // Implement CRS->CRS84 reprojection here and return (updated, true)
        // Return (nil, false) to skip/indicate failure
        return nil, false
    }),
)

// Then pass cfg to ConvertGeometryLogicalValue / ConvertGeographyLogicalValue
result := types.ConvertGeographyLogicalValue(wkbBytes, geogType, cfg)
```

The default config (`types.DefaultGeospatialConfig()`) uses Hex mode for GEOMETRY, GeoJSON mode for GEOGRAPHY, Feature wrapping enabled, and 6-decimal coordinate precision.

### Output Examples

Given a WKB Point in CRS84 (hex `0101000000000000000000F03F0000000000000040` -> (1,2)):

- **GeoJSON** (Geography default, Feature):
```json
{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [1, 2] }, "properties": { "crs": "OGC:CRS84", "algorithm": "SPHERICAL" } }
```

- **Hex** (Geometry default):
```json
{ "wkb_hex": "0101000000000000000000F03F0000000000000040", "crs": "OGC:CRS84" }
```

- **Hybrid + base64 raw**:
```json
{ "geojson": { "type": "Point", "coordinates": [1, 2] }, "wkb_b64": "AQAAAAAAAADwP4AAAAAAAABA", "crs": "OGC:CRS84" }
```

### Reprojection Hook

If your `GEOMETRY` provides a non-CRS84 CRS (e.g., `EPSG:3857`), you can register a reprojection callback to emit GeoJSON in CRS84:

```go
cfg := types.NewGeospatialConfig(
    types.WithGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
        if crs == "EPSG:3857" {
            // Convert geometry coordinates from EPSG:3857 to CRS84 (lon/lat degrees)
            return gj, true
        }
        return nil, false
    }),
)
```

If reprojection fails or the hook returns false, the original GeoJSON is emitted unchanged.

### Fallback Behavior

- If WKB parsing fails or the geometry type is not supported by the built-in converter, the converter falls back to raw WKB including CRS/algorithm when applicable.
- In Hybrid mode, fallback uses hex (`wkb_hex`) regardless of the base64 selection flag.

### Supported Geometry Types in Converter

- Point (2D)
- LineString (2D)
- Polygon (2D)

Other WKB types currently fall back to raw WKB.

## Concurrency

Optimize performance with parallel marshaling/unmarshaling:

```go
func NewParquetReader(pFile source.ParquetFileReader, obj any, opts ...ReaderOption) (*ParquetReader, error)
func NewParquetWriter(pFile source.ParquetFileWriter, obj any, opts ...WriterOption) (*ParquetWriter, error)
func NewJSONWriter(jsonSchema string, pfile source.ParquetFileWriter, opts ...WriterOption) (*JSONWriter, error)
func NewCSVWriter(md []string, pfile source.ParquetFileWriter, opts ...WriterOption) (*CSVWriter, error)
func NewArrowWriter(arrowSchema *arrow.Schema, pfile source.ParquetFileWriter, opts ...WriterOption) (*ArrowWriter, error)
```

Use `WithNP(n)` to set the number of parallel goroutines (default is 4).
Use `WithCompressionCodec` to override defaults.
Use `WithCompressionLevel` to set codec-specific compression levels for codecs that support them.

## Examples

Build examples with the `example` build tag:

```bash
go build -tags example ./example/local_flat        # Basic flat structure
go build -tags example ./example/local_nested      # Nested structures
go build -tags example ./example/json_write        # JSON to Parquet
go build -tags example ./example/csv_write         # CSV to Parquet
go build -tags example ./example/new_logical       # FLOAT16 + INTEGER
go build -tags example ./example/geospatial        # GEOMETRY + GEOGRAPHY
go build -tags example ./example/bloom_filter      # Bloom filter
go build -tags example ./example/all_types         # Comprehensive sample
```

|Example|Description|
|-|-|
|[local_flat](example/local_flat)|Write/read flat parquet file|
|[local_nested](example/local_nested)|Write/read nested structures|
|[read_partial](example/read_partial)|Read partial fields|
|[read_partial2](example/read_partial2)|Read sub-structs|
|[read_without_schema_predefined](example/read_without_schema_predefined)|Read without predefined schema|
|[read_partial_without_schema_predefined](example/read_partial_without_schema_predefined)|Read partial without predefined schema|
|[json_schema](example/json_schema)|Define schema with JSON|
|[json_write](example/json_write)|Convert JSON to Parquet|
|[convert_to_json](example/convert_to_json)|Convert Parquet to JSON|
|[csv_write](example/csv_write)|CSV writer|
|[csv_to_parquet](example/csv_to_parquet)|CSV file to Parquet|
|[column_read](example/column_read)|Read raw column data|
|[type](example/type)|Type examples|
|[type_alias](example/type_alias)|Type alias examples|
|[new_logical](example/new_logical)|New logical types (FLOAT16, INTEGER)|
|[geospatial](example/geospatial)|Geospatial types (GEOMETRY, GEOGRAPHY)|
|[bloom_filter](example/bloom_filter)|Bloom filter|
|[datapagev2](example/datapagev2)|Data Page V2|
|[date](example/date)|Date type|
|[all_types](example/all_types)|All type support|
|[arrow_to_parquet](example/arrow_to_parquet)|Arrow schema to Parquet|
|[variant-fine-control](example/variant-fine-control)|VARIANT type fine control|
|[dot_in_name](example/dot_in_name)|Dot in field name|
|[keyvalue_metadata](example/keyvalue_metadata)|Key-value metadata|
|[writer](example/writer)|ParquetWriter from io.Writer|
|[writer_file](example/writer_file)|WriterFile example|
|[mem](example/mem)|In-memory file system|

## Documentation

* [v1 README](READMEv1.md) - Original v1 documentation
* [v2 README](READMEv2.md) - v2 documentation
* [source/README.md](source/README.md) - File source implementations

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

Apache License 2.0
