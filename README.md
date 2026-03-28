# parquet-go/v3

[![](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hangxie/parquet-go/blob/main/LICENSE)
[![](https://img.shields.io/github/v/tag/hangxie/parquet-go.svg?color=brightgreen&label=version&sort=semver)](https://github.com/hangxie/parquet-go/releases)
[![[parquet-go]](https://github.com/hangxie/parquet-go/actions/workflows/build.yml/badge.svg)](https://github.com/hangxie/parquet-go/actions/workflows/build.yml)
[![](https://github.com/hangxie/parquet-go/wiki/coverage.svg)](https://github.com/hangxie/parquet-go/wiki/Coverage-Report)

parquet-go is a pure-go implementation of reading and writing the Apache Parquet format.

* Read/Write nested and flat Parquet files
* Per-instance configuration — no mutable global state
* Comprehensive encoding and compression support
* New logical types including geospatial (GEOMETRY/GEOGRAPHY), FLOAT16, UUID, VARIANT
* CRC checksum validation and generation

## Installation

```sh
go get github.com/hangxie/parquet-go/v3
```

## What's New in v3

v3 removes all mutable global state. Configuration is per-instance via functional options on writers, readers, and JSON converters. This eliminates race conditions from shared globals and makes concurrent usage straightforward.

### Breaking Changes from v2

1. **All `Set*`/`Get*` global functions removed** — use per-instance options instead (see [Migration from v2](#migration-from-v2))
2. **Writer fields are now private** — use `With*` functional options at construction time (e.g., `writer.WithNP(4)`, `writer.WithCompressionType(...)`)
3. **`NewParquetReader` signature changed** — `np` parameter removed, use `reader.WithNP(N)` option; uses `...ReaderOption` instead of `ParquetReaderOptions` struct
4. **`Compressor.Compress` returns `([]byte, error)`** — previously returned only `[]byte`
5. **`WithCompressionLevel(codec, level)`** — replaces `WithCompressionLevels(map)` and global `SetCompressionLevel`
6. **`ParquetTypeToJSONType`** — consolidated function with `LogicalType` and `JSONTypeConfig` support
7. **Module path** — `github.com/hangxie/parquet-go/v3`

See [v2 README](READMEv2.md) for v2 documentation and [v1 README](READMEv1.md) for v1 documentation.

## Quick Start

### Writing Parquet Files

```go
package main

import (
    "log"

    "github.com/hangxie/parquet-go/v3/parquet"
    "github.com/hangxie/parquet-go/v3/source/local"
    "github.com/hangxie/parquet-go/v3/writer"
)

type Student struct {
    Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
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

    pw, err := writer.NewParquetWriter(fw, new(Student),
        writer.WithNP(4),
        writer.WithCompressionType(parquet.CompressionCodec_GZIP),
        writer.WithCompressionLevel(parquet.CompressionCodec_GZIP, 6),
        writer.WithWriteCRC(true),
    )
    if err != nil {
        log.Fatal("Can't create parquet writer", err)
    }

    for i := range 10 {
        if err = pw.Write(Student{
            Name:   "StudentName",
            Age:    int32(20 + i%5),
            ID:     int64(i),
            Weight: float32(50.0 + float32(i)*0.1),
            Sex:    i%2 == 0,
        }); err != nil {
            log.Fatal("Write error", err)
        }
    }

    if err = pw.WriteStop(); err != nil {
        log.Fatal("WriteStop error", err)
    }
    if err = fw.Close(); err != nil {
        log.Fatal("Close error", err)
    }
}
```

### Reading Parquet Files

```go
package main

import (
    "log"

    "github.com/hangxie/parquet-go/v3/common"
    "github.com/hangxie/parquet-go/v3/reader"
    "github.com/hangxie/parquet-go/v3/source/local"
)

type Student struct {
    Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
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

    pr, err := reader.NewParquetReader(fr, new(Student),
        reader.WithNP(4),
        reader.WithCRCMode(common.CRCAuto),
        reader.WithMaxDecompressedSize(256*1024*1024),
    )
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
    if err = fr.Close(); err != nil {
        log.Fatal("Close error", err)
    }
}
```

## Per-Instance Configuration

v3 uses functional options for all configuration. No mutable global state exists.

### Writer Options

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithNP(4),
    writer.WithCompressionType(parquet.CompressionCodec_GZIP),
    writer.WithCompressionLevel(parquet.CompressionCodec_GZIP, 9),
    writer.WithCompressionLevel(parquet.CompressionCodec_ZSTD, 3),
    writer.WithRowGroupSize(128*1024*1024),
    writer.WithPageSize(8*1024),
    writer.WithWriteCRC(true),
    writer.WithDataPageVersion(2),
)
```

| Option | Default | Description |
|-|-|-|
| `WithNP(n)` | `4` | Number of goroutines for parallel page encoding |
| `WithCompressionType(codec)` | `SNAPPY` | Compression codec for all columns |
| `WithCompressionLevel(codec, level)` | codec default | Per-codec compression level. Call multiple times for different codecs. |
| `WithRowGroupSize(n)` | 128 MB | Target row group size (max ~34 GB) |
| `WithPageSize(n)` | 8 KB | Target page size (max 2 GB) |
| `WithWriteCRC(bool)` | `false` | Write CRC checksums on pages |
| `WithDataPageVersion(v)` | `1` | Data page version (1 or 2) |

Writer config fields are private. Use `NP()`, `PageSize()`, `RowGroupSize()`, `CompressionType()`, `DataPageVersion()`, `WriteCRC()` getters to read values after construction.

### Reader Options

```go
pr, err := reader.NewParquetReader(fr, new(Student),
    reader.WithNP(4),
    reader.WithCaseInsensitive(),
    reader.WithCRCMode(common.CRCStrict),
    reader.WithMaxDecompressedSize(128*1024*1024),
    reader.WithMaxPageSize(128*1024*1024),
)
```

| Option | Default | Description |
|-|-|-|
| `WithNP(n)` | `4` | Number of goroutines for parallel row reading |
| `WithCaseInsensitive()` | `false` | Case-insensitive column name matching |
| `WithCRCMode(mode)` | `CRCIgnore` | CRC validation mode: `CRCIgnore`, `CRCAuto`, `CRCStrict` |
| `WithMaxDecompressedSize(n)` | `0` (256 MB) | Max decompressed page size in bytes. 0 = built-in default |
| `WithMaxPageSize(n)` | `0` (256 MB) | Max compressed page size in bytes. 0 = built-in default |

### JSON Conversion Options

```go
jsonFriendly, err := marshal.ConvertToJSONFriendly(row, schemaHandler,
    marshal.WithGeospatialOptions(
        types.WithGeographyJSONMode(types.GeospatialModeGeoJSON),
        types.WithGeometryJSONMode(types.GeospatialModeHex),
        types.WithCoordinatePrecision(6),
        types.WithGeoJSONAsFeature(true),
    ),
)
```

### Geospatial Options

| Option | Description |
|-|-|
| `WithGeographyJSONMode(mode)` | JSON output mode for GEOGRAPHY: `GeospatialModeHex`, `GeospatialModeBase64`, `GeospatialModeGeoJSON` (default), `GeospatialModeHybrid` |
| `WithGeometryJSONMode(mode)` | JSON output mode for GEOMETRY: `GeospatialModeHex` (default), `GeospatialModeBase64`, `GeospatialModeGeoJSON`, `GeospatialModeHybrid` |
| `WithReprojector(fn)` | CRS reprojection callback for GeoJSON output |
| `WithoutReprojector()` | Disable reprojection |
| `WithHybridRawBase64(bool)` | Use base64 (true) or hex (false, default) for raw WKB in Hybrid mode |
| `WithGeoJSONAsFeature(bool)` | Emit GeoJSON as Feature (true, default) or geometry object directly |
| `WithCoordinatePrecision(n)` | Round coordinates to n decimal places (default 6, max 12, -1 to disable) |

## CRC Checksum Handling

The Parquet format defines an optional CRC32 field in `PageHeader` (polynomial 0x04C11DB7, same as GZip). The checksum covers the compressed page data.

### Reader: CRC Validation Modes

| Scenario | Strict | Auto | Ignore (default) |
|-|-|-|-|
| CRC absent in header | **Fail** | Pass | Pass |
| CRC present, valid | Pass | Pass | Pass |
| CRC present, invalid | **Fail** | **Fail** | Pass |

### Writer: CRC Computation

Enable CRC writes with `writer.WithWriteCRC(true)`. CRC is computed on all page types (data v1, data v2, dictionary).

## GeoParquet: Geospatial Logical Types

Supports Apache Parquet's geospatial logical types (GEOMETRY and GEOGRAPHY) with configurable JSON output.

- **Physical storage**: WKB (Well-Known Binary) in `BYTE_ARRAY` fields
- **GEOMETRY**: planar coordinates with optional CRS
- **GEOGRAPHY**: spherical coordinates with optional CRS and edge interpolation algorithm
- **CRS**: defaults to `"OGC:CRS84"` when not provided

### JSON Output Modes

1. **Hex** — `{ "wkb_hex": "...", "crs": "..." }`
2. **Base64** — `{ "wkb_b64": "...", "crs": "..." }`
3. **GeoJSON** — RFC 7946 `{ "type": "Feature", "geometry": {...}, "properties": {...} }`
4. **Hybrid** — Both GeoJSON and raw WKB together

### Reprojection

Register a callback to reproject coordinates to CRS84 for GeoJSON output:

```go
types.WithReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
    if crs == "EPSG:3857" {
        // Convert to CRS84 and return (updated, true)
        return gj, true
    }
    return nil, false
})
```

### Supported Geometry Types

Point (2D), LineString (2D), Polygon (2D). Other WKB types fall back to raw WKB.

## Type System

### Primitive Types

| Primitive Type | Go Type |
|-|-|
| BOOLEAN | bool |
| INT32 | int32 |
| INT64 | int64 |
| INT96 ([deprecated](https://github.com/xitongsys/parquet-go/issues/420)) | string |
| FLOAT | float32 |
| DOUBLE | float64 |
| BYTE_ARRAY | string |
| FIXED_LEN_BYTE_ARRAY | string |

### Logical Types

| Logical Type | Primitive Type | Go Type |
|-|-|-|
| UTF8 | BYTE_ARRAY | string |
| INT_8 | INT32 | int32 |
| INT_16 | INT32 | int32 |
| INT_32 | INT32 | int32 |
| INT_64 | INT64 | int64 |
| UINT_8 | INT32 | int32 |
| UINT_16 | INT32 | int32 |
| UINT_32 | INT32 | int32 |
| UINT_64 | INT64 | int64 |
| DATE | INT32 | int32 |
| TIME_MILLIS | INT32 | int32 |
| TIME_MICROS | INT64 | int64 |
| TIMESTAMP_MILLIS | INT64 | int64 |
| TIMESTAMP_MICROS | INT64 | int64 |
| INTERVAL | FIXED_LEN_BYTE_ARRAY | string |
| DECIMAL | INT32, INT64, FIXED_LEN_BYTE_ARRAY, BYTE_ARRAY | int32, int64, string, string |
| UUID | FIXED_LEN_BYTE_ARRAY | string |
| FLOAT16 | FIXED_LEN_BYTE_ARRAY | string |
| GEOMETRY | BYTE_ARRAY | string |
| GEOGRAPHY | BYTE_ARRAY | string |
| JSON | BYTE_ARRAY | string |
| BSON | BYTE_ARRAY | string |
| LIST | - | slice |
| MAP | - | map |

### Type Notes

* Type aliases are supported (e.g., `type MyString string`), but the base type must follow the table
* Use [converter.go](https://github.com/hangxie/parquet-go/blob/master/types/converter.go) for type conversion utilities

## Encoding Support

| Encoding | Types | Read | Write |
|-|-|-|-|
| PLAIN | All types | Y | Y |
| PLAIN_DICTIONARY | All types | Y | Y |
| RLE_DICTIONARY | All types | Y | Y |
| DELTA_BINARY_PACKED | Integer types | Y | Y |
| DELTA_BYTE_ARRAY | BYTE_ARRAY, UTF8 | Y | Y |
| DELTA_LENGTH_BYTE_ARRAY | BYTE_ARRAY, UTF8 | Y | Y |
| BYTE_STREAM_SPLIT | INT32, INT64, FIXED_LEN_BYTE_ARRAY | Y | Y |
| BIT_PACKED | Boolean, Integer | Y | Y |

## Compression Support

| Compression | Supported |
|-|-|
| UNCOMPRESSED | Y |
| SNAPPY | Y |
| GZIP | Y |
| LZO | N |
| BROTLI | Y |
| LZ4 | Y |
| LZ4_RAW | Y |
| ZSTD | Y |

Codecs that support per-writer compression levels: GZIP, ZSTD, BROTLI, LZ4, LZ4_RAW.

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
* `PARGO_PREFIX_` is reserved
* Use `\x01` as delimiter to support `.` in field names

## Repetition Types

| Repetition Type | Go Declaration | Description |
|-|-|-|
| REQUIRED | `V1 int32` with tag `parquet:"name=v1, type=INT32"` | Standard required field |
| OPTIONAL | `V1 *int32` with tag `parquet:"name=v1, type=INT32"` | Use pointer for optional fields |
| REPEATED | `V1 []int32` with tag `parquet:"name=v1, type=INT32, repetitiontype=REPEATED"` | Use slice with repetitiontype tag |

## Writers

1. **ParquetWriter** — Write Go structs: [example](example/local_flat)
2. **JSONWriter** — Convert JSON to Parquet: [example](example/json_write)
3. **CSVWriter** — Write CSV-like data: [example](example/csv_write)
4. **ArrowWriter** — Write using Arrow schemas

## Readers

1. **ParquetReader** — Read into Go structs: [example](example/local_nested)
2. **ColumnReader** — Read raw column data: [example](example/column_read)

For large files, read in chunks to avoid OOM.

## File Sources

All file sources implement the `ParquetFile` interface:

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

| Source | Package |
|-|-|
| Local filesystem | `source/local` |
| HDFS | `source/hdfs` |
| S3 (AWS SDK v2) | `source/s3v2` |
| Google Cloud Storage | `source/gcs` |
| Azure Blob Storage | `source/azblob` |
| HTTP (read-only) | `source/http` |
| Memory buffer | `source/buffer` |
| In-memory filesystem | `source/mem` |
| GoCloud CDK | `source/gocloud` |
| OpenStack Swift | `source/swift` |

### HTTP Reader

```go
// Default client
fr, err := phttp.NewHttpReader("https://example.com/data.parquet", true)

// Custom client with timeouts
client := &http.Client{Timeout: 30 * time.Second}
fr, err := phttp.NewHttpReaderWithClient("https://example.com/data.parquet", client)
```

Callers must configure timeouts on custom clients. Call `Transport.CloseIdleConnections()` when done if using persistent connections.

### In-Memory Filesystem

```go
// Explicit filesystem (recommended)
memFs := afero.NewMemMapFs()
fw, err := mem.NewMemFileWriterWithFs("output.parquet", closerFunc, memFs)

// Auto-creates a fresh filesystem
fw, err := mem.NewMemFileWriter("output.parquet", closerFunc)
```

## Migration from v2

### Global Functions Removed

| v2 Global | v3 Replacement |
|-|-|
| `compress.SetMaxDecompressedSize(n)` | `reader.WithMaxDecompressedSize(n)` |
| `compress.SetCompressionLevel(codec, level)` | `writer.WithCompressionLevel(codec, level)` |
| `layout.SetMaxPageSize(n)` | `reader.WithMaxPageSize(n)` |
| `phttp.SetDefaultClient(c)` | `phttp.NewHttpReaderWithClient(url, c)` |
| `mem.SetInMemFileFs(fs)` | `mem.NewMemFileWriterWithFs(name, closer, fs)` |
| `types.SetGeographyJSONMode(m)` | `marshal.WithGeospatialOptions(types.WithGeographyJSONMode(m))` via `marshal.ConvertToJSONFriendly(...)` |
| `types.SetGeometryJSONMode(m)` | `marshal.WithGeospatialOptions(types.WithGeometryJSONMode(m))` via `marshal.ConvertToJSONFriendly(...)` |
| `types.SetGeospatialReprojector(fn)` | `marshal.WithGeospatialOptions(types.WithReprojector(fn))` via `marshal.ConvertToJSONFriendly(...)` |
| `types.SetGeospatialHybridRawBase64(v)` | `marshal.WithGeospatialOptions(types.WithHybridRawBase64(v))` via `marshal.ConvertToJSONFriendly(...)` |
| `types.SetGeospatialGeoJSONAsFeature(v)` | `marshal.WithGeospatialOptions(types.WithGeoJSONAsFeature(v))` via `marshal.ConvertToJSONFriendly(...)` |
| `types.SetGeospatialCoordinatePrecision(n)` | `marshal.WithGeospatialOptions(types.WithCoordinatePrecision(n))` via `marshal.ConvertToJSONFriendly(...)` |

### Constructor Changes

| v2 | v3 |
|-|-|
| `writer.NewParquetWriter(fw, obj, 4)` | `writer.NewParquetWriter(fw, obj, writer.WithNP(4))` |
| `writer.NewParquetWriterFromWriter(w, obj, 4)` | `writer.NewParquetWriterFromWriter(w, obj, writer.WithNP(4))` |
| `writer.NewCSVWriter(md, fw, 4)` | `writer.NewCSVWriter(md, fw, writer.WithNP(4))` |
| `writer.NewCSVWriterFromWriter(md, w, 4)` | `writer.NewCSVWriterFromWriter(md, w, writer.WithNP(4))` |
| `writer.NewJSONWriter(js, fw, 4)` | `writer.NewJSONWriter(js, fw, writer.WithNP(4))` |
| `writer.NewJSONWriterFromWriter(js, w, 4)` | `writer.NewJSONWriterFromWriter(js, w, writer.WithNP(4))` |
| `writer.NewArrowWriter(as, fw, 4)` | `writer.NewArrowWriter(as, fw, writer.WithNP(4))` |
| `pw.CompressionType = codec` | `writer.WithCompressionType(codec)` |
| `pw.RowGroupSize = n` | `writer.WithRowGroupSize(n)` |
| `pw.PageSize = n` | `writer.WithPageSize(n)` |
| `pw.DataPageVersion = 2` | `writer.WithDataPageVersion(2)` |
| `pw.WriteCRC = true` | `writer.WithWriteCRC(true)` |
| ArrowWriter default GZIP | All writers default SNAPPY |
| `reader.NewParquetReader(pf, obj, 4)` | `reader.NewParquetReader(pf, obj, reader.WithNP(4))` |
| `reader.NewParquetReader(pf, obj, 4, opts...)` | `reader.NewParquetReader(pf, obj, reader.WithNP(4), opts...)` |
| `reader.NewParquetColumnReader(pf, 4)` | `reader.NewParquetColumnReader(pf, reader.WithNP(4))` |

### Moved Types

| v2 | v3 |
|-|-|
| `common.PageReadOptions` | `layout.PageReadOptions` (expanded with `MaxDecompressedSize` and `MaxPageSize` fields) |

### Reader Constructor

```go
// v2
pr, err := reader.NewParquetReader(fr, new(Student), 4, reader.ParquetReaderOptions{
    CaseInsensitive: true,
    CRCMode:         common.CRCStrict,
})

// v3
pr, err := reader.NewParquetReader(fr, new(Student),
    reader.WithNP(4),
    reader.WithCaseInsensitive(),
    reader.WithCRCMode(common.CRCStrict),
)
```

### Compression Levels

```go
// v2
compress.SetCompressionLevel(parquet.CompressionCodec_GZIP, 9) // global, affects all writers

// v3
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithNP(4),
    writer.WithCompressionLevel(parquet.CompressionCodec_GZIP, 9), // per-writer
)
```

### Geospatial Configuration

```go
// v2
types.SetGeographyJSONMode(types.GeospatialModeGeoJSON)
types.SetCoordinatePrecision(6)
jsonFriendly, _ := marshal.ConvertToJSONFriendly(row, sh)

// v3
jsonFriendly, _ := marshal.ConvertToJSONFriendly(row, sh,
    marshal.WithGeospatialOptions(
        types.WithGeographyJSONMode(types.GeospatialModeGeoJSON),
        types.WithCoordinatePrecision(6),
    ),
)
```

## Examples

Build examples with the `example` build tag:

```bash
go build -tags example ./example/local_flat
go build -tags example ./example/geospatial
go build -tags example ./example/all_types
```

| Example | Description |
|-|-|
| [local_flat](example/local_flat) | Write/read flat parquet file |
| [local_nested](example/local_nested) | Write/read nested structures |
| [read_partial](example/read_partial) | Read partial fields |
| [read_partial2](example/read_partial2) | Read sub-structs |
| [read_without_schema_predefined](example/read_without_schema_predefined) | Read without predefined schema |
| [json_schema](example/json_schema) | Define schema with JSON |
| [json_write](example/json_write) | Convert JSON to Parquet |
| [convert_to_json](example/convert_to_json) | Convert Parquet to JSON |
| [csv_write](example/csv_write) | CSV writer |
| [column_read](example/column_read) | Read raw column data |
| [mem](example/mem) | In-memory filesystem writer |
| [geospatial](example/geospatial) | Geospatial types |
| [all_types](example/all_types) | All type support |
| [datapagev2](example/datapagev2) | Data Page V2 format |

## Concurrency

Use `WithNP(n)` to control parallel goroutines for marshaling/unmarshaling (default: 4):

```go
pw, err := writer.NewParquetWriter(fw, new(Student), writer.WithNP(4))
pr, err := reader.NewParquetReader(fr, new(Student), reader.WithNP(4))
```

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

Apache License 2.0
