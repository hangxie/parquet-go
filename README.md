# parquet-go/v3

[![](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hangxie/parquet-go/blob/main/LICENSE)
[![](https://img.shields.io/github/v/tag/hangxie/parquet-go.svg?color=brightgreen&label=version&sort=semver)](https://github.com/hangxie/parquet-go/releases)
[![[parquet-go]](https://github.com/hangxie/parquet-go/actions/workflows/build.yml/badge.svg)](https://github.com/hangxie/parquet-go/actions/workflows/build.yml)
[![](https://github.com/hangxie/parquet-go/wiki/coverage.svg)](https://github.com/hangxie/parquet-go/wiki/Coverage-Report)

parquet-go is a pure-Go library for reading and writing Apache Parquet files.

## Highlights

- Read and write flat and nested Parquet data.
- Use Go struct tags, JSON schema, CSV metadata, or Arrow schemas.
- Work with local files, memory buffers, cloud object stores, HDFS, HTTP, and GoCloud CDK blobs.
- Configure readers and writers with per-instance functional options.
- Use modern Parquet features including Data Page V2, CRC page checksums, modular encryption, bloom filters, and newer logical types.
- Convert geospatial logical types with configurable GeoJSON, hex, base64, or hybrid JSON output.

## Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Write a File](#write-a-file)
  - [Read a File](#read-a-file)
- [Configuration](#configuration)
- [Schema Definition](#schema-definition)
- [Type System](#type-system)
- [Encoding Support](#encoding-support)
- [Compression Support](#compression-support)
- [Readers and Writers](#readers-and-writers)
- [File Sources](#file-sources)
- [Advanced Features](#advanced-features)
  - [CRC Page Checksums](#crc-page-checksums)
  - [Encryption](#encryption)
  - [GeoParquet](#geoparquet)
  - [Concurrency](#concurrency)
- [Examples](#examples)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Installation

```sh
go get github.com/hangxie/parquet-go/v3
```

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

Readers and writers are configured per instance with functional options. This keeps independent readers and writers safe to use with different settings in the same process.

Common writer options:

- `writer.WithNP`
- `writer.WithPageSize`
- `writer.WithRowGroupSize`
- `writer.WithCompressionCodec`
- `writer.WithCompressionLevel`
- `writer.WithDataPageVersion`
- `writer.WithWriteCRC`

Common reader options:

- `reader.WithNP`
- `reader.WithCaseInsensitive`
- `reader.WithCRCMode`

Encryption-related options are covered in [Encryption](#encryption).

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
    },
    nil,
)
```

Schema notes:

- `InName` is the Go field name. `ExName` is the Parquet field name.
- Avoid field names that differ only by first-letter case.
- `PARGO_PREFIX_` is reserved and should not be used as a field prefix.
- Use `\x01` as a delimiter when a field name needs to contain `.`.

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
| `GEOMETRY` | `BYTE_ARRAY` | `string` |
| `GEOGRAPHY` | `BYTE_ARRAY` | `string` |
| `JSON` | `BYTE_ARRAY` | `string` |
| `BSON` | `BYTE_ARRAY` | `string` |
| `LIST` | - | slice |
| `MAP` | - | map |

Type aliases are supported, for example `type MyString string`, when the base type follows the table. Conversion utilities are available in [types/converter.go](types/converter.go).

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
- Avoid dictionary encoding for high-cardinality fields because dictionaries can consume significant memory.
- Use `omitstats=true` in a field tag to skip statistics for large array fields.

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
- `LZ4` uses the legacy framed LZ4 format. `LZ4_RAW` uses raw LZ4 blocks and is the preferred LZ4 variant in the Parquet specification.
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

### Encryption

The reader and writer support Apache Parquet modular encryption for encrypted footers (`PARE`) and plaintext footers signed with AES-GCM (`PAR1`). Page headers, data pages, dictionary pages, column metadata, column indexes, offset indexes, and bloom filter headers/bitsets are encrypted and decrypted when encryption metadata and the required keys are available.

Shared reader/writer concepts:

- `WithFooterKey` sets the footer key used for encrypted footers, plaintext-footer signatures, and columns encrypted with the footer key.
- `WithColumnKey` sets a column key for a dot-separated schema path without the root element, such as `name` or `address.city`.
- `WithKeyRetriever` sets a callback for KMS-backed key management. The callback receives the `key_metadata` bytes stored in the file and returns key bytes.
- `WithAADPrefix` supplies the AAD prefix. It is required on read when the file was written with `supply_aad_prefix=true`.

Without a KMS, supply keys directly with `WithFooterKey` and `WithColumnKey`. With a KMS, `key_metadata` stores the KMS key ID for each key.

On write, the key ID must be specified explicitly because there is no file to read from. Use the optional metadata argument to `WithFooterKey` or `WithColumnKey`, or use `WithFooterKeyMetadata` and `WithColumnKeyMetadata` when key bytes come from `WithKeyRetriever`. On read, the key ID is already in the file and is passed to `WithKeyRetriever`.

When explicit keys and `WithKeyRetriever` are both configured, explicit keys take priority. `WithKeyRetriever` is called only when no direct key is provided.

Reader behavior by footer and column mode:

- Encrypted footer (`PARE`): the footer key is required to open the file because schema and row-group metadata are encrypted.
- Plaintext footer (`PAR1`): the file can be opened without keys. If a footer key is supplied or resolved, the reader verifies the plaintext-footer AES-GCM signature; without the footer key, the footer is readable but not authenticated by this reader.
- Mixed plaintext/encrypted columns: plaintext columns can be read without encryption keys. Encrypted columns require either the column key from `WithColumnKey`/`WithKeyRetriever` or the footer key when the column metadata says it uses the footer key.
- Encrypted column indexes, offset indexes, and bloom filters require the key for that encrypted column. The plain versions can be read without keys.
- Low-level page-header inspection helpers do not inspect encrypted columns; use normal row reads with the required keys instead.

Read a file encrypted with a single footer key:

```go
pr, err := reader.NewParquetReader(
    fr,
    new(Student),
    reader.WithFooterKey(footerKey),
)
```

Read with KMS-backed key management:

```go
keyRetriever := func(keyMetadata []byte) ([]byte, error) {
    return lookupKey(keyMetadata)
}

pr, err := reader.NewParquetReader(
    fr,
    new(Student),
    reader.WithKeyRetriever(keyRetriever),
)
```

Read only plaintext columns from a plaintext-footer file that also has encrypted columns:

```go
pr, err := reader.NewParquetReader(fr, new(StudentPublicFields))
if err == nil {
    err = pr.ReadPartial(&rows, "public")
}
```

Write a file encrypted with a single footer key:

```go
pw, err := writer.NewParquetWriter(
    fw,
    new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithAADPrefix([]byte("dataset/part-0")),
    writer.WithAADFileUnique([]byte("unique-file-id")),
)
```

Write with KMS-backed key management:

```go
keyRetriever := func(keyMetadata []byte) ([]byte, error) {
    return lookupKey(keyMetadata)
}

pw, err := writer.NewParquetWriter(
    fw,
    new(Student),
    writer.WithFooterKeyMetadata([]byte("footer-key")),
    writer.WithColumnKeyMetadata("name", []byte("name-key")),
    writer.WithKeyRetriever(keyRetriever),
    writer.WithAADPrefix([]byte("dataset/part-0")),
    writer.WithAADFileUnique([]byte("unique-file-id")),
)
```

Write encrypted columns with a plaintext footer:

```go
pw, err := writer.NewParquetWriter(
    fw,
    new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithColumnKey("name", nameKey),
    writer.WithAADPrefix([]byte("dataset/part-0")),
    writer.WithAADFileUnique([]byte("unique-file-id")),
    writer.WithPlaintextFooter(true),
)
```

Use AES-GCM-CTR page encryption and an external AAD prefix:

```go
pw, err := writer.NewParquetWriter(
    fw,
    new(Student),
    writer.WithEncryptionAlgorithm(writer.EncryptionAESGCMCTRV1),
    writer.WithFooterKey(footerKey),
    writer.WithAADPrefix([]byte("external-prefix")),
    writer.WithAADFileUnique([]byte("unique-file-id")),
    writer.WithSupplyAADPrefix(true),
)
```

When `WithSupplyAADPrefix(true)` is used, readers must pass the same value with `reader.WithAADPrefix`. See [example/encrypt_write](example/encrypt_write) for complete encrypted write/read-back examples.

#### Security notes

**Footer mode**: The default writer mode is encrypted footer (`PARE`). The entire file metadata - including schema (column names and types), row counts, and statistics - is encrypted and invisible to readers without the footer key. Use `WithPlaintextFooter(true)` to write a `PAR1` file where the footer is plaintext but signed with AES-GCM. This is appropriate when downstream readers need schema or statistics access without possessing column keys, accepting that schema information (column names) is exposed. Readers without the footer key can parse plaintext footers, but cannot verify the footer signature.

**AAD uniqueness**: Each file should have a unique `(AADPrefix, AADFileUnique)` pair. The AAD binds each encrypted module to its intended file so that ciphertext from one file cannot be replayed into another. Omitting `WithAADFileUnique` generates a random 12-byte value automatically; supply it explicitly for deterministic file identity. Reusing the same pair with the same key across different files breaks this binding.

**Per-page IVs and random access**: Each encrypted module (page header, page body, column metadata, bloom filter, column index, offset index) receives its own independently generated random nonce. Because IVs are per-module rather than per-column-chunk, individual pages can be decrypted in isolation — the reader does not need to process the entire column chunk from the start to reach a single page.

**Algorithm choice**: `AES_GCM_V1` (default) uses AES-GCM for every module and provides authenticated encryption for data and metadata alike. `AES_GCM_CTR_V1` uses AES-GCM for metadata modules and AES-CTR for data pages; CTR page bodies have no authentication tag, so tampering with the body ciphertext produces corrupted plaintext without being detected by the cipher. The GCM-authenticated page header covers only header bytes and does not protect the body. Use `AES_GCM_V1` when integrity detection for page data is required.

**Compatibility**: The reader follows the Apache Parquet Encryption Specification for plaintext-footer files with mixed plaintext/encrypted columns. The current writer encrypts every column when encryption is enabled; columns without an explicit column key use the footer key. Files produced by this library are interoperable with other spec-compliant readers - including Apache Arrow (C++/Python) and Apache Spark - when the same keys and AAD configuration are used.

### GeoParquet

parquet-go supports Apache Parquet geospatial logical types and configurable JSON output through `marshal.ConvertToJSONFriendly`.

Overview:

- `GEOMETRY` stores planar coordinates with optional CRS.
- `GEOGRAPHY` stores spherical coordinates with optional CRS and edge interpolation algorithm.
- Physical storage is WKB in `BYTE_ARRAY` fields.
- CRS defaults to `OGC:CRS84` when not provided.
- GEOGRAPHY algorithms include `SPHERICAL`, `VINCENTY`, `THOMAS`, `ANDOYER`, and `KARNEY`.

JSON output modes:

| Mode | Output |
| --- | --- |
| Hex | WKB data as hexadecimal strings |
| Base64 | WKB data as base64 strings |
| GeoJSON | RFC 7946 compliant GeoJSON output |
| Hybrid | GeoJSON plus raw WKB |

Defaults are GeoJSON for `GEOGRAPHY` and hex for `GEOMETRY`.

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

Supported WKB geometry types in the built-in converter:

- Point (2D)
- LineString (2D)
- Polygon (2D)

Other WKB types fall back to raw WKB. If WKB parsing fails, the converter also falls back to raw WKB with CRS or algorithm metadata when applicable.

### Concurrency

Use `WithNP(n)` to set the number of parallel goroutines. The default is 4.

```go
func NewParquetReader(pFile source.ParquetFileReader, obj any, opts ...ReaderOption) (*ParquetReader, error)
func NewParquetWriter(pFile source.ParquetFileWriter, obj any, opts ...WriterOption) (*ParquetWriter, error)
func NewJSONWriter(jsonSchema string, pfile source.ParquetFileWriter, opts ...WriterOption) (*JSONWriter, error)
func NewCSVWriter(md []string, pfile source.ParquetFileWriter, opts ...WriterOption) (*CSVWriter, error)
func NewArrowWriter(arrowSchema *arrow.Schema, pfile source.ParquetFileWriter, opts ...WriterOption) (*ArrowWriter, error)
```

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
| [csv_write](example/csv_write) | CSV writer |
| [csv_to_parquet](example/csv_to_parquet) | CSV file to Parquet |
| [column_read](example/column_read) | Read raw column data |
| [type](example/type) | Type examples |
| [type_alias](example/type_alias) | Type alias examples |
| [new_logical](example/new_logical) | New logical types including FLOAT16 and INTEGER |
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

## Documentation

- [v1 README](READMEv1.md): original v1 documentation.
- [v2 README](READMEv2.md): v2 documentation.
- [source/README.md](source/README.md): file source implementations.

## Contributing

Contributions are welcome. Please submit issues or pull requests.

## License

Apache License 2.0
