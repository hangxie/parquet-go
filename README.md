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

Readers and writers are configured per instance by passing functional options to their constructors. This keeps independent readers and writers safe to use with different settings in the same process. Options are construction inputs, not a public API for mutating an already-created reader or writer.

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

Footer mode and column classification interact as follows:

| Footer mode | Plaintext column | Footer-key column | Column-key column |
| --- | --- | --- | --- |
| Encrypted footer (`PARE`) | Page/index/bloom modules are plaintext. The footer key is required to open the encrypted file metadata, regardless of column classification. | Page/index/bloom modules use the footer key; column metadata stays in the encrypted footer. | Page/index/bloom modules use the column key; encrypted column metadata is stored for readers with only the column key. |
| Signed plaintext footer (`PAR1`) | Page/index/bloom modules and column statistics are plaintext; readers without keys can read projected plaintext columns. | Page/index/bloom modules use the footer key; plaintext footer metadata is present but statistics are stripped. | Page/index/bloom modules use the column key; plaintext footer metadata is present but statistics are stripped. |

Writer column classification is selected with `writer.WithColumnEncrypted(path, opts...)`, where `path` is the dot-separated schema path without the root element:

```go
// Omitted path: default is footer-key encryption; mixed mode makes it plaintext.
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithPlaintextUnkeyedColumns(true),
)
```

```go
// Footer-key column.
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithPlaintextUnkeyedColumns(true),
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

For callers that build `EncryptionConfig` literally, `ColumnKeys[p] = writer.EncryptionColumnKey{}` is equivalent to `WithColumnEncrypted(p)` and produces `ENCRYPTION_WITH_FOOTER_KEY`. This is a semantic change for direct struct-literal users: older code treated the zero value as "no direct key" and could call `KeyRetriever(nil)` or fail validation when no retriever was configured. To retrieve by metadata now, set non-empty `KeyMetadata` and configure `KeyRetriever`; to use the footer key, keep the zero value or use `ColumnFooterKey()`.

| `ColumnKeys[p]` state | How to produce | Resolved behavior |
| --- | --- | --- |
| not in map | omit `WithColumnEncrypted(p, ...)` | mixed=true -> plaintext; mixed=false -> footer-key |
| `{}` | `WithColumnEncrypted(p)` or `WithColumnEncrypted(p, ColumnFooterKey())` | footer-key, no `KeyMetadata` stored |
| `{Key: bytes}` | `WithColumnEncrypted(p, ColumnKey(key))` | column-key, no `KeyMetadata` stored |
| `{Key: bytes, KeyMetadata: md}` | `WithColumnEncrypted(p, ColumnKey(key, md))` | column-key plus `KeyMetadata` stored in file |
| `{Key: nil, KeyMetadata: md}` plus writer `KeyRetriever` | `WithColumnEncrypted(p, ColumnKeyByMetadata(md))` | retriever called at write time; empty result is an error |
| `{Key: nil, KeyMetadata: md}` with no writer `KeyRetriever` | same call without retriever | construction error |

Repeated column options follow standard Go map semantics: the last call wins and no conflict detection runs. This keeps deprecated wrapper calls and the new option API order-dependent in the same way; applications that compose options dynamically should keep a single owner for each column path.

```go
writer.WithColumnEncrypted("ssn", writer.ColumnKey(oldKey))
writer.WithColumnEncrypted("ssn", writer.ColumnFooterKey()) // final state
```

`WithColumnKey` and `WithColumnKeyMetadata` remain as deprecated wrappers. They will be removed in a future release alongside `PlaintextUnkeyedColumns`; after that removal, absence from `ColumnKeys` will mean plaintext. Until that release, the default remains the legacy behavior where omitted columns use the footer key. The default flip is security-relevant and should be called out in the release notes, changelog, and migration guide for that release.

| Deprecated call | Replacement |
| --- | --- |
| `WithColumnKey("p", k)` | `WithColumnEncrypted("p", ColumnKey(k))` |
| `WithColumnKey("p", k, md)` | `WithColumnEncrypted("p", ColumnKey(k, md))` |
| `WithColumnKey("p", nil)` | `WithColumnEncrypted("p")` |
| `WithColumnKey("p", nil, md)` | `WithColumnEncrypted("p", ColumnKeyByMetadata(md))` |
| `WithColumnKeyMetadata("p", md)` | `WithColumnEncrypted("p", ColumnKeyByMetadata(md))` |

Encrypted footer with mixed plaintext plus a per-column key:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithPlaintextUnkeyedColumns(true),
    writer.WithColumnEncrypted("ssn", writer.ColumnKey(ssnKey)),
)
```

Signed plaintext footer with mixed plaintext plus a per-column key:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithPlaintextFooter(true),
    writer.WithPlaintextUnkeyedColumns(true),
    writer.WithColumnEncrypted("ssn", writer.ColumnKey(ssnKey)),
)
```

Encrypted footer with a footer-key column and mixed plaintext:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithPlaintextUnkeyedColumns(true),
    writer.WithColumnEncrypted("name"),
)
```

In the current default mode, `WithColumnEncrypted(p, ColumnFooterKey())` is observationally the same as omitting `p` because omitted columns also use the footer key. In mixed mode, and after the future removal of `PlaintextUnkeyedColumns`, `ColumnFooterKey()` is the explicit selector that keeps that column encrypted with the footer key while omitted sibling columns are plaintext.

Three-way mix in one file:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithPlaintextUnkeyedColumns(true),
    writer.WithColumnEncrypted("name"),
    writer.WithColumnEncrypted("ssn", writer.ColumnKey(ssnKey)),
)
```

All columns plaintext with an encrypted footer:

```go
pw, err := writer.NewParquetWriter(fw, new(Student),
    writer.WithFooterKey(footerKey),
    writer.WithPlaintextUnkeyedColumns(true),
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

Reader behavior is driven by the file's per-column `CryptoMetadata`: nil means plaintext, `ENCRYPTION_WITH_FOOTER_KEY` means footer-key column, and `ENCRYPTION_WITH_COLUMN_KEY` means column-key column. `reader.WithColumnKey(path, key)` supplies a direct key for a column, while `reader.WithKeyRetriever` resolves keys from stored `key_metadata`. If a writer intentionally stores `ENCRYPTION_WITH_COLUMN_KEY` while using bytes equal to the footer key, downstream readers can decrypt it either with `reader.WithColumnKey(path, footerKey)` or with a retriever that returns `footerKey` for that column metadata.

Writer metadata-based keys are strict: `ColumnKeyByMetadata(md)` requires the writer's `KeyRetriever` to return a non-empty AES key at construction time. The reader API still treats missing column-key material as a read-time decryption failure because `reader.WithColumnKey` carries only key bytes, not expected key metadata.

Compatibility: `apache/parquet-testing` includes mixed plaintext/encrypted plaintext-footer fixtures generated through Parquet C++/parquet-mr test paths, and this repository reads them in interop tests. It does not currently include parquet-cpp 1.x mixed writer fixtures, so compatibility with older readers on writer-produced mixed files is based on Parquet spec compliance rather than direct fixture coverage. Files produced by this library should interoperate with spec-compliant readers when the same keys and AAD configuration are used.

Spec references:

- Modular encryption: https://parquet.apache.org/docs/file-format/data-pages/encryption/
- Bloom filter encryption: https://parquet.apache.org/docs/file-format/bloomfilter/

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
