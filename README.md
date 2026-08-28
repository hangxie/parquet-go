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
- Dictionary indices use the minimum bit width required by the completed row-group dictionary. Encoded dictionary value bytes are capped at 1 MiB by default, after which the writer uses `PLAIN` encoding for subsequent pages; tune the cap with `writer.WithMaxDictionarySize`.
- `writer.WithDataPageVersion(2)` applies to both dictionary-encoded and plain data pages.
- Use `omitstats=true` in a field tag to skip statistics for large array fields.
- Whenever min/max statistics are available, the current `min_value`/`max_value` fields are written. The deprecated `min`/`max` fields (PARQUET-251) are limited to signed sort orders; for unsigned-ordered columns (e.g. `BYTE_ARRAY`/UTF8 and unsigned integer logical types) they are omitted so legacy readers do not misinterpret them.
- A column chunk whose data pages are all dictionary encoded carries an exact `distinct_count` statistic taken from its dictionary, which holds one entry per distinct non-null value, and covers that row group alone. It is omitted for columns tagged `omitstats=true`, and once the dictionary reaches `writer.WithMaxDictionarySize` and the remaining pages fall back to `PLAIN`, because the dictionary then covers only part of the chunk. It is omitted again wherever a dictionary entry does not correspond one-to-one with a distinct logical value: for `FLOAT`/`DOUBLE` columns holding a NaN, whose entries do not deduplicate; for `DECIMAL` backed by `BYTE_ARRAY`, whose variable width admits several two's-complement encodings of the same unscaled value (`FIXED_LEN_BYTE_ARRAY` decimals keep the statistic, since their width pins one encoding per value); for `FLOAT16`, where `-0.0` and `+0.0` occupy separate entries; and for `GEOMETRY`/`GEOGRAPHY`, whose WKB payloads carry a byte-order flag and so encode one geometry two ways.
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

`NaN` and infinite `FLOAT`, `DOUBLE`, and `FLOAT16` values have no JSON number form, so `marshal.ConvertToJSONFriendly` and `types.ConvertToJSONType` render them as the quoted strings `"NaN"`, `"Infinity"`, and `"-Infinity"`, in struct fields, list elements, and map values alike. `JSONWriter` accepts those strings on input, along with `Inf`, `+Inf`, and `-Inf`, case-insensitive. Finite values are unchanged. See [example/json_nan](example/json_nan) for a round trip.

Quoted strings are used rather than the bare `NaN` and `Infinity` literals that Python and DuckDB emit, because those are not valid JSON and Go's `encoding/json` refuses to produce them; a quoted string is ordinary JSON that every parser accepts.

The infinity spelling is `"Infinity"` rather than Go's native `"+Inf"` because the output has to survive being read somewhere else. Both round trip through this library, and `"NaN"` is recovered as a float everywhere, but `"+Inf"` and `"-Inf"` are not consistently accepted across ecosystems. Go, Python, and DuckDB parse them as infinities, while Java's `Double.parseDouble` and Jackson reject them and JavaScript's `Number()` silently returns `NaN`, turning an infinity into a different value with no error; Spark's JSON reader is documented as handling quoted non-numeric tokens inconsistently (SPARK-38060). `"Infinity"` and `"-Infinity"` are recovered correctly by all of them. This differs from Apache Arrow's Go implementation, which emits `"+Inf"`.

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
