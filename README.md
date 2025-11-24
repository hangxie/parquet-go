# parquet-go/v2

[![](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/hangxie/parquet-go/blob/main/LICENSE)
[![](https://img.shields.io/github/v/tag/hangxie/parquet-go.svg?color=brightgreen&label=version&sort=semver)](https://github.com/hangxie/parquet-go/releases)
[![[parquet-go]](https://github.com/hangxie/parquet-go/actions/workflows/build.yml/badge.svg)](https://github.com/hangxie/parquet-go/actions/workflows/build.yml)
[![](https://github.com/hangxie/parquet-go/wiki/coverage.svg)](https://github.com/hangxie/parquet-go/wiki/Coverage-Report)

Please read [v1 README.md](READMEv1.md) and [parquet-go-source README.md](source/README.md) first.

This repo was forked from https://github.com/xitongsys/parquet-go, and merged https://github.com/xitongsys/parquet-go-source. v2 introduce incompatible changes that can benefit future enhancements, eg:
1. most functions now return error to indicate something's wrong
2. replace panic/recover style of code with proper error handling
3. performance improvement, like SkipRows
4. interpret logical type/converted type properly

there are also bug fixes like race condition, nil panic, out of bound index, etc.

## New Logical Types & Geospatial JSON

This fork updates to the latest `parquet.thrift` and adds support for new logical types and JSON conversions:

- New logical types: `FLOAT16`, `VARIANT`, `GEOMETRY`, `GEOGRAPHY` (plus existing `UUID`, `INTEGER`, etc.)
- JSON conversion (used by `marshal.ConvertToJSONFriendly`) now includes:
  - `FLOAT16`: decodes `FIXED[2]` half-precision to `float32`
  - `INTEGER`: maps based on `bitWidth` and `isSigned` (e.g., 8→int8/uint8; 16→int16/uint16; 32→int32/uint32; 64→int64/uint64)
  - `UUID`: 16-byte raw → canonical UUID string
- Geospatial (`GEOMETRY`/`GEOGRAPHY`): configurable JSON output (see GeoParquet doc)

### Compatibility Notes

- VARIANT logical type: Not widely supported by Apache Parquet tooling today, e.g. the Apache parquet-cli may fail with errors like:
  - `State error: VARIANT(1) can not be applied to a primitive type`
  For broad compatibility, prefer `logicaltype=JSON` (or `BSON`) over `VARIANT`.

See `geoparquet.md` for:
- Output modes (Hex, Base64, GeoJSON, Hybrid) and defaults
- CRS/algorithm handling and reprojection rules
- Fallback behavior when WKB parsing fails
- Supported GeoJSON shapes

### Examples

Build examples with the `example` build tag:

```bash
go build -tags example ./example/new_logical     # FLOAT16 + INTEGER logical types
go build -tags example ./example/geospatial      # GEOMETRY + GEOGRAPHY JSON modes
go build -tags example ./example/all_types       # Comprehensive sample
```

Then run the produced binaries in each example directory.
