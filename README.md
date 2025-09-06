# parquet-go/v2

Please read [v1 README.md](READMEv1.md) first.

This repo was forked from https://github.com/xitongsys/parquet-go, v2 introduce incompatible changes that can benifit future enhancements, it is experienmetal at the moment, please take into consideration before using it.

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
