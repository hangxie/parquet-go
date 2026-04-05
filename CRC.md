# CRC Checksum Handling

This document specifies how CRC32 checksums are handled during
Parquet page reads and writes.

## Background

The Parquet format defines an optional CRC32 field in `PageHeader`
(polynomial 0x04C11DB7, same as GZip). The checksum covers the
compressed page data (not the page header itself).

## Reader: CRC Validation Modes

CRC validation on read is controlled by a three-way flag applied
globally to all columns.

| Scenario               | Strict     | Auto           | Ignore (default) |
|------------------------|------------|----------------|-------------------|
| CRC absent in header   | **Fail**   | Pass           | Pass              |
| CRC present, valid     | Pass       | Pass           | Pass              |
| CRC present, invalid   | **Fail**   | **Fail**       | Pass              |

- **Ignore** -- Default mode. Skips all CRC validation regardless
  of whether a checksum is present in the page header. This
  preserves backward-compatible behavior.
- **Auto** -- Validates CRC when present, silently passes when
  absent.
- **Strict** -- Requires CRC on every page and validates it.
  Returns an error if CRC is missing or if the computed checksum
  does not match.

The flag applies to all columns uniformly; there is no per-column
CRC setting.

## Writer: CRC Computation

CRC computation on write is controlled by the `WriteCRC` field on
`ParquetWriter`. When enabled, the writer computes CRC32 (IEEE
polynomial) over the compressed page data and sets `PageHeader.Crc`
before serialization. The default is `false` (no CRC written).

```go
pw, _ := writer.NewParquetWriter(pFile, obj, writer.WithWriteCRC(true))
```

CRC is computed on all page types: data pages (v1 and v2),
dictionary pages, and dictionary-encoded data pages. The checksum
covers the same bytes that the reader validates — the full
`CompressedPageSize` payload after the page header.
