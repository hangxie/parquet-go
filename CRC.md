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

*TODO: Define CRC write behavior (see #207).*

The writer should compute CRC32 of the compressed page data and set
the `Crc` field in `PageHeader` before serialization.
