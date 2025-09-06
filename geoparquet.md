# GeoParquet: Geospatial Logical Types (GEOMETRY/GEOGRAPHY)

This project supports Apache Parquet's geospatial logical types and provides configurable JSON output when converting data to a "JSON‑friendly" form (via `marshal.ConvertToJSONFriendly`).

## Overview

- Physical storage: WKB (Well‑Known Binary) in `BYTE_ARRAY` fields
- Logical types:
  - `GEOMETRY`: planar coordinates with an optional `crs`
  - `GEOGRAPHY`: spherical coordinates with an optional `crs` and an `algorithm` for edge interpolation
- CRS: defaults to `"OGC:CRS84"` when not provided
- Algorithm (GEOGRAPHY): `SPHERICAL` (default), `VINCENTY`, `THOMAS`, `ANDOYER`, `KARNEY`

## JSON Output Modes

You can choose how geospatial values are emitted during JSON conversion:

1. Hex
   - Geometry: `{ "wkb_hex": "...", "crs": "..." }`
   - Geography: `{ "wkb_hex": "...", "crs": "...", "algorithm": "..." }`

2. Base64
   - Geometry: `{ "wkb_b64": "...", "crs": "..." }`
   - Geography: `{ "wkb_b64": "...", "crs": "...", "algorithm": "..." }`

3. GeoJSON (RFC 7946 Feature by default)
   - Default: `{ "type": "Feature", "geometry": { ... }, "properties": { ... } }`
     - Geometry properties include: `crs`
     - Geography properties include: `crs`, `algorithm`
   - Optional (when toggled): emit the geometry object directly `{ ... }`

4. Hybrid (GeoJSON + raw WKB)
   - Geometry: `{ "geojson": { ... }, "wkb_hex|wkb_b64": "...", "crs": "..." }`
   - Geography: `{ "geojson": { ... }, "wkb_hex|wkb_b64": "...", "crs": "...", "algorithm": "..." }`

Defaults:

- `GEOGRAPHY` → GeoJSON
- `GEOMETRY` → Hex

## Configuration API

```go
// Select per‑type JSON output mode
types.SetGeographyJSONMode(types.GeospatialModeGeoJSON) // or Hex, Base64, Hybrid
types.SetGeometryJSONMode(types.GeospatialModeHex)

// In Hybrid mode, choose raw encoding: false → hex (default), true → base64
types.SetGeospatialHybridRawBase64(true)

// Optional reprojection to CRS84
// Applied for:
// - GEOGRAPHY in GeoJSON and Hybrid modes when input CRS != "OGC:CRS84"
// - GEOMETRY in GeoJSON mode when input CRS != "OGC:CRS84"
// Note: GEOMETRY Hybrid currently does not apply reprojection.
types.SetGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
    // Implement CRS→CRS84 reprojection here and return (updated, true)
    // Return (nil, false) to skip/indicate failure
    return nil, false
})

// Optional: emit GeoJSON geometry object instead of Feature in GeoJSON mode
types.SetGeospatialGeoJSONAsFeature(false) // default is true

// Optional: round coordinates to a fixed number of decimals in GeoJSON
// (RFC 7946 §11.2 discusses precision considerations)
types.SetGeospatialCoordinatePrecision(6) // default is 6; set -1 to disable
```

## Output Examples

Given a WKB Point in CRS84 (hex `0101000000000000000000F03F0000000000000040` → (1,2)):

- GeoJSON (Geography default, Feature):

```json
{ "type": "Feature", "geometry": { "type": "Point", "coordinates": [1, 2] }, "properties": { "crs": "OGC:CRS84", "algorithm": "SPHERICAL" } }
```

- Hex (Geometry default):

```json
{ "wkb_hex": "0101000000000000000000F03F0000000000000040", "crs": "OGC:CRS84" }
```

- Hybrid + base64 raw:

```json
{ "geojson": { "type": "Point", "coordinates": [1, 2] }, "wkb_b64": "AQAAAAAAAADwP4AAAAAAAABA", "crs": "OGC:CRS84" }
```

## Reprojection Hook

If your `GEOMETRY` provides a non‑CRS84 CRS (e.g., `EPSG:3857`), you can register a reprojection callback to emit GeoJSON in CRS84:

```go
types.SetGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
    if crs == "EPSG:3857" {
        // Convert geometry coordinates from EPSG:3857 to CRS84 (lon/lat degrees)
        // using your preferred library; return the updated GeoJSON
        return gj, true
    }
    return nil, false
})
```

Notes:
- Reprojection applies as described above. If reprojection fails or the hook returns false, the original GeoJSON is emitted unchanged.

## Fallback Behavior

- If WKB parsing fails or the geometry type is not supported by the built‑in converter, the converter falls back to raw WKB including CRS/algorithm when applicable.
- In Hybrid mode, fallback uses hex (`wkb_hex`) regardless of the base64 selection flag.

## Supported Geometry Types in Converter

- Point (2D)
- LineString (2D)
- Polygon (2D)

Other WKB types currently fall back to raw WKB. Contributions to extend coverage are welcome.

## Examples

Build and run the examples using the `example` build tag:

```bash
go build -tags example ./example/geospatial && ./geospatial
```

This example writes a mixture of Point/LineString/Polygon as WKB and prints JSON‑friendly output according to the configured modes and reprojection hook.
