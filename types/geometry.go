package types

import (
	"encoding/base64"
	"encoding/hex"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ConvertGeometryLogicalValue converts WKB bytes to a JSON-friendly wrapper with hex and CRS.
func ConvertGeometryLogicalValue(val any, geom *parquet.GeometryType, cfg *GeospatialConfig) any {
	rendered, _ := convertGeometryValue(val, geom, cfg)
	return rendered
}

// convertGeometryValue renders a GEOMETRY, reporting bytes the requested rendering cannot
// be produced from. Hex and base64 render any bytes; GeoJSON and hybrid need WKB this
// parser understands, and fall back to a wkb_hex map when it is not.
func convertGeometryValue(val any, geom *parquet.GeometryType, cfg *GeospatialConfig) (any, error) {
	if val == nil {
		return nil, nil
	}
	b, ok := valueBytes(val)
	if !ok {
		return val, errUnrenderable("GEOMETRY", "value is %T, not bytes", val)
	}
	crs := "OGC:CRS84"
	if geom != nil && geom.CRS != nil && *geom.CRS != "" {
		crs = *geom.CRS
	}
	// Built only where it is returned: hex-encoding every value would cost twice the
	// WKB in bytes on the GeoJSON and base64 paths, which never use it.
	hexForm := func() map[string]any {
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs}
	}

	reproject := func(gj map[string]any) map[string]any {
		if crs != "OGC:CRS84" && cfg.Reprojector != nil {
			if rj, ok := cfg.Reprojector(crs, gj); ok {
				return rj
			}
		}
		return gj
	}

	switch cfg.GeometryJSONMode {
	case GeospatialModeGeoJSON:
		gj, ok := wkbToGeoJSON(b, cfg.CoordPrecision)
		if !ok {
			return hexForm(), errNotWKB("GEOMETRY")
		}
		gj = reproject(gj)
		if cfg.GeoJSONAsFeature {
			return makeGeoJSONFeature(gj, map[string]any{"crs": crs}), nil
		}
		return gj, nil
	case GeospatialModeBase64:
		return map[string]any{"wkb_b64": base64.StdEncoding.EncodeToString(b), "crs": crs}, nil
	case GeospatialModeHybrid:
		gj, ok := wkbToGeoJSON(b, cfg.CoordPrecision)
		if !ok {
			return hexForm(), errNotWKB("GEOMETRY")
		}
		// Unlike GEOGRAPHY's hybrid branch, this one does not reproject. The difference
		// predates the strict path and changing it would change output, so it stays.
		m := wrapGeoJSONHybrid(gj, b, cfg.HybridUseBase64, true)
		m["crs"] = crs
		return m, nil
	default: // hex
		return hexForm(), nil
	}
}
