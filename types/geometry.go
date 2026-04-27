package types

import (
	"encoding/base64"
	"encoding/hex"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ConvertGeometryLogicalValue converts WKB bytes to a JSON-friendly wrapper with hex and CRS.
func ConvertGeometryLogicalValue(val any, geom *parquet.GeometryType, cfg *GeospatialConfig) any {
	if val == nil {
		return nil
	}
	var b []byte
	switch v := val.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return val
	}
	crs := "OGC:CRS84"
	if geom != nil && geom.CRS != nil && *geom.CRS != "" {
		crs = *geom.CRS
	}
	switch cfg.GeometryJSONMode {
	case GeospatialModeGeoJSON:
		if gj, ok := wkbToGeoJSON(b, cfg.CoordPrecision); ok {
			if crs != "OGC:CRS84" && cfg.Reprojector != nil {
				if rj, ok2 := cfg.Reprojector(crs, gj); ok2 {
					gj = rj
				}
			}
			if cfg.GeoJSONAsFeature {
				return makeGeoJSONFeature(gj, map[string]any{"crs": crs})
			}
			return gj
		}
		// fallback
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs}
	case GeospatialModeBase64:
		return map[string]any{"wkb_b64": base64.StdEncoding.EncodeToString(b), "crs": crs}
	case GeospatialModeHybrid:
		if gj, ok := wkbToGeoJSON(b, cfg.CoordPrecision); ok {
			m := wrapGeoJSONHybrid(gj, b, cfg.HybridUseBase64, true)
			m["crs"] = crs
			return m
		}
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs}
	default: // hex
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs}
	}
}
