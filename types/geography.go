package types

import (
	"encoding/base64"
	"encoding/hex"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ConvertGeographyLogicalValue converts WKB bytes to a JSON-friendly wrapper with hex, CRS and algorithm.
func ConvertGeographyLogicalValue(val any, geo *parquet.GeographyType, cfg *GeospatialConfig) any {
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
	if geo != nil && geo.CRS != nil && *geo.CRS != "" {
		crs = *geo.CRS
	}
	algo := "SPHERICAL"
	if geo != nil && geo.Algorithm != nil {
		algo = geo.Algorithm.String()
	}
	switch cfg.GeographyJSONMode {
	case GeospatialModeGeoJSON:
		if gj, ok := wkbToGeoJSON(b, cfg.CoordPrecision); ok {
			if crs != "OGC:CRS84" && cfg.Reprojector != nil {
				if rj, ok2 := cfg.Reprojector(crs, gj); ok2 {
					gj = rj
				}
			}
			if cfg.GeoJSONAsFeature {
				return makeGeoJSONFeature(gj, map[string]any{"crs": crs, "algorithm": algo})
			}
			return gj
		}
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs, "algorithm": algo}
	case GeospatialModeBase64:
		return map[string]any{"wkb_b64": base64.StdEncoding.EncodeToString(b), "crs": crs, "algorithm": algo}
	case GeospatialModeHybrid:
		if gj, ok := wkbToGeoJSON(b, cfg.CoordPrecision); ok {
			if crs != "OGC:CRS84" && cfg.Reprojector != nil {
				if rj, ok2 := cfg.Reprojector(crs, gj); ok2 {
					gj = rj
				}
			}
			m := wrapGeoJSONHybrid(gj, b, cfg.HybridUseBase64, true)
			m["crs"], m["algorithm"] = crs, algo
			return m
		}
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs, "algorithm": algo}
	default:
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs, "algorithm": algo}
	}
}
