package types

import (
	"encoding/base64"
	"encoding/hex"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ConvertGeographyLogicalValue converts WKB bytes to a JSON-friendly wrapper with hex, CRS and algorithm.
func ConvertGeographyLogicalValue(val any, geo *parquet.GeographyType, cfg *GeospatialConfig) any {
	rendered, _ := convertGeographyValue(val, geo, cfg)
	return rendered
}

// convertGeographyValue renders a GEOGRAPHY, reporting bytes the requested rendering cannot
// be produced from. GeoJSON is the default here, so that is the usual path.
func convertGeographyValue(val any, geo *parquet.GeographyType, cfg *GeospatialConfig) (any, error) {
	if val == nil {
		return nil, nil
	}
	b, ok := valueBytes(val)
	if !ok {
		return val, errUnrenderable("GEOGRAPHY", "value is %T, not bytes", val)
	}
	crs := "OGC:CRS84"
	if geo != nil && geo.CRS != nil && *geo.CRS != "" {
		crs = *geo.CRS
	}
	algo := "SPHERICAL"
	if geo != nil && geo.Algorithm != nil {
		algo = geo.Algorithm.String()
	}
	// Built only where it is returned: hex-encoding every value would cost twice the
	// WKB in bytes on the GeoJSON and base64 paths, which never use it.
	hexForm := func() map[string]any {
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs, "algorithm": algo}
	}

	reproject := func(gj map[string]any) map[string]any {
		if crs != "OGC:CRS84" && cfg.Reprojector != nil {
			if rj, ok := cfg.Reprojector(crs, gj); ok {
				return rj
			}
		}
		return gj
	}

	switch cfg.GeographyJSONMode {
	case GeospatialModeGeoJSON:
		gj, ok := wkbToGeoJSON(b, cfg.CoordPrecision)
		if !ok {
			return hexForm(), errNotWKB("GEOGRAPHY")
		}
		gj = reproject(gj)
		if cfg.GeoJSONAsFeature {
			return makeGeoJSONFeature(gj, map[string]any{"crs": crs, "algorithm": algo}), nil
		}
		return gj, nil
	case GeospatialModeBase64:
		return map[string]any{"wkb_b64": base64.StdEncoding.EncodeToString(b), "crs": crs, "algorithm": algo}, nil
	case GeospatialModeHybrid:
		gj, ok := wkbToGeoJSON(b, cfg.CoordPrecision)
		if !ok {
			return hexForm(), errNotWKB("GEOGRAPHY")
		}
		m := wrapGeoJSONHybrid(reproject(gj), b, cfg.HybridUseBase64, true)
		m["crs"], m["algorithm"] = crs, algo
		return m, nil
	default: // hex
		return hexForm(), nil
	}
}
