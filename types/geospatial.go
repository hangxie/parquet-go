package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"math"
)

// GeospatialJSONMode controls how GEOMETRY/GEOGRAPHY values are rendered to JSON
type GeospatialJSONMode int

const (
	GeospatialModeHex     GeospatialJSONMode = iota // wkb_hex (+ crs/algorithm)
	GeospatialModeBase64                            // wkb_b64 (+ crs/algorithm)
	GeospatialModeGeoJSON                           // GeoJSON geometry (fallback to hex on parse failure)
	GeospatialModeHybrid                            // both: {geojson:..., wkb_hex/base64:..., crs, algorithm}
)

var (
	// Defaults: GEOGRAPHY -> GeoJSON, GEOMETRY -> Hex
	geographyJSONMode = GeospatialModeGeoJSON
	geometryJSONMode  = GeospatialModeHex

	// Optional reprojection hook used when emitting GeoJSON and CRS != OGC:CRS84
	geospatialReprojector GeospatialReprojector

	// Controls raw encoding used in Hybrid mode: false => hex, true => base64
	geospatialHybridUseBase64 bool

	// Controls whether GeoJSON mode emits a Feature wrapper
	// true => {type: Feature, geometry: {...}, properties: {...}}
	// false => emit the geometry object directly
	geospatialGeoJSONAsFeature = true

	// Optional coordinate rounding precision for GeoJSON output.
	// Default is 6 decimal places. Set to -1 to disable rounding.
	// See RFC 7946 §11.2 for discussion on coordinate precision:
	// https://datatracker.ietf.org/doc/html/rfc7946#section-11.2
	geospatialCoordPrecision = 6
)

func SetGeographyJSONMode(m GeospatialJSONMode) { geographyJSONMode = m }
func SetGeometryJSONMode(m GeospatialJSONMode)  { geometryJSONMode = m }

// GeospatialReprojector transforms a GeoJSON geometry from an input CRS to CRS84 (lon/lat degrees)
// Return (geojson, true) if reprojection applied; (nil, false) to indicate failure or not supported.
type GeospatialReprojector func(crs string, geojson map[string]any) (map[string]any, bool)

// SetGeospatialReprojector registers a reprojection function. Pass nil to disable.
func SetGeospatialReprojector(r GeospatialReprojector) { geospatialReprojector = r }

// SetGeospatialHybridRawBase64 selects base64 (true) or hex (false) for raw WKB in Hybrid mode
func SetGeospatialHybridRawBase64(useBase64 bool) { geospatialHybridUseBase64 = useBase64 }

// SetGeospatialGeoJSONAsFeature toggles whether GeoJSON mode emits Feature objects
func SetGeospatialGeoJSONAsFeature(asFeature bool) { geospatialGeoJSONAsFeature = asFeature }

// SetGeospatialCoordinatePrecision sets decimal places to round coordinates for GeoJSON.
// Default is 6. Use -1 to disable rounding.
func SetGeospatialCoordinatePrecision(precision int) { geospatialCoordPrecision = precision }

// wkbToGeoJSON converts a subset of WKB (2D Point/LineString/Polygon) to a GeoJSON geometry map
// Returns (geoJSON, true) on success; (nil, false) on failure
func wkbToGeoJSON(b []byte) (map[string]any, bool) {
	if len(b) < 5 {
		return nil, false
	}
	order := b[0] // 0 = BigEndian, 1 = LittleEndian
	be := order == 0
	u32 := func(off int) (uint32, bool) {
		if off+4 > len(b) {
			return 0, false
		}
		if be {
			return binary.BigEndian.Uint32(b[off : off+4]), true
		}
		return binary.LittleEndian.Uint32(b[off : off+4]), true
	}
	f64 := func(off int) (float64, bool) {
		if off+8 > len(b) {
			return 0, false
		}
		var u uint64
		if be {
			u = binary.BigEndian.Uint64(b[off : off+8])
		} else {
			u = binary.LittleEndian.Uint64(b[off : off+8])
		}
		return math.Float64frombits(u), true
	}

	gType, ok := u32(1)
	if !ok {
		return nil, false
	}
	off := 5

	round := func(v float64) float64 {
		if geospatialCoordPrecision < 0 {
			return v
		}
		// pow10 limited to reasonable range
		p := geospatialCoordPrecision
		if p > 12 {
			p = 12
		}
		if p < 0 {
			p = 0
		}
		pow := math.Pow(10, float64(p))
		return math.Round(v*pow) / pow
	}

	switch gType {
	case 1: // Point
		x, ok1 := f64(off)
		y, ok2 := f64(off + 8)
		if !ok1 || !ok2 {
			return nil, false
		}
		return map[string]any{
			"type":        "Point",
			"coordinates": []float64{round(x), round(y)},
		}, true
	case 2: // LineString
		n, ok := u32(off)
		if !ok {
			return nil, false
		}
		off += 4
		coords := make([][]float64, 0, n)
		for i := uint32(0); i < n; i++ {
			x, ok1 := f64(off)
			y, ok2 := f64(off + 8)
			if !ok1 || !ok2 {
				return nil, false
			}
			coords = append(coords, []float64{round(x), round(y)})
			off += 16
		}
		return map[string]any{"type": "LineString", "coordinates": coords}, true
	case 3: // Polygon
		ringsN, ok := u32(off)
		if !ok {
			return nil, false
		}
		off += 4
		rings := make([][][]float64, 0, ringsN)
		for r := uint32(0); r < ringsN; r++ {
			n, ok := u32(off)
			if !ok {
				return nil, false
			}
			off += 4
			ring := make([][]float64, 0, n)
			for i := uint32(0); i < n; i++ {
				x, ok1 := f64(off)
				y, ok2 := f64(off + 8)
				if !ok1 || !ok2 {
					return nil, false
				}
				ring = append(ring, []float64{round(x), round(y)})
				off += 16
			}
			rings = append(rings, ring)
		}
		return map[string]any{"type": "Polygon", "coordinates": rings}, true
	default:
		return nil, false
	}
}

func wrapGeoJSONHybrid(geo map[string]any, raw []byte, useBase64, include bool) map[string]any {
	out := map[string]any{"geojson": geo}
	if include {
		if useBase64 {
			out["wkb_b64"] = base64.StdEncoding.EncodeToString(raw)
		} else {
			out["wkb_hex"] = hex.EncodeToString(raw)
		}
	}
	return out
}

func makeGeoJSONFeature(geo, props map[string]any) map[string]any {
	return map[string]any{
		"type":       "Feature",
		"geometry":   geo,
		"properties": props,
	}
}
