package types

import (
	"encoding/binary"
	"fmt"
	"math"
)

// geoCoord reads one ordinate, in any of the shapes a decoded JSON number arrives as.
func geoCoord(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	// json.Number under UseNumber, which JSONWriter sets.
	if s, ok := v.(interface{ Float64() (float64, error) }); ok {
		f, err := s.Float64()
		return f, err == nil
	}
	return 0, false
}

// geoSlice accepts decoded JSON arrays and the renderer's native slice types.
func geoSlice(v any) ([]any, bool) {
	switch s := v.(type) {
	case []any:
		return s, true
	case []float64:
		return geoSliceValues(s), true
	case [][]float64:
		return geoSliceValues(s), true
	case [][][]float64:
		return geoSliceValues(s), true
	case [][][][]float64:
		return geoSliceValues(s), true
	case []map[string]any:
		return geoSliceValues(s), true
	default:
		return nil, false
	}
}

// geoSliceValues boxes one level of a typed slice for the shared geometry encoder.
func geoSliceValues[T any](s []T) []any {
	values := make([]any, len(s))
	for i, v := range s {
		values[i] = v
	}
	return values
}

// putPoint appends one 2D coordinate pair.
func putPoint(b []byte, v any) ([]byte, error) {
	pt, ok := geoSlice(v)
	if !ok {
		return nil, fmt.Errorf("position is %T, not an array", v)
	}
	if len(pt) < 2 {
		return nil, fmt.Errorf("position has %d ordinates, need 2", len(pt))
	}
	if len(pt) > 2 {
		// Refused rather than dropped: #439 is the issue that would let a third
		// ordinate through, and silently discarding elevation is the worse answer.
		return nil, fmt.Errorf("position has %d ordinates, only 2D is supported", len(pt))
	}
	for _, o := range pt {
		f, ok := geoCoord(o)
		if !ok {
			return nil, fmt.Errorf("ordinate is %T, not a number", o)
		}
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(f))
	}
	return b, nil
}

// putPoints appends a counted run of coordinate pairs.
func putPoints(b []byte, v any) ([]byte, error) {
	pts, ok := geoSlice(v)
	if !ok {
		return nil, fmt.Errorf("coordinates are %T, not an array", v)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(pts)))
	for _, p := range pts {
		var err error
		if b, err = putPoint(b, p); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// putRings appends a counted run of linear rings, which is a polygon's body.
func putRings(b []byte, v any) ([]byte, error) {
	rings, ok := geoSlice(v)
	if !ok {
		return nil, fmt.Errorf("coordinates are %T, not an array", v)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(rings)))
	for _, r := range rings {
		var err error
		if b, err = putPoints(b, r); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// putHeader appends a little-endian WKB header.
func putHeader(b []byte, gType uint32) []byte {
	b = append(b, 0x01)
	return binary.LittleEndian.AppendUint32(b, gType)
}

// geoJSONToWKB encodes a GeoJSON geometry as little-endian WKB, inverting wkbToGeoJSON
// for the seven basic 2D geometries.
func geoJSONToWKB(gj map[string]any, depth int) ([]byte, error) {
	gTypeName, _ := gj["type"].(string)
	gType, ok := map[string]uint32{
		"Point": WKBPoint, "LineString": WKBLineString, "Polygon": WKBPolygon,
		"MultiPoint": WKBMultiPoint, "MultiLineString": WKBMultiLineString,
		"MultiPolygon": WKBMultiPolygon, "GeometryCollection": WKBGeometryCollection,
	}[gTypeName]
	if !ok {
		return nil, fmt.Errorf("geometry type %q is not one this library writes", gTypeName)
	}

	b := putHeader(nil, gType)
	if gType == WKBGeometryCollection {
		if depth >= maxGeometryDepth {
			return nil, fmt.Errorf("GeometryCollection nests deeper than %d levels", maxGeometryDepth)
		}
		return putCollection(b, gj, depth)
	}
	coords, present := gj["coordinates"]
	if !present {
		return nil, fmt.Errorf("%s has no coordinates", gTypeName)
	}

	switch gType {
	case WKBPoint:
		return putPoint(b, coords)
	case WKBLineString:
		return putPoints(b, coords)
	case WKBPolygon:
		return putRings(b, coords)
	}
	return putMulti(b, gType, coords)
}

// putMulti appends the members of a Multi* geometry, each a WKB value of its own.
func putMulti(b []byte, gType uint32, coords any) ([]byte, error) {
	members, ok := geoSlice(coords)
	if !ok {
		return nil, fmt.Errorf("coordinates are %T, not an array", coords)
	}
	memberType := map[uint32]uint32{
		WKBMultiPoint: WKBPoint, WKBMultiLineString: WKBLineString, WKBMultiPolygon: WKBPolygon,
	}[gType]
	b = binary.LittleEndian.AppendUint32(b, uint32(len(members)))
	for _, m := range members {
		b = putHeader(b, memberType)
		var err error
		switch memberType {
		case WKBPoint:
			b, err = putPoint(b, m)
		case WKBLineString:
			b, err = putPoints(b, m)
		default:
			b, err = putRings(b, m)
		}
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// putCollection appends a GeometryCollection's members, each a whole geometry.
func putCollection(b []byte, gj map[string]any, depth int) ([]byte, error) {
	members, ok := geoSlice(gj["geometries"])
	if !ok {
		return nil, fmt.Errorf("GeometryCollection has no geometries array")
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(members)))
	for _, m := range members {
		sub, ok := m.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("collection member is %T, not a geometry", m)
		}
		enc, err := geoJSONToWKB(sub, depth+1)
		if err != nil {
			return nil, err
		}
		b = append(b, enc...)
	}
	return b, nil
}
