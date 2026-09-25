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

// putPoint appends one position, of the ordinate count its geometry declares.
func putPoint(b []byte, v any, ordinates int) ([]byte, error) {
	pt, ok := geoSlice(v)
	if !ok {
		return nil, fmt.Errorf("position is %T, not an array", v)
	}
	if len(pt) != ordinates {
		// One geometry carries one dimension, so a position of another length would be
		// written as ordinates of the neighbours around it.
		return nil, fmt.Errorf("position has %d ordinates, geometry declares %d", len(pt), ordinates)
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
func putPoints(b []byte, v any, ordinates int) ([]byte, error) {
	pts, ok := geoSlice(v)
	if !ok {
		return nil, fmt.Errorf("coordinates are %T, not an array", v)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(pts)))
	for _, p := range pts {
		var err error
		if b, err = putPoint(b, p, ordinates); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// putRings appends a counted run of linear rings, which is a polygon's body.
func putRings(b []byte, v any, ordinates int) ([]byte, error) {
	rings, ok := geoSlice(v)
	if !ok {
		return nil, fmt.Errorf("coordinates are %T, not an array", v)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(rings)))
	for _, r := range rings {
		var err error
		if b, err = putPoints(b, r, ordinates); err != nil {
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
	// The positions say which dimension the geometry is; zero asks it to decide for itself.
	return geoJSONToWKBAt(gj, depth, 0)
}

// geoJSONTypeCode reads the geometry type a GeoJSON object names.
func geoJSONTypeCode(gj map[string]any) (uint32, string, bool) {
	gTypeName, _ := gj["type"].(string)
	gType, ok := map[string]uint32{
		"Point": WKBPoint, "LineString": WKBLineString, "Polygon": WKBPolygon,
		"MultiPoint": WKBMultiPoint, "MultiLineString": WKBMultiLineString,
		"MultiPolygon": WKBMultiPolygon, "GeometryCollection": WKBGeometryCollection,
	}[gTypeName]
	return gType, gTypeName, ok
}

// geoJSONToWKBAt encodes a geometry at the given ordinate count, or at the one its own
// positions declare when that is zero.
func geoJSONToWKBAt(gj map[string]any, depth, ordinates int) ([]byte, error) {
	gType, gTypeName, ok := geoJSONTypeCode(gj)
	if !ok {
		return nil, fmt.Errorf("geometry type %q is not one this library writes", gTypeName)
	}

	if gType == WKBGeometryCollection {
		if depth >= maxGeometryDepth {
			return nil, fmt.Errorf("GeometryCollection nests deeper than %d levels", maxGeometryDepth)
		}
		return putCollection(gj, depth, ordinates)
	}
	coords, present := gj["coordinates"]
	if !present {
		return nil, fmt.Errorf("%s has no coordinates", gTypeName)
	}

	if ordinates == 0 {
		ordinates = geoJSONOrdinates(coords, geoJSONNesting[gType])
	}
	if ordinates == 0 {
		// Nothing to measure, and nothing to write: an empty geometry is two-dimensional.
		ordinates = 2
	}
	if ordinates != 2 && ordinates != 3 {
		return nil, fmt.Errorf("position has %d ordinates, need 2 or 3", ordinates)
	}
	if ordinates == 3 {
		gType += 1000
	}
	b := putHeader(nil, gType)

	switch gType % 1000 {
	case WKBPoint:
		return putPoint(b, coords, ordinates)
	case WKBLineString:
		return putPoints(b, coords, ordinates)
	case WKBPolygon:
		return putRings(b, coords, ordinates)
	}
	return putMulti(b, gType, coords, ordinates)
}

// geoJSONNesting is how many arrays sit between a geometry's coordinates and a position.
var geoJSONNesting = map[uint32]int{
	WKBPoint: 0, WKBLineString: 1, WKBPolygon: 2,
	WKBMultiPoint: 1, WKBMultiLineString: 2, WKBMultiPolygon: 3,
}

// geoJSONOrdinates samples the ordinate count of the geometry's first position, which names
// the dimension for all of it: WKB carries the dimension once, in the type code. It returns
// zero where there is no position to measure, and a position that disagrees with the one
// sampled is reported by putPoint.
func geoJSONOrdinates(v any, nesting int) int {
	items, ok := geoSlice(v)
	if !ok {
		return 0
	}
	if nesting == 0 {
		return len(items)
	}
	// An empty member measures nothing, so the first one carrying a position decides.
	for _, item := range items {
		if ordinates := geoJSONOrdinates(item, nesting-1); ordinates != 0 {
			return ordinates
		}
	}
	return 0
}

// putMulti appends the members of a Multi* geometry, each a WKB value of its own.
func putMulti(b []byte, gType uint32, coords any, ordinates int) ([]byte, error) {
	members, ok := geoSlice(coords)
	if !ok {
		return nil, fmt.Errorf("coordinates are %T, not an array", coords)
	}
	base := map[uint32]uint32{
		WKBMultiPoint: WKBPoint, WKBMultiLineString: WKBLineString, WKBMultiPolygon: WKBPolygon,
	}[gType%1000]
	// Members carry the container's dimension, which is what the readers require of them.
	memberType := wkbMemberType(gType, base)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(members)))
	for _, m := range members {
		b = putHeader(b, memberType)
		var err error
		switch base {
		case WKBPoint:
			b, err = putPoint(b, m, ordinates)
		case WKBLineString:
			b, err = putPoints(b, m, ordinates)
		default:
			b, err = putRings(b, m, ordinates)
		}
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// putCollection appends a GeometryCollection's members, each a whole geometry.
func putCollection(gj map[string]any, depth, ordinates int) ([]byte, error) {
	members, ok := geoSlice(gj["geometries"])
	if !ok {
		return nil, fmt.Errorf("GeometryCollection has no geometries array")
	}
	// One dimension covers a collection and every member of it, which is what the readers
	// require, so it is settled before any member is written: an empty member carries no
	// position to decide it and takes the one its neighbours declare.
	if ordinates == 0 {
		ordinates = geoJSONGeometryOrdinates(gj)
	}
	if ordinates == 0 {
		ordinates = 2
	}

	b := putHeader(nil, WKBGeometryCollection+uint32(ordinates-2)*1000)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(members)))
	for _, m := range members {
		sub, ok := m.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("collection member is %T, not a geometry", m)
		}
		enc, err := geoJSONToWKBAt(sub, depth+1, ordinates)
		if err != nil {
			return nil, err
		}
		b = append(b, enc...)
	}
	return b, nil
}

// geoJSONGeometryOrdinates samples one geometry's positions, descending into a collection's
// members until one carries a position. It returns zero where none does.
func geoJSONGeometryOrdinates(gj map[string]any) int {
	gType, _, ok := geoJSONTypeCode(gj)
	if !ok {
		return 0
	}
	if gType != WKBGeometryCollection {
		return geoJSONOrdinates(gj["coordinates"], geoJSONNesting[gType])
	}
	members, _ := geoSlice(gj["geometries"])
	for _, m := range members {
		sub, ok := m.(map[string]any)
		if !ok {
			continue
		}
		if ordinates := geoJSONGeometryOrdinates(sub); ordinates != 0 {
			return ordinates
		}
	}
	return 0
}
