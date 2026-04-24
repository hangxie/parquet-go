package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"math"
)

// roundCoordinate rounds coordinates based on precision setting.
// Precision is clamped to [0, 12]. Use precision < 0 to disable rounding.
func roundCoordinate(v float64, precision int) float64 {
	if precision < 0 {
		return v
	}
	if precision > maxCoordPrecision {
		precision = maxCoordPrecision
	}
	pow := math.Pow(10, float64(precision))
	return math.Round(v*pow) / pow
}

// wkbToGeoJSON converts WKB (2D Point/LineString/Polygon/Multi*) to a GeoJSON geometry map
// Returns (geoJSON, true) on success; (nil, false) on failure
// readSubGeomHeader reads a sub-geometry's byte order and type from the buffer, returning
// the sub-geometry's big-endian flag, the new offset, and whether it matched the expected type.
// outerBE is used for reading the type field to match original WKB parsing behavior.
func readSubGeomHeader(b []byte, off int, outerBE bool, expectedType uint32) (bool, int, bool) {
	if off >= len(b) {
		return false, 0, false
	}
	subBE := b[off] == 0
	off++
	gType, ok := u32(b, off, outerBE)
	if !ok || gType != expectedType {
		return false, 0, false
	}
	return subBE, off + 4, true
}

func multiPointToGeoJSON(b []byte, off int, be bool, precision int) (map[string]any, bool) {
	n, ok := u32(b, off, be)
	if !ok {
		return nil, false
	}
	off += 4
	coords := make([][]float64, 0, n)
	for i := uint32(0); i < n; i++ {
		pointBE, newOff, ok := readSubGeomHeader(b, off, be, WKBPoint)
		if !ok {
			return nil, false
		}
		pt, ptOff, ok := parsePoint(b, pointBE, newOff, precision)
		if !ok {
			return nil, false
		}
		coords = append(coords, pt)
		off = ptOff
	}
	return map[string]any{"type": "MultiPoint", "coordinates": coords}, true
}

func multiLineStringToGeoJSON(b []byte, off int, be bool, precision int) (map[string]any, bool) {
	n, ok := u32(b, off, be)
	if !ok {
		return nil, false
	}
	off += 4
	lines := make([][][]float64, 0, n)
	for i := uint32(0); i < n; i++ {
		lineBE, newOff, ok := readSubGeomHeader(b, off, be, WKBLineString)
		if !ok {
			return nil, false
		}
		coords, lineOff, ok := parseLineString(b, lineBE, newOff, precision)
		if !ok {
			return nil, false
		}
		lines = append(lines, coords)
		off = lineOff
	}
	return map[string]any{"type": "MultiLineString", "coordinates": lines}, true
}

func multiPolygonToGeoJSON(b []byte, off int, be bool, precision int) (map[string]any, bool) {
	n, ok := u32(b, off, be)
	if !ok {
		return nil, false
	}
	off += 4
	polygons := make([][][][]float64, 0, n)
	for i := uint32(0); i < n; i++ {
		polyBE, newOff, ok := readSubGeomHeader(b, off, be, WKBPolygon)
		if !ok {
			return nil, false
		}
		rings, polyOff, ok := parsePolygon(b, polyBE, newOff, precision)
		if !ok {
			return nil, false
		}
		polygons = append(polygons, rings)
		off = polyOff
	}
	return map[string]any{"type": "MultiPolygon", "coordinates": polygons}, true
}

func geometryCollectionToGeoJSON(b []byte, off int, be bool, precision int) (map[string]any, bool) {
	n, ok := u32(b, off, be)
	if !ok {
		return nil, false
	}
	off += 4
	geometries := make([]map[string]any, 0, n)
	for i := uint32(0); i < n; i++ {
		geomSize, ok := calculateWKBSize(b[off:])
		if !ok {
			return nil, false
		}
		geomEnd := off + geomSize
		if geomEnd > len(b) {
			return nil, false
		}
		subGeom, ok := wkbToGeoJSON(b[off:geomEnd], precision)
		if !ok {
			return nil, false
		}
		geometries = append(geometries, subGeom)
		off = geomEnd
	}
	return map[string]any{"type": "GeometryCollection", "geometries": geometries}, true
}

func wkbToGeoJSON(b []byte, precision int) (map[string]any, bool) {
	if len(b) < 5 {
		return nil, false
	}
	be := b[0] == 0

	gType, ok := u32(b, 1, be)
	if !ok {
		return nil, false
	}
	off := 5

	switch gType % 1000 {
	case WKBPoint:
		coords, _, ok := parsePoint(b, be, off, precision)
		if !ok {
			return nil, false
		}
		return map[string]any{"type": "Point", "coordinates": coords}, true
	case WKBLineString:
		coords, _, ok := parseLineString(b, be, off, precision)
		if !ok {
			return nil, false
		}
		return map[string]any{"type": "LineString", "coordinates": coords}, true
	case WKBPolygon:
		coords, _, ok := parsePolygon(b, be, off, precision)
		if !ok {
			return nil, false
		}
		return map[string]any{"type": "Polygon", "coordinates": coords}, true
	case WKBMultiPoint:
		return multiPointToGeoJSON(b, off, be, precision)
	case WKBMultiLineString:
		return multiLineStringToGeoJSON(b, off, be, precision)
	case WKBMultiPolygon:
		return multiPolygonToGeoJSON(b, off, be, precision)
	case WKBGeometryCollection:
		return geometryCollectionToGeoJSON(b, off, be, precision)
	default:
		return nil, false
	}
}

// parsePoint parses a WKB Point and returns the coordinates and bytes consumed
func parsePoint(b []byte, be bool, off, precision int) ([]float64, int, bool) {
	if off+16 > len(b) {
		return nil, 0, false
	}
	var x, y float64
	if be {
		x = math.Float64frombits(binary.BigEndian.Uint64(b[off : off+8]))
		y = math.Float64frombits(binary.BigEndian.Uint64(b[off+8 : off+16]))
	} else {
		x = math.Float64frombits(binary.LittleEndian.Uint64(b[off : off+8]))
		y = math.Float64frombits(binary.LittleEndian.Uint64(b[off+8 : off+16]))
	}
	return []float64{roundCoordinate(x, precision), roundCoordinate(y, precision)}, off + 16, true
}

// parseLineString parses a WKB LineString and returns the coordinates and bytes consumed
func parseLineString(b []byte, be bool, off, precision int) ([][]float64, int, bool) {
	if off+4 > len(b) {
		return nil, 0, false
	}
	var numPoints uint32
	if be {
		numPoints = binary.BigEndian.Uint32(b[off : off+4])
	} else {
		numPoints = binary.LittleEndian.Uint32(b[off : off+4])
	}
	off += 4

	if off+int(numPoints)*16 > len(b) {
		return nil, 0, false
	}

	coords := make([][]float64, 0, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		point, newOff, ok := parsePoint(b, be, off, precision)
		if !ok {
			return nil, 0, false
		}
		coords = append(coords, point)
		off = newOff
	}
	return coords, off, true
}

// parsePolygon parses a WKB Polygon and returns the coordinates and bytes consumed
func parsePolygon(b []byte, be bool, off, precision int) ([][][]float64, int, bool) {
	if off+4 > len(b) {
		return nil, 0, false
	}
	var numRings uint32
	if be {
		numRings = binary.BigEndian.Uint32(b[off : off+4])
	} else {
		numRings = binary.LittleEndian.Uint32(b[off : off+4])
	}
	off += 4

	rings := make([][][]float64, 0, numRings)
	for r := uint32(0); r < numRings; r++ {
		if off+4 > len(b) {
			return nil, 0, false
		}
		var numPoints uint32
		if be {
			numPoints = binary.BigEndian.Uint32(b[off : off+4])
		} else {
			numPoints = binary.LittleEndian.Uint32(b[off : off+4])
		}
		off += 4

		if off+int(numPoints)*16 > len(b) {
			return nil, 0, false
		}

		ring := make([][]float64, 0, numPoints)
		for i := uint32(0); i < numPoints; i++ {
			point, newOff, ok := parsePoint(b, be, off, precision)
			if !ok {
				return nil, 0, false
			}
			ring = append(ring, point)
			off = newOff
		}
		rings = append(rings, ring)
	}
	return rings, off, true
}

// calculateWKBSize determines the total byte size of a WKB geometry
func calcMultiLineStringSize(b []byte, off int, be bool) (int, bool) {
	numLines, ok := u32(b, off, be)
	if !ok {
		return 0, false
	}
	off += 4

	for l := uint32(0); l < numLines; l++ {
		if off+1+4+4 > len(b) {
			return 0, false
		}
		lineBE := b[off] == 0
		off += 1 + 4 // skip byte order + type
		linePoints, ok := u32(b, off, lineBE)
		if !ok {
			return 0, false
		}
		off += 4 + int(linePoints)*16
	}
	return off, true
}

func calcMultiPolygonSize(b []byte, off int, be bool) (int, bool) {
	numPolys, ok := u32(b, off, be)
	if !ok {
		return 0, false
	}
	off += 4

	for p := uint32(0); p < numPolys; p++ {
		if off+1+4+4 > len(b) {
			return 0, false
		}
		polyBE := b[off] == 0
		off += 1 + 4 // skip byte order + type
		numRings, ok := u32(b, off, polyBE)
		if !ok {
			return 0, false
		}
		off += 4

		for r := uint32(0); r < numRings; r++ {
			ringPoints, ok := u32(b, off, polyBE)
			if !ok {
				return 0, false
			}
			off += 4 + int(ringPoints)*16
		}
	}
	return off, true
}

func calcGeometryCollectionSize(b []byte, off int, be bool) (int, bool) {
	numGeoms, ok := u32(b, off, be)
	if !ok {
		return 0, false
	}
	off += 4

	for g := uint32(0); g < numGeoms; g++ {
		subSize, ok := calculateWKBSize(b[off:])
		if !ok {
			return 0, false
		}
		off += subSize
	}
	return off, true
}

func calculateWKBSize(b []byte) (int, bool) {
	if len(b) < 5 {
		return 0, false
	}

	be := b[0] == 0
	gType, _ := u32(b, 1, be)
	off := 5 // byte order + type

	// gType may contain extended wkbType (https://libgeos.org/specifications/wkb/#iso-wkb)
	switch gType % 1000 {
	case WKBPoint:
		_, newOff, ok := parsePoint(b, be, off, -1)
		if !ok {
			return 0, false
		}
		return newOff, true

	case WKBLineString:
		_, newOff, ok := parseLineString(b, be, off, -1)
		if !ok {
			return 0, false
		}
		return newOff, true

	case WKBPolygon:
		_, newOff, ok := parsePolygon(b, be, off, -1)
		if !ok {
			return 0, false
		}
		return newOff, true

	case WKBMultiPoint:
		numPoints, ok := u32(b, off, be)
		if !ok {
			return 0, false
		}
		return off + 4 + int(numPoints)*(1+4+16), true

	case WKBMultiLineString:
		return calcMultiLineStringSize(b, off, be)

	case WKBMultiPolygon:
		return calcMultiPolygonSize(b, off, be)

	case WKBGeometryCollection:
		return calcGeometryCollectionSize(b, off, be)

	default:
		return 0, false
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
