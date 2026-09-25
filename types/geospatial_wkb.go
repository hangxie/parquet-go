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

// WKBGeometryType reads the geometry type a WKB value declares, reporting false when the
// bytes do not open with a header the format defines.
func WKBGeometryType(b []byte) (int32, bool) {
	gType, _, ok := readWKBHeader(b)
	if !ok {
		return 0, false
	}
	return int32(gType), true
}

// readWKBHeader reads the byte order and geometry type a WKB value opens with.
func readWKBHeader(b []byte) (gType uint32, bigEndian, ok bool) {
	// Refused here is any header the format does not define: a byte order other than 0 or 1,
	// a base type outside the standardized code space, or a dimension beyond ZM. Bytes that
	// are not WKB read as a type number all the same, so every reader checks this before
	// trusting what follows.
	if len(b) < 5 {
		return 0, false, false
	}
	switch b[0] {
	case 0:
		bigEndian = true
	case 1:
	default:
		return 0, false, false
	}
	gType, _ = u32(b, 1, bigEndian)
	base := gType % 1000
	if base < 1 || base > wkbMaxGeometryType || gType-base > 3000 {
		return 0, false, false
	}
	return gType, bigEndian, true
}

// wkbHasZ reports whether the type declares an elevation, which is the third ordinate.
func wkbHasZ(gType uint32) bool {
	dimension := gType / 1000
	return dimension == 1 || dimension == 3
}

// wkbHasM reports whether the type declares a measure, which GeoJSON has no place for.
func wkbHasM(gType uint32) bool {
	dimension := gType / 1000
	return dimension == 2 || dimension == 3
}

// wkbMemberType names the member a container of this type holds, at the same dimension.
func wkbMemberType(gType, base uint32) uint32 {
	return gType - gType%1000 + base
}

// readSubGeomHeader reads a sub-geometry's own byte order and type from the buffer.
func readSubGeomHeader(b []byte, off int, expectedType uint32) (bool, int, bool) {
	// Each member of a Multi* or collection carries its own byte order byte, so the type
	// after it is read with that byte order, not the outer geometry's. Reading it with the
	// outer one turned a legal mixed-endianness value into a parse failure.
	if off >= len(b) {
		return false, 0, false
	}
	var subBE bool
	switch b[off] {
	case 0:
		subBE = true
	case 1:
	default:
		return false, 0, false
	}
	off++
	gType, ok := u32(b, off, subBE)
	if !ok || gType != expectedType {
		return false, 0, false
	}
	return subBE, off + 4, true
}

// cappedCap returns min(int(n), remaining) to prevent huge pre-allocations from
// untrusted count fields when the available bytes can't possibly hold that many elements.
func cappedCap(n uint32, remaining int) int {
	if remaining < 0 {
		remaining = 0
	}
	if int(n) > remaining {
		return remaining
	}
	return int(n)
}

func multiPointToGeoJSON(b []byte, off int, be bool, precision int, gType uint32) (map[string]any, bool) {
	n, ok := u32(b, off, be)
	if !ok {
		return nil, false
	}
	off += 4
	coords := make([][]float64, 0, cappedCap(n, len(b)-off))
	for i := uint32(0); i < n; i++ {
		pointBE, newOff, ok := readSubGeomHeader(b, off, wkbMemberType(gType, WKBPoint))
		if !ok {
			return nil, false
		}
		pt, ptOff, ok := parsePoint(b, pointBE, newOff, precision, gType)
		if !ok {
			return nil, false
		}
		coords = append(coords, pt)
		off = ptOff
	}
	return map[string]any{"type": "MultiPoint", "coordinates": coords}, true
}

func multiLineStringToGeoJSON(b []byte, off int, be bool, precision int, gType uint32) (map[string]any, bool) {
	n, ok := u32(b, off, be)
	if !ok {
		return nil, false
	}
	off += 4
	lines := make([][][]float64, 0, cappedCap(n, len(b)-off))
	for i := uint32(0); i < n; i++ {
		lineBE, newOff, ok := readSubGeomHeader(b, off, wkbMemberType(gType, WKBLineString))
		if !ok {
			return nil, false
		}
		coords, lineOff, ok := parseLineString(b, lineBE, newOff, precision, gType)
		if !ok {
			return nil, false
		}
		lines = append(lines, coords)
		off = lineOff
	}
	return map[string]any{"type": "MultiLineString", "coordinates": lines}, true
}

func multiPolygonToGeoJSON(b []byte, off int, be bool, precision int, gType uint32) (map[string]any, bool) {
	n, ok := u32(b, off, be)
	if !ok {
		return nil, false
	}
	off += 4
	polygons := make([][][][]float64, 0, cappedCap(n, len(b)-off))
	for i := uint32(0); i < n; i++ {
		polyBE, newOff, ok := readSubGeomHeader(b, off, wkbMemberType(gType, WKBPolygon))
		if !ok {
			return nil, false
		}
		rings, polyOff, ok := parsePolygon(b, polyBE, newOff, precision, gType)
		if !ok {
			return nil, false
		}
		polygons = append(polygons, rings)
		off = polyOff
	}
	return map[string]any{"type": "MultiPolygon", "coordinates": polygons}, true
}

func geometryCollectionToGeoJSON(b []byte, off int, be bool, precision, depth int, gType uint32) (map[string]any, bool) {
	n, ok := u32(b, off, be)
	if !ok {
		return nil, false
	}
	off += 4
	geometries := make([]map[string]any, 0, cappedCap(n, len(b)-off))
	for i := uint32(0); i < n; i++ {
		// One dimension covers a geometry and every part of it, which the size walk and
		// the write path already require of a collection's members.
		if memberType, _, ok := readWKBHeader(b[off:]); !ok || memberType/1000 != gType/1000 {
			return nil, false
		}
		geomSize, ok := calculateWKBSize(b[off:])
		if !ok {
			return nil, false
		}
		geomEnd := off + geomSize
		if geomEnd > len(b) {
			return nil, false
		}
		subGeom, ok := wkbToGeoJSONAt(b[off:geomEnd], precision, depth+1)
		if !ok {
			return nil, false
		}
		geometries = append(geometries, subGeom)
		off = geomEnd
	}
	return map[string]any{"type": "GeometryCollection", "geometries": geometries}, true
}

// wkbToGeoJSON converts WKB (Point, LineString, Polygon, Multi*, GeometryCollection), in
// two dimensions or Z, to a GeoJSON geometry map. It returns false for bytes it cannot
// read and for the dimensions a position cannot carry, which are M and ZM.
func wkbToGeoJSON(b []byte, precision int) (map[string]any, bool) {
	geo, ok := wkbToGeoJSONAt(b, precision, 0)
	if !ok {
		return nil, false
	}
	// A Z geometry carrying no position renders exactly as an empty 2D one, so the
	// rendering would write back as 2D and drop the dimension the value declares.
	if gType, _, headerOK := readWKBHeader(b); headerOK && wkbHasZ(gType) && geoJSONGeometryOrdinates(geo) == 0 {
		return nil, false
	}
	return geo, true
}

func wkbToGeoJSONAt(b []byte, precision, depth int) (map[string]any, bool) {
	if len(b) < 5 {
		return nil, false
	}
	gType, be, ok := readWKBHeader(b)
	// A measure has no place in a GeoJSON position, so M and ZM keep their hex substitute
	// where Z renders as an elevation.
	if !ok || wkbHasM(gType) {
		return nil, false
	}
	off := 5

	switch gType % 1000 {
	case WKBPoint:
		coords, _, ok := parsePoint(b, be, off, precision, gType)
		if !ok {
			return nil, false
		}
		return map[string]any{"type": "Point", "coordinates": coords}, true
	case WKBLineString:
		coords, _, ok := parseLineString(b, be, off, precision, gType)
		if !ok {
			return nil, false
		}
		return map[string]any{"type": "LineString", "coordinates": coords}, true
	case WKBPolygon:
		coords, _, ok := parsePolygon(b, be, off, precision, gType)
		if !ok {
			return nil, false
		}
		return map[string]any{"type": "Polygon", "coordinates": coords}, true
	case WKBMultiPoint:
		return multiPointToGeoJSON(b, off, be, precision, gType)
	case WKBMultiLineString:
		return multiLineStringToGeoJSON(b, off, be, precision, gType)
	case WKBMultiPolygon:
		return multiPolygonToGeoJSON(b, off, be, precision, gType)
	case WKBGeometryCollection:
		if depth >= maxGeometryDepth {
			return nil, false
		}
		return geometryCollectionToGeoJSON(b, off, be, precision, depth, gType)
	default:
		return nil, false
	}
}

// parsePoint parses a WKB Point and returns the coordinates and bytes consumed
func parsePoint(b []byte, be bool, off, precision int, gType uint32) ([]float64, int, bool) {
	ordinates := wkbOrdinates(gType)
	if off+ordinates*8 > len(b) {
		return nil, 0, false
	}
	read := func(at int) float64 {
		if be {
			return math.Float64frombits(binary.BigEndian.Uint64(b[at : at+8]))
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(b[at : at+8]))
	}
	// A position carries x, y and an elevation; a measure stays in the bytes.
	position := []float64{roundCoordinate(read(off), precision), roundCoordinate(read(off+8), precision)}
	if wkbHasZ(gType) {
		position = append(position, roundCoordinate(read(off+16), precision))
	}
	return position, off + ordinates*8, true
}

// parseLineString parses a WKB LineString and returns the coordinates and bytes consumed
func parseLineString(b []byte, be bool, off, precision int, gType uint32) ([][]float64, int, bool) {
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

	if off+int(numPoints)*wkbOrdinates(gType)*8 > len(b) {
		return nil, 0, false
	}

	coords := make([][]float64, 0, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		point, newOff, ok := parsePoint(b, be, off, precision, gType)
		if !ok {
			return nil, 0, false
		}
		coords = append(coords, point)
		off = newOff
	}
	return coords, off, true
}

// parsePolygon parses a WKB Polygon and returns the coordinates and bytes consumed
func parsePolygon(b []byte, be bool, off, precision int, gType uint32) ([][][]float64, int, bool) {
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

	rings := make([][][]float64, 0, cappedCap(numRings, len(b)-off))
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

		if off+int(numPoints)*wkbOrdinates(gType)*8 > len(b) {
			return nil, 0, false
		}

		ring := make([][]float64, 0, numPoints)
		for i := uint32(0); i < numPoints; i++ {
			point, newOff, ok := parsePoint(b, be, off, precision, gType)
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
func calculateWKBSize(b []byte) (int, bool) {
	// wkbEnd reads each member's own header and checks its type and dimension, where this
	// walk assumed a fixed member size for MultiPoint and stepped over the other Multi*
	// member headers unread.
	end, _, state := wkbEnd(b, 0, 0)
	// An opaque body has no measurable structure at all.
	if state != wkbMeasured {
		return 0, false
	}
	return end, true
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
