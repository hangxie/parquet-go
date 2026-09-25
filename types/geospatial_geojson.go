package types

import (
	"encoding/base64"
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
