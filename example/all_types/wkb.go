//go:build example

package main

import (
	"encoding/binary"
	"math"
)

// wkbPoint builds a little-endian WKB for 2D Point(x,y)
func wkbPoint(x, y float64) string {
	buf := make([]byte, 0, 1+4+16)
	// little-endian byte order marker
	buf = append(buf, 1)
	// geometry type = 1 (Point)
	tmp4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp4, 1)
	buf = append(buf, tmp4...)
	// coordinates
	tmp8 := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp8, math.Float64bits(x))
	buf = append(buf, tmp8...)
	binary.LittleEndian.PutUint64(tmp8, math.Float64bits(y))
	buf = append(buf, tmp8...)
	return string(buf)
}

// wkbLineString builds a little-endian WKB for 2D LineString
func wkbLineString(coords [][2]float64) string {
	buf := make([]byte, 0, 1+4+4+len(coords)*16)
	buf = append(buf, 1) // LE
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 2)
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(coords)))
	buf = append(buf, n...)
	tmp := make([]byte, 8)
	for _, c := range coords {
		binary.LittleEndian.PutUint64(tmp, math.Float64bits(c[0]))
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint64(tmp, math.Float64bits(c[1]))
		buf = append(buf, tmp...)
	}
	return string(buf)
}

// wkbPolygon builds a little-endian WKB for 2D Polygon with one or more rings
func wkbPolygon(rings [][][2]float64) string {
	buf := make([]byte, 0)
	buf = append(buf, 1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 3)
	buf = append(buf, t...)
	rn := make([]byte, 4)
	binary.LittleEndian.PutUint32(rn, uint32(len(rings)))
	buf = append(buf, rn...)
	tmp4 := make([]byte, 4)
	tmp8 := make([]byte, 8)
	for _, ring := range rings {
		binary.LittleEndian.PutUint32(tmp4, uint32(len(ring)))
		buf = append(buf, tmp4...)
		for _, c := range ring {
			binary.LittleEndian.PutUint64(tmp8, math.Float64bits(c[0]))
			buf = append(buf, tmp8...)
			binary.LittleEndian.PutUint64(tmp8, math.Float64bits(c[1]))
			buf = append(buf, tmp8...)
		}
	}
	return string(buf)
}

// wkbMultiPoint builds a little-endian WKB for MultiPoint from a list of points
func wkbMultiPoint(points [][2]float64) string {
	buf := make([]byte, 0)
	buf = append(buf, 1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 4)
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(points)))
	buf = append(buf, n...)
	for _, p := range points {
		// Each point is its own WKB geometry
		buf = append(buf, []byte(wkbPoint(p[0], p[1]))...)
	}
	return string(buf)
}

// wkbMultiLineString builds a little-endian WKB for MultiLineString
func wkbMultiLineString(lines [][][2]float64) string {
	buf := make([]byte, 0)
	buf = append(buf, 1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 5)
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(lines)))
	buf = append(buf, n...)
	for _, ls := range lines {
		buf = append(buf, []byte(wkbLineString(ls))...)
	}
	return string(buf)
}

// wkbMultiPolygon builds a little-endian WKB for MultiPolygon
func wkbMultiPolygon(polys [][][][2]float64) string {
	buf := make([]byte, 0)
	buf = append(buf, 1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 6)
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(polys)))
	buf = append(buf, n...)
	for _, poly := range polys {
		buf = append(buf, []byte(wkbPolygon(poly))...)
	}
	return string(buf)
}

// wkbGeometryCollection builds a little-endian WKB for GeometryCollection
// Each element in geoms should be a complete WKB geometry (as returned by
// helpers like wkbPoint, wkbLineString, etc.)
func wkbGeometryCollection(geoms []string) string {
	buf := make([]byte, 0)
	buf = append(buf, 1) // LE
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 7) // GeometryCollection type id
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(geoms)))
	buf = append(buf, n...)
	for _, g := range geoms {
		buf = append(buf, []byte(g)...)
	}
	return string(buf)
}

// sampleGeometry returns varied geometry types across rows
func sampleGeometry(i int) string {
	switch i % 7 {
	case 0:
		return wkbPoint(float64(i), float64(i)+1)
	case 1:
		return wkbLineString([][2]float64{{0, 0}, {float64(i), float64(i)}})
	case 2:
		return wkbPolygon([][][2]float64{{{0, 0}, {float64(i), 0}, {float64(i), float64(i)}, {0, float64(i)}, {0, 0}}})
	case 3:
		return wkbMultiPoint([][2]float64{{float64(i), float64(i) + 0.1}, {float64(i) + 1, float64(i) + 1}})
	case 4:
		return wkbMultiLineString([][][2]float64{{{0, 0}, {1, 1}}, {{2, 2}, {3, 3}}})
	case 5:
		return wkbMultiPolygon([][][][2]float64{
			{{{0, 0}, {2, 0}, {2, 2}, {0, 2}, {0, 0}}},
			{{{3, 3}, {4, 3}, {4, 4}, {3, 4}, {3, 3}}},
		})
	default:
		return wkbGeometryCollection([]string{
			wkbPoint(float64(i), float64(i)+0.5),
			wkbLineString([][2]float64{{0, 0}, {1, 1}}),
		})
	}
}

// sampleGeography returns varied geography types across rows
func sampleGeography(i int) string {
	switch i % 7 {
	case 0:
		return wkbPoint(-122.4+float64(i)*0.01, 37.8+float64(i)*0.01)
	case 1:
		return wkbLineString([][2]float64{{-122.4, 37.8}, {-122.41, 37.81}})
	case 2:
		return wkbPolygon([][][2]float64{{{-122.5, 37.8}, {-122.4, 37.8}, {-122.4, 37.9}, {-122.5, 37.9}, {-122.5, 37.8}}})
	case 3:
		return wkbMultiPoint([][2]float64{{-122.4, 37.8}, {-122.41, 37.81}, {-122.42, 37.82}})
	case 4:
		return wkbMultiLineString([][][2]float64{{{-122.4, 37.8}, {-122.41, 37.81}}, {{-122.42, 37.82}, {-122.43, 37.83}}})
	case 5:
		return wkbMultiPolygon([][][][2]float64{
			{{{-122.5, 37.8}, {-122.4, 37.8}, {-122.4, 37.9}, {-122.5, 37.9}, {-122.5, 37.8}}},
			{{{-122.45, 37.85}, {-122.44, 37.85}, {-122.44, 37.86}, {-122.45, 37.86}, {-122.45, 37.85}}},
		})
	default:
		return wkbGeometryCollection([]string{
			wkbPoint(-122.4, 37.8),
			wkbLineString([][2]float64{{-122.41, 37.81}, {-122.42, 37.82}}),
		})
	}
}
