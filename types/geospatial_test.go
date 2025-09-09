package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

// helpers to build minimal WKB payloads for tests
func wkbPoint(order byte, x, y float64) []byte {
	buf := make([]byte, 0, 1+4+16)
	buf = append(buf, order)
	t := make([]byte, 4)
	if order == 0 { // big-endian
		binary.BigEndian.PutUint32(t, 1)
	} else { // little-endian
		binary.LittleEndian.PutUint32(t, 1)
	}
	buf = append(buf, t...)

	xb := make([]byte, 8)
	yb := make([]byte, 8)
	if order == 0 {
		binary.BigEndian.PutUint64(xb, math.Float64bits(x))
		binary.BigEndian.PutUint64(yb, math.Float64bits(y))
	} else {
		binary.LittleEndian.PutUint64(xb, math.Float64bits(x))
		binary.LittleEndian.PutUint64(yb, math.Float64bits(y))
	}
	buf = append(buf, xb...)
	buf = append(buf, yb...)
	return buf
}

func wkbLineStringLE(coords [][]float64) []byte {
	buf := make([]byte, 0, 1+4+4+len(coords)*16)
	buf = append(buf, 1) // little-endian
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 2)
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(coords)))
	buf = append(buf, n...)
	for _, c := range coords {
		xb := make([]byte, 8)
		yb := make([]byte, 8)
		binary.LittleEndian.PutUint64(xb, math.Float64bits(c[0]))
		binary.LittleEndian.PutUint64(yb, math.Float64bits(c[1]))
		buf = append(buf, xb...)
		buf = append(buf, yb...)
	}
	return buf
}

func wkbPolygonLE(rings [][][]float64) []byte {
	buf := make([]byte, 0)
	buf = append(buf, 1) // little-endian
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 3)
	buf = append(buf, t...)
	rn := make([]byte, 4)
	binary.LittleEndian.PutUint32(rn, uint32(len(rings)))
	buf = append(buf, rn...)
	for _, ring := range rings {
		n := make([]byte, 4)
		binary.LittleEndian.PutUint32(n, uint32(len(ring)))
		buf = append(buf, n...)
		for _, c := range ring {
			xb := make([]byte, 8)
			yb := make([]byte, 8)
			binary.LittleEndian.PutUint64(xb, math.Float64bits(c[0]))
			binary.LittleEndian.PutUint64(yb, math.Float64bits(c[1]))
			buf = append(buf, xb...)
			buf = append(buf, yb...)
		}
	}
	return buf
}

func Test_wkbToGeoJSON_PointEndianness(t *testing.T) {
	// Little-endian point
	bLE := wkbPoint(1, 1.5, -2.25)
	gj, ok := wkbToGeoJSON(bLE)
	require.True(t, ok)
	require.Equal(t, "Point", gj["type"])
	require.Equal(t, []float64{1.5, -2.25}, gj["coordinates"])

	// Big-endian point
	bBE := wkbPoint(0, -10.0, 42.0)
	gj2, ok := wkbToGeoJSON(bBE)
	require.True(t, ok)
	require.Equal(t, "Point", gj2["type"])
	require.Equal(t, []float64{-10.0, 42.0}, gj2["coordinates"])
}

func Test_wkbToGeoJSON_LineStringAndPolygon(t *testing.T) {
	ls := wkbLineStringLE([][]float64{{0, 0}, {1, 1.2}, {2, -3.4}})
	gj, ok := wkbToGeoJSON(ls)
	require.True(t, ok)
	require.Equal(t, "LineString", gj["type"])
	require.Equal(t, [][]float64{{0, 0}, {1, 1.2}, {2, -3.4}}, gj["coordinates"])

	poly := wkbPolygonLE([][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}})
	gj2, ok := wkbToGeoJSON(poly)
	require.True(t, ok)
	require.Equal(t, "Polygon", gj2["type"])
	require.Equal(t, [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}}, gj2["coordinates"])
}

func Test_wkbToGeoJSON_InvalidInputs(t *testing.T) {
	// too short
	if _, ok := wkbToGeoJSON([]byte{1, 1, 0}); ok {
		t.Fatal("expected failure for too short header")
	}
	// unknown type
	unk := make([]byte, 5)
	unk[0] = 1 // little-endian
	binary.LittleEndian.PutUint32(unk[1:5], 99)
	if _, ok := wkbToGeoJSON(unk); ok {
		t.Fatal("expected failure for unknown type")
	}
	// truncated LineString: declares 2 points but provides 1
	trunc := make([]byte, 0, 1+4+4+16)
	trunc = append(trunc, 1)
	tbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(tbuf, 2)
	trunc = append(trunc, tbuf...)
	nbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(nbuf, 2)
	trunc = append(trunc, nbuf...)
	// only one point worth of coords
	xb := make([]byte, 8)
	yb := make([]byte, 8)
	binary.LittleEndian.PutUint64(xb, math.Float64bits(0))
	binary.LittleEndian.PutUint64(yb, math.Float64bits(0))
	trunc = append(trunc, xb...)
	trunc = append(trunc, yb...)
	if _, ok := wkbToGeoJSON(trunc); ok {
		t.Fatal("expected failure for truncated LineString")
	}
	// truncated Point: only X present, missing Y
	ptTrunc := make([]byte, 0, 1+4+8)
	ptTrunc = append(ptTrunc, 1) // little-endian
	tbufPt := make([]byte, 4)
	binary.LittleEndian.PutUint32(tbufPt, 1) // type=1 point
	ptTrunc = append(ptTrunc, tbufPt...)
	xb2 := make([]byte, 8)
	binary.LittleEndian.PutUint64(xb2, math.Float64bits(123.0))
	ptTrunc = append(ptTrunc, xb2...)
	if _, ok := wkbToGeoJSON(ptTrunc); ok {
		t.Fatal("expected failure for truncated Point missing Y")
	}

	// Polygon missing ring point count (u32 read overflow)
	// Header for Polygon with 1 ring but do not include the ring's point count
	polyTrunc := make([]byte, 0, 1+4+4)
	polyTrunc = append(polyTrunc, 1) // little-endian
	tbuf2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(tbuf2, 3)
	polyTrunc = append(polyTrunc, tbuf2...) // type=3 polygon
	rn := make([]byte, 4)
	binary.LittleEndian.PutUint32(rn, 1)
	polyTrunc = append(polyTrunc, rn...) // ringsN=1
	// No further bytes -> should fail when reading first ring's point count
	if _, ok := wkbToGeoJSON(polyTrunc); ok {
		t.Fatal("expected failure for truncated Polygon ring count")
	}

	// Polygon with ring count and point count but incomplete coordinates
	polyCoordsTrunc := make([]byte, 0)
	polyCoordsTrunc = append(polyCoordsTrunc, 1) // little-endian
	tpoly := make([]byte, 4)
	binary.LittleEndian.PutUint32(tpoly, 3) // type=3 polygon
	polyCoordsTrunc = append(polyCoordsTrunc, tpoly...)
	rn2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(rn2, 1) // 1 ring
	polyCoordsTrunc = append(polyCoordsTrunc, rn2...)
	pn := make([]byte, 4)
	binary.LittleEndian.PutUint32(pn, 1) // 1 point
	polyCoordsTrunc = append(polyCoordsTrunc, pn...)
	// only 8 bytes for X, missing Y
	polyCoordsTrunc = append(polyCoordsTrunc, xb...)
	if _, ok := wkbToGeoJSON(polyCoordsTrunc); ok {
		t.Fatal("expected failure for truncated Polygon coordinate")
	}
}

func Test_wrapGeoJSONHybrid(t *testing.T) {
	geo := map[string]any{"type": "Point", "coordinates": []float64{1, 2}}
	raw := []byte{0x01, 0x02, 0xFF}

	// include=false: only geojson
	out := wrapGeoJSONHybrid(geo, raw, false, false)
	require.Equal(t, geo, out["geojson"])
	require.NotContains(t, out, "wkb_hex")
	require.NotContains(t, out, "wkb_b64")

	// include=true, hex
	outHex := wrapGeoJSONHybrid(geo, raw, false, true)
	require.Equal(t, hex.EncodeToString(raw), outHex["wkb_hex"]) // 0102ff
	require.NotContains(t, outHex, "wkb_b64")

	// include=true, base64
	outB64 := wrapGeoJSONHybrid(geo, raw, true, true)
	require.Equal(t, base64.StdEncoding.EncodeToString(raw), outB64["wkb_b64"]) // AQH/
	require.NotContains(t, outB64, "wkb_hex")
}

func Test_SetGeospatialGeoJSONAsFeature(t *testing.T) {
	// Save original setting
	orig := geospatialGeoJSONAsFeature
	defer func() { geospatialGeoJSONAsFeature = orig }()

	// Test setting to true
	SetGeospatialGeoJSONAsFeature(true)
	require.True(t, geospatialGeoJSONAsFeature)

	// Test setting to false
	SetGeospatialGeoJSONAsFeature(false)
	require.False(t, geospatialGeoJSONAsFeature)

	// Test makeGeoJSONFeature function is called when flag is true
	SetGeospatialGeoJSONAsFeature(true)
	geo := map[string]any{"type": "Point", "coordinates": []float64{1, 2}}
	props := map[string]any{"name": "test"}

	feature := makeGeoJSONFeature(geo, props)
	require.Equal(t, "Feature", feature["type"])
	require.Equal(t, geo, feature["geometry"])
	require.Equal(t, props, feature["properties"])
}

func Test_SetGeospatialCoordinatePrecision(t *testing.T) {
	// Save original setting
	orig := geospatialCoordPrecision
	defer func() { geospatialCoordPrecision = orig }()

	// Test setting precision to 3
	SetGeospatialCoordinatePrecision(3)
	require.Equal(t, 3, geospatialCoordPrecision)

	// Test setting precision to 8
	SetGeospatialCoordinatePrecision(8)
	require.Equal(t, 8, geospatialCoordPrecision)

	// Test precision affects coordinate rounding
	// Create a point with high precision coordinates
	wkb := wkbPoint(1, 1.123456789, 2.987654321)

	// Set precision to 2 decimal places
	SetGeospatialCoordinatePrecision(2)
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	coords := gj["coordinates"].([]float64)

	// Verify coordinates are rounded to 2 decimal places
	require.InDelta(t, 1.12, coords[0], 0.001)
	require.InDelta(t, 2.99, coords[1], 0.001)

	// Test with precision 0 (integers only)
	SetGeospatialCoordinatePrecision(0)
	gj2, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	coords2 := gj2["coordinates"].([]float64)

	require.InDelta(t, 1.0, coords2[0], 0.001)
	require.InDelta(t, 3.0, coords2[1], 0.001)
}

// Multi-geometry helper functions and tests

func buildWKBMultiPoint(points [][2]float64) []byte {
	buf := make([]byte, 0)
	buf = append(buf, 1) // little-endian
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 4) // MultiPoint type
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(points)))
	buf = append(buf, n...)
	for _, p := range points {
		// Each point is its own WKB geometry
		buf = append(buf, 1) // little-endian for point
		pt := make([]byte, 4)
		binary.LittleEndian.PutUint32(pt, 1) // Point type
		buf = append(buf, pt...)
		xb := make([]byte, 8)
		yb := make([]byte, 8)
		binary.LittleEndian.PutUint64(xb, math.Float64bits(p[0]))
		binary.LittleEndian.PutUint64(yb, math.Float64bits(p[1]))
		buf = append(buf, xb...)
		buf = append(buf, yb...)
	}
	return buf
}

func buildWKBMultiLineString(lines [][][2]float64) []byte {
	buf := make([]byte, 0)
	buf = append(buf, 1) // little-endian
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 5) // MultiLineString type
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(lines)))
	buf = append(buf, n...)
	for _, line := range lines {
		// Each linestring is its own WKB geometry
		buf = append(buf, 1) // little-endian for linestring
		lst := make([]byte, 4)
		binary.LittleEndian.PutUint32(lst, 2) // LineString type
		buf = append(buf, lst...)
		pn := make([]byte, 4)
		binary.LittleEndian.PutUint32(pn, uint32(len(line)))
		buf = append(buf, pn...)
		for _, p := range line {
			xb := make([]byte, 8)
			yb := make([]byte, 8)
			binary.LittleEndian.PutUint64(xb, math.Float64bits(p[0]))
			binary.LittleEndian.PutUint64(yb, math.Float64bits(p[1]))
			buf = append(buf, xb...)
			buf = append(buf, yb...)
		}
	}
	return buf
}

func buildWKBMultiPointBE(points [][2]float64) []byte {
	buf := make([]byte, 0)
	buf = append(buf, 0) // big-endian
	t := make([]byte, 4)
	binary.BigEndian.PutUint32(t, 4) // MultiPoint type
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.BigEndian.PutUint32(n, uint32(len(points)))
	buf = append(buf, n...)
	for _, p := range points {
		// Each point is its own WKB geometry
		buf = append(buf, 0) // big-endian for point
		pt := make([]byte, 4)
		binary.BigEndian.PutUint32(pt, 1) // Point type
		buf = append(buf, pt...)
		xb := make([]byte, 8)
		yb := make([]byte, 8)
		binary.BigEndian.PutUint64(xb, math.Float64bits(p[0]))
		binary.BigEndian.PutUint64(yb, math.Float64bits(p[1]))
		buf = append(buf, xb...)
		buf = append(buf, yb...)
	}
	return buf
}

func buildWKBMultiPolygon(polygons [][][][2]float64) []byte {
	buf := make([]byte, 0)
	buf = append(buf, 1) // little-endian
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 6) // MultiPolygon type
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(polygons)))
	buf = append(buf, n...)
	for _, poly := range polygons {
		// Each polygon is its own WKB geometry
		buf = append(buf, 1) // little-endian for polygon
		pt := make([]byte, 4)
		binary.LittleEndian.PutUint32(pt, 3) // Polygon type
		buf = append(buf, pt...)
		rn := make([]byte, 4)
		binary.LittleEndian.PutUint32(rn, uint32(len(poly)))
		buf = append(buf, rn...)
		for _, ring := range poly {
			pn := make([]byte, 4)
			binary.LittleEndian.PutUint32(pn, uint32(len(ring)))
			buf = append(buf, pn...)
			for _, p := range ring {
				xb := make([]byte, 8)
				yb := make([]byte, 8)
				binary.LittleEndian.PutUint64(xb, math.Float64bits(p[0]))
				binary.LittleEndian.PutUint64(yb, math.Float64bits(p[1]))
				buf = append(buf, xb...)
				buf = append(buf, yb...)
			}
		}
	}
	return buf
}

func Test_wkbToGeoJSON_MultiPoint(t *testing.T) {
	wkb := buildWKBMultiPoint([][2]float64{{1, 2}, {3, 4}, {5, 6}})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "MultiPoint", gj["type"])

	coords := gj["coordinates"].([][]float64)
	require.Len(t, coords, 3)
	require.Equal(t, []float64{1, 2}, coords[0])
	require.Equal(t, []float64{3, 4}, coords[1])
	require.Equal(t, []float64{5, 6}, coords[2])
}

func Test_wkbToGeoJSON_MultiLineString(t *testing.T) {
	wkb := buildWKBMultiLineString([][][2]float64{
		{{0, 0}, {1, 1}},
		{{2, 2}, {3, 3}, {4, 4}},
	})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "MultiLineString", gj["type"])

	coords := gj["coordinates"].([][][]float64)
	require.Len(t, coords, 2)

	// First LineString
	require.Len(t, coords[0], 2)
	require.Equal(t, []float64{0, 0}, coords[0][0])
	require.Equal(t, []float64{1, 1}, coords[0][1])

	// Second LineString
	require.Len(t, coords[1], 3)
	require.Equal(t, []float64{2, 2}, coords[1][0])
	require.Equal(t, []float64{3, 3}, coords[1][1])
	require.Equal(t, []float64{4, 4}, coords[1][2])
}

func Test_wkbToGeoJSON_MultiPolygon(t *testing.T) {
	wkb := buildWKBMultiPolygon([][][][2]float64{
		// First polygon with one ring
		{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
		// Second polygon with one ring
		{{{2, 2}, {3, 2}, {3, 3}, {2, 3}, {2, 2}}},
	})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "MultiPolygon", gj["type"])

	coords := gj["coordinates"].([][][][]float64)
	require.Len(t, coords, 2)

	// First polygon
	require.Len(t, coords[0], 1)    // one ring
	require.Len(t, coords[0][0], 5) // 5 points (closed ring)
	require.Equal(t, []float64{0, 0}, coords[0][0][0])
	require.Equal(t, []float64{1, 0}, coords[0][0][1])
	require.Equal(t, []float64{1, 1}, coords[0][0][2])
	require.Equal(t, []float64{0, 1}, coords[0][0][3])
	require.Equal(t, []float64{0, 0}, coords[0][0][4])

	// Second polygon
	require.Len(t, coords[1], 1)    // one ring
	require.Len(t, coords[1][0], 5) // 5 points (closed ring)
	require.Equal(t, []float64{2, 2}, coords[1][0][0])
	require.Equal(t, []float64{3, 2}, coords[1][0][1])
	require.Equal(t, []float64{3, 3}, coords[1][0][2])
	require.Equal(t, []float64{2, 3}, coords[1][0][3])
	require.Equal(t, []float64{2, 2}, coords[1][0][4])
}

func Test_wkbToGeoJSON_EmptyMultiGeometries(t *testing.T) {
	// Empty MultiPoint
	wkb := buildWKBMultiPoint([][2]float64{})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "MultiPoint", gj["type"])
	coords := gj["coordinates"].([][]float64)
	require.Len(t, coords, 0)

	// Empty MultiLineString
	wkb = buildWKBMultiLineString([][][2]float64{})
	gj, ok = wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "MultiLineString", gj["type"])
	coords2 := gj["coordinates"].([][][]float64)
	require.Len(t, coords2, 0)

	// Empty MultiPolygon
	wkb = buildWKBMultiPolygon([][][][2]float64{})
	gj, ok = wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "MultiPolygon", gj["type"])
	coords3 := gj["coordinates"].([][][][]float64)
	require.Len(t, coords3, 0)
}

func Test_wkbToGeoJSON_InvalidMultiGeometries(t *testing.T) {
	// MultiPoint with invalid point type
	wkb := make([]byte, 0)
	wkb = append(wkb, 1) // little-endian
	t4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(t4, 4) // MultiPoint type
	wkb = append(wkb, t4...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, 1) // 1 point
	wkb = append(wkb, n...)
	wkb = append(wkb, 1) // little-endian for point
	pt := make([]byte, 4)
	binary.LittleEndian.PutUint32(pt, 99) // Invalid type (not 1 for Point)
	wkb = append(wkb, pt...)

	_, ok := wkbToGeoJSON(wkb)
	require.False(t, ok, "Should fail for MultiPoint with invalid point type")

	// Truncated MultiPoint
	wkb = make([]byte, 0)
	wkb = append(wkb, 1)                 // little-endian
	binary.LittleEndian.PutUint32(t4, 4) // MultiPoint type
	wkb = append(wkb, t4...)
	binary.LittleEndian.PutUint32(n, 1) // 1 point
	wkb = append(wkb, n...)
	// Missing point data

	_, ok = wkbToGeoJSON(wkb)
	require.False(t, ok, "Should fail for truncated MultiPoint")
}

func Test_wkbToGeoJSON_CoordinatePrecision_MultiGeometries(t *testing.T) {
	// Save original precision
	orig := geospatialCoordPrecision
	defer func() { geospatialCoordPrecision = orig }()

	// Set precision to 2 decimal places
	SetGeospatialCoordinatePrecision(2)

	// Test with high precision coordinates
	wkb := buildWKBMultiPoint([][2]float64{{1.123456, 2.987654}})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)

	coords := gj["coordinates"].([][]float64)
	require.Len(t, coords, 1)
	require.InDelta(t, 1.12, coords[0][0], 0.001)
	require.InDelta(t, 2.99, coords[0][1], 0.001)
}

func Test_wkbToGeoJSON_MultiGeometries_Endianness(t *testing.T) {
	// Test big-endian MultiPoint
	wkb := buildWKBMultiPointBE([][2]float64{{-1.5, 2.25}, {3.75, -4.0}})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "MultiPoint", gj["type"])

	coords := gj["coordinates"].([][]float64)
	require.Len(t, coords, 2)
	require.Equal(t, []float64{-1.5, 2.25}, coords[0])
	require.Equal(t, []float64{3.75, -4.0}, coords[1])

	// Note: Mixed endianness within multi-geometries is an edge case
	// that current implementation doesn't fully support, so we skip that test
}

// SPHERICAL GeoJSON tests

func Test_SPHERICAL_GeoJSON_Encoding(t *testing.T) {
	// Create valid WKB Point data (1, 2)
	wkbPoint := []byte{
		0x01,                   // little-endian
		0x01, 0x00, 0x00, 0x00, // Point type (1)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
	}

	// Create Geography type with SPHERICAL algorithm
	geog := parquet.NewGeographyType()
	sphericalAlgo := parquet.EdgeInterpolationAlgorithm_SPHERICAL
	geog.Algorithm = &sphericalAlgo
	crs := "OGC:CRS84"
	geog.CRS = &crs

	// Save original modes to restore later
	origGeogMode := geographyJSONMode
	origAsFeature := geospatialGeoJSONAsFeature
	defer func() {
		geographyJSONMode = origGeogMode
		geospatialGeoJSONAsFeature = origAsFeature
	}()

	t.Run("SPHERICAL in Hex mode", func(t *testing.T) {
		SetGeographyJSONMode(GeospatialModeHex)
		result := ConvertGeographyLogicalValue(wkbPoint, geog).(map[string]any)

		require.Equal(t, "SPHERICAL", result["algorithm"])
		require.Equal(t, crs, result["crs"])
		require.Contains(t, result, "wkb_hex")

		// Verify JSON roundtrip
		jsonBytes, err := json.Marshal(result)
		require.NoError(t, err)

		var unmarshaled map[string]any
		err = json.Unmarshal(jsonBytes, &unmarshaled)
		require.NoError(t, err)
		require.Equal(t, "SPHERICAL", unmarshaled["algorithm"])
	})

	t.Run("SPHERICAL in Base64 mode", func(t *testing.T) {
		SetGeographyJSONMode(GeospatialModeBase64)
		result := ConvertGeographyLogicalValue(wkbPoint, geog).(map[string]any)

		require.Equal(t, "SPHERICAL", result["algorithm"])
		require.Equal(t, crs, result["crs"])
		require.Contains(t, result, "wkb_b64")

		// Verify JSON roundtrip
		jsonBytes, err := json.Marshal(result)
		require.NoError(t, err)

		var unmarshaled map[string]any
		err = json.Unmarshal(jsonBytes, &unmarshaled)
		require.NoError(t, err)
		require.Equal(t, "SPHERICAL", unmarshaled["algorithm"])
	})

	t.Run("SPHERICAL in GeoJSON mode", func(t *testing.T) {
		SetGeographyJSONMode(GeospatialModeGeoJSON)
		SetGeospatialGeoJSONAsFeature(true)
		result := ConvertGeographyLogicalValue(wkbPoint, geog).(map[string]any)

		// Verify Feature structure
		require.Equal(t, "Feature", result["type"])
		require.Contains(t, result, "geometry")
		require.Contains(t, result, "properties")

		// Verify geometry
		geometry := result["geometry"].(map[string]any)
		require.Equal(t, "Point", geometry["type"])
		require.Equal(t, []float64{1, 2}, geometry["coordinates"])

		// Verify properties contain SPHERICAL algorithm
		properties := result["properties"].(map[string]any)
		require.Equal(t, "SPHERICAL", properties["algorithm"])
		require.Equal(t, crs, properties["crs"])

		// Verify JSON roundtrip
		jsonBytes, err := json.Marshal(result)
		require.NoError(t, err)

		var unmarshaled map[string]any
		err = json.Unmarshal(jsonBytes, &unmarshaled)
		require.NoError(t, err)

		props := unmarshaled["properties"].(map[string]any)
		require.Equal(t, "SPHERICAL", props["algorithm"])
		require.Equal(t, crs, props["crs"])
	})

	t.Run("SPHERICAL in Hybrid mode", func(t *testing.T) {
		SetGeographyJSONMode(GeospatialModeHybrid)
		result := ConvertGeographyLogicalValue(wkbPoint, geog).(map[string]any)

		require.Equal(t, "SPHERICAL", result["algorithm"])
		require.Equal(t, crs, result["crs"])
		require.Contains(t, result, "geojson")
		require.Contains(t, result, "wkb_hex")

		// Verify GeoJSON portion
		geoJSON := result["geojson"].(map[string]any)
		require.Equal(t, "Point", geoJSON["type"])
		require.Equal(t, []float64{1, 2}, geoJSON["coordinates"])

		// Verify JSON roundtrip
		jsonBytes, err := json.Marshal(result)
		require.NoError(t, err)

		var unmarshaled map[string]any
		err = json.Unmarshal(jsonBytes, &unmarshaled)
		require.NoError(t, err)
		require.Equal(t, "SPHERICAL", unmarshaled["algorithm"])
	})

	t.Run("Default to SPHERICAL when algorithm is nil", func(t *testing.T) {
		geogDefault := parquet.NewGeographyType()
		geogDefault.CRS = &crs
		geogDefault.Algorithm = nil // nil should default to SPHERICAL

		SetGeographyJSONMode(GeospatialModeGeoJSON)
		result := ConvertGeographyLogicalValue(wkbPoint, geogDefault).(map[string]any)

		properties := result["properties"].(map[string]any)
		require.Equal(t, "SPHERICAL", properties["algorithm"])
		require.Equal(t, crs, properties["crs"])
	})
}

func Test_EdgeInterpolationAlgorithm_JSON_Marshaling(t *testing.T) {
	algorithms := []parquet.EdgeInterpolationAlgorithm{
		parquet.EdgeInterpolationAlgorithm_SPHERICAL,
		parquet.EdgeInterpolationAlgorithm_VINCENTY,
		parquet.EdgeInterpolationAlgorithm_THOMAS,
		parquet.EdgeInterpolationAlgorithm_ANDOYER,
		parquet.EdgeInterpolationAlgorithm_KARNEY,
	}

	for _, algo := range algorithms {
		t.Run(algo.String(), func(t *testing.T) {
			// Test direct marshaling
			jsonBytes, err := json.Marshal(algo)
			require.NoError(t, err)
			require.Equal(t, `"`+algo.String()+`"`, string(jsonBytes))

			// Test direct unmarshaling
			var unmarshaled parquet.EdgeInterpolationAlgorithm
			err = json.Unmarshal(jsonBytes, &unmarshaled)
			require.NoError(t, err)
			require.Equal(t, algo, unmarshaled)

			// Test in struct context
			type TestStruct struct {
				Algorithm parquet.EdgeInterpolationAlgorithm `json:"algorithm"`
			}

			original := TestStruct{Algorithm: algo}
			structBytes, err := json.Marshal(original)
			require.NoError(t, err)

			var unmarshaledStruct TestStruct
			err = json.Unmarshal(structBytes, &unmarshaledStruct)
			require.NoError(t, err)
			require.Equal(t, algo, unmarshaledStruct.Algorithm)
		})
	}
}

func Test_EdgeInterpolationAlgorithm_Invalid_JSON(t *testing.T) {
	type TestStruct struct {
		Algorithm parquet.EdgeInterpolationAlgorithm `json:"algorithm"`
	}

	testCases := []struct {
		name      string
		jsonStr   string
		shouldErr bool
	}{
		{"numeric value", `{"algorithm": 0}`, true},
		{"invalid string", `{"algorithm": "INVALID"}`, true},
		{"empty string", `{"algorithm": ""}`, true},
		{"null value", `{"algorithm": null}`, false}, // null is valid (zero value)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var ts TestStruct
			err := json.Unmarshal([]byte(tc.jsonStr), &ts)
			if tc.shouldErr {
				require.Error(t, err, "Expected error when unmarshaling %s", tc.jsonStr)
			} else {
				require.NoError(t, err, "Expected no error when unmarshaling %s", tc.jsonStr)
			}
		})
	}
}

func Test_wkbToGeoJSON_MultiGeometries_EdgeCases(t *testing.T) {
	// Test MultiLineString with invalid LineString type
	wkb := make([]byte, 0)
	wkb = append(wkb, 1) // little-endian
	typeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(typeBuf, 5) // MultiLineString type
	wkb = append(wkb, typeBuf...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, 1) // 1 linestring
	wkb = append(wkb, n...)
	wkb = append(wkb, 1) // little-endian for linestring
	lst := make([]byte, 4)
	binary.LittleEndian.PutUint32(lst, 99) // Invalid type (not 2 for LineString)
	wkb = append(wkb, lst...)

	_, ok := wkbToGeoJSON(wkb)
	require.False(t, ok, "Should fail for MultiLineString with invalid linestring type")

	// Test MultiPolygon with invalid Polygon type
	wkb = make([]byte, 0)
	wkb = append(wkb, 1)                      // little-endian
	binary.LittleEndian.PutUint32(typeBuf, 6) // MultiPolygon type
	wkb = append(wkb, typeBuf...)
	binary.LittleEndian.PutUint32(n, 1) // 1 polygon
	wkb = append(wkb, n...)
	wkb = append(wkb, 1) // little-endian for polygon
	pt := make([]byte, 4)
	binary.LittleEndian.PutUint32(pt, 99) // Invalid type (not 3 for Polygon)
	wkb = append(wkb, pt...)

	_, ok = wkbToGeoJSON(wkb)
	require.False(t, ok, "Should fail for MultiPolygon with invalid polygon type")

	// Test MultiLineString with truncated linestring point count
	wkb = make([]byte, 0)
	wkb = append(wkb, 1)                      // little-endian
	binary.LittleEndian.PutUint32(typeBuf, 5) // MultiLineString type
	wkb = append(wkb, typeBuf...)
	binary.LittleEndian.PutUint32(n, 1) // 1 linestring
	wkb = append(wkb, n...)
	wkb = append(wkb, 1)                  // little-endian for linestring
	binary.LittleEndian.PutUint32(lst, 2) // LineString type
	wkb = append(wkb, lst...)
	// Missing point count

	_, ok = wkbToGeoJSON(wkb)
	require.False(t, ok, "Should fail for MultiLineString with truncated point count")

	// Test completely empty multi-geometry headers
	for _, geomType := range []uint32{4, 5, 6} { // MultiPoint, MultiLineString, MultiPolygon
		wkb = make([]byte, 0)
		wkb = append(wkb, 1) // little-endian
		binary.LittleEndian.PutUint32(typeBuf, geomType)
		wkb = append(wkb, typeBuf...)
		// Missing count entirely

		_, ok = wkbToGeoJSON(wkb)
		require.False(t, ok, "Should fail for truncated multi-geometry type %d", geomType)
	}
}

func Test_wkbToGeoJSON_MultiGeometries_BigEndian(t *testing.T) {
	// Test big-endian MultiLineString
	buf := make([]byte, 0)
	buf = append(buf, 0) // big-endian
	typeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(typeBuf, 5) // MultiLineString type
	buf = append(buf, typeBuf...)
	n := make([]byte, 4)
	binary.BigEndian.PutUint32(n, 1) // 1 linestring
	buf = append(buf, n...)
	// LineString with big-endian
	buf = append(buf, 0) // big-endian for linestring
	lst := make([]byte, 4)
	binary.BigEndian.PutUint32(lst, 2) // LineString type
	buf = append(buf, lst...)
	pc := make([]byte, 4)
	binary.BigEndian.PutUint32(pc, 2) // 2 points
	buf = append(buf, pc...)
	// Points
	for _, p := range [][]float64{{0.5, 1.5}, {2.5, 3.5}} {
		xb := make([]byte, 8)
		yb := make([]byte, 8)
		binary.BigEndian.PutUint64(xb, math.Float64bits(p[0]))
		binary.BigEndian.PutUint64(yb, math.Float64bits(p[1]))
		buf = append(buf, xb...)
		buf = append(buf, yb...)
	}

	gj, ok := wkbToGeoJSON(buf)
	require.True(t, ok)
	require.Equal(t, "MultiLineString", gj["type"])

	coords := gj["coordinates"].([][][]float64)
	require.Len(t, coords, 1)
	require.Len(t, coords[0], 2)
	require.Equal(t, []float64{0.5, 1.5}, coords[0][0])
	require.Equal(t, []float64{2.5, 3.5}, coords[0][1])

	// Test big-endian MultiPolygon
	buf = make([]byte, 0)
	buf = append(buf, 0)                   // big-endian
	binary.BigEndian.PutUint32(typeBuf, 6) // MultiPolygon type
	buf = append(buf, typeBuf...)
	binary.BigEndian.PutUint32(n, 1) // 1 polygon
	buf = append(buf, n...)
	// Polygon with big-endian
	buf = append(buf, 0) // big-endian for polygon
	pt := make([]byte, 4)
	binary.BigEndian.PutUint32(pt, 3) // Polygon type
	buf = append(buf, pt...)
	rn := make([]byte, 4)
	binary.BigEndian.PutUint32(rn, 1) // 1 ring
	buf = append(buf, rn...)
	binary.BigEndian.PutUint32(pc, 4) // 4 points (triangle)
	buf = append(buf, pc...)
	// Ring points
	for _, p := range [][]float64{{0, 0}, {1, 0}, {0.5, 1}, {0, 0}} {
		xb := make([]byte, 8)
		yb := make([]byte, 8)
		binary.BigEndian.PutUint64(xb, math.Float64bits(p[0]))
		binary.BigEndian.PutUint64(yb, math.Float64bits(p[1]))
		buf = append(buf, xb...)
		buf = append(buf, yb...)
	}

	gj, ok = wkbToGeoJSON(buf)
	require.True(t, ok)
	require.Equal(t, "MultiPolygon", gj["type"])

	coords2 := gj["coordinates"].([][][][]float64)
	require.Len(t, coords2, 1)
	require.Len(t, coords2[0], 1)
	require.Len(t, coords2[0][0], 4)
	require.Equal(t, []float64{0, 0}, coords2[0][0][0])
	require.Equal(t, []float64{1, 0}, coords2[0][0][1])
	require.Equal(t, []float64{0.5, 1}, coords2[0][0][2])
	require.Equal(t, []float64{0, 0}, coords2[0][0][3])
}

func Test_wkbToGeoJSON_MultiGeometries_PrecisionHandling(t *testing.T) {
	// Save original precision
	orig := geospatialCoordPrecision
	defer func() { geospatialCoordPrecision = orig }()

	// Test with precision 0 (integer only)
	SetGeospatialCoordinatePrecision(0)
	wkb := buildWKBMultiLineString([][][2]float64{
		{{1.789, 2.123}, {3.456, 4.987}},
	})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)

	coords := gj["coordinates"].([][][]float64)
	require.Equal(t, []float64{2, 2}, coords[0][0])
	require.Equal(t, []float64{3, 5}, coords[0][1])

	// Test with precision 4
	SetGeospatialCoordinatePrecision(4)
	wkb = buildWKBMultiLineString([][][2]float64{
		{{1.123456, 2.987654}},
	})
	gj, ok = wkbToGeoJSON(wkb)
	require.True(t, ok)

	coords = gj["coordinates"].([][][]float64)
	require.InDelta(t, 1.1235, coords[0][0][0], 0.0001)
	require.InDelta(t, 2.9877, coords[0][0][1], 0.0001)

	// Test with disabled precision (-1)
	SetGeospatialCoordinatePrecision(-1)
	wkb = buildWKBMultiPoint([][2]float64{{1.123456789, 2.987654321}})
	gj, ok = wkbToGeoJSON(wkb)
	require.True(t, ok)

	coords2 := gj["coordinates"].([][]float64)
	require.Equal(t, 1.123456789, coords2[0][0])
	require.Equal(t, 2.987654321, coords2[0][1])
}

func buildWKBGeometryCollection(geoms [][]byte) []byte {
	buf := make([]byte, 0)
	buf = append(buf, 1) // little-endian
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 7) // GeometryCollection type
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(geoms)))
	buf = append(buf, n...)
	for _, geom := range geoms {
		buf = append(buf, geom...)
	}
	return buf
}

func Test_wkbToGeoJSON_GeometryCollection(t *testing.T) {
	// Create simple GeometryCollection with Point and LineString
	pointWKB := wkbPoint(1, 1.5, 2.5)
	lineWKB := wkbLineStringLE([][]float64{{0, 0}, {1, 1}})

	wkb := buildWKBGeometryCollection([][]byte{pointWKB, lineWKB})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "GeometryCollection", gj["type"])

	geometries := gj["geometries"].([]map[string]any)
	require.Len(t, geometries, 2)

	// First geometry should be Point
	require.Equal(t, "Point", geometries[0]["type"])
	require.Equal(t, []float64{1.5, 2.5}, geometries[0]["coordinates"])

	// Second geometry should be LineString
	require.Equal(t, "LineString", geometries[1]["type"])
	require.Equal(t, [][]float64{{0, 0}, {1, 1}}, geometries[1]["coordinates"])
}

func Test_wkbToGeoJSON_GeometryCollection_Complex(t *testing.T) {
	// Create complex GeometryCollection with multiple geometry types
	pointWKB := wkbPoint(1, 1, 2)
	multiPointWKB := buildWKBMultiPoint([][2]float64{{3, 4}, {5, 6}})
	polygonWKB := wkbPolygonLE([][][]float64{{{0, 0}, {2, 0}, {2, 2}, {0, 2}, {0, 0}}})

	wkb := buildWKBGeometryCollection([][]byte{pointWKB, multiPointWKB, polygonWKB})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "GeometryCollection", gj["type"])

	geometries := gj["geometries"].([]map[string]any)
	require.Len(t, geometries, 3)

	// First geometry: Point
	require.Equal(t, "Point", geometries[0]["type"])
	require.Equal(t, []float64{1, 2}, geometries[0]["coordinates"])

	// Second geometry: MultiPoint
	require.Equal(t, "MultiPoint", geometries[1]["type"])
	coords := geometries[1]["coordinates"].([][]float64)
	require.Len(t, coords, 2)
	require.Equal(t, []float64{3, 4}, coords[0])
	require.Equal(t, []float64{5, 6}, coords[1])

	// Third geometry: Polygon
	require.Equal(t, "Polygon", geometries[2]["type"])
	polyCoords := geometries[2]["coordinates"].([][][]float64)
	require.Len(t, polyCoords, 1)
	require.Len(t, polyCoords[0], 5)
}

func Test_wkbToGeoJSON_GeometryCollection_Empty(t *testing.T) {
	// Test empty GeometryCollection
	wkb := buildWKBGeometryCollection([][]byte{})
	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "GeometryCollection", gj["type"])

	geometries := gj["geometries"].([]map[string]any)
	require.Len(t, geometries, 0)
}

func Test_wkbToGeoJSON_GeometryCollection_Errors(t *testing.T) {
	// Test GeometryCollection with truncated data
	wkb := make([]byte, 0)
	wkb = append(wkb, 1) // little-endian
	typeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(typeBuf, 7) // GeometryCollection type
	wkb = append(wkb, typeBuf...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, 1) // 1 geometry
	wkb = append(wkb, n...)
	// Missing geometry data

	_, ok := wkbToGeoJSON(wkb)
	require.False(t, ok, "Should fail for GeometryCollection with missing geometry data")

	// Test GeometryCollection with invalid geometry
	invalidGeom := []byte{1, 99, 0, 0, 0} // invalid geometry type 99
	wkb2 := buildWKBGeometryCollection([][]byte{invalidGeom})
	_, ok = wkbToGeoJSON(wkb2)
	require.False(t, ok, "Should fail for GeometryCollection with invalid geometry")
}

func Test_calculateWKBSize(t *testing.T) {
	// Test Point
	pointWKB := wkbPoint(1, 1, 2)
	size, ok := calculateWKBSize(pointWKB)
	require.True(t, ok)
	require.Equal(t, len(pointWKB), size)

	// Test LineString
	lineWKB := wkbLineStringLE([][]float64{{0, 0}, {1, 1}, {2, 2}})
	size, ok = calculateWKBSize(lineWKB)
	require.True(t, ok)
	require.Equal(t, len(lineWKB), size)

	// Test Polygon
	polygonWKB := wkbPolygonLE([][][]float64{{{0, 0}, {2, 0}, {2, 2}, {0, 2}, {0, 0}}})
	size, ok = calculateWKBSize(polygonWKB)
	require.True(t, ok)
	require.Equal(t, len(polygonWKB), size)

	// Test MultiPoint
	multiPointWKB := buildWKBMultiPoint([][2]float64{{1, 2}, {3, 4}})
	size, ok = calculateWKBSize(multiPointWKB)
	require.True(t, ok)
	require.Equal(t, len(multiPointWKB), size)

	// Test MultiLineString
	multiLineWKB := buildWKBMultiLineString([][][2]float64{{{0, 0}, {1, 1}}, {{2, 2}, {3, 3}}})
	size, ok = calculateWKBSize(multiLineWKB)
	require.True(t, ok)
	require.Equal(t, len(multiLineWKB), size)

	// Test MultiPolygon
	multiPolygonWKB := buildWKBMultiPolygon([][][][2]float64{
		{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
		{{{2, 2}, {3, 2}, {3, 3}, {2, 3}, {2, 2}}},
	})
	size, ok = calculateWKBSize(multiPolygonWKB)
	require.True(t, ok)
	require.Equal(t, len(multiPolygonWKB), size)

	// Test nested GeometryCollection
	pointWKB1 := wkbPoint(1, 1, 2)
	pointWKB2 := wkbPoint(1, 3, 4)
	collectionWKB := buildWKBGeometryCollection([][]byte{pointWKB1, pointWKB2})
	size, ok = calculateWKBSize(collectionWKB)
	require.True(t, ok)
	require.Equal(t, len(collectionWKB), size)

	// Test errors
	// Too short
	_, ok = calculateWKBSize([]byte{1, 1})
	require.False(t, ok)

	// Invalid geometry type
	invalidWKB := []byte{1, 99, 0, 0, 0} // invalid geometry type 99
	_, ok = calculateWKBSize(invalidWKB)
	require.False(t, ok)

	// Truncated LineString
	truncatedLine := []byte{1, 2, 0, 0, 0, 2, 0, 0, 0} // declares 2 points but no data
	_, ok = calculateWKBSize(truncatedLine)
	require.False(t, ok)
}

func Test_wkbToGeoJSON_GeometryCollection_BigEndian(t *testing.T) {
	// Test big-endian GeometryCollection
	pointWKB := wkbPoint(0, 1.5, 2.5) // big-endian point
	lineWKB := make([]byte, 0)
	lineWKB = append(lineWKB, 0) // big-endian
	typeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(typeBuf, 2) // LineString
	lineWKB = append(lineWKB, typeBuf...)
	nPts := make([]byte, 4)
	binary.BigEndian.PutUint32(nPts, 2) // 2 points
	lineWKB = append(lineWKB, nPts...)
	for _, p := range [][]float64{{0, 0}, {1, 1}} {
		xb := make([]byte, 8)
		yb := make([]byte, 8)
		binary.BigEndian.PutUint64(xb, math.Float64bits(p[0]))
		binary.BigEndian.PutUint64(yb, math.Float64bits(p[1]))
		lineWKB = append(lineWKB, xb...)
		lineWKB = append(lineWKB, yb...)
	}

	// Build big-endian GeometryCollection
	wkb := make([]byte, 0)
	wkb = append(wkb, 0)                   // big-endian
	binary.BigEndian.PutUint32(typeBuf, 7) // GeometryCollection
	wkb = append(wkb, typeBuf...)
	binary.BigEndian.PutUint32(nPts, 2) // 2 geometries
	wkb = append(wkb, nPts...)
	wkb = append(wkb, pointWKB...)
	wkb = append(wkb, lineWKB...)

	gj, ok := wkbToGeoJSON(wkb)
	require.True(t, ok)
	require.Equal(t, "GeometryCollection", gj["type"])

	geometries := gj["geometries"].([]map[string]any)
	require.Len(t, geometries, 2)
	require.Equal(t, "Point", geometries[0]["type"])
	require.Equal(t, "LineString", geometries[1]["type"])
}
