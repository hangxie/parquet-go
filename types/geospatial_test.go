package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
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
