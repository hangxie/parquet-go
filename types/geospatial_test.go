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

// helpers to construct little-endian WKB for selected integration cases
func buildWKBLineString(coords [][]float64) []byte {
	buf := []byte{1, 2, 0, 0, 0}
	n := uint32(len(coords))
	buf = append(buf, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
	for _, c := range coords {
		buf = append(buf, f64le(c[0])...)
		buf = append(buf, f64le(c[1])...)
	}
	return buf
}

func buildWKBPolygon(rings [][][]float64) []byte {
	buf := []byte{1, 3, 0, 0, 0}
	rn := uint32(len(rings))
	buf = append(buf, byte(rn), byte(rn>>8), byte(rn>>16), byte(rn>>24))
	for _, ring := range rings {
		n := uint32(len(ring))
		buf = append(buf, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
		for _, c := range ring {
			buf = append(buf, f64le(c[0])...)
			buf = append(buf, f64le(c[1])...)
		}
	}
	return buf
}

func f64le(v float64) []byte {
	u := math.Float64bits(v)
	return []byte{byte(u), byte(u >> 8), byte(u >> 16), byte(u >> 24), byte(u >> 32), byte(u >> 40), byte(u >> 48), byte(u >> 56)}
}

// Moved from types/types_test.go: tests exercising geospatial.go behaviours
func Test_ConvertGeometryAndGeographyLogicalValue(t *testing.T) {
	// sample WKB: little-endian, Point(1,2)
	sample := []byte{
		1, 1, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 240, 63,
		0, 0, 0, 0, 0, 0, 0, 64,
	}

	// Geometry (GeoJSON mode returns GeoJSON Feature)
	geom := parquet.NewGeometryType()
	crs := "EPSG:3857"
	geom.CRS = &crs
	SetGeometryJSONMode(GeospatialModeGeoJSON)
	gRes := ConvertGeometryLogicalValue(sample, geom)
	feat, ok := gRes.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Feature", feat["type"])
	ggeom := feat["geometry"].(map[string]any)
	require.Equal(t, "Point", ggeom["type"])
	require.Equal(t, []float64{1, 2}, ggeom["coordinates"])
	gprops := feat["properties"].(map[string]any)
	require.Equal(t, crs, gprops["crs"]) // properties carries crs

	// Geography with algorithm
	geog := parquet.NewGeographyType()
	crs2 := "OGC:CRS84"
	geog.CRS = &crs2
	algo := parquet.EdgeInterpolationAlgorithm_VINCENTY
	geog.Algorithm = &algo
	SetGeographyJSONMode(GeospatialModeGeoJSON)
	gaRes := ConvertGeographyLogicalValue(sample, geog)
	feat2, ok := gaRes.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Feature", feat2["type"])
	g2 := feat2["geometry"].(map[string]any)
	require.Equal(t, "Point", g2["type"])
	require.Equal(t, []float64{1, 2}, g2["coordinates"])
	props2 := feat2["properties"].(map[string]any)
	require.Equal(t, "OGC:CRS84", props2["crs"])
	require.Equal(t, "VINCENTY", props2["algorithm"])

	// nil/empty safety
	require.Nil(t, ConvertGeometryLogicalValue(nil, geom))
	require.Nil(t, ConvertGeographyLogicalValue(nil, geog))

	// Reprojection hook test: fake reprojection that adds +1 to coords
	SetGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
		if crs == "EPSG:3857" && gj["type"] == "Point" {
			coords := gj["coordinates"].([]float64)
			return map[string]any{"type": "Point", "coordinates": []float64{coords[0] + 1, coords[1] + 1}}, true
		}
		return nil, false
	})
	gRes2 := ConvertGeometryLogicalValue(sample, geom).(map[string]any)
	ggeom2 := gRes2["geometry"].(map[string]any)
	require.Equal(t, []float64{2, 3}, ggeom2["coordinates"]) // reprojected
	gaRes2 := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	g3 := gaRes2["geometry"].(map[string]any)
	require.Equal(t, []float64{1, 2}, g3["coordinates"]) // unchanged for CRS84
	SetGeospatialReprojector(nil)

	// LineString and Polygon parsing
	ls := buildWKBLineString([][]float64{{0, 0}, {1, 1}})
	SetGeometryJSONMode(GeospatialModeGeoJSON)
	gLS := ConvertGeometryLogicalValue(ls, geom).(map[string]any)
	ggLS := gLS["geometry"].(map[string]any)
	require.Equal(t, "LineString", ggLS["type"])
	require.Equal(t, [][]float64{{0, 0}, {1, 1}}, ggLS["coordinates"])

	poly := buildWKBPolygon([][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}})
	gPoly := ConvertGeometryLogicalValue(poly, geom).(map[string]any)
	gPolyGeo := gPoly["geometry"].(map[string]any)
	require.Equal(t, "Polygon", gPolyGeo["type"])
	require.Equal(t, [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}}, gPolyGeo["coordinates"])

	// Hybrid raw selection: base64 vs hex
	SetGeographyJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(true)
	gaHybrid := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	_, hasHex := gaHybrid["wkb_hex"]
	b64, hasB64 := gaHybrid["wkb_b64"].(string)
	require.False(t, hasHex)
	require.True(t, hasB64)
	require.NotEmpty(t, b64)
	SetGeospatialHybridRawBase64(false)
}

func Test_GeometryAndGeography_AdditionalBranches(t *testing.T) {
	invalid := []byte{1, 99, 0, 0, 0}
	geom := parquet.NewGeometryType()
	crs := "EPSG:4326"
	geom.CRS = &crs
	SetGeometryJSONMode(GeospatialModeGeoJSON)
	g := ConvertGeometryLogicalValue(invalid, geom).(map[string]any)
	require.Equal(t, crs, g["crs"])
	require.NotEmpty(t, g["wkb_hex"].(string))

	SetGeometryJSONMode(GeospatialModeBase64)
	g2 := ConvertGeometryLogicalValue(string(invalid), geom).(map[string]any)
	require.Equal(t, base64.StdEncoding.EncodeToString(invalid), g2["wkb_b64"])
	require.Equal(t, crs, g2["crs"])

	SetGeometryJSONMode(GeospatialModeHybrid)
	g3 := ConvertGeometryLogicalValue(invalid, geom).(map[string]any)
	require.Equal(t, crs, g3["crs"])
	require.NotEmpty(t, g3["wkb_hex"]) // hex fallback

	geog := parquet.NewGeographyType()
	crs2 := "OGC:CRS84"
	geog.CRS = &crs2
	geog.Algorithm = nil
	SetGeographyJSONMode(GeospatialModeBase64)
	ga := ConvertGeographyLogicalValue(invalid, geog).(map[string]any)
	require.Equal(t, "SPHERICAL", ga["algorithm"]) // default
	require.Equal(t, crs2, ga["crs"])
	require.Equal(t, base64.StdEncoding.EncodeToString(invalid), ga["wkb_b64"])

	crs3 := "EPSG:3857"
	geog2 := parquet.NewGeographyType()
	geog2.CRS = &crs3
	algo := parquet.EdgeInterpolationAlgorithm_VINCENTY
	geog2.Algorithm = &algo
	SetGeographyJSONMode(GeospatialModeHybrid)
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	SetGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
		if crs == crs3 {
			coords := gj["coordinates"].([]float64)
			return map[string]any{"type": "Point", "coordinates": []float64{coords[0] + 0.5, coords[1] + 0.5}}, true
		}
		return nil, false
	})
	defer SetGeospatialReprojector(nil)
	out := ConvertGeographyLogicalValue(sample, geog2).(map[string]any)
	require.Equal(t, crs3, out["crs"])
	require.Equal(t, "VINCENTY", out["algorithm"])
	gj := out["geojson"].(map[string]any)
	require.Equal(t, "Point", gj["type"])
	require.Equal(t, []float64{1.5, 2.5}, gj["coordinates"]) // shifted
}

func Test_GeometryAndGeography_MoreModes(t *testing.T) {
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	SetGeometryJSONMode(GeospatialModeHex)
	geom := parquet.NewGeometryType()
	crs := "OGC:CRS84"
	geom.CRS = &crs
	g := ConvertGeometryLogicalValue(sample, geom).(map[string]any)
	require.Equal(t, crs, g["crs"])
	require.NotEmpty(t, g["wkb_hex"]) // raw hex

	SetGeometryJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(true)
	gh := ConvertGeometryLogicalValue(sample, geom).(map[string]any)
	require.Equal(t, crs, gh["crs"])
	require.NotNil(t, gh["geojson"]) // includes parsed geojson
	require.NotEmpty(t, gh["wkb_b64"])
	require.NotContains(t, gh, "wkb_hex")
	SetGeospatialHybridRawBase64(false)

	SetGeographyJSONMode(GeospatialModeGeoJSON)
	invalid := []byte{1, 99, 0, 0, 0}
	geog := parquet.NewGeographyType()
	crs2 := "OGC:CRS84"
	geog.CRS = &crs2
	out := ConvertGeographyLogicalValue(invalid, geog).(map[string]any)
	require.Equal(t, crs2, out["crs"])
	require.Equal(t, "SPHERICAL", out["algorithm"]) // default
	require.NotEmpty(t, out["wkb_hex"])             // fallback hex

	SetGeographyJSONMode(GeospatialModeHex)
	out2 := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	require.Equal(t, crs2, out2["crs"])
	require.Equal(t, "SPHERICAL", out2["algorithm"]) // still default
	require.NotEmpty(t, out2["wkb_hex"])             // hex

	SetGeographyJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(true)
	out3 := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	require.Equal(t, crs2, out3["crs"])
	require.Equal(t, "SPHERICAL", out3["algorithm"]) // default
	require.NotNil(t, out3["geojson"])
	require.NotEmpty(t, out3["wkb_b64"]) // base64 chosen
	require.NotContains(t, out3, "wkb_hex")
	SetGeospatialHybridRawBase64(false)
}

func Test_Geography_HybridFallbackAndStringInput(t *testing.T) {
	invalid := []byte{1, 99, 0, 0, 0}
	geog := parquet.NewGeographyType()
	crs := "OGC:CRS84"
	geog.CRS = &crs
	SetGeographyJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(true)
	out := ConvertGeographyLogicalValue(invalid, geog).(map[string]any)
	require.Equal(t, crs, out["crs"])
	require.NotEmpty(t, out["wkb_hex"]) // fallback is hex regardless
	SetGeospatialHybridRawBase64(false)

	SetGeographyJSONMode(GeospatialModeHex)
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	out2 := ConvertGeographyLogicalValue(string(sample), geog).(map[string]any)
	require.Equal(t, crs, out2["crs"])
	require.NotEmpty(t, out2["wkb_hex"]) // hex

	SetGeographyJSONMode(GeospatialModeHybrid)
	SetGeospatialHybridRawBase64(false)
	out3 := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	require.Equal(t, crs, out3["crs"])
	require.NotNil(t, out3["geojson"])   // include geojson
	require.NotEmpty(t, out3["wkb_hex"]) // hex raw selected
	require.NotContains(t, out3, "wkb_b64")
}

func Test_ConvertGeography_ReprojectorNoOp(t *testing.T) {
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	geog := parquet.NewGeographyType()
	crs := "EPSG:3857"
	geog.CRS = &crs
	SetGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
		return nil, false
	})
	defer SetGeospatialReprojector(nil)
	SetGeographyJSONMode(GeospatialModeGeoJSON)
	out := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	g := out["geometry"].(map[string]any)
	require.Equal(t, []float64{1, 2}, g["coordinates"]) // unchanged
}

func Test_ConvertGeography_GeoJSON_NoReproject(t *testing.T) {
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	geog := parquet.NewGeographyType()
	crs := "EPSG:4326"
	geog.CRS = &crs
	SetGeospatialReprojector(nil)
	SetGeographyJSONMode(GeospatialModeGeoJSON)
	out := ConvertGeographyLogicalValue(sample, geog).(map[string]any)
	g := out["geometry"].(map[string]any)
	require.Equal(t, []float64{1, 2}, g["coordinates"]) // unchanged geometry only
}

func Test_ConvertGeography_Defaults_NilGeoPointer(t *testing.T) {
	invalid := []byte{1, 99, 0, 0, 0}
	SetGeographyJSONMode(GeospatialModeBase64)
	out := ConvertGeographyLogicalValue(invalid, nil).(map[string]any)
	require.Equal(t, "OGC:CRS84", out["crs"])       // default CRS
	require.Equal(t, "SPHERICAL", out["algorithm"]) // default algorithm
	require.Equal(t, base64.StdEncoding.EncodeToString(invalid), out["wkb_b64"])
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

// Test roundCoordinate function edge cases
func Test_roundCoordinate(t *testing.T) {
	// Save original precision setting
	originalPrecision := geospatialCoordPrecision
	defer func() {
		geospatialCoordPrecision = originalPrecision
	}()

	tests := []struct {
		name      string
		precision int
		input     float64
		expected  float64
	}{
		{
			name:      "disabled_rounding",
			precision: -1,
			input:     1.23456789,
			expected:  1.23456789,
		},
		{
			name:      "zero_precision",
			precision: 0,
			input:     1.23456789,
			expected:  1.0,
		},
		{
			name:      "default_precision_6",
			precision: 6,
			input:     1.23456789,
			expected:  1.234568,
		},
		{
			name:      "high_precision_12",
			precision: 12,
			input:     1.123456789012345,
			expected:  1.123456789012,
		},
		{
			name:      "very_high_precision_20",
			precision: 20, // No clamping in roundCoordinate, unlike inline round function
			input:     1.123456789012345,
			expected:  1.123456789012345, // No change due to floating point precision limits
		},
		{
			name:      "negative_input",
			precision: 2,
			input:     -1.236,
			expected:  -1.24,
		},
		{
			name:      "zero_input",
			precision: 3,
			input:     0.0,
			expected:  0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geospatialCoordPrecision = tt.precision
			result := roundCoordinate(tt.input)
			require.Equal(t, tt.expected, result, "roundCoordinate failed for %s", tt.name)
		})
	}
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
	wkb := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x36, 0x96, 0x73, 0xd3, 0xad, 0xf9, 0xf1, 0x3f, 0xae, 0x95, 0x03, 0x4f, 0xb7, 0xe6, 0x07, 0x40}

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

func Test_wkbToGeoJSON_MultiGeometries(t *testing.T) {
	tests := []struct {
		name           string
		geometryType   string
		wkb            []byte
		expectedType   string
		validateCoords func(t *testing.T, coords interface{})
	}{
		{
			name:         "multipoint_with_points",
			geometryType: "MultiPoint",
			wkb:          []byte{0x01, 0x04, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40},
			expectedType: "MultiPoint",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][]float64)
				require.Len(t, c, 3)
				require.Equal(t, []float64{1, 2}, c[0])
				require.Equal(t, []float64{3, 4}, c[1])
				require.Equal(t, []float64{5, 6}, c[2])
			},
		},
		{
			name:         "multilinestring_with_lines",
			geometryType: "MultiLineString",
			wkb:          []byte{0x01, 0x05, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x01, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40},
			expectedType: "MultiLineString",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][][]float64)
				require.Len(t, c, 2)
				require.Len(t, c[0], 2)
				require.Equal(t, []float64{0, 0}, c[0][0])
				require.Equal(t, []float64{1, 1}, c[0][1])
				require.Len(t, c[1], 3)
				require.Equal(t, []float64{2, 2}, c[1][0])
				require.Equal(t, []float64{3, 3}, c[1][1])
				require.Equal(t, []float64{4, 4}, c[1][2])
			},
		},
		{
			name:         "multipolygon_with_polygons",
			geometryType: "MultiPolygon",
			wkb: buildWKBMultiPolygon([][][][2]float64{
				{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
				{{{2, 2}, {3, 2}, {3, 3}, {2, 3}, {2, 2}}},
			}),
			expectedType: "MultiPolygon",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][][][]float64)
				require.Len(t, c, 2)
				require.Len(t, c[0], 1)
				require.Len(t, c[0][0], 5)
				require.Equal(t, []float64{0, 0}, c[0][0][0])
				require.Equal(t, []float64{1, 0}, c[0][0][1])
				require.Equal(t, []float64{1, 1}, c[0][0][2])
				require.Equal(t, []float64{0, 1}, c[0][0][3])
				require.Equal(t, []float64{0, 0}, c[0][0][4])
				require.Len(t, c[1], 1)
				require.Len(t, c[1][0], 5)
				require.Equal(t, []float64{2, 2}, c[1][0][0])
				require.Equal(t, []float64{3, 2}, c[1][0][1])
				require.Equal(t, []float64{3, 3}, c[1][0][2])
				require.Equal(t, []float64{2, 3}, c[1][0][3])
				require.Equal(t, []float64{2, 2}, c[1][0][4])
			},
		},
		{
			name:         "empty_multipoint",
			geometryType: "MultiPoint",
			wkb:          []byte{0x01, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectedType: "MultiPoint",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][]float64)
				require.Len(t, c, 0)
			},
		},
		{
			name:         "empty_multilinestring",
			geometryType: "MultiLineString",
			wkb:          []byte{0x01, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectedType: "MultiLineString",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][][]float64)
				require.Len(t, c, 0)
			},
		},
		{
			name:         "empty_multipolygon",
			geometryType: "MultiPolygon",
			wkb:          buildWKBMultiPolygon([][][][2]float64{}),
			expectedType: "MultiPolygon",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][][][]float64)
				require.Len(t, c, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gj, ok := wkbToGeoJSON(tt.wkb)
			require.True(t, ok)
			require.Equal(t, tt.expectedType, gj["type"])
			tt.validateCoords(t, gj["coordinates"])
		})
	}
}

func Test_wkbToGeoJSON_PrecisionHandling(t *testing.T) {
	// Save original precision
	orig := geospatialCoordPrecision
	defer func() { geospatialCoordPrecision = orig }()

	tests := []struct {
		name           string
		precision      int
		geometryType   string
		wkb            []byte
		expectedType   string
		validateCoords func(t *testing.T, coords interface{})
	}{
		{
			name:         "precision_2_multipoint",
			precision:    2,
			geometryType: "MultiPoint",
			wkb:          []byte{0x01, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x6c, 0xeb, 0xa7, 0xff, 0xac, 0xf9, 0xf1, 0x3f, 0xd2, 0x1b, 0xee, 0x23, 0xb7, 0xe6, 0x07, 0x40},
			expectedType: "MultiPoint",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][]float64)
				require.Len(t, c, 1)
				require.InDelta(t, 1.12, c[0][0], 0.001)
				require.InDelta(t, 2.99, c[0][1], 0.001)
			},
		},
		{
			name:         "precision_0_multilinestring",
			precision:    0,
			geometryType: "MultiLineString",
			wkb:          []byte{0x01, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x39, 0xb4, 0xc8, 0x76, 0xbe, 0x9f, 0xfc, 0x3f, 0x96, 0x43, 0x8b, 0x6c, 0xe7, 0xfb, 0x00, 0x40, 0xd9, 0xce, 0xf7, 0x53, 0xe3, 0xa5, 0x0b, 0x40, 0xa6, 0x9b, 0xc4, 0x20, 0xb0, 0xf2, 0x13, 0x40},
			expectedType: "MultiLineString",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][][]float64)
				require.Equal(t, []float64{2, 2}, c[0][0])
				require.Equal(t, []float64{3, 5}, c[0][1])
			},
		},
		{
			name:         "precision_4_multilinestring",
			precision:    4,
			geometryType: "MultiLineString",
			wkb:          []byte{0x01, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x6c, 0xeb, 0xa7, 0xff, 0xac, 0xf9, 0xf1, 0x3f, 0xd2, 0x1b, 0xee, 0x23, 0xb7, 0xe6, 0x07, 0x40},
			expectedType: "MultiLineString",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][][]float64)
				require.InDelta(t, 1.1235, c[0][0][0], 0.0001)
				require.InDelta(t, 2.9877, c[0][0][1], 0.0001)
			},
		},
		{
			name:         "precision_disabled_multipoint",
			precision:    -1,
			geometryType: "MultiPoint",
			wkb:          []byte{0x01, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x36, 0x96, 0x73, 0xd3, 0xad, 0xf9, 0xf1, 0x3f, 0xae, 0x95, 0x03, 0x4f, 0xb7, 0xe6, 0x07, 0x40},
			expectedType: "MultiPoint",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][]float64)
				require.Equal(t, 1.123456789, c[0][0])
				require.Equal(t, 2.987654321, c[0][1])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetGeospatialCoordinatePrecision(tt.precision)
			gj, ok := wkbToGeoJSON(tt.wkb)
			require.True(t, ok)
			require.Equal(t, tt.expectedType, gj["type"])
			tt.validateCoords(t, gj["coordinates"])
		})
	}
}

func Test_wkbToGeoJSON_Endianness(t *testing.T) {
	tests := []struct {
		name           string
		geometryType   string
		wkb            []byte
		expectedType   string
		validateCoords func(t *testing.T, coords interface{})
	}{
		{
			name:         "big_endian_multipoint",
			geometryType: "MultiPoint",
			wkb:          []byte{0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x01, 0xbf, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x40, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectedType: "MultiPoint",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][]float64)
				require.Len(t, c, 2)
				require.Equal(t, []float64{-1.5, 2.25}, c[0])
				require.Equal(t, []float64{3.75, -4.0}, c[1])
			},
		},
		{
			name:         "big_endian_multilinestring",
			geometryType: "MultiLineString",
			wkb:          []byte{0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x3f, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectedType: "MultiLineString",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][][]float64)
				require.Len(t, c, 1)
				require.Len(t, c[0], 2)
				require.Equal(t, []float64{0.5, 1.5}, c[0][0])
				require.Equal(t, []float64{2.5, 3.5}, c[0][1])
			},
		},
		{
			name:         "big_endian_multipolygon",
			geometryType: "MultiPolygon",
			wkb:          []byte{0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectedType: "MultiPolygon",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([][][][]float64)
				require.Len(t, c, 1)
				require.Len(t, c[0], 1)
				require.Len(t, c[0][0], 4)
				require.Equal(t, []float64{0, 0}, c[0][0][0])
				require.Equal(t, []float64{1, 0}, c[0][0][1])
				require.Equal(t, []float64{0.5, 1}, c[0][0][2])
				require.Equal(t, []float64{0, 0}, c[0][0][3])
			},
		},
		{
			name:         "little_endian_point",
			geometryType: "Point",
			wkb:          []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xc0},
			expectedType: "Point",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([]float64)
				require.Equal(t, []float64{1.5, -2.25}, c)
			},
		},
		{
			name:         "big_endian_point",
			geometryType: "Point",
			wkb:          []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0xc0, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectedType: "Point",
			validateCoords: func(t *testing.T, coords interface{}) {
				c := coords.([]float64)
				require.Equal(t, []float64{-10.0, 42.0}, c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gj, ok := wkbToGeoJSON(tt.wkb)
			require.True(t, ok)
			require.Equal(t, tt.expectedType, gj["type"])
			tt.validateCoords(t, gj["coordinates"])
		})
	}
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
	pointWKB := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x40}
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
	pointWKB := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40}
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
	pointWKB := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40}
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

// Test error handling in parseLineString
func Test_parseLineString_ErrorHandling(t *testing.T) {
	tests := []struct {
		name     string
		buffer   []byte
		be       bool
		off      int
		expectOK bool
	}{
		{
			name:     "insufficient_buffer_for_point_count",
			buffer:   []byte{1, 2, 3}, // too short for 4 bytes
			be:       false,
			off:      0,
			expectOK: false,
		},
		{
			name:     "insufficient_buffer_for_coordinates",
			buffer:   append([]byte{2, 0, 0, 0}, make([]byte, 10)...), // says 2 points but only 10 bytes available
			be:       false,
			off:      0,
			expectOK: false,
		},
		{
			name:     "valid_empty_linestring",
			buffer:   []byte{0, 0, 0, 0}, // 0 points
			be:       false,
			off:      0,
			expectOK: true,
		},
		{
			name:     "valid_single_point_linestring",
			buffer:   append([]byte{1, 0, 0, 0}, make([]byte, 16)...), // 1 point, 16 bytes of coords
			be:       false,
			off:      0,
			expectOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, ok := parseLineString(tt.buffer, tt.be, tt.off)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test error handling in parsePolygon
func Test_parsePolygon_ErrorHandling(t *testing.T) {
	tests := []struct {
		name     string
		buffer   []byte
		be       bool
		off      int
		expectOK bool
	}{
		{
			name:     "insufficient_buffer_for_ring_count",
			buffer:   []byte{1, 2}, // too short for 4 bytes
			be:       false,
			off:      0,
			expectOK: false,
		},
		{
			name:     "insufficient_buffer_for_point_count_in_ring",
			buffer:   []byte{1, 0, 0, 0, 2, 3}, // 1 ring but insufficient data for point count
			be:       false,
			off:      0,
			expectOK: false,
		},
		{
			name:     "insufficient_buffer_for_coordinates_in_ring",
			buffer:   append([]byte{1, 0, 0, 0, 3, 0, 0, 0}, make([]byte, 10)...), // 1 ring, 3 points but only 10 bytes
			be:       false,
			off:      0,
			expectOK: false,
		},
		{
			name:     "valid_empty_polygon",
			buffer:   []byte{0, 0, 0, 0}, // 0 rings
			be:       false,
			off:      0,
			expectOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, ok := parsePolygon(tt.buffer, tt.be, tt.off)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test calculateWKBSize error paths and edge cases
func Test_calculateWKBSize_ErrorHandling(t *testing.T) {
	tests := []struct {
		name     string
		wkb      []byte
		expectOK bool
	}{
		{
			name:     "insufficient_header",
			wkb:      []byte{1, 2, 3}, // less than 5 bytes
			expectOK: false,
		},
		{
			name:     "unknown_geometry_type",
			wkb:      []byte{1, 99, 0, 0, 0}, // unknown type 99
			expectOK: false,
		},
		{
			name:     "truncated_multipoint",
			wkb:      []byte{1, 4, 0, 0, 0}, // MultiPoint but no count
			expectOK: false,
		},
		{
			name:     "truncated_multilinestring",
			wkb:      []byte{1, 5, 0, 0, 0}, // MultiLineString but no count
			expectOK: false,
		},
		{
			name:     "truncated_multipolygon",
			wkb:      []byte{1, 6, 0, 0, 0}, // MultiPolygon but no count
			expectOK: false,
		},
		{
			name:     "truncated_geometrycollection",
			wkb:      []byte{1, 7, 0, 0, 0}, // GeometryCollection but no count
			expectOK: false,
		},
		{
			name:     "multilinestring_invalid_linestring",
			wkb:      []byte{1, 5, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0}, // MultiLineString with invalid nested linestring
			expectOK: false,
		},
		{
			name:     "multipolygon_invalid_polygon",
			wkb:      []byte{1, 6, 0, 0, 0, 1, 0, 0, 0, 1, 3, 0}, // MultiPolygon with invalid nested polygon
			expectOK: false,
		},
		{
			name:     "geometrycollection_invalid_subgeometry",
			wkb:      []byte{1, 7, 0, 0, 0, 1, 0, 0, 0, 1, 99}, // GeometryCollection with invalid subgeometry
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := calculateWKBSize(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test wkbToGeoJSON comprehensive error handling
func Test_wkbToGeoJSON_ComprehensiveErrorHandling(t *testing.T) {
	tests := []struct {
		name     string
		wkb      []byte
		expectOK bool
	}{
		{
			name:     "empty_buffer",
			wkb:      []byte{},
			expectOK: false,
		},
		{
			name:     "too_short_buffer",
			wkb:      []byte{1, 2},
			expectOK: false,
		},
		{
			name:     "invalid_geometry_type_in_header",
			wkb:      []byte{1, 2, 3, 4}, // only 4 bytes, need 5
			expectOK: false,
		},
		{
			name:     "unknown_geometry_type",
			wkb:      []byte{1, 99, 0, 0, 0}, // unknown type 99
			expectOK: false,
		},
		{
			name:     "point_insufficient_data",
			wkb:      []byte{1, 1, 0, 0, 0, 1, 2, 3}, // Point but insufficient coordinate data
			expectOK: false,
		},
		{
			name:     "linestring_insufficient_data",
			wkb:      []byte{1, 2, 0, 0, 0, 1, 0, 0, 0}, // LineString with 1 point but no coordinates
			expectOK: false,
		},
		{
			name:     "polygon_insufficient_data",
			wkb:      []byte{1, 3, 0, 0, 0, 1, 0, 0, 0}, // Polygon with 1 ring but no points
			expectOK: false,
		},
		{
			name:     "multipoint_invalid_point_type",
			wkb:      []byte{1, 4, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0}, // MultiPoint with wrong inner type
			expectOK: false,
		},
		{
			name:     "multilinestring_invalid_linestring_type",
			wkb:      []byte{1, 5, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0}, // MultiLineString with wrong inner type (Point instead of LineString)
			expectOK: false,
		},
		{
			name:     "multipolygon_invalid_polygon_type",
			wkb:      []byte{1, 6, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0}, // MultiPolygon with wrong inner type (Point instead of Polygon)
			expectOK: false,
		},
		{
			name:     "multipoint_invalid_inner_type",
			wkb:      []byte{1, 4, 0, 0, 0, 1, 0, 0, 0, 1, 99, 0, 0, 0},
			expectOK: false,
		},
		{
			name:     "multipoint_truncated",
			wkb:      []byte{1, 4, 0, 0, 0, 1, 0, 0, 0},
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := wkbToGeoJSON(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test SetGeospatialHybridRawBase64 configuration function
func Test_SetGeospatialHybridRawBase64(t *testing.T) {
	// Save original setting
	originalSetting := geospatialHybridUseBase64
	defer func() {
		geospatialHybridUseBase64 = originalSetting
	}()

	// Test setting to true
	SetGeospatialHybridRawBase64(true)
	require.True(t, geospatialHybridUseBase64)

	// Test setting to false
	SetGeospatialHybridRawBase64(false)
	require.False(t, geospatialHybridUseBase64)
}

// Test SetGeospatialReprojector configuration function
func Test_SetGeospatialReprojector(t *testing.T) {
	// Save original reprojector
	originalReprojector := geospatialReprojector
	defer func() {
		geospatialReprojector = originalReprojector
	}()

	// Test setting a custom reprojector
	customReprojector := func(crs string, geojson map[string]any) (map[string]any, bool) {
		return map[string]any{"transformed": true}, true
	}
	SetGeospatialReprojector(customReprojector)
	require.NotNil(t, geospatialReprojector)

	// Test setting nil reprojector
	SetGeospatialReprojector(nil)
	require.Nil(t, geospatialReprojector)
}

// Test edge cases for wkbToGeoJSON with big-endian byte order issues
func Test_wkbToGeoJSON_BigEndian_EdgeCases(t *testing.T) {
	// Test MultiPoint with mixed endianness in header vs points
	tests := []struct {
		name     string
		wkb      []byte
		expectOK bool
	}{
		{
			name:     "multipoint_big_endian_header_little_endian_points",
			wkb:      []byte{0, 0, 0, 0, 4, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64},
			expectOK: false, // Mixed endianness within multi-geometries may not be supported
		},
		{
			name:     "multilinestring_insufficient_data_for_linestring_header",
			wkb:      []byte{1, 5, 0, 0, 0, 1, 0, 0, 0, 1},
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := wkbToGeoJSON(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test wrapGeoJSONHybrid edge cases
func Test_wrapGeoJSONHybrid_EdgeCases(t *testing.T) {
	geo := map[string]any{"type": "Point", "coordinates": []float64{1, 2}}
	raw := []byte{1, 2, 3, 4}

	// Test with include=false
	result := wrapGeoJSONHybrid(geo, raw, false, false)
	require.Contains(t, result, "geojson")
	require.NotContains(t, result, "wkb_hex")
	require.NotContains(t, result, "wkb_b64")

	// Test with include=true, useBase64=false
	result = wrapGeoJSONHybrid(geo, raw, false, true)
	require.Contains(t, result, "geojson")
	require.Contains(t, result, "wkb_hex")
	require.Equal(t, hex.EncodeToString(raw), result["wkb_hex"])

	// Test with include=true, useBase64=true
	result = wrapGeoJSONHybrid(geo, raw, true, true)
	require.Contains(t, result, "geojson")
	require.Contains(t, result, "wkb_b64")
	require.Equal(t, base64.StdEncoding.EncodeToString(raw), result["wkb_b64"])
}

// Test calculateWKBSize with valid geometries to improve coverage
func Test_calculateWKBSize_ValidGeometries(t *testing.T) {
	tests := []struct {
		name         string
		wkb          []byte
		expectedSize int
		expectOK     bool
	}{
		{
			name:         "point_little_endian",
			wkb:          wkbPoint(1, 1.0, 2.0),
			expectedSize: 21, // 1 + 4 + 16
			expectOK:     true,
		},
		{
			name:         "point_big_endian",
			wkb:          wkbPoint(0, 1.0, 2.0),
			expectedSize: 21, // 1 + 4 + 16
			expectOK:     true,
		},
		{
			name:         "linestring_empty",
			wkb:          []byte{1, 2, 0, 0, 0, 0, 0, 0, 0},
			expectedSize: 9, // 1 + 4 + 4
			expectOK:     true,
		},
		{
			name:         "multipoint_empty",
			wkb:          []byte{1, 4, 0, 0, 0, 0, 0, 0, 0},
			expectedSize: 9, // 1 + 4 + 4
			expectOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, ok := calculateWKBSize(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
			if ok {
				require.Equal(t, tt.expectedSize, size)
			}
		})
	}
}

// Test inline round function edge cases within wkbToGeoJSON
func Test_wkbToGeoJSON_InlineRoundFunction(t *testing.T) {
	// Save original precision setting
	originalPrecision := geospatialCoordPrecision
	defer func() {
		geospatialCoordPrecision = originalPrecision
	}()

	tests := []struct {
		name      string
		precision int
		wkb       []byte
		expectedX float64
		expectedY float64
	}{
		{
			name:      "inline_round_negative_precision",
			precision: -5, // This should return unrounded values
			wkb:       wkbPoint(1, 1.23456789, 2.87654321),
			expectedX: 1.23456789, // no rounding applied
			expectedY: 2.87654321,
		},
		{
			name:      "inline_round_positive_precision",
			precision: 2, // Simple case
			wkb:       wkbPoint(1, 1.236, 2.874),
			expectedX: 1.24, // rounded to 2 decimal places
			expectedY: 2.87,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geospatialCoordPrecision = tt.precision
			result, ok := wkbToGeoJSON(tt.wkb)
			require.True(t, ok)
			require.Equal(t, "Point", result["type"])

			coords := result["coordinates"].([]float64)
			require.Len(t, coords, 2)
			require.Equal(t, tt.expectedX, coords[0])
			require.Equal(t, tt.expectedY, coords[1])
		})
	}
}

// Test calculateWKBSize big-endian edge cases for better coverage
func Test_calculateWKBSize_BigEndianPaths(t *testing.T) {
	tests := []struct {
		name         string
		wkb          []byte
		expectedSize int
		expectOK     bool
	}{
		{
			name:         "multilinestring_big_endian",
			wkb:          []byte{0, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 63, 240, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 8, 0, 0, 0, 0, 0, 0},
			expectedSize: 50, // 1 + 4 + 4 + (1 + 4 + 4 + 32)
			expectOK:     true,
		},
		{
			name:         "multipolygon_big_endian",
			wkb:          []byte{0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 63, 240, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 8, 0, 0, 0, 0, 0, 0, 64, 16, 0, 0, 0, 0, 0, 0, 64, 20, 0, 0, 0, 0, 0, 0, 64, 24, 0, 0, 0, 0, 0, 0, 64, 28, 0, 0, 0, 0, 0, 0},
			expectedSize: 86, // 1+4+4 + (1+4+4 + 4 + 64)
			expectOK:     true,
		},
		{
			name:     "multilinestring_insufficient_data_big_endian",
			wkb:      []byte{0, 0, 0, 0, 5, 0, 0, 0, 1, 0, 2, 0, 0, 0},
			expectOK: false,
		},
		{
			name:     "multipolygon_insufficient_data_big_endian",
			wkb:      []byte{0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 3, 0, 0, 0},
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, ok := calculateWKBSize(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
			if ok {
				require.Equal(t, tt.expectedSize, size)
			}
		})
	}
}

// Test more comprehensive error paths in wkbToGeoJSON
func Test_wkbToGeoJSON_AdvancedErrorPaths(t *testing.T) {
	tests := []struct {
		name     string
		wkb      []byte
		expectOK bool
	}{
		{
			name:     "multipoint_insufficient_data_for_coordinates",
			wkb:      []byte{1, 4, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectOK: false,
		},
		{
			name:     "multilinestring_insufficient_point_data",
			wkb:      []byte{1, 5, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectOK: false,
		},
		{
			name:     "multipolygon_insufficient_ring_data",
			wkb:      []byte{1, 6, 0, 0, 0, 1, 0, 0, 0, 1, 3, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectOK: false,
		},
		{
			name:     "geometry_collection_recursive_failure",
			wkb:      []byte{1, 7, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := wkbToGeoJSON(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test boundary conditions and edge cases for calculateWKBSize
func Test_calculateWKBSize_BoundaryConditions(t *testing.T) {
	tests := []struct {
		name     string
		wkb      []byte
		expectOK bool
	}{
		{
			name:     "multilinestring_empty_with_zero_lines",
			wkb:      []byte{1, 5, 0, 0, 0, 0, 0, 0, 0},
			expectOK: true,
		},
		{
			name:     "geometrycollection_empty_with_zero_geometries",
			wkb:      []byte{1, 7, 0, 0, 0, 0, 0, 0, 0},
			expectOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := calculateWKBSize(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test edge cases in parseLineString and parsePolygon with boundary conditions
func Test_parseGeometry_BoundaryConditions(t *testing.T) {
	// Test parseLineString with edge case: point count at buffer boundary
	t.Run("parseLineString_point_count_at_boundary", func(t *testing.T) {
		// Buffer with exactly enough space for point count but no coordinates
		buffer := []byte{1, 0, 0, 0} // 1 point
		_, _, ok := parseLineString(buffer, false, 0)
		require.False(t, ok) // Should fail because no space for coordinates
	})

	t.Run("parsePolygon_ring_count_at_boundary", func(t *testing.T) {
		// Buffer with exactly enough space for ring count but no ring data
		buffer := []byte{1, 0, 0, 0} // 1 ring
		_, _, ok := parsePolygon(buffer, false, 0)
		require.False(t, ok) // Should fail because no space for ring data
	})
}

// Test specific uncovered paths in parsePolygon
func Test_parsePolygon_UncoveredPaths(t *testing.T) {
	tests := []struct {
		name     string
		buffer   []byte
		be       bool
		off      int
		expectOK bool
	}{
		{
			name:     "parsePolygon_big_endian_numRings",
			buffer:   []byte{0, 0, 0, 1, 0, 0, 0, 3},
			be:       true, // This should trigger the big-endian path for numRings
			off:      0,
			expectOK: false, // Will fail due to insufficient coordinate data
		},
		{
			name:     "parsePolygon_big_endian_numPoints_in_ring",
			buffer:   []byte{1, 0, 0, 0, 0, 0, 0, 2},
			be:       true, // This should trigger big-endian path for numPoints in ring
			off:      0,
			expectOK: false, // Will fail due to insufficient coordinate data
		},
		{
			name:     "parsePolygon_parsePoint_failure_in_ring",
			buffer:   []byte{1, 0, 0, 0, 1, 0, 0, 0, 120, 86, 52, 18},
			be:       false,
			off:      0,
			expectOK: false, // Should fail when parsePoint fails
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, ok := parsePolygon(tt.buffer, tt.be, tt.off)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test specific uncovered paths in wkbToGeoJSON MultiLineString
func Test_wkbToGeoJSON_MultiLineString_UncoveredPaths(t *testing.T) {
	tests := []struct {
		name     string
		wkb      []byte
		expectOK bool
	}{
		{
			name:     "multilinestring_big_endian_point_count",
			wkb:      []byte{0, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectOK: false, // Should fail due to insufficient coordinate data
		},
		{
			name:     "multipolygon_big_endian_ring_point_count",
			wkb:      []byte{0, 0, 0, 0, 6, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectOK: false, // Should fail due to insufficient coordinate data
		},
		{
			name:     "multipoint_at_buffer_boundary",
			wkb:      []byte{1, 4, 0, 0, 0, 1, 0, 0, 0},
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := wkbToGeoJSON(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test very specific boundary conditions for maximum coverage
func Test_wkbToGeoJSON_PrecisionBoundaryConditions(t *testing.T) {
	// Save original precision setting
	originalPrecision := geospatialCoordPrecision
	defer func() {
		geospatialCoordPrecision = originalPrecision
	}()

	t.Run("inline_round_exactly_12_precision", func(t *testing.T) {
		// Test the p > 12 branch and p = 12 assignment in inline round function
		geospatialCoordPrecision = 12 // Exactly at the boundary
		wkb := wkbPoint(1, 1.123456789012345, 2.0)
		result, ok := wkbToGeoJSON(wkb)
		require.True(t, ok)
		require.Equal(t, "Point", result["type"])

		coords := result["coordinates"].([]float64)
		require.Len(t, coords, 2)
		require.Equal(t, 1.123456789012, coords[0]) // Should be rounded to 12 places
	})

	t.Run("inline_round_exactly_0_precision", func(t *testing.T) {
		// Test the p < 0 branch and p = 0 assignment
		geospatialCoordPrecision = 0 // Exactly at the boundary
		wkb := wkbPoint(1, 1.7, 2.3)
		result, ok := wkbToGeoJSON(wkb)
		require.True(t, ok)
		require.Equal(t, "Point", result["type"])

		coords := result["coordinates"].([]float64)
		require.Len(t, coords, 2)
		require.Equal(t, 2.0, coords[0]) // Should be rounded to 0 places
		require.Equal(t, 2.0, coords[1])
	})
}

// Test u32 function error paths in wkbToGeoJSON
func Test_wkbToGeoJSON_u32_ErrorPaths(t *testing.T) {
	tests := []struct {
		name     string
		wkb      []byte
		expectOK bool
	}{
		{
			name:     "u32_read_geometry_type_fails",
			wkb:      []byte{1, 2, 3, 4}, // Only 4 bytes total, u32(1) will fail
			expectOK: false,
		},
		{
			name:     "multipoint_u32_point_type_fails",
			wkb:      []byte{1, 4, 0, 0, 0, 1, 0, 0, 0, 1, 1, 2},
			expectOK: false,
		},
		{
			name:     "multilinestring_u32_linestring_type_fails",
			wkb:      []byte{1, 5, 0, 0, 0, 1, 0, 0, 0, 1, 2, 3},
			expectOK: false,
		},
		{
			name:     "multipolygon_u32_polygon_type_fails",
			wkb:      []byte{1, 6, 0, 0, 0, 1, 0, 0, 0, 1, 3, 4},
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := wkbToGeoJSON(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test the remaining very specific paths to maximize coverage
func Test_wkbToGeoJSON_MaximumCoverage(t *testing.T) {
	tests := []struct {
		name     string
		wkb      []byte
		expectOK bool
	}{
		{
			name:     "multilinestring_little_endian_point_count",
			wkb:      []byte{1, 5, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectOK: false,
		},
		{
			name:     "multipolygon_little_endian_ring_point_count",
			wkb:      []byte{1, 6, 0, 0, 0, 1, 0, 0, 0, 1, 3, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := wkbToGeoJSON(tt.wkb)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test the remaining parsePolygon big-endian paths
func Test_parsePolygon_MaximumCoverage(t *testing.T) {
	t.Run("parsePolygon_big_endian_ring_point_count", func(t *testing.T) {
		// Test the big-endian path for reading ring point count
		buffer := make([]byte, 16)
		binary.BigEndian.PutUint32(buffer[0:4], 1) // numRings = 1 (big-endian)
		binary.BigEndian.PutUint32(buffer[4:8], 2) // numPoints = 2 (big-endian)
		// Not enough coordinate data will cause failure

		_, _, ok := parsePolygon(buffer, true, 0) // be = true to trigger big-endian path
		require.False(t, ok)                      // Should fail due to insufficient coordinate data
	})

	t.Run("parsePolygon_little_endian_ring_point_count", func(t *testing.T) {
		// Test the little-endian path for reading ring point count
		buffer := make([]byte, 16)
		binary.LittleEndian.PutUint32(buffer[0:4], 1) // numRings = 1 (little-endian)
		binary.LittleEndian.PutUint32(buffer[4:8], 2) // numPoints = 2 (little-endian)
		// Not enough coordinate data will cause failure

		_, _, ok := parsePolygon(buffer, false, 0) // be = false to trigger little-endian path
		require.False(t, ok)                       // Should fail due to insufficient coordinate data
	})
}

// Test edge case where GeometryCollection buffer ends exactly at geometry boundary
func Test_wkbToGeoJSON_GeometryCollection_BufferBoundary(t *testing.T) {
	t.Run("geometrycollection_buffer_ends_at_geometry_boundary", func(t *testing.T) {
		wkb := []byte{1, 7, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0}

		_, ok := wkbToGeoJSON(wkb)
		require.False(t, ok) // Should fail when calculateWKBSize fails
	})
}
