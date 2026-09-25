package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
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
func TestConvertGeometryAndGeographyLogicalValue(t *testing.T) {
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
	cfgGeoJSON := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON))
	gRes := ConvertGeometryLogicalValue(sample, geom, cfgGeoJSON)
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
	cfgGeogGeoJSON := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeGeoJSON))
	gaRes := ConvertGeographyLogicalValue(sample, geog, cfgGeogGeoJSON)
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
	require.Nil(t, ConvertGeometryLogicalValue(nil, geom, cfgGeoJSON))
	require.Nil(t, ConvertGeographyLogicalValue(nil, geog, cfgGeogGeoJSON))

	// Reprojection hook test: fake reprojection that adds +1 to coords
	cfgReproj := NewGeospatialConfig(
		WithGeometryJSONMode(GeospatialModeGeoJSON),
		WithGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
			if crs == "EPSG:3857" && gj["type"] == "Point" {
				coords := gj["coordinates"].([]float64)
				return map[string]any{"type": "Point", "coordinates": []float64{coords[0] + 1, coords[1] + 1}}, true
			}
			return nil, false
		}),
	)
	cfgReprojGeog := NewGeospatialConfig(
		WithGeographyJSONMode(GeospatialModeGeoJSON),
		WithGeospatialReprojector(cfgReproj.Reprojector),
	)
	gRes2 := ConvertGeometryLogicalValue(sample, geom, cfgReproj).(map[string]any)
	ggeom2 := gRes2["geometry"].(map[string]any)
	require.Equal(t, []float64{2, 3}, ggeom2["coordinates"]) // reprojected
	gaRes2 := ConvertGeographyLogicalValue(sample, geog, cfgReprojGeog).(map[string]any)
	g3 := gaRes2["geometry"].(map[string]any)
	require.Equal(t, []float64{1, 2}, g3["coordinates"]) // unchanged for CRS84

	// LineString and Polygon parsing
	ls := buildWKBLineString([][]float64{{0, 0}, {1, 1}})
	gLS := ConvertGeometryLogicalValue(ls, geom, cfgGeoJSON).(map[string]any)
	ggLS := gLS["geometry"].(map[string]any)
	require.Equal(t, "LineString", ggLS["type"])
	require.Equal(t, [][]float64{{0, 0}, {1, 1}}, ggLS["coordinates"])

	poly := buildWKBPolygon([][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}})
	gPoly := ConvertGeometryLogicalValue(poly, geom, cfgGeoJSON).(map[string]any)
	gPolyGeo := gPoly["geometry"].(map[string]any)
	require.Equal(t, "Polygon", gPolyGeo["type"])
	require.Equal(t, [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}}, gPolyGeo["coordinates"])

	// Hybrid raw selection: base64 vs hex
	cfgHybridB64 := NewGeospatialConfig(
		WithGeographyJSONMode(GeospatialModeHybrid),
		WithGeospatialHybridRawBase64(true),
	)
	gaHybrid := ConvertGeographyLogicalValue(sample, geog, cfgHybridB64).(map[string]any)
	_, hasHex := gaHybrid["wkb_hex"]
	b64, hasB64 := gaHybrid["wkb_b64"].(string)
	require.False(t, hasHex)
	require.True(t, hasB64)
	require.NotEmpty(t, b64)
}

func TestGeometryAndGeography_AdditionalBranches(t *testing.T) {
	invalid := []byte{1, 99, 0, 0, 0}
	geom := parquet.NewGeometryType()
	crs := "EPSG:4326"
	geom.CRS = &crs
	cfgGeoJSON := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON))
	g := ConvertGeometryLogicalValue(invalid, geom, cfgGeoJSON).(map[string]any)
	require.Equal(t, crs, g["crs"])
	require.NotEmpty(t, g["wkb_hex"].(string))

	cfgBase64 := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeBase64))
	g2 := ConvertGeometryLogicalValue(string(invalid), geom, cfgBase64).(map[string]any)
	require.Equal(t, base64.StdEncoding.EncodeToString(invalid), g2["wkb_b64"])
	require.Equal(t, crs, g2["crs"])

	cfgHybrid := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeHybrid))
	g3 := ConvertGeometryLogicalValue(invalid, geom, cfgHybrid).(map[string]any)
	require.Equal(t, crs, g3["crs"])
	require.NotEmpty(t, g3["wkb_hex"]) // hex fallback

	geog := parquet.NewGeographyType()
	crs2 := "OGC:CRS84"
	geog.CRS = &crs2
	geog.Algorithm = nil
	cfgGeogBase64 := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeBase64))
	ga := ConvertGeographyLogicalValue(invalid, geog, cfgGeogBase64).(map[string]any)
	require.Equal(t, "SPHERICAL", ga["algorithm"]) // default
	require.Equal(t, crs2, ga["crs"])
	require.Equal(t, base64.StdEncoding.EncodeToString(invalid), ga["wkb_b64"])

	crs3 := "EPSG:3857"
	geog2 := parquet.NewGeographyType()
	geog2.CRS = &crs3
	algo := parquet.EdgeInterpolationAlgorithm_VINCENTY
	geog2.Algorithm = &algo
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	cfgGeogHybridReproj := NewGeospatialConfig(
		WithGeographyJSONMode(GeospatialModeHybrid),
		WithGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
			if crs == crs3 {
				coords := gj["coordinates"].([]float64)
				return map[string]any{"type": "Point", "coordinates": []float64{coords[0] + 0.5, coords[1] + 0.5}}, true
			}
			return nil, false
		}),
	)
	out := ConvertGeographyLogicalValue(sample, geog2, cfgGeogHybridReproj).(map[string]any)
	require.Equal(t, crs3, out["crs"])
	require.Equal(t, "VINCENTY", out["algorithm"])
	gj := out["geojson"].(map[string]any)
	require.Equal(t, "Point", gj["type"])
	require.Equal(t, []float64{1.5, 2.5}, gj["coordinates"]) // shifted
}

func TestGeometryAndGeography_MoreModes(t *testing.T) {
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	geom := parquet.NewGeometryType()
	crs := "OGC:CRS84"
	geom.CRS = &crs
	cfgHex := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeHex))
	g := ConvertGeometryLogicalValue(sample, geom, cfgHex).(map[string]any)
	require.Equal(t, crs, g["crs"])
	require.NotEmpty(t, g["wkb_hex"]) // raw hex

	cfgHybridB64 := NewGeospatialConfig(
		WithGeometryJSONMode(GeospatialModeHybrid),
		WithGeospatialHybridRawBase64(true),
	)
	gh := ConvertGeometryLogicalValue(sample, geom, cfgHybridB64).(map[string]any)
	require.Equal(t, crs, gh["crs"])
	require.NotNil(t, gh["geojson"]) // includes parsed geojson
	require.NotEmpty(t, gh["wkb_b64"])
	require.NotContains(t, gh, "wkb_hex")

	cfgGeogGeoJSON := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeGeoJSON))
	invalid := []byte{1, 99, 0, 0, 0}
	geog := parquet.NewGeographyType()
	crs2 := "OGC:CRS84"
	geog.CRS = &crs2
	out := ConvertGeographyLogicalValue(invalid, geog, cfgGeogGeoJSON).(map[string]any)
	require.Equal(t, crs2, out["crs"])
	require.Equal(t, "SPHERICAL", out["algorithm"]) // default
	require.NotEmpty(t, out["wkb_hex"])             // fallback hex

	cfgGeogHex := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeHex))
	out2 := ConvertGeographyLogicalValue(sample, geog, cfgGeogHex).(map[string]any)
	require.Equal(t, crs2, out2["crs"])
	require.Equal(t, "SPHERICAL", out2["algorithm"]) // still default
	require.NotEmpty(t, out2["wkb_hex"])             // hex

	cfgGeogHybridB64 := NewGeospatialConfig(
		WithGeographyJSONMode(GeospatialModeHybrid),
		WithGeospatialHybridRawBase64(true),
	)
	out3 := ConvertGeographyLogicalValue(sample, geog, cfgGeogHybridB64).(map[string]any)
	require.Equal(t, crs2, out3["crs"])
	require.Equal(t, "SPHERICAL", out3["algorithm"]) // default
	require.NotNil(t, out3["geojson"])
	require.NotEmpty(t, out3["wkb_b64"]) // base64 chosen
	require.NotContains(t, out3, "wkb_hex")
}

func TestGeography_HybridFallbackAndStringInput(t *testing.T) {
	invalid := []byte{1, 99, 0, 0, 0}
	geog := parquet.NewGeographyType()
	crs := "OGC:CRS84"
	geog.CRS = &crs
	cfgHybridB64 := NewGeospatialConfig(
		WithGeographyJSONMode(GeospatialModeHybrid),
		WithGeospatialHybridRawBase64(true),
	)
	out := ConvertGeographyLogicalValue(invalid, geog, cfgHybridB64).(map[string]any)
	require.Equal(t, crs, out["crs"])
	require.NotEmpty(t, out["wkb_hex"]) // fallback is hex regardless

	cfgHex := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeHex))
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	out2 := ConvertGeographyLogicalValue(string(sample), geog, cfgHex).(map[string]any)
	require.Equal(t, crs, out2["crs"])
	require.NotEmpty(t, out2["wkb_hex"]) // hex

	cfgHybridHex := NewGeospatialConfig(
		WithGeographyJSONMode(GeospatialModeHybrid),
		WithGeospatialHybridRawBase64(false),
	)
	out3 := ConvertGeographyLogicalValue(sample, geog, cfgHybridHex).(map[string]any)
	require.Equal(t, crs, out3["crs"])
	require.NotNil(t, out3["geojson"])   // include geojson
	require.NotEmpty(t, out3["wkb_hex"]) // hex raw selected
	require.NotContains(t, out3, "wkb_b64")
}

func TestConvertGeography_ReprojectorNoOp(t *testing.T) {
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	geog := parquet.NewGeographyType()
	crs := "EPSG:3857"
	geog.CRS = &crs
	cfg := NewGeospatialConfig(
		WithGeographyJSONMode(GeospatialModeGeoJSON),
		WithGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
			return nil, false
		}),
	)
	out := ConvertGeographyLogicalValue(sample, geog, cfg).(map[string]any)
	g := out["geometry"].(map[string]any)
	require.Equal(t, []float64{1, 2}, g["coordinates"]) // unchanged
}

func TestConvertGeography_GeoJSON_NoReproject(t *testing.T) {
	sample := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64}
	geog := parquet.NewGeographyType()
	crs := "EPSG:4326"
	geog.CRS = &crs
	cfg := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeGeoJSON))
	out := ConvertGeographyLogicalValue(sample, geog, cfg).(map[string]any)
	g := out["geometry"].(map[string]any)
	require.Equal(t, []float64{1, 2}, g["coordinates"]) // unchanged geometry only
}

func TestConvertGeography_Defaults_NilGeoPointer(t *testing.T) {
	invalid := []byte{1, 99, 0, 0, 0}
	cfg := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeBase64))
	out := ConvertGeographyLogicalValue(invalid, nil, cfg).(map[string]any)
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
func TestWrapGeoJSONHybrid(t *testing.T) {
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

func TestWithGeospatialGeoJSONAsFeature(t *testing.T) {
	// Test setting to true
	cfg := NewGeospatialConfig(WithGeospatialGeoJSONAsFeature(true))
	require.True(t, cfg.GeoJSONAsFeature)

	// Test setting to false
	cfg2 := NewGeospatialConfig(WithGeospatialGeoJSONAsFeature(false))
	require.False(t, cfg2.GeoJSONAsFeature)

	// Test makeGeoJSONFeature function
	geo := map[string]any{"type": "Point", "coordinates": []float64{1, 2}}
	props := map[string]any{"name": "test"}

	feature := makeGeoJSONFeature(geo, props)
	require.Equal(t, "Feature", feature["type"])
	require.Equal(t, geo, feature["geometry"])
	require.Equal(t, props, feature["properties"])
}

func TestWithGeospatialCoordinatePrecision(t *testing.T) {
	// Test setting precision to 3
	cfg := NewGeospatialConfig(WithGeospatialCoordinatePrecision(3))
	require.Equal(t, 3, cfg.CoordPrecision)

	// Test setting precision to 8
	cfg2 := NewGeospatialConfig(WithGeospatialCoordinatePrecision(8))
	require.Equal(t, 8, cfg2.CoordPrecision)

	// Test precision affects coordinate rounding
	// Create a point with high precision coordinates
	wkb := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x36, 0x96, 0x73, 0xd3, 0xad, 0xf9, 0xf1, 0x3f, 0xae, 0x95, 0x03, 0x4f, 0xb7, 0xe6, 0x07, 0x40}

	// Set precision to 2 decimal places
	gj, ok := wkbToGeoJSON(wkb, 2)
	require.True(t, ok)
	coords := gj["coordinates"].([]float64)

	// Verify coordinates are rounded to 2 decimal places
	require.InDelta(t, 1.12, coords[0], 0.001)
	require.InDelta(t, 2.99, coords[1], 0.001)

	// Test with precision 0 (integers only)
	gj2, ok := wkbToGeoJSON(wkb, 0)
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

func TestSPHERICAL_GeoJSON_Encoding(t *testing.T) {
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

	t.Run("SPHERICAL in Hex mode", func(t *testing.T) {
		cfg := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeHex))
		result := ConvertGeographyLogicalValue(wkbPoint, geog, cfg).(map[string]any)

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
		cfg := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeBase64))
		result := ConvertGeographyLogicalValue(wkbPoint, geog, cfg).(map[string]any)

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
		cfg := NewGeospatialConfig(
			WithGeographyJSONMode(GeospatialModeGeoJSON),
			WithGeospatialGeoJSONAsFeature(true),
		)
		result := ConvertGeographyLogicalValue(wkbPoint, geog, cfg).(map[string]any)

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
		cfg := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeHybrid))
		result := ConvertGeographyLogicalValue(wkbPoint, geog, cfg).(map[string]any)

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

		cfg := NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeGeoJSON))
		result := ConvertGeographyLogicalValue(wkbPoint, geogDefault, cfg).(map[string]any)

		properties := result["properties"].(map[string]any)
		require.Equal(t, "SPHERICAL", properties["algorithm"])
		require.Equal(t, crs, properties["crs"])
	})
}

func TestEdgeInterpolationAlgorithm_JSON_Marshaling(t *testing.T) {
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

func TestEdgeInterpolationAlgorithm_Invalid_JSON(t *testing.T) {
	type TestStruct struct {
		Algorithm parquet.EdgeInterpolationAlgorithm `json:"algorithm"`
	}

	testCases := []struct {
		name    string
		jsonStr string
		errMsg  string
	}{
		{"numeric value", `{"algorithm": 0}`, "cannot unmarshal number"},
		{"invalid string", `{"algorithm": "INVALID"}`, "not a valid EdgeInterpolationAlgorithm"},
		{"empty string", `{"algorithm": ""}`, "not a valid EdgeInterpolationAlgorithm"},
		{"null value", `{"algorithm": null}`, ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var ts TestStruct
			err := json.Unmarshal([]byte(tc.jsonStr), &ts)
			if tc.errMsg != "" {
				require.Error(t, err, "Expected error when unmarshaling %s", tc.jsonStr)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err, "Expected no error when unmarshaling %s", tc.jsonStr)
			}
		})
	}
}

func TestWithGeospatialHybridRawBase64(t *testing.T) {
	// Test setting to true
	cfg := NewGeospatialConfig(WithGeospatialHybridRawBase64(true))
	require.True(t, cfg.HybridUseBase64)

	// Test setting to false
	cfg2 := NewGeospatialConfig(WithGeospatialHybridRawBase64(false))
	require.False(t, cfg2.HybridUseBase64)
}

// Test WithGeospatialReprojector configuration option
func TestWithGeospatialReprojector(t *testing.T) {
	// Test setting a custom reprojector
	customReprojector := func(crs string, geojson map[string]any) (map[string]any, bool) {
		return map[string]any{"transformed": true}, true
	}
	cfg := NewGeospatialConfig(WithGeospatialReprojector(customReprojector))
	require.NotNil(t, cfg.Reprojector)

	// Test setting nil reprojector
	cfg2 := NewGeospatialConfig(WithGeospatialReprojector(nil))
	require.Nil(t, cfg2.Reprojector)
}

// Test edge cases for wkbToGeoJSON with big-endian byte order issues
func TestWrapGeoJSONHybrid_EdgeCases(t *testing.T) {
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
func TestWkbToGeoJSON_GeometryCollection_BufferBoundary(t *testing.T) {
	t.Run("geometrycollection_buffer_ends_at_geometry_boundary", func(t *testing.T) {
		wkb := []byte{1, 7, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0}

		_, ok := wkbToGeoJSON(wkb, 6)
		require.False(t, ok) // Should fail when calculateWKBSize fails
	})
}

// Helper functions to create various WKB geometries for testing (from AddWKB_test.go)

func createWKBMultiPoint(points [][]float64, littleEndian bool) []byte {
	buf := make([]byte, 0, 9+len(points)*21) // header + points (each point has its own header)
	if littleEndian {
		buf = append(buf, 1) // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPoint)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, uint32(len(points)))
		buf = append(buf, tmp...)

		for _, point := range points {
			// Each point has its own byte order and type
			buf = append(buf, 1) // little-endian
			binary.LittleEndian.PutUint32(tmp, WKBPoint)
			buf = append(buf, tmp...)
			coordBuf := make([]byte, 8)
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(point[0]))
			buf = append(buf, coordBuf...)
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(point[1]))
			buf = append(buf, coordBuf...)
		}
	} else {
		buf = append(buf, 0) // big-endian
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, WKBMultiPoint)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, uint32(len(points)))
		buf = append(buf, tmp...)

		for _, point := range points {
			// Each point has its own byte order and type
			buf = append(buf, 0) // big-endian
			binary.BigEndian.PutUint32(tmp, WKBPoint)
			buf = append(buf, tmp...)
			coordBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(coordBuf, math.Float64bits(point[0]))
			buf = append(buf, coordBuf...)
			binary.BigEndian.PutUint64(coordBuf, math.Float64bits(point[1]))
			buf = append(buf, coordBuf...)
		}
	}
	return buf
}

func createWKBMultiLineString(lines [][][]float64, littleEndian bool) []byte {
	buf := make([]byte, 0, 1000) // Start with reasonable capacity
	if littleEndian {
		buf = append(buf, 1) // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, uint32(len(lines)))
		buf = append(buf, tmp...)

		for _, line := range lines {
			// Each linestring has its own byte order and type
			buf = append(buf, 1) // little-endian
			binary.LittleEndian.PutUint32(tmp, WKBLineString)
			buf = append(buf, tmp...)
			binary.LittleEndian.PutUint32(tmp, uint32(len(line)))
			buf = append(buf, tmp...)

			coordBuf := make([]byte, 8)
			for _, point := range line {
				binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(point[0]))
				buf = append(buf, coordBuf...)
				binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(point[1]))
				buf = append(buf, coordBuf...)
			}
		}
	} else {
		buf = append(buf, 0) // big-endian
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, WKBMultiLineString)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, uint32(len(lines)))
		buf = append(buf, tmp...)

		for _, line := range lines {
			// Each linestring has its own byte order and type
			buf = append(buf, 0) // big-endian
			binary.BigEndian.PutUint32(tmp, WKBLineString)
			buf = append(buf, tmp...)
			binary.BigEndian.PutUint32(tmp, uint32(len(line)))
			buf = append(buf, tmp...)

			coordBuf := make([]byte, 8)
			for _, point := range line {
				binary.BigEndian.PutUint64(coordBuf, math.Float64bits(point[0]))
				buf = append(buf, coordBuf...)
				binary.BigEndian.PutUint64(coordBuf, math.Float64bits(point[1]))
				buf = append(buf, coordBuf...)
			}
		}
	}
	return buf
}

func createWKBMultiPolygon(polygons [][][][]float64, littleEndian bool) []byte {
	buf := make([]byte, 0, 2000) // Start with reasonable capacity
	if littleEndian {
		buf = append(buf, 1) // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, uint32(len(polygons)))
		buf = append(buf, tmp...)

		for _, polygon := range polygons {
			// Each polygon has its own byte order and type
			buf = append(buf, 1) // little-endian
			binary.LittleEndian.PutUint32(tmp, WKBPolygon)
			buf = append(buf, tmp...)
			binary.LittleEndian.PutUint32(tmp, uint32(len(polygon)))
			buf = append(buf, tmp...)

			for _, ring := range polygon {
				binary.LittleEndian.PutUint32(tmp, uint32(len(ring)))
				buf = append(buf, tmp...)

				coordBuf := make([]byte, 8)
				for _, point := range ring {
					binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(point[0]))
					buf = append(buf, coordBuf...)
					binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(point[1]))
					buf = append(buf, coordBuf...)
				}
			}
		}
	} else {
		buf = append(buf, 0) // big-endian
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, uint32(len(polygons)))
		buf = append(buf, tmp...)

		for _, polygon := range polygons {
			// Each polygon has its own byte order and type
			buf = append(buf, 0) // big-endian
			binary.BigEndian.PutUint32(tmp, WKBPolygon)
			buf = append(buf, tmp...)
			binary.BigEndian.PutUint32(tmp, uint32(len(polygon)))
			buf = append(buf, tmp...)

			for _, ring := range polygon {
				binary.BigEndian.PutUint32(tmp, uint32(len(ring)))
				buf = append(buf, tmp...)

				coordBuf := make([]byte, 8)
				for _, point := range ring {
					binary.BigEndian.PutUint64(coordBuf, math.Float64bits(point[0]))
					buf = append(buf, coordBuf...)
					binary.BigEndian.PutUint64(coordBuf, math.Float64bits(point[1]))
					buf = append(buf, coordBuf...)
				}
			}
		}
	}
	return buf
}

func createWKBGeometryCollection(geometries [][]byte, littleEndian bool) []byte {
	buf := make([]byte, 0, 1000)
	if littleEndian {
		buf = append(buf, 1) // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBGeometryCollection)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, uint32(len(geometries)))
		buf = append(buf, tmp...)
	} else {
		buf = append(buf, 0) // big-endian
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, WKBGeometryCollection)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, uint32(len(geometries)))
		buf = append(buf, tmp...)
	}

	for _, geom := range geometries {
		buf = append(buf, geom...)
	}
	return buf
}

func createSimpleWKBPoint(x, y float64, littleEndian bool) []byte {
	buf := make([]byte, 21) // 1 + 4 + 16 bytes
	if littleEndian {
		buf[0] = 1
		binary.LittleEndian.PutUint32(buf[1:5], WKBPoint)
		binary.LittleEndian.PutUint64(buf[5:13], math.Float64bits(x))
		binary.LittleEndian.PutUint64(buf[13:21], math.Float64bits(y))
	} else {
		buf[0] = 0
		binary.BigEndian.PutUint32(buf[1:5], WKBPoint)
		binary.BigEndian.PutUint64(buf[5:13], math.Float64bits(x))
		binary.BigEndian.PutUint64(buf[13:21], math.Float64bits(y))
	}
	return buf
}

func createSimpleWKBLineString(coords [][2]float64, littleEndian bool) []byte {
	buf := make([]byte, 0, 9+len(coords)*16)
	if littleEndian {
		buf = append(buf, 1)
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, uint32(len(coords)))
		buf = append(buf, tmp...)

		coordBuf := make([]byte, 8)
		for _, coord := range coords {
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(coord[0]))
			buf = append(buf, coordBuf...)
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(coord[1]))
			buf = append(buf, coordBuf...)
		}
	} else {
		buf = append(buf, 0)
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, uint32(len(coords)))
		buf = append(buf, tmp...)

		coordBuf := make([]byte, 8)
		for _, coord := range coords {
			binary.BigEndian.PutUint64(coordBuf, math.Float64bits(coord[0]))
			buf = append(buf, coordBuf...)
			binary.BigEndian.PutUint64(coordBuf, math.Float64bits(coord[1]))
			buf = append(buf, coordBuf...)
		}
	}
	return buf
}

func createSimpleWKBPolygon(rings [][][2]float64, littleEndian bool) []byte {
	buf := make([]byte, 0, 500)
	if littleEndian {
		buf = append(buf, 1)
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, uint32(len(rings)))
		buf = append(buf, tmp...)

		coordBuf := make([]byte, 8)
		for _, ring := range rings {
			binary.LittleEndian.PutUint32(tmp, uint32(len(ring)))
			buf = append(buf, tmp...)
			for _, coord := range ring {
				binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(coord[0]))
				buf = append(buf, coordBuf...)
				binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(coord[1]))
				buf = append(buf, coordBuf...)
			}
		}
	} else {
		buf = append(buf, 0)
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, uint32(len(rings)))
		buf = append(buf, tmp...)

		coordBuf := make([]byte, 8)
		for _, ring := range rings {
			binary.BigEndian.PutUint32(tmp, uint32(len(ring)))
			buf = append(buf, tmp...)
			for _, coord := range ring {
				binary.BigEndian.PutUint64(coordBuf, math.Float64bits(coord[0]))
				buf = append(buf, coordBuf...)
				binary.BigEndian.PutUint64(coordBuf, math.Float64bits(coord[1]))
				buf = append(buf, coordBuf...)
			}
		}
	}
	return buf
}

// nestedCollectionWKB wraps a Point in the given number of GeometryCollections.
func nestedCollectionWKB(levels int) []byte {
	b := wkbPoint(1, 1.0, 2.0)
	for range levels {
		head := binary.LittleEndian.AppendUint32([]byte{1}, WKBGeometryCollection)
		head = binary.LittleEndian.AppendUint32(head, 1)
		b = append(head, b...)
	}
	return b
}

// nestedCollectionGeoJSON wraps a Point in the given number of GeometryCollections.
func nestedCollectionGeoJSON(levels int) map[string]any {
	geometry := map[string]any{"type": "Point", "coordinates": []float64{1.0, 2.0}}
	for range levels {
		geometry = map[string]any{"type": "GeometryCollection", "geometries": []any{geometry}}
	}
	return geometry
}

// nestedEmptyCollectionWKB nests levels GeometryCollections, the innermost one empty.
func nestedEmptyCollectionWKB(levels int) []byte {
	b := binary.LittleEndian.AppendUint32([]byte{1}, WKBGeometryCollection)
	b = binary.LittleEndian.AppendUint32(b, 0)
	for range levels - 1 {
		head := binary.LittleEndian.AppendUint32([]byte{1}, WKBGeometryCollection)
		head = binary.LittleEndian.AppendUint32(head, 1)
		b = append(head, b...)
	}
	return b
}

// nestedEmptyCollectionGeoJSON nests levels GeometryCollections, the innermost one empty.
func nestedEmptyCollectionGeoJSON(levels int) map[string]any {
	geometry := map[string]any{"type": "GeometryCollection", "geometries": []any{}}
	for range levels - 1 {
		geometry = map[string]any{"type": "GeometryCollection", "geometries": []any{geometry}}
	}
	return geometry
}

// nestedMultiPointWKB nests levels MultiPoints, each declaring one member.
func nestedMultiPointWKB(levels int) []byte {
	var b []byte
	for range levels {
		b = binary.LittleEndian.AppendUint32(append(b, 1), WKBMultiPoint)
		b = binary.LittleEndian.AppendUint32(b, 1)
	}
	return append(b, wkbPoint(1, 1.0, 2.0)...)
}
