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

func TestBoundingBoxCalculatorAddGeometryCollectionTruncatedCount(t *testing.T) {
	calc := NewBoundingBoxCalculator()

	calc.addGeometryCollectionWKB([]byte{1, 2, 3}, 0, false)

	_, _, _, _, ok := calc.GetBounds()
	require.False(t, ok)
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

func TestBoundingBoxCalculator(t *testing.T) {
	t.Run("single_point", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		calc.AddPoint(10.5, 20.3)

		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 10.5, minX)
		require.Equal(t, 20.3, minY)
		require.Equal(t, 10.5, maxX)
		require.Equal(t, 20.3, maxY)
	})

	t.Run("multiple_points", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		calc.AddPoint(10.5, 20.3)
		calc.AddPoint(5.2, 25.7)
		calc.AddPoint(15.8, 18.1)

		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 5.2, minX)
		require.Equal(t, 18.1, minY)
		require.Equal(t, 15.8, maxX)
		require.Equal(t, 25.7, maxY)
	})

	t.Run("empty_calculator", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()

		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("point_wkb", func(t *testing.T) {
		wkb := wkbPoint(1, 10.5, 20.3) // little-endian
		calc := NewBoundingBoxCalculator()

		err := calc.AddWKB(wkb)
		require.NoError(t, err)

		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 10.5, minX)
		require.Equal(t, 20.3, minY)
		require.Equal(t, 10.5, maxX)
		require.Equal(t, 20.3, maxY)
	})

	t.Run("linestring_wkb", func(t *testing.T) {
		coords := [][]float64{{0, 0}, {10, 5}, {5, 15}}
		wkb := buildWKBLineString(coords)
		calc := NewBoundingBoxCalculator()

		err := calc.AddWKB(wkb)
		require.NoError(t, err)

		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 10.0, maxX)
		require.Equal(t, 15.0, maxY)
	})

	t.Run("polygon_wkb", func(t *testing.T) {
		// Simple square polygon
		coords := [][][]float64{{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}}
		wkb := buildWKBPolygon(coords)
		calc := NewBoundingBoxCalculator()

		err := calc.AddWKB(wkb)
		require.NoError(t, err)

		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 10.0, maxX)
		require.Equal(t, 10.0, maxY)
	})

	t.Run("invalid_wkb", func(t *testing.T) {
		// Too short WKB
		wkb := []byte{1, 2}
		calc := NewBoundingBoxCalculator()

		err := calc.AddWKB(wkb)
		require.NoError(t, err) // Should not error, just ignore invalid data

		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok) // Should have no bounds since no valid data was added
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

func TestBoundingBoxCalculator_AddWKB(t *testing.T) {
	t.Run("point_little_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		wkb := createSimpleWKBPoint(10.5, 20.3, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 10.5, minX)
		require.Equal(t, 20.3, minY)
		require.Equal(t, 10.5, maxX)
		require.Equal(t, 20.3, maxY)
	})

	t.Run("point_big_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		wkb := createSimpleWKBPoint(15.7, 25.1, false)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 15.7, minX)
		require.Equal(t, 25.1, minY)
		require.Equal(t, 15.7, maxX)
		require.Equal(t, 25.1, maxY)
	})

	t.Run("linestring_little_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		coords := [][2]float64{{0, 0}, {10, 5}, {5, 15}}
		wkb := createSimpleWKBLineString(coords, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 10.0, maxX)
		require.Equal(t, 15.0, maxY)
	})

	t.Run("linestring_big_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		coords := [][2]float64{{-5, -3}, {10, 5}, {2, 15}}
		wkb := createSimpleWKBLineString(coords, false)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, -5.0, minX)
		require.Equal(t, -3.0, minY)
		require.Equal(t, 10.0, maxX)
		require.Equal(t, 15.0, maxY)
	})

	t.Run("polygon_little_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		rings := [][][2]float64{{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}}
		wkb := createSimpleWKBPolygon(rings, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 10.0, maxX)
		require.Equal(t, 10.0, maxY)
	})

	t.Run("polygon_big_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		rings := [][][2]float64{{{-5, -3}, {5, -3}, {5, 7}, {-5, 7}, {-5, -3}}}
		wkb := createSimpleWKBPolygon(rings, false)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, -5.0, minX)
		require.Equal(t, -3.0, minY)
		require.Equal(t, 5.0, maxX)
		require.Equal(t, 7.0, maxY)
	})

	t.Run("multipoint_little_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		points := [][]float64{{10, 20}, {30, 40}, {5, 15}}
		wkb := createWKBMultiPoint(points, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 5.0, minX)
		require.Equal(t, 15.0, minY)
		require.Equal(t, 30.0, maxX)
		require.Equal(t, 40.0, maxY)
	})

	t.Run("multipoint_big_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		points := [][]float64{{-10, -5}, {20, 25}, {0, 0}}
		wkb := createWKBMultiPoint(points, false)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, -10.0, minX)
		require.Equal(t, -5.0, minY)
		require.Equal(t, 20.0, maxX)
		require.Equal(t, 25.0, maxY)
	})

	t.Run("multilinestring_little_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		lines := [][][]float64{{{0, 0}, {10, 10}}, {{5, 15}, {25, 5}}}
		wkb := createWKBMultiLineString(lines, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 25.0, maxX)
		require.Equal(t, 15.0, maxY)
	})

	t.Run("multilinestring_big_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		lines := [][][]float64{{{-5, -3}, {5, 7}}, {{10, 2}, {15, 12}}}
		wkb := createWKBMultiLineString(lines, false)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, -5.0, minX)
		require.Equal(t, -3.0, minY)
		require.Equal(t, 15.0, maxX)
		require.Equal(t, 12.0, maxY)
	})

	t.Run("multipolygon_little_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		polygons := [][][][]float64{
			{{{0, 0}, {5, 0}, {5, 5}, {0, 5}, {0, 0}}},
			{{{10, 10}, {15, 10}, {15, 15}, {10, 15}, {10, 10}}},
		}
		wkb := createWKBMultiPolygon(polygons, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 15.0, maxX)
		require.Equal(t, 15.0, maxY)
	})

	t.Run("multipolygon_big_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		polygons := [][][][]float64{
			{{{-10, -5}, {-5, -5}, {-5, 0}, {-10, 0}, {-10, -5}}},
			{{{5, 5}, {10, 5}, {10, 10}, {5, 10}, {5, 5}}},
		}
		wkb := createWKBMultiPolygon(polygons, false)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, -10.0, minX)
		require.Equal(t, -5.0, minY)
		require.Equal(t, 10.0, maxX)
		require.Equal(t, 10.0, maxY)
	})

	t.Run("geometry_collection_simple", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()

		// Create individual geometries
		point := createSimpleWKBPoint(5, 10, true)
		linestring := createSimpleWKBLineString([][2]float64{{0, 0}, {15, 20}}, true)

		// Create geometry collection
		wkb := createWKBGeometryCollection([][]byte{point, linestring}, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 15.0, maxX)
		require.Equal(t, 20.0, maxY)
	})

	t.Run("geometry_collection_complex", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()

		// Create individual geometries
		point := createSimpleWKBPoint(-5, -3, true)
		polygon := createSimpleWKBPolygon([][][2]float64{{{10, 10}, {20, 10}, {20, 20}, {10, 20}, {10, 10}}}, true)
		multipoint := createWKBMultiPoint([][]float64{{0, 0}, {25, 25}}, true)

		// Create geometry collection
		wkb := createWKBGeometryCollection([][]byte{point, polygon, multipoint}, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, -5.0, minX)
		require.Equal(t, -3.0, minY)
		require.Equal(t, 25.0, maxX)
		require.Equal(t, 25.0, maxY)
	})

	t.Run("empty_wkb", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		wkb := []byte{}

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("too_short_wkb", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		wkb := []byte{1, 2, 3} // Less than 5 bytes

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("invalid_geometry_type", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create WKB with invalid geometry type (999)
		wkb := []byte{1, 231, 3, 0, 0} // little-endian, type 999

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("corrupted_u32_function", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create WKB where u32 function would fail (reading past buffer)
		wkb := []byte{1, 1} // little-endian, but missing type bytes

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipoint_invalid_point_type", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPoint with invalid inner point type
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPoint)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 point
		buf = append(buf, tmp...)

		// Add invalid point (type 2 instead of 1)
		buf = append(buf, 1)                  // little-endian
		binary.LittleEndian.PutUint32(tmp, 2) // LineString type instead of Point
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipoint_insufficient_data", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPoint with insufficient data for coordinates
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPoint)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 point
		buf = append(buf, tmp...)

		// Add point header but no coordinate data
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPoint)
		buf = append(buf, tmp...)
		// Missing coordinate data (need 16 bytes)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multilinestring_invalid_linestring_type", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiLineString with invalid inner linestring type
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 linestring
		buf = append(buf, tmp...)

		// Add invalid linestring (type 1 instead of 2)
		buf = append(buf, 1)                         // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPoint) // Point type instead of LineString
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipolygon_invalid_polygon_type", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPolygon with invalid inner polygon type
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 polygon
		buf = append(buf, tmp...)

		// Add invalid polygon (type 1 instead of 3)
		buf = append(buf, 1)                         // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPoint) // Point type instead of Polygon
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("geometry_collection_empty", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		wkb := createWKBGeometryCollection([][]byte{}, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("geometry_collection_with_invalid_subgeometry", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()

		// Create valid point and invalid geometry
		validPoint := createSimpleWKBPoint(5, 10, true)
		invalidGeom := []byte{1, 2} // Too short

		wkb := createWKBGeometryCollection([][]byte{validPoint, invalidGeom}, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok) // Should still work with valid geometry
		require.Equal(t, 5.0, minX)
		require.Equal(t, 10.0, minY)
		require.Equal(t, 5.0, maxX)
		require.Equal(t, 10.0, maxY)
	})

	t.Run("multipoint_mixed_endianness", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()

		// Create MultiPoint header (little-endian)
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPoint)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 2) // 2 points
		buf = append(buf, tmp...)

		// First point: little-endian
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPoint)
		buf = append(buf, tmp...)
		coordBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(5.0))
		buf = append(buf, coordBuf...)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(10.0))
		buf = append(buf, coordBuf...)

		// Second point: big-endian (this will cause type reading to fail due to endianness mismatch)
		buf = append(buf, 0) // big-endian
		binary.BigEndian.PutUint32(tmp, WKBPoint)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint64(coordBuf, math.Float64bits(15.0))
		buf = append(buf, coordBuf...)
		binary.BigEndian.PutUint64(coordBuf, math.Float64bits(20.0))
		buf = append(buf, coordBuf...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		// A MultiPoint with mixed endianness is considered invalid.
		// With atomic processing, the whole geometry is considered invalid and no bounds should be calculated.
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok) // Should not have bounds
	})

	// Additional error path tests for better coverage
	t.Run("multilinestring_insufficient_data_for_header", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiLineString with insufficient data for linestring header
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 linestring
		buf = append(buf, tmp...)

		// Add linestring header but cut off before point count
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		// Missing point count and coordinate data

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipolygon_insufficient_data_for_header", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPolygon with insufficient data for polygon header
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 polygon
		buf = append(buf, tmp...)

		// Add polygon header but cut off before ring count
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		// Missing ring count and coordinate data

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multilinestring_big_endian_point_count_read", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Test big-endian point count reading in MultiLineString
		buf := []byte{0} // big-endian
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, WKBMultiLineString)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, 1) // 1 linestring
		buf = append(buf, tmp...)

		// Add linestring with big-endian point count
		buf = append(buf, 0) // big-endian
		binary.BigEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, 2) // 2 points
		buf = append(buf, tmp...)

		// Add coordinate data
		coordBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)
		binary.BigEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)
		binary.BigEndian.PutUint64(coordBuf, math.Float64bits(10.0))
		buf = append(buf, coordBuf...)
		binary.BigEndian.PutUint64(coordBuf, math.Float64bits(10.0))
		buf = append(buf, coordBuf...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 10.0, maxX)
		require.Equal(t, 10.0, maxY)
	})

	t.Run("multipolygon_big_endian_ring_point_count_read", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Test big-endian ring point count reading in MultiPolygon
		buf := []byte{0} // big-endian
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, 1) // 1 polygon
		buf = append(buf, tmp...)

		// Add polygon with big-endian ring count
		buf = append(buf, 0) // big-endian
		binary.BigEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, 1) // 1 ring
		buf = append(buf, tmp...)

		// Add ring with big-endian point count
		binary.BigEndian.PutUint32(tmp, 4) // 4 points
		buf = append(buf, tmp...)

		// Add coordinate data for square
		coordBuf := make([]byte, 8)
		coords := []float64{0, 0, 5, 0, 5, 5, 0, 5}
		for i := 0; i < len(coords); i += 2 {
			binary.BigEndian.PutUint64(coordBuf, math.Float64bits(coords[i]))
			buf = append(buf, coordBuf...)
			binary.BigEndian.PutUint64(coordBuf, math.Float64bits(coords[i+1]))
			buf = append(buf, coordBuf...)
		}

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 5.0, maxX)
		require.Equal(t, 5.0, maxY)
	})

	t.Run("multilinestring_insufficient_coordinate_data", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiLineString with insufficient coordinate data
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 linestring
		buf = append(buf, tmp...)

		// Add linestring header
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 2) // 2 points
		buf = append(buf, tmp...)

		// Add only one point worth of data instead of two
		coordBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)
		// Missing second point data (need 16 more bytes)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipolygon_insufficient_ring_coordinate_data", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPolygon with insufficient ring coordinate data
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 polygon
		buf = append(buf, tmp...)

		// Add polygon header
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 ring
		buf = append(buf, tmp...)

		// Add ring with point count but insufficient coordinate data
		binary.LittleEndian.PutUint32(tmp, 4) // 4 points
		buf = append(buf, tmp...)

		// Add only one point worth of data instead of four
		coordBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)
		// Missing three more points worth of data (need 48 more bytes)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	// Additional error path tests for AddWKB coverage
	t.Run("parseLineString_insufficient_points", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create LineString with point count but insufficient point data
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 3) // 3 points
		buf = append(buf, tmp...)

		// Add only 2 points worth of data (32 bytes) instead of 3 (48 bytes)
		coordBuf := make([]byte, 8)
		for i := 0; i < 4; i++ { // Only 4 coordinates (2 points)
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(float64(i)))
			buf = append(buf, coordBuf...)
		}

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("parsePolygon_insufficient_ring_points", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create Polygon where ring claims more points than available
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 ring
		buf = append(buf, tmp...)

		binary.LittleEndian.PutUint32(tmp, 5) // Claims 5 points
		buf = append(buf, tmp...)

		// Add only 3 points worth of data
		coordBuf := make([]byte, 8)
		for i := 0; i < 6; i++ { // Only 6 coordinates (3 points)
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(float64(i)))
			buf = append(buf, coordBuf...)
		}

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("parsePolygon_insufficient_ring_header", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create Polygon where second ring header is truncated
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 2) // 2 rings
		buf = append(buf, tmp...)

		// First ring - complete
		binary.LittleEndian.PutUint32(tmp, 4) // 4 points
		buf = append(buf, tmp...)
		coordBuf := make([]byte, 8)
		for i := 0; i < 8; i++ { // 8 coordinates (4 points)
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(float64(i)))
			buf = append(buf, coordBuf...)
		}

		// Second ring - truncated point count
		buf = append(buf, 1, 2) // Only 2 bytes instead of 4 needed for point count

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("calculateWKBSize_multilinestring_insufficient_data", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create GeometryCollection with MultiLineString that has insufficient data for calculateWKBSize
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBGeometryCollection)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 geometry
		buf = append(buf, tmp...)

		// Add MultiLineString with insufficient data for calculateWKBSize
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBMultiLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 linestring
		buf = append(buf, tmp...)

		// Add linestring header but truncate before point count
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		// Missing point count (need 4 more bytes)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("calculateWKBSize_multipolygon_insufficient_data", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create GeometryCollection with MultiPolygon that has insufficient data for calculateWKBSize
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBGeometryCollection)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 geometry
		buf = append(buf, tmp...)

		// Add MultiPolygon with insufficient data for calculateWKBSize
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 polygon
		buf = append(buf, tmp...)

		// Add polygon header but truncate before ring count
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		// Missing ring count (need 4 more bytes)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("calculateWKBSize_multipolygon_ring_insufficient_data", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create GeometryCollection with MultiPolygon ring that has insufficient data
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBGeometryCollection)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 geometry
		buf = append(buf, tmp...)

		// Add MultiPolygon with ring data issue
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 polygon
		buf = append(buf, tmp...)

		// Add polygon header with ring count
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 ring
		buf = append(buf, tmp...)

		// Truncate at ring point count
		buf = append(buf, 1, 2) // Only 2 bytes instead of 4 needed for ring point count

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("calculateWKBSize_unknown_geometry_type", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create GeometryCollection with unknown geometry type
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBGeometryCollection)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 geometry
		buf = append(buf, tmp...)

		// Add geometry with unknown type
		buf = append(buf, 1)                    // little-endian
		binary.LittleEndian.PutUint32(tmp, 999) // Unknown type
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("empty_multipoint", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPoint with zero points
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPoint)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 0) // 0 points
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("empty_multilinestring", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiLineString with zero linestrings
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 0) // 0 linestrings
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("empty_multipolygon", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPolygon with zero polygons
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 0) // 0 polygons
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("linestring_with_zero_points", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create LineString with zero points
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 0) // 0 points
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("polygon_with_zero_rings", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create Polygon with zero rings
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 0) // 0 rings
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("polygon_ring_with_zero_points", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create Polygon with ring that has zero points
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 ring
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 0) // 0 points in ring
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multilinestring_with_empty_linestring", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiLineString with one empty linestring
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 linestring
		buf = append(buf, tmp...)

		// Add empty linestring
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 0) // 0 points
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipolygon_with_empty_polygon", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPolygon with one empty polygon
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 polygon
		buf = append(buf, tmp...)

		// Add empty polygon
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 0) // 0 rings
		buf = append(buf, tmp...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	// Additional coverage tests for remaining edge cases
	t.Run("mixed_valid_invalid_geometry_collection", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()

		// Create valid point first to have some bounds
		validPoint := createSimpleWKBPoint(1, 2, true)
		err := calc.AddWKB(validPoint)
		require.NoError(t, err)

		// Now create GeometryCollection with mixed valid/invalid geometries
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBGeometryCollection)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 2) // 2 geometries
		buf = append(buf, tmp...)

		// Add valid point
		validPoint2 := createSimpleWKBPoint(5, 10, true)
		buf = append(buf, validPoint2...)

		// Add invalid geometry (truncated)
		buf = append(buf, 1, 2) // Too short

		err = calc.AddWKB(buf)
		require.NoError(t, err)

		// Should still have bounds from the valid geometries
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 1.0, minX)
		require.Equal(t, 2.0, minY)
		require.Equal(t, 5.0, maxX)
		require.Equal(t, 10.0, maxY)
	})

	t.Run("point_coordinates_handling", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Test Point with only one coordinate pair
		coords := [][]float64{{1.5, 2.5}}
		wkb := createWKBMultiPoint(coords, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 1.5, minX)
		require.Equal(t, 2.5, minY)
		require.Equal(t, 1.5, maxX)
		require.Equal(t, 2.5, maxY)
	})

	t.Run("parseLineString_exact_size", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create LineString with exactly the right amount of data
		coords := [][2]float64{{1, 2}, {3, 4}, {5, 6}}
		wkb := createSimpleWKBLineString(coords, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 1.0, minX)
		require.Equal(t, 2.0, minY)
		require.Equal(t, 5.0, maxX)
		require.Equal(t, 6.0, maxY)
	})

	t.Run("parsePolygon_multiple_rings", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create Polygon with multiple rings
		rings := [][][2]float64{
			{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}, // Outer ring
			{{2, 2}, {8, 2}, {8, 8}, {2, 8}, {2, 2}},     // Inner ring (hole)
		}
		wkb := createSimpleWKBPolygon(rings, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 10.0, maxX)
		require.Equal(t, 10.0, maxY)
	})

	t.Run("multipolygon_with_valid_and_empty", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPolygon with one valid polygon and one with empty ring
		polygons := [][][][]float64{
			{{{0, 0}, {5, 0}, {5, 5}, {0, 5}, {0, 0}}}, // Valid polygon
		}
		wkb := createWKBMultiPolygon(polygons, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 5.0, maxX)
		require.Equal(t, 5.0, maxY)
	})

	t.Run("roundCoordinate_coverage", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create point with high precision coordinates to test rounding
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBPoint)
		buf = append(buf, tmp...)

		// Add high precision coordinates
		coordBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(1.123456789))
		buf = append(buf, coordBuf...)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(2.987654321))
		buf = append(buf, coordBuf...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		// Coordinates should be used as-is in bounding box (rounding is for GeoJSON)
		require.InDelta(t, 1.123456789, minX, 0.000001)
		require.InDelta(t, 2.987654321, minY, 0.000001)
		require.InDelta(t, 1.123456789, maxX, 0.000001)
		require.InDelta(t, 2.987654321, maxY, 0.000001)
	})

	t.Run("comprehensive_geometry_collection_recursive", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()

		// Create nested geometry collection
		point1 := createSimpleWKBPoint(1, 1, true)
		point2 := createSimpleWKBPoint(10, 10, true)

		// Inner geometry collection
		innerGC := createWKBGeometryCollection([][]byte{point1, point2}, true)

		// Outer geometry collection containing the inner one
		line := createSimpleWKBLineString([][2]float64{{0, 0}, {5, 5}}, true)
		wkb := createWKBGeometryCollection([][]byte{innerGC, line}, true)

		err := calc.AddWKB(wkb)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 0.0, minX)
		require.Equal(t, 0.0, minY)
		require.Equal(t, 10.0, maxX)
		require.Equal(t, 10.0, maxY)
	})

	t.Run("geometry_collection_calculateWKBSize_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()

		// Create GeometryCollection with a sub-geometry that will cause calculateWKBSize to fail
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBGeometryCollection)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 geometry
		buf = append(buf, tmp...)

		// Add a geometry with invalid header (too short)
		buf = append(buf, 1, 2) // Invalid geometry (too short for calculateWKBSize)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("geometry_collection_buffer_overrun", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()

		// Create GeometryCollection with calculated size that exceeds buffer
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBGeometryCollection)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 geometry
		buf = append(buf, tmp...)

		// Add a valid point but truncated
		point := createSimpleWKBPoint(5, 10, true)
		buf = append(buf, point[:10]...) // Only include part of the point

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipoint_at_buffer_boundary", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPoint that exactly reaches buffer boundary
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPoint)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 point
		buf = append(buf, tmp...)

		// Add point that exactly fills remaining buffer
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPoint)
		buf = append(buf, tmp...)
		coordBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(5.0))
		buf = append(buf, coordBuf...)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(10.0))
		buf = append(buf, coordBuf...)
		// Buffer should be exactly the right size

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, 5.0, minX)
		require.Equal(t, 10.0, minY)
		require.Equal(t, 5.0, maxX)
		require.Equal(t, 10.0, maxY)
	})

	t.Run("multipoint_u32_read_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPoint where u32 read for point count fails
		buf := []byte{1, 4, 0, 0} // little-endian, type 4 (MultiPoint), but truncated

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multilinestring_u32_read_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiLineString where u32 read for linestring count fails
		buf := []byte{1, 5, 0, 0} // little-endian, type 5 (MultiLineString), but truncated

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipolygon_u32_read_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPolygon where u32 read for polygon count fails
		buf := []byte{1, 6, 0, 0} // little-endian, type 6 (MultiPolygon), but truncated

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("geometry_collection_u32_read_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create GeometryCollection where u32 read for geometry count fails
		buf := []byte{1, 7, 0, 0} // little-endian, type 7 (GeometryCollection), but truncated

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("point_parsePoint_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create Point with insufficient coordinate data
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBPoint)
		buf = append(buf, tmp...)
		// Missing coordinate data (need 16 bytes for x,y)
		buf = append(buf, 1, 2, 3) // Only 3 bytes instead of 16

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("linestring_parseLineString_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create LineString with insufficient data
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBLineString)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 2) // 2 points
		buf = append(buf, tmp...)
		// Missing coordinate data (need 32 bytes for 2 points)
		buf = append(buf, 1, 2, 3) // Only 3 bytes instead of 32

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("polygon_parsePolygon_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create Polygon with insufficient data
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 ring
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 4) // 4 points in ring
		buf = append(buf, tmp...)
		// Missing coordinate data (need 64 bytes for 4 points)
		buf = append(buf, 1, 2, 3) // Only 3 bytes instead of 64

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipoint_insufficient_data_for_second_point", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPoint with one complete point and one incomplete point
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPoint)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 2) // 2 points
		buf = append(buf, tmp...)

		// Add one valid point
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPoint)
		buf = append(buf, tmp...)
		coordBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)

		// Add header for second point, but not enough data for coords
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPoint)
		buf = append(buf, tmp...)
		buf = append(buf, 1, 2, 3, 4) // not enough data

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipolygon_insufficient_data_for_second_ring", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPolygon with one complete ring and one incomplete ring
		buf := []byte{1} // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, WKBMultiPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 1) // 1 polygon
		buf = append(buf, tmp...)

		// Add polygon header
		buf = append(buf, 1) // little-endian
		binary.LittleEndian.PutUint32(tmp, WKBPolygon)
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, 2) // 2 rings
		buf = append(buf, tmp...)

		// Add first ring (4 points)
		binary.LittleEndian.PutUint32(tmp, 4)
		buf = append(buf, tmp...)
		coordBuf := make([]byte, 8)
		for i := 0; i < 4; i++ {
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(float64(i)))
			buf = append(buf, coordBuf...)
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(float64(i)))
			buf = append(buf, coordBuf...)
		}

		// Add header for second ring, but not enough data for points
		binary.LittleEndian.PutUint32(tmp, 4) // 4 points
		buf = append(buf, tmp...)
		// only one point
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)
		binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(0.0))
		buf = append(buf, coordBuf...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})
}
