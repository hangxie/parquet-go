package types

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// geoJSONOf decodes GeoJSON text, so the cases below read as geometry rather than as Go
// literals.
func geoJSONOf(t *testing.T, text string) map[string]any {
	t.Helper()
	var gj map[string]any
	require.NoError(t, json.Unmarshal([]byte(text), &gj))
	return gj
}

// TestGeoJSONToWKBRoundTrip runs every geometry the reader renders back through the
// encoder. wkbToGeoJSON at full precision is the oracle: what it reads, this must write.
func TestGeoJSONToWKBRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		gj   string
	}{
		{"Point", `{"type":"Point","coordinates":[1.5,2.25]}`},
		{"Point negative", `{"type":"Point","coordinates":[-122.4194,37.7749]}`},
		{"LineString", `{"type":"LineString","coordinates":[[0,0],[1,1],[2,4]]}`},
		{"LineString empty", `{"type":"LineString","coordinates":[]}`},
		{"Polygon", `{"type":"Polygon","coordinates":[[[0,0],[4,0],[4,4],[0,4],[0,0]]]}`},
		{
			"Polygon with hole",
			`{"type":"Polygon","coordinates":[[[0,0],[9,0],[9,9],[0,9],[0,0]],[[2,2],[3,2],[3,3],[2,3],[2,2]]]}`,
		},
		{"MultiPoint", `{"type":"MultiPoint","coordinates":[[1,2],[3,4]]}`},
		{"MultiLineString", `{"type":"MultiLineString","coordinates":[[[0,0],[1,1]],[[2,2],[3,3]]]}`},
		{
			"MultiPolygon",
			`{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]],[[[5,5],[6,5],[6,6],[5,5]]]]}`,
		},
		{
			"GeometryCollection",
			`{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]},` +
				`{"type":"LineString","coordinates":[[0,0],[1,1]]}]}`,
		},
		{"GeometryCollection empty", `{"type":"GeometryCollection","geometries":[]}`},
		{
			// The only shape that exercises putCollection's recursion on both sides.
			"GeometryCollection nested",
			`{"type":"GeometryCollection","geometries":[{"type":"GeometryCollection",` +
				`"geometries":[{"type":"Point","coordinates":[1,2]}]}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gj := geoJSONOf(t, tt.gj)
			wkb, err := geoJSONToWKB(gj)
			require.NoError(t, err)

			// -1 disables rounding, so the reader returns what the encoder was given.
			back, ok := wkbToGeoJSON(wkb, -1)
			require.True(t, ok, "reader refused the encoder's own WKB: % x", wkb)

			for _, asFeature := range []bool{true, false} {
				cfg := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON),
					WithGeographyJSONMode(GeospatialModeGeoJSON),
					WithGeospatialGeoJSONAsFeature(asFeature), WithGeospatialCoordinatePrecision(-1))
				for _, annotation := range []string{"GEOMETRY", "GEOGRAPHY"} {
					fromJSON, fromCSV := geoWriteBack(t, annotation, wkb, cfg)
					require.Equal(t, string(wkb), fromJSON)
					require.Equal(t, string(wkb), fromCSV)
				}
			}

			wantJSON, err := json.Marshal(gj)
			require.NoError(t, err)
			gotJSON, err := json.Marshal(back)
			require.NoError(t, err)
			require.JSONEq(t, string(wantJSON), string(gotJSON))
		})
	}
}

func TestGeoJSONToWKBRejects(t *testing.T) {
	tests := []struct {
		name, gj, errMsg string
	}{
		{"unknown type", `{"type":"Circle","coordinates":[1,2]}`, "not one this library writes"},
		{"no type", `{"coordinates":[1,2]}`, "not one this library writes"},
		{"no coordinates", `{"type":"Point"}`, "has no coordinates"},
		{"one ordinate", `{"type":"Point","coordinates":[1]}`, "need 2"},
		{"three ordinates", `{"type":"Point","coordinates":[1,2,3]}`, "only 2D is supported"},
		{"ordinate not a number", `{"type":"Point","coordinates":["a","b"]}`, "not a number"},
		{"coordinates not an array", `{"type":"LineString","coordinates":5}`, "not an array"},
		{"position not an array", `{"type":"Point","coordinates":5}`, "position is"},
		{"collection without geometries", `{"type":"GeometryCollection"}`, "no geometries array"},
		{
			"collection member not an object",
			`{"type":"GeometryCollection","geometries":[3]}`, "not a geometry",
		},
		{
			"nested member is unknown",
			`{"type":"GeometryCollection","geometries":[{"type":"Circle"}]}`,
			"not one this library writes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := geoJSONToWKB(geoJSONOf(t, tt.gj))
			require.ErrorContains(t, err, tt.errMsg)
		})
	}
}

// TestGeoCoordAcceptsEveryNumberShape covers the numeric kinds a position can arrive as:
// float64 from encoding/json, json.Number under UseNumber, and the plain Go numbers a
// caller writing the map by hand would reach for.
func TestGeoCoordAcceptsEveryNumberShape(t *testing.T) {
	for _, v := range []any{1.5, float32(1.5), json.Number("1.5")} {
		f, ok := geoCoord(v)
		require.True(t, ok, "%T", v)
		require.InDelta(t, 1.5, f, 1e-9)
	}
	for _, v := range []any{2, int64(2)} {
		f, ok := geoCoord(v)
		require.True(t, ok, "%T", v)
		require.Equal(t, 2.0, f)
	}
	for _, v := range []any{"1.5", nil, json.Number("nope"), []any{1}} {
		_, ok := geoCoord(v)
		require.False(t, ok, "%T", v)
	}
}

// TestGeoJSONToWKBRejectsNested covers the error paths inside each container, which a
// top-level check would not reach.
func TestGeoJSONToWKBRejectsNested(t *testing.T) {
	tests := []struct{ name, gj, errMsg string }{
		{"ring is not an array", `{"type":"Polygon","coordinates":[5]}`, "not an array"},
		{"ring holds a bad position", `{"type":"Polygon","coordinates":[[[1]]]}`, "need 2"},
		{"MultiPoint member is bad", `{"type":"MultiPoint","coordinates":[[1]]}`, "need 2"},
		{
			"MultiLineString member is not an array",
			`{"type":"MultiLineString","coordinates":[5]}`, "not an array",
		},
		{
			"MultiPolygon member is not an array",
			`{"type":"MultiPolygon","coordinates":[5]}`, "not an array",
		},
		{
			"MultiPolygon ring holds a bad position",
			`{"type":"MultiPolygon","coordinates":[[[[1]]]]}`, "need 2",
		},
		{"LineString holds a bad position", `{"type":"LineString","coordinates":[[1]]}`, "need 2"},
		{
			"Multi* coordinates are not an array",
			`{"type":"MultiPoint","coordinates":5}`, "not an array",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := geoJSONToWKB(geoJSONOf(t, tt.gj))
			require.ErrorContains(t, err, tt.errMsg)
		})
	}
}

// FuzzGeoJSONWriteBack checks that accepted JSON survives encoding and native rendering.
func FuzzGeoJSONWriteBack(f *testing.F) {
	for _, s := range []string{
		`null`, `{}`, `{"type":"Point","coordinates":[1,2]}`,
		`{"type":"Point","coordinates":[1,2,3]}`,
		`{"type":"LineString","coordinates":[[0,0],[1,1]]}`,
		`{"type":"Polygon","coordinates":[[[0,0],[1,0],[0,1],[0,0]]]}`,
		`{"type":"MultiPoint","coordinates":[[1,2]]}`,
		`{"type":"MultiLineString","coordinates":[[[1,2],[3,4]]]}`,
		`{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[0,1],[0,0]]]]}`,
		`{"type":"GeometryCollection","geometries":[{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}]}`,
		`{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]}}`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		se, lT := geoSE(t, "GEOMETRY")
		opt := WithGeospatialConfig(NewGeospatialConfig(
			WithGeometryJSONMode(GeospatialModeGeoJSON), WithGeospatialCoordinatePrecision(-1),
		))
		encoded, err := StrToParquetTypeWithLogical(s, se.Type, nil, lT, 0, 0, opt)
		if err != nil {
			return
		}
		// WKB fallback objects may contain opaque bodies that GeoJSON cannot render.
		var input map[string]any
		require.NoError(t, json.Unmarshal([]byte(s), &input))
		if kind, _ := input["type"].(string); kind == "" {
			return
		}
		rendered, err := ConvertValue(encoded, se, opt)
		require.NoError(t, err)
		back, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(rendered), se.Type, nil, lT, 0, 0, opt)
		require.NoError(t, err)
		require.Equal(t, encoded, back)
	})
}
