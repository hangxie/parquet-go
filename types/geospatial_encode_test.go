package types

import (
	"encoding/binary"
	"encoding/json"
	"math"
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
			wkb, err := geoJSONToWKB(gj, 0)
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
		{"one ordinate", `{"type":"Point","coordinates":[1]}`, "need 2 or 3"},
		{"four ordinates", `{"type":"Point","coordinates":[1,2,3,4]}`, "need 2 or 3"},
		{"mixed dimensions", `{"type":"LineString","coordinates":[[1,2,3],[4,5]]}`, "ordinates"},
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
			_, err := geoJSONToWKB(geoJSONOf(t, tt.gj), 0)
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
			_, err := geoJSONToWKB(geoJSONOf(t, tt.gj), 0)
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

func TestGeoJSONToWKB_NestingDepth(t *testing.T) {
	testCases := []struct {
		name   string
		geo    func(int) map[string]any
		nested func(int) []byte
	}{
		{"innermost collection holds a point", nestedCollectionGeoJSON, nestedCollectionWKB},
		{"innermost collection is empty", nestedEmptyCollectionGeoJSON, nestedEmptyCollectionWKB},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := geoJSONToWKB(tc.geo(maxGeometryDepth), 0)
			require.NoError(t, err)
			require.Equal(t, tc.nested(maxGeometryDepth), encoded)

			_, err = geoJSONToWKB(tc.geo(maxGeometryDepth+1), 0)
			require.Error(t, err)
		})
	}
}

// TestGeoJSONToWKB_Z covers the round trip the Z rendering needs: a geometry read to GeoJSON
// with elevations must write back to the bytes it came from.
func TestGeoJSONToWKB_Z(t *testing.T) {
	container := func(gType uint32, members ...[]byte) []byte {
		out := binary.LittleEndian.AppendUint32([]byte{1}, gType)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(members)))
		for _, member := range members {
			out = append(out, member...)
		}
		return out
	}
	ordinates := func(gType uint32, counts []uint32, points [][]float64) []byte {
		buf := binary.LittleEndian.AppendUint32([]byte{1}, gType)
		for _, c := range counts {
			buf = binary.LittleEndian.AppendUint32(buf, c)
		}
		for _, p := range points {
			for _, o := range p {
				buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(o))
			}
		}
		return buf
	}
	pointZ := ordinates(1001, nil, [][]float64{{1, 2, 99}})
	lineZ := ordinates(1002, []uint32{2}, [][]float64{{1, 2, 99}, {3, 4, 98}})
	polygonZ := ordinates(1003, []uint32{1, 4}, [][]float64{{0, 0, 9}, {1, 0, 9}, {1, 1, 9}, {0, 0, 9}})

	testCases := []struct {
		name string
		wkb  []byte
	}{
		{"Point Z", pointZ},
		{"LineString Z", lineZ},
		{"Polygon Z", polygonZ},
		{"MultiPoint Z", container(1004, pointZ)},
		{"MultiLineString Z", container(1005, lineZ)},
		{"MultiPolygon Z", container(1006, polygonZ)},
		{"GeometryCollection Z", container(1007, pointZ)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			geo, ok := wkbToGeoJSON(tc.wkb, -1)
			require.True(t, ok)
			back, err := geoJSONToWKB(geo, 0)
			require.NoError(t, err)
			require.Equal(t, tc.wkb, back)
		})
	}

	t.Run("an empty member does not settle the dimension", func(t *testing.T) {
		// The first member carries no position, so the dimension is the next one's.
		geo := map[string]any{"type": "MultiLineString", "coordinates": []any{
			[]any{},
			[]any{[]float64{1, 2, 99}, []float64{3, 4, 98}},
		}}
		encoded, err := geoJSONToWKB(geo, 0)
		require.NoError(t, err)
		gType, _, ok := readWKBHeader(encoded)
		require.True(t, ok)
		require.Equal(t, uint32(1005), gType)

		back, ok := wkbToGeoJSON(encoded, -1)
		require.True(t, ok)
		require.Equal(t, [][][]float64{{}, {{1, 2, 99}, {3, 4, 98}}}, back["coordinates"])
	})

	t.Run("an empty collection member takes the dimension too", func(t *testing.T) {
		// An empty LineString Z carries no position of its own, so its dimension is the
		// one the collection's other members declare.
		emptyLineZ := binary.LittleEndian.AppendUint32([]byte{1}, 1002)
		emptyLineZ = binary.LittleEndian.AppendUint32(emptyLineZ, 0)
		wkb := container(1007, emptyLineZ, pointZ)

		geo, ok := wkbToGeoJSON(wkb, -1)
		require.True(t, ok)
		back, err := geoJSONToWKB(geo, 0)
		require.NoError(t, err)
		require.Equal(t, wkb, back)
	})

	t.Run("collection members of one dimension", func(t *testing.T) {
		_, err := geoJSONToWKB(map[string]any{
			"type": "GeometryCollection", "geometries": []any{
				map[string]any{"type": "Point", "coordinates": []float64{1, 2, 3}},
				map[string]any{"type": "Point", "coordinates": []float64{4, 5}},
			},
		}, 0)
		// The collection settles one dimension and the member that disagrees is reported
		// where its position is written, as a Multi* member would be.
		require.ErrorContains(t, err, "geometry declares 3")
	})

	t.Run("positions of one dimension", func(t *testing.T) {
		_, err := geoJSONToWKB(map[string]any{
			"type": "LineString", "coordinates": []any{[]float64{1, 2, 3}, []float64{4, 5}},
		}, 0)
		require.ErrorContains(t, err, "ordinates")
	})
}
