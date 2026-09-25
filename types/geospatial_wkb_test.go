package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestCappedCap(t *testing.T) {
	require.Equal(t, 0, cappedCap(5, -1)) // negative remaining is clamped to 0
	require.Equal(t, 3, cappedCap(5, 3))  // count exceeds remaining -> remaining
	require.Equal(t, 5, cappedCap(5, 10)) // count fits -> count
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

// Test edge cases in parseLineString and parsePolygon with boundary conditions
func TestParseGeometry_BoundaryConditions(t *testing.T) {
	// Test parseLineString with edge case: point count at buffer boundary
	t.Run("parseLineString_point_count_at_boundary", func(t *testing.T) {
		// Buffer with exactly enough space for point count but no coordinates
		buffer := []byte{1, 0, 0, 0} // 1 point
		_, _, ok := parseLineString(buffer, false, 0, 6, WKBLineString)
		require.False(t, ok) // Should fail because no space for coordinates
	})

	t.Run("parsePolygon_ring_count_at_boundary", func(t *testing.T) {
		// Buffer with exactly enough space for ring count but no ring data
		buffer := []byte{1, 0, 0, 0} // 1 ring
		_, _, ok := parsePolygon(buffer, false, 0, 6, WKBPolygon)
		require.False(t, ok) // Should fail because no space for ring data
	})
}

// Test edge case where GeometryCollection buffer ends exactly at geometry boundary

// wkbWithOrdinates builds an ISO WKB geometry whose header declares gType and whose points
// carry as many ordinates as that type implies.
func wkbWithOrdinates(gType uint32, counts []uint32, points [][]float64) []byte {
	buf := binary.LittleEndian.AppendUint32([]byte{1}, gType)
	for _, c := range counts {
		buf = binary.LittleEndian.AppendUint32(buf, c)
	}
	for _, p := range points {
		for _, ordinate := range p {
			buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(ordinate))
		}
	}
	return buf
}

// TestWkbMAndZMAreNotRendered covers a declared measure: not rendered, but measured.
func TestWkbMAndZMAreNotRendered(t *testing.T) {
	tests := []struct {
		name string
		wkb  []byte
	}{
		{"Point M", wkbWithOrdinates(2001, nil, [][]float64{{1, 2, 99}})},
		{"Point ZM", wkbWithOrdinates(3001, nil, [][]float64{{1, 2, 99, 98}})},
		{
			"LineString M",
			wkbWithOrdinates(2002, []uint32{3}, [][]float64{{1, 2, 99}, {3, 4, 98}, {5, 6, 97}}),
		},
		{
			"Polygon ZM",
			wkbWithOrdinates(3003, []uint32{1, 4}, [][]float64{{0, 0, 9, 8}, {1, 0, 9, 8}, {1, 1, 9, 8}, {0, 0, 9, 8}}),
		},
	}

	geoJSON := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON))
	hybrid := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeHybrid))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := wkbToGeoJSON(tt.wkb, 6)
			require.False(t, ok)

			// Both renderings that place coordinates fall back to the hex substitute;
			// hybrid would otherwise pair the raw bytes with GeoJSON they do not hold.
			for mode, cfg := range map[string]*GeospatialConfig{"geojson": geoJSON, "hybrid": hybrid} {
				rendered := ConvertGeometryLogicalValue(tt.wkb, nil, cfg)
				require.Contains(t, rendered, "wkb_hex", mode)
				require.NotContains(t, rendered, "type", mode)
			}

			// Measuring reads structure rather than coordinates, so it covers every
			// dimension, and the bounding box takes the x and y it is defined over.
			size, ok := calculateWKBSize(tt.wkb)
			require.True(t, ok)
			require.Equal(t, len(tt.wkb), size)

			calc := NewBoundingBoxCalculator()
			require.NoError(t, calc.AddWKB(tt.wkb))
			_, _, _, _, ok = calc.GetBounds()
			require.True(t, ok)
		})
	}
}

// TestBoundingBoxWithdrawnForUnreadableGeometry covers a set mixing geometries the walk can
// read with one it cannot. Bounds built from the readable ones alone would be smaller than
// the data they describe, and a reader pushing a spatial filter down to them would skip
// rows that match, so the whole set reports none.
func TestBoundingBoxWithdrawnForUnreadableGeometry(t *testing.T) {
	twoD := func(x, y float64) []byte {
		b := binary.LittleEndian.AppendUint32([]byte{1}, 1)
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(x))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(y))
	}
	// A byte order byte the format does not define: unreadable at any dimension.
	unreadable := wkbWithOrdinates(1, nil, [][]float64{{100, 200}})
	unreadable[0] = 2

	// The readable values alone have bounds, and no doubt about them.
	calc := NewBoundingBoxCalculator()
	require.NoError(t, calc.AddWKB(twoD(1, 2)))
	require.NoError(t, calc.AddWKB(twoD(3, 4)))
	minX, minY, maxX, maxY, ok := calc.GetBounds()
	require.True(t, ok)
	require.Equal(t, []float64{1, 2, 3, 4}, []float64{minX, minY, maxX, maxY})
	require.False(t, calc.BoundsUnknown())

	// A calculator given nothing has no bounds either, which is not the same thing: the
	// chunk skips such a page rather than withholding its own box.
	require.False(t, NewBoundingBoxCalculator().BoundsUnknown())

	// An unreadable geometry outside their extent withdraws them, in any order.
	for _, values := range [][][]byte{
		{twoD(1, 2), twoD(3, 4), unreadable},
		{unreadable, twoD(1, 2), twoD(3, 4)},
		{twoD(1, 2), []byte{1, 2}, twoD(3, 4)},
	} {
		mixed := NewBoundingBoxCalculator()
		for _, v := range values {
			require.NoError(t, mixed.AddWKB(v))
		}
		_, _, _, _, ok := mixed.GetBounds()
		require.False(t, ok)
		require.True(t, mixed.BoundsUnknown())
	}
}

// TestBoundingBoxWithdrawnForTruncatedGeometry covers the points where a multi-geometry or
// collection walk gives up part way. Whatever it had already measured describes less than
// the value does, so the bounds go with it.
func TestBoundingBoxWithdrawnForTruncatedGeometry(t *testing.T) {
	header := func(gType uint32, counts ...uint32) []byte {
		b := binary.LittleEndian.AppendUint32([]byte{1}, gType)
		for _, c := range counts {
			b = binary.LittleEndian.AppendUint32(b, c)
		}
		return b
	}
	point := func(x, y float64) []byte {
		b := binary.LittleEndian.AppendUint32([]byte{1}, 1)
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(x))
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(y))
	}

	tests := []struct {
		name string
		wkb  []byte
	}{
		{"MultiPoint with no count", header(4)},
		{"MultiPoint missing a member", append(header(4, 2), point(1, 2)...)},
		{"MultiPoint with truncated coordinates", append(header(4, 1), 1, 1, 0, 0, 0, 0)},
		// A member's byte order byte is held to the same two values as the outer one.
		{"MultiPoint member with an undefined byte order", append(header(4, 1), 7, 1, 0, 0, 0)},
		{"MultiLineString with no count", header(5)},
		{"MultiLineString missing a member", append(header(5, 2), header(2, 1)...)},
		{"MultiPolygon with no count", header(6)},
		{"MultiPolygon missing a member", append(header(6, 2), header(3, 1)...)},
		{"GeometryCollection with no count", header(7)},
		{"GeometryCollection with unreadable member", append(header(7, 1), 1, 9, 9, 9, 9)},
		{"GeometryCollection with a member past the end", append(header(7, 2), point(1, 2)...)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calc := NewBoundingBoxCalculator()
			require.NoError(t, calc.AddWKB(point(5, 6)))
			require.NoError(t, calc.AddWKB(tt.wkb))
			_, _, _, _, ok := calc.GetBounds()
			require.False(t, ok)
		})
	}
}

// wkbHeader spells a WKB header: a byte order byte followed by the geometry type.
func wkbHeader(order byte, gType uint32, bigEndian bool) []byte {
	if bigEndian {
		return binary.BigEndian.AppendUint32([]byte{order}, gType)
	}
	return binary.LittleEndian.AppendUint32([]byte{order}, gType)
}

// TestReadWKBHeader covers the header rules every geospatial reader here shares. Bytes that
// are not WKB read as a byte order and a type number all the same, and a code the format
// does not define is worse in a file's geospatial_types than no list at all.
func TestReadWKBHeader(t *testing.T) {
	tests := []struct {
		name     string
		wkb      []byte
		wantType uint32
		wantBE   bool
		wantOK   bool
		wantIs2D bool
	}{
		{"Point little-endian", wkbHeader(1, 1, false), 1, false, true, true},
		{"Point big-endian", wkbHeader(0, 1, true), 1, true, true, true},
		{"GeometryCollection", wkbHeader(1, 7, false), 7, false, true, true},
		{"Point Z", wkbHeader(1, 1001, false), 1001, false, true, false},
		{"Point M", wkbHeader(1, 2001, false), 2001, false, true, false},
		{"Point ZM", wkbHeader(1, 3001, false), 3001, false, true, false},
		{"byte order 2", wkbHeader(2, 1, false), 0, false, false, false},
		{"byte order 255", wkbHeader(255, 1, false), 0, false, false, false},
		{"base type 0", wkbHeader(1, 0, false), 0, false, false, false},
		// The standardized code space runs to Triangle, and codes above 7 have no reader: the
		// header takes them so the type list can carry them, and the coordinate readers
		// refuse them as they refuse a Z or M geometry.
		{"CircularString", wkbHeader(1, 8, false), 8, false, true, true},
		{"PolyhedralSurface", wkbHeader(1, 15, false), 15, false, true, true},
		{"TIN", wkbHeader(1, 16, false), 16, false, true, true},
		{"Triangle", wkbHeader(1, 17, false), 17, false, true, true},
		{"Triangle Z", wkbHeader(1, 1017, false), 1017, false, true, false},
		{"base type 18", wkbHeader(1, 18, false), 0, false, false, false},
		{"dimension 4000", wkbHeader(1, 4001, false), 0, false, false, false},
		{"EWKB SRID flag", wkbHeader(1, 0x20000001, false), 0, false, false, false},
		{"EWKB Z flag", wkbHeader(1, 0x80000001, false), 0, false, false, false},
		{"bytes that are not WKB", []byte("not-wkb"), 0, false, false, false},
		{"too short", []byte{1, 1, 0, 0}, 0, false, false, false},
		{"empty", nil, 0, false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gType, bigEndian, ok := readWKBHeader(tt.wkb)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantType, gType)
			require.Equal(t, tt.wantBE, bigEndian)
			if ok {
				require.Equal(t, !tt.wantIs2D, wkbHasZ(gType) || wkbHasM(gType))
			}

			// The rendering and the bounds read the same header, so a value this
			// rejects contributes to neither.
			if !ok {
				_, geoOK := wkbToGeoJSON(tt.wkb, 6)
				require.False(t, geoOK)
				calc := NewBoundingBoxCalculator()
				require.NoError(t, calc.AddWKB(tt.wkb))
				require.True(t, calc.BoundsUnknown())
			}
		})
	}
}

// TestWkbZAndMSurviveByteModes covers the modes that do not read coordinates. Hex, base64
// and raw carry a geometry's bytes whatever its dimension, so a Z, M or ZM value round
// trips through them untouched; only the readings that place coordinates decline it.
func TestWkbZAndMSurviveByteModes(t *testing.T) {
	values := map[string][]byte{
		"Point Z":      wkbWithOrdinates(1001, nil, [][]float64{{1, 2, 3}}),
		"Point M":      wkbWithOrdinates(2001, nil, [][]float64{{1, 2, 3}}),
		"Point ZM":     wkbWithOrdinates(3001, nil, [][]float64{{1, 2, 3, 4}}),
		"LineString Z": wkbWithOrdinates(1002, []uint32{2}, [][]float64{{1, 2, 3}, {4, 5, 6}}),
	}

	hexSubstitute := func(wkb []byte) map[string]any {
		return map[string]any{"wkb_hex": hexEncode(wkb), "crs": "OGC:CRS84"}
	}

	se := &parquet.SchemaElement{
		Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
		LogicalType: &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()},
	}

	for name, wkb := range values {
		t.Run(name, func(t *testing.T) {
			// Raw mode carries the bytes whatever the geometry's dimension, which is how
			// such a value round trips through this library: it reads back as the base64
			// the raw write path takes.
			got, err := ConvertValue(string(wkb), se, WithValueMode(ValueModeRaw))
			require.NoError(t, err)
			require.Equal(t, base64.StdEncoding.EncodeToString(wkb), got)

			hexCfg := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeHex))
			require.Equal(t, hexSubstitute(wkb), ConvertGeometryLogicalValue(wkb, nil, hexCfg))

			b64Cfg := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeBase64))
			require.Equal(t, map[string]any{
				"wkb_b64": base64.StdEncoding.EncodeToString(wkb), "crs": "OGC:CRS84",
			}, ConvertGeometryLogicalValue(wkb, nil, b64Cfg))

			// The rendering that places coordinates reads Z and declines a measure, which
			// a GeoJSON position has no element for.
			geoJSON := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON))
			rendered := ConvertGeometryLogicalValue(wkb, nil, geoJSON)
			if strings.Contains(name, "M") {
				require.Equal(t, hexSubstitute(wkb), rendered)
				return
			}
			require.Contains(t, rendered, "type")
		})
	}
}

// hexEncode spells the WKB the hex mode emits.
func hexEncode(b []byte) string {
	return hex.EncodeToString(b)
}

// TestWKBGeometryType covers the one header reading this package exports, which the writer
// uses to build the geometry type list. It reports the declared type, Z and M included,
// since a dimension the coordinate readers cannot place is still a type the format defines.
func TestWKBGeometryType(t *testing.T) {
	tests := []struct {
		name     string
		wkb      []byte
		wantType int32
		wantOK   bool
	}{
		{"point", wkbHeader(1, WKBPoint, false), 1, true},
		{"polygon big endian", wkbHeader(0, WKBPolygon, true), 3, true},
		{"point Z", wkbHeader(1, 1001, false), 1001, true},
		{"point ZM", wkbHeader(1, 3001, false), 3001, true},
		// Recognized codes with no reader here still name a type the format defines, so the
		// writer's type list keeps them rather than going unknown.
		{"Triangle", wkbHeader(1, 17, false), 17, true},
		{"TIN", wkbHeader(1, 16, false), 16, true},
		{"base type 18", wkbHeader(1, 18, false), 0, false},
		{"byte order 7", wkbHeader(7, WKBPoint, false), 0, false},
		{"dimension above ZM", wkbHeader(1, 4001, false), 0, false},
		{"EWKB SRID flag", wkbHeader(1, 0x20000001, false), 0, false},
		{"not WKB", []byte("not-wkb"), 0, false},
		{"empty", nil, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gType, ok := WKBGeometryType(tt.wkb)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantType, gType)
		})
	}
}

func TestWKBReaders_NestingDepth(t *testing.T) {
	testCases := []struct {
		name   string
		nested func(int) []byte
	}{
		{"innermost collection holds a point", nestedCollectionWKB},
		{"innermost collection is empty", nestedEmptyCollectionWKB},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			atLimit := tc.nested(maxGeometryDepth)
			pastLimit := tc.nested(maxGeometryDepth + 1)

			_, ok := wkbToGeoJSON(atLimit, -1)
			require.True(t, ok)
			_, ok = wkbToGeoJSON(pastLimit, -1)
			require.False(t, ok)

			size, ok := calculateWKBSize(atLimit)
			require.True(t, ok)
			require.Equal(t, len(atLimit), size)
			_, ok = calculateWKBSize(pastLimit)
			require.False(t, ok)
		})
	}
}

// TestWkbZIsRead covers ISO geometries carrying Z, which render with their elevation.
func TestWkbZIsRead(t *testing.T) {
	container := func(gType uint32, members ...[]byte) []byte {
		out := binary.LittleEndian.AppendUint32([]byte{1}, gType)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(members)))
		for _, member := range members {
			out = append(out, member...)
		}
		return out
	}
	pointZ := wkbWithOrdinates(1001, nil, [][]float64{{1, 2, 99}})
	lineZ := wkbWithOrdinates(1002, []uint32{3}, [][]float64{{1, 2, 99}, {3, 4, 98}, {5, 6, 97}})
	polygonZ := wkbWithOrdinates(1003, []uint32{1, 4}, [][]float64{{0, 0, 9}, {1, 0, 9}, {1, 1, 9}, {0, 0, 9}})

	testCases := []struct {
		name string
		wkb  []byte
		want map[string]any
	}{
		{"Point Z", pointZ, map[string]any{"type": "Point", "coordinates": []float64{1, 2, 99}}},
		{
			"LineString Z", lineZ,
			map[string]any{"type": "LineString", "coordinates": [][]float64{{1, 2, 99}, {3, 4, 98}, {5, 6, 97}}},
		},
		{
			"Polygon Z", polygonZ,
			map[string]any{"type": "Polygon", "coordinates": [][][]float64{{{0, 0, 9}, {1, 0, 9}, {1, 1, 9}, {0, 0, 9}}}},
		},
		{
			"MultiPoint Z", container(1004, pointZ),
			map[string]any{"type": "MultiPoint", "coordinates": [][]float64{{1, 2, 99}}},
		},
		{
			"MultiLineString Z", container(1005, lineZ),
			map[string]any{"type": "MultiLineString", "coordinates": [][][]float64{{{1, 2, 99}, {3, 4, 98}, {5, 6, 97}}}},
		},
		{
			"MultiPolygon Z", container(1006, polygonZ),
			map[string]any{"type": "MultiPolygon", "coordinates": [][][][]float64{
				{{{0, 0, 9}, {1, 0, 9}, {1, 1, 9}, {0, 0, 9}}},
			}},
		},
		{
			"GeometryCollection Z", container(1007, pointZ),
			map[string]any{"type": "GeometryCollection", "geometries": []map[string]any{
				{"type": "Point", "coordinates": []float64{1, 2, 99}},
			}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := wkbToGeoJSON(tc.wkb, 6)
			require.True(t, ok)
			require.Equal(t, tc.want, got)

			size, ok := calculateWKBSize(tc.wkb)
			require.True(t, ok)
			require.Equal(t, len(tc.wkb), size)

			// The Parquet bounding box is 2D, so a Z geometry contributes x and y.
			calc := NewBoundingBoxCalculator()
			require.NoError(t, calc.AddWKB(tc.wkb))
			_, _, _, _, ok = calc.GetBounds()
			require.True(t, ok)
		})
	}
}

// TestWkbEmptyZIsNotRendered covers a Z geometry carrying no position at all: its GeoJSON
// is the same as an empty 2D one, so rendering it would write back as 2D and lose the
// dimension the value declares.
func TestWkbEmptyZIsNotRendered(t *testing.T) {
	emptyLine := func(gType uint32) []byte {
		b := binary.LittleEndian.AppendUint32([]byte{1}, gType)
		return binary.LittleEndian.AppendUint32(b, 0)
	}
	collection := func(gType uint32, member []byte) []byte {
		b := binary.LittleEndian.AppendUint32([]byte{1}, gType)
		b = binary.LittleEndian.AppendUint32(b, 1)
		return append(b, member...)
	}
	pointZ := wkbWithOrdinates(1001, nil, [][]float64{{1, 2, 99}})

	t.Run("an empty Z geometry keeps its bytes", func(t *testing.T) {
		for _, wkb := range [][]byte{emptyLine(1002), collection(1007, emptyLine(1002))} {
			_, ok := wkbToGeoJSON(wkb, -1)
			require.False(t, ok)

			cfg := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON))
			require.Contains(t, ConvertGeometryLogicalValue(wkb, nil, cfg), "wkb_hex")

			// Measuring reads structure rather than positions, so it is unaffected.
			size, ok := calculateWKBSize(wkb)
			require.True(t, ok)
			require.Equal(t, len(wkb), size)
		}
	})

	t.Run("an empty 2D geometry renders, having no dimension to lose", func(t *testing.T) {
		got, ok := wkbToGeoJSON(emptyLine(WKBLineString), -1)
		require.True(t, ok)
		require.Equal(t, map[string]any{"type": "LineString", "coordinates": [][]float64{}}, got)
	})

	t.Run("one position is enough to keep the dimension", func(t *testing.T) {
		wkb := collection(1007, pointZ)
		got, ok := wkbToGeoJSON(wkb, -1)
		require.True(t, ok)
		back, err := geoJSONToWKB(got, 0)
		require.NoError(t, err)
		require.Equal(t, wkb, back)
	})
}

// TestWkbCollectionMemberDimension covers a GeometryCollection whose member declares
// another dimension. One dimension covers a geometry and every part of it, which is what
// the size walk and the write path already require, so the rendering requires it too.
func TestWkbCollectionMemberDimension(t *testing.T) {
	collection := func(gType uint32, members ...[]byte) []byte {
		b := binary.LittleEndian.AppendUint32([]byte{1}, gType)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(members)))
		for _, member := range members {
			b = append(b, member...)
		}
		return b
	}
	pointZ := wkbWithOrdinates(1001, nil, [][]float64{{1, 2, 99}})
	point2D := wkbWithOrdinates(1, nil, [][]float64{{3, 4}})

	testCases := []struct {
		name string
		wkb  []byte
	}{
		{"a 2D member of a Z collection", collection(1007, pointZ, point2D)},
		{"a Z member of a 2D collection", collection(WKBGeometryCollection, point2D, pointZ)},
		{"through a nested collection", collection(1007, collection(WKBGeometryCollection, point2D))},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := wkbToGeoJSON(tc.wkb, -1)
			require.False(t, ok)

			cfg := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON))
			require.Contains(t, ConvertGeometryLogicalValue(tc.wkb, nil, cfg), "wkb_hex")

			// The size walk and the write path refuse it, which is what the rendering
			// now agrees with.
			_, ok = calculateWKBSize(tc.wkb)
			require.False(t, ok)
			require.Error(t, checkWKB(tc.wkb, "GEOMETRY"))
		})
	}

	t.Run("members of the collection's own dimension render", func(t *testing.T) {
		wkb := collection(1007, pointZ, wkbWithOrdinates(1001, nil, [][]float64{{5, 6, 7}}))
		_, ok := wkbToGeoJSON(wkb, -1)
		require.True(t, ok)
	})
}
