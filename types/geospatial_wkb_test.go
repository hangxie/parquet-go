package types

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundCoordinate(t *testing.T) {
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
			name:      "very_high_precision_clamped_to_12",
			precision: 20, // clamped to maxCoordPrecision (12)
			input:     1.123456789012345,
			expected:  1.123456789012,
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
			result := roundCoordinate(tt.input, tt.precision)
			require.Equal(t, tt.expected, result, "roundCoordinate failed for %s", tt.name)
		})
	}
}

func TestWkbToGeoJSON_LineStringAndPolygon(t *testing.T) {
	ls := wkbLineStringLE([][]float64{{0, 0}, {1, 1.2}, {2, -3.4}})
	gj, ok := wkbToGeoJSON(ls, 6)
	require.True(t, ok)
	require.Equal(t, "LineString", gj["type"])
	require.Equal(t, [][]float64{{0, 0}, {1, 1.2}, {2, -3.4}}, gj["coordinates"])

	poly := wkbPolygonLE([][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}})
	gj2, ok := wkbToGeoJSON(poly, 6)
	require.True(t, ok)
	require.Equal(t, "Polygon", gj2["type"])
	require.Equal(t, [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}}, gj2["coordinates"])
}

func TestWkbToGeoJSON_InvalidInputs(t *testing.T) {
	// too short
	if _, ok := wkbToGeoJSON([]byte{1, 1, 0}, 6); ok {
		t.Fatal("expected failure for too short header")
	}
	// unknown type
	unk := make([]byte, 5)
	unk[0] = 1 // little-endian
	binary.LittleEndian.PutUint32(unk[1:5], 99)
	if _, ok := wkbToGeoJSON(unk, 6); ok {
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
	if _, ok := wkbToGeoJSON(trunc, 6); ok {
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
	if _, ok := wkbToGeoJSON(ptTrunc, 6); ok {
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
	if _, ok := wkbToGeoJSON(polyTrunc, 6); ok {
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
	if _, ok := wkbToGeoJSON(polyCoordsTrunc, 6); ok {
		t.Fatal("expected failure for truncated Polygon coordinate")
	}
}

func TestWkbToGeoJSON_MultiGeometries(t *testing.T) {
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
			gj, ok := wkbToGeoJSON(tt.wkb, 6)
			require.True(t, ok)
			require.Equal(t, tt.expectedType, gj["type"])
			tt.validateCoords(t, gj["coordinates"])
		})
	}
}

func TestWkbToGeoJSON_PrecisionHandling(t *testing.T) {
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
			gj, ok := wkbToGeoJSON(tt.wkb, tt.precision)
			require.True(t, ok)
			require.Equal(t, tt.expectedType, gj["type"])
			tt.validateCoords(t, gj["coordinates"])
		})
	}
}

func TestWkbToGeoJSON_Endianness(t *testing.T) {
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
			gj, ok := wkbToGeoJSON(tt.wkb, 6)
			require.True(t, ok)
			require.Equal(t, tt.expectedType, gj["type"])
			tt.validateCoords(t, gj["coordinates"])
		})
	}
}

// SPHERICAL GeoJSON tests

func TestWkbToGeoJSON_MultiGeometries_EdgeCases(t *testing.T) {
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

	_, ok := wkbToGeoJSON(wkb, 6)
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

	_, ok = wkbToGeoJSON(wkb, 6)
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

	_, ok = wkbToGeoJSON(wkb, 6)
	require.False(t, ok, "Should fail for MultiLineString with truncated point count")

	// Test completely empty multi-geometry headers
	for _, geomType := range []uint32{4, 5, 6} { // MultiPoint, MultiLineString, MultiPolygon
		wkb = make([]byte, 0)
		wkb = append(wkb, 1) // little-endian
		binary.LittleEndian.PutUint32(typeBuf, geomType)
		wkb = append(wkb, typeBuf...)
		// Missing count entirely

		_, ok = wkbToGeoJSON(wkb, 6)
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

func TestWkbToGeoJSON_GeometryCollection(t *testing.T) {
	// Create simple GeometryCollection with Point and LineString
	pointWKB := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x40}
	lineWKB := wkbLineStringLE([][]float64{{0, 0}, {1, 1}})

	wkb := buildWKBGeometryCollection([][]byte{pointWKB, lineWKB})
	gj, ok := wkbToGeoJSON(wkb, 6)
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

func TestWkbToGeoJSON_GeometryCollection_Complex(t *testing.T) {
	// Create complex GeometryCollection with multiple geometry types
	pointWKB := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40}
	multiPointWKB := buildWKBMultiPoint([][2]float64{{3, 4}, {5, 6}})
	polygonWKB := wkbPolygonLE([][][]float64{{{0, 0}, {2, 0}, {2, 2}, {0, 2}, {0, 0}}})

	wkb := buildWKBGeometryCollection([][]byte{pointWKB, multiPointWKB, polygonWKB})
	gj, ok := wkbToGeoJSON(wkb, 6)
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

func TestWkbToGeoJSON_GeometryCollection_Empty(t *testing.T) {
	// Test empty GeometryCollection
	wkb := buildWKBGeometryCollection([][]byte{})
	gj, ok := wkbToGeoJSON(wkb, 6)
	require.True(t, ok)
	require.Equal(t, "GeometryCollection", gj["type"])

	geometries := gj["geometries"].([]map[string]any)
	require.Len(t, geometries, 0)
}

func TestWkbToGeoJSON_GeometryCollection_Errors(t *testing.T) {
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

	_, ok := wkbToGeoJSON(wkb, 6)
	require.False(t, ok, "Should fail for GeometryCollection with missing geometry data")

	// Test GeometryCollection with invalid geometry
	invalidGeom := []byte{1, 99, 0, 0, 0} // invalid geometry type 99
	wkb2 := buildWKBGeometryCollection([][]byte{invalidGeom})
	_, ok = wkbToGeoJSON(wkb2, 6)
	require.False(t, ok, "Should fail for GeometryCollection with invalid geometry")
}

func TestCalculateWKBSize(t *testing.T) {
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

func TestWkbToGeoJSON_GeometryCollection_BigEndian(t *testing.T) {
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

	gj, ok := wkbToGeoJSON(wkb, 6)
	require.True(t, ok)
	require.Equal(t, "GeometryCollection", gj["type"])

	geometries := gj["geometries"].([]map[string]any)
	require.Len(t, geometries, 2)
	require.Equal(t, "Point", geometries[0]["type"])
	require.Equal(t, "LineString", geometries[1]["type"])
}

// Test error handling in parseLineString
func TestParseLineString_ErrorHandling(t *testing.T) {
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
			_, _, ok := parseLineString(tt.buffer, tt.be, tt.off, 6)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test error handling in parsePolygon
func TestParsePolygon_ErrorHandling(t *testing.T) {
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
			_, _, ok := parsePolygon(tt.buffer, tt.be, tt.off, 6)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test calculateWKBSize error paths and edge cases
func TestCalculateWKBSize_ErrorHandling(t *testing.T) {
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
func TestWkbToGeoJSON_ComprehensiveErrorHandling(t *testing.T) {
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
			_, ok := wkbToGeoJSON(tt.wkb, 6)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test WithGeospatialHybridRawBase64 configuration option
func TestWkbToGeoJSON_BigEndian_EdgeCases(t *testing.T) {
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
			_, ok := wkbToGeoJSON(tt.wkb, 6)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test wrapGeoJSONHybrid edge cases
func TestCalculateWKBSize_ValidGeometries(t *testing.T) {
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
func TestWkbToGeoJSON_InlineRoundFunction(t *testing.T) {
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
			result, ok := wkbToGeoJSON(tt.wkb, tt.precision)
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
func TestCalculateWKBSize_BigEndianPaths(t *testing.T) {
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
func TestWkbToGeoJSON_AdvancedErrorPaths(t *testing.T) {
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
			_, ok := wkbToGeoJSON(tt.wkb, 6)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test boundary conditions and edge cases for calculateWKBSize
func TestCalculateWKBSize_BoundaryConditions(t *testing.T) {
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
func TestParseGeometry_BoundaryConditions(t *testing.T) {
	// Test parseLineString with edge case: point count at buffer boundary
	t.Run("parseLineString_point_count_at_boundary", func(t *testing.T) {
		// Buffer with exactly enough space for point count but no coordinates
		buffer := []byte{1, 0, 0, 0} // 1 point
		_, _, ok := parseLineString(buffer, false, 0, 6)
		require.False(t, ok) // Should fail because no space for coordinates
	})

	t.Run("parsePolygon_ring_count_at_boundary", func(t *testing.T) {
		// Buffer with exactly enough space for ring count but no ring data
		buffer := []byte{1, 0, 0, 0} // 1 ring
		_, _, ok := parsePolygon(buffer, false, 0, 6)
		require.False(t, ok) // Should fail because no space for ring data
	})
}

// Test specific uncovered paths in parsePolygon
func TestParsePolygon_UncoveredPaths(t *testing.T) {
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
			_, _, ok := parsePolygon(tt.buffer, tt.be, tt.off, 6)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test specific uncovered paths in wkbToGeoJSON MultiLineString
func TestWkbToGeoJSON_MultiLineString_UncoveredPaths(t *testing.T) {
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
			_, ok := wkbToGeoJSON(tt.wkb, 6)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test very specific boundary conditions for maximum coverage
func TestWkbToGeoJSON_PrecisionBoundaryConditions(t *testing.T) {
	t.Run("inline_round_exactly_12_precision", func(t *testing.T) {
		// Test the p > 12 branch and p = 12 assignment in inline round function
		wkb := wkbPoint(1, 1.123456789012345, 2.0)
		result, ok := wkbToGeoJSON(wkb, 12)
		require.True(t, ok)
		require.Equal(t, "Point", result["type"])

		coords := result["coordinates"].([]float64)
		require.Len(t, coords, 2)
		require.Equal(t, 1.123456789012, coords[0]) // Should be rounded to 12 places
	})

	t.Run("inline_round_exactly_0_precision", func(t *testing.T) {
		// Test the p < 0 branch and p = 0 assignment
		wkb := wkbPoint(1, 1.7, 2.3)
		result, ok := wkbToGeoJSON(wkb, 0)
		require.True(t, ok)
		require.Equal(t, "Point", result["type"])

		coords := result["coordinates"].([]float64)
		require.Len(t, coords, 2)
		require.Equal(t, 2.0, coords[0]) // Should be rounded to 0 places
		require.Equal(t, 2.0, coords[1])
	})
}

// Test u32 function error paths in wkbToGeoJSON
func TestWkbToGeoJSON_u32_ErrorPaths(t *testing.T) {
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
			_, ok := wkbToGeoJSON(tt.wkb, 6)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test the remaining very specific paths to maximize coverage
func TestWkbToGeoJSON_MaximumCoverage(t *testing.T) {
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
			_, ok := wkbToGeoJSON(tt.wkb, 6)
			require.Equal(t, tt.expectOK, ok)
		})
	}
}

// Test the remaining parsePolygon big-endian paths
func TestParsePolygon_MaximumCoverage(t *testing.T) {
	t.Run("parsePolygon_big_endian_ring_point_count", func(t *testing.T) {
		// Test the big-endian path for reading ring point count
		buffer := make([]byte, 16)
		binary.BigEndian.PutUint32(buffer[0:4], 1) // numRings = 1 (big-endian)
		binary.BigEndian.PutUint32(buffer[4:8], 2) // numPoints = 2 (big-endian)
		// Not enough coordinate data will cause failure

		_, _, ok := parsePolygon(buffer, true, 0, 6) // be = true to trigger big-endian path
		require.False(t, ok)                         // Should fail due to insufficient coordinate data
	})

	t.Run("parsePolygon_little_endian_ring_point_count", func(t *testing.T) {
		// Test the little-endian path for reading ring point count
		buffer := make([]byte, 16)
		binary.LittleEndian.PutUint32(buffer[0:4], 1) // numRings = 1 (little-endian)
		binary.LittleEndian.PutUint32(buffer[4:8], 2) // numPoints = 2 (little-endian)
		// Not enough coordinate data will cause failure

		_, _, ok := parsePolygon(buffer, false, 0, 6) // be = false to trigger little-endian path
		require.False(t, ok)                          // Should fail due to insufficient coordinate data
	})
}

func TestGeometryCollectionToGeoJSONTruncatedCount(t *testing.T) {
	gj, ok := geometryCollectionToGeoJSON([]byte{1, 2, 3}, 0, false, 6)

	require.False(t, ok)
	require.Nil(t, gj)
}

func TestGeometryCollectionToGeoJSONInvalidSubGeometryBranches(t *testing.T) {
	t.Run("calculated_size_exceeds_buffer", func(t *testing.T) {
		wkb := []byte{1}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, 1)
		wkb = append(wkb, buf...)
		wkb = append(wkb, 1)
		binary.LittleEndian.PutUint32(buf, WKBMultiPoint)
		wkb = append(wkb, buf...)
		binary.LittleEndian.PutUint32(buf, 1)
		wkb = append(wkb, buf...)

		gj, ok := geometryCollectionToGeoJSON(wkb, 1, false, 6)

		require.False(t, ok)
		require.Nil(t, gj)
	})

	t.Run("subgeometry_parse_fails_after_size_calculation", func(t *testing.T) {
		wkb := []byte{1}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, 1)
		wkb = append(wkb, buf...)
		wkb = append(wkb, 1)
		binary.LittleEndian.PutUint32(buf, WKBMultiPoint)
		wkb = append(wkb, buf...)
		binary.LittleEndian.PutUint32(buf, 1)
		wkb = append(wkb, buf...)
		wkb = append(wkb, 1)
		binary.LittleEndian.PutUint32(buf, WKBLineString)
		wkb = append(wkb, buf...)
		wkb = append(wkb, make([]byte, 16)...)

		gj, ok := geometryCollectionToGeoJSON(wkb, 1, false, 6)

		require.False(t, ok)
		require.Nil(t, gj)
	})
}

// Test edge case where GeometryCollection buffer ends exactly at geometry boundary
