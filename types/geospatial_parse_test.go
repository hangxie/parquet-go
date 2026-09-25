package types

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

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
			_, _, ok := parseLineString(tt.buffer, tt.be, tt.off, 6, WKBLineString)
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
			_, _, ok := parsePolygon(tt.buffer, tt.be, tt.off, 6, WKBPolygon)
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
			_, _, ok := parsePolygon(tt.buffer, tt.be, tt.off, 6, WKBPolygon)
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

		_, _, ok := parsePolygon(buffer, true, 0, 6, WKBPolygon) // be = true to trigger big-endian path
		require.False(t, ok)                                     // Should fail due to insufficient coordinate data
	})

	t.Run("parsePolygon_little_endian_ring_point_count", func(t *testing.T) {
		// Test the little-endian path for reading ring point count
		buffer := make([]byte, 16)
		binary.LittleEndian.PutUint32(buffer[0:4], 1) // numRings = 1 (little-endian)
		binary.LittleEndian.PutUint32(buffer[4:8], 2) // numPoints = 2 (little-endian)
		// Not enough coordinate data will cause failure

		_, _, ok := parsePolygon(buffer, false, 0, 6, WKBPolygon) // be = false to trigger little-endian path
		require.False(t, ok)                                      // Should fail due to insufficient coordinate data
	})
}

// TestCalculateWKBSize_MemberHeaders covers the member checks the size walk shares with the
// other readers: a member whose type is not the one its container requires, a byte order byte
// the format does not define, and a MultiPoint member that is not the fixed size a Point takes.
func TestCalculateWKBSize_MemberHeaders(t *testing.T) {
	multi := func(gType uint32, members ...[]byte) []byte {
		out := binary.LittleEndian.AppendUint32([]byte{1}, gType)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(members)))
		for _, member := range members {
			out = append(out, member...)
		}
		return out
	}
	point := wkbPoint(1, 1.0, 2.0)                                        // 21 bytes
	line := wkbLineStringLE([][]float64{{3.0, 4.0}})                      // 25 bytes
	poly := wkbPolygonLE([][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}}) // 77 bytes
	badOrder := append([]byte{2}, line[1:]...)
	// A member carries its own byte order, so a big-endian one inside a little-endian
	// container is legal and must measure the same as its little-endian twin.
	lineBE := binary.BigEndian.AppendUint32([]byte{0}, WKBLineString)
	lineBE = binary.BigEndian.AppendUint32(lineBE, 1)
	lineBE = binary.BigEndian.AppendUint64(lineBE, math.Float64bits(3.0))
	lineBE = binary.BigEndian.AppendUint64(lineBE, math.Float64bits(4.0))

	tests := []struct {
		name string
		wkb  []byte
		size int
	}{
		{"multipoint", multi(WKBMultiPoint, point, point), 9 + 21 + 21},
		{"multipoint with line member", multi(WKBMultiPoint, point, line), 0},
		{"multipoint with big-endian member", multi(WKBMultiPoint, point, wkbPoint(0, 3.0, 4.0)), 9 + 21 + 21},
		{"multilinestring", multi(WKBMultiLineString, line, line), 9 + 25 + 25},
		{"multilinestring with big-endian member", multi(WKBMultiLineString, line, lineBE), 9 + 25 + 25},
		{"multilinestring with point member", multi(WKBMultiLineString, line, point), 0},
		{"multilinestring with undefined byte order", multi(WKBMultiLineString, line, badOrder), 0},
		{"multipolygon", multi(WKBMultiPolygon, poly), 9 + 77},
		{"multipolygon with line member", multi(WKBMultiPolygon, poly, line), 0},
		{"collection", multi(WKBGeometryCollection, point, line), 9 + 21 + 25},
		{"collection with big-endian member", multi(WKBGeometryCollection, point, lineBE), 9 + 21 + 25},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, ok := calculateWKBSize(tt.wkb)
			require.Equal(t, tt.size != 0, ok)
			require.Equal(t, tt.size, size)
		})
	}
}
