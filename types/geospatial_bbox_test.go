package types

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundingBoxCalculatorAddGeometryCollectionTruncatedCount(t *testing.T) {
	calc := NewBoundingBoxCalculator()

	calc.addGeometryCollectionWKB([]byte{1, 2, 3}, 0, false, 0)

	_, _, _, _, ok := calc.GetBounds()
	require.False(t, ok)
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
		// The collection holds a geometry whose coordinates the walk cannot read, so a
		// box built from the rest would be smaller than the value it describes.
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
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

		// Second point: big-endian, which the member's own byte order byte declares
		buf = append(buf, 0) // big-endian
		binary.BigEndian.PutUint32(tmp, WKBPoint)
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint64(coordBuf, math.Float64bits(15.0))
		buf = append(buf, coordBuf...)
		binary.BigEndian.PutUint64(coordBuf, math.Float64bits(20.0))
		buf = append(buf, coordBuf...)

		err := calc.AddWKB(buf)

		require.NoError(t, err)
		// Each member of a Multi* carries its own byte order byte, so mixing the two is
		// legal WKB and both points are read.
		minX, minY, maxX, maxY, ok := calc.GetBounds()
		require.True(t, ok)
		require.Equal(t, []float64{5, 10, 15, 20}, []float64{minX, minY, maxX, maxY})
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

		// The truncated member withdraws the bounds the valid ones built: what it holds
		// is unknown, and a box that leaves it out is one a spatial filter would trust.
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
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

func TestAddWKB_NestingDepth(t *testing.T) {
	// An empty collection leaves nothing to measure, so what is asserted there is that the
	// value was read at all rather than withdrawn as unreadable.
	for _, nested := range []func(int) []byte{nestedCollectionWKB, nestedEmptyCollectionWKB} {
		calc := NewBoundingBoxCalculator()
		require.NoError(t, calc.AddWKB(nested(maxGeometryDepth)))
		require.False(t, calc.BoundsUnknown())

		calc = NewBoundingBoxCalculator()
		require.NoError(t, calc.AddWKB(nested(maxGeometryDepth+1)))
		require.True(t, calc.BoundsUnknown())
	}
}
