package types

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Helper functions to create various WKB geometries for testing

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

func Test_BoundingBoxCalculator_processWKB(t *testing.T) {
	t.Run("point_little_endian", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		wkb := createSimpleWKBPoint(10.5, 20.3, true)

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("too_short_wkb", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		wkb := []byte{1, 2, 3} // Less than 5 bytes

		err := calc.processWKB(wkb)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("invalid_geometry_type", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create WKB with invalid geometry type (999)
		wkb := []byte{1, 231, 3, 0, 0} // little-endian, type 999

		err := calc.processWKB(wkb)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("corrupted_u32_function", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create WKB where u32 function would fail (reading past buffer)
		wkb := []byte{1, 1} // little-endian, but missing type bytes

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("geometry_collection_empty", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		wkb := createWKBGeometryCollection([][]byte{}, true)

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(wkb)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multilinestring_u32_read_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiLineString where u32 read for linestring count fails
		buf := []byte{1, 5, 0, 0} // little-endian, type 5 (MultiLineString), but truncated

		err := calc.processWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("multipolygon_u32_read_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create MultiPolygon where u32 read for polygon count fails
		buf := []byte{1, 6, 0, 0} // little-endian, type 6 (MultiPolygon), but truncated

		err := calc.processWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})

	t.Run("geometry_collection_u32_read_failure", func(t *testing.T) {
		calc := NewBoundingBoxCalculator()
		// Create GeometryCollection where u32 read for geometry count fails
		buf := []byte{1, 7, 0, 0} // little-endian, type 7 (GeometryCollection), but truncated

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

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

		err := calc.processWKB(buf)

		require.NoError(t, err)
		_, _, _, _, ok := calc.GetBounds()
		require.False(t, ok)
	})
}
