package layout

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Helper function to create WKB point data
func createWKBPoint(x, y float64, littleEndian bool) []byte {
	buf := make([]byte, 21) // 1 + 4 + 16 bytes
	if littleEndian {
		buf[0] = 1                                 // little-endian
		binary.LittleEndian.PutUint32(buf[1:5], 1) // Point type
		binary.LittleEndian.PutUint64(buf[5:13], math.Float64bits(x))
		binary.LittleEndian.PutUint64(buf[13:21], math.Float64bits(y))
	} else {
		buf[0] = 0                              // big-endian
		binary.BigEndian.PutUint32(buf[1:5], 1) // Point type
		binary.BigEndian.PutUint64(buf[5:13], math.Float64bits(x))
		binary.BigEndian.PutUint64(buf[13:21], math.Float64bits(y))
	}
	return buf
}

// Helper function to create WKB LineString data
func createWKBLineString(coords [][2]float64, littleEndian bool) []byte {
	buf := make([]byte, 0, 9+len(coords)*16) // header + points
	if littleEndian {
		buf = append(buf, 1) // little-endian
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, 2) // LineString type
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint32(tmp, uint32(len(coords))) // point count
		buf = append(buf, tmp...)

		coordBuf := make([]byte, 8)
		for _, coord := range coords {
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(coord[0]))
			buf = append(buf, coordBuf...)
			binary.LittleEndian.PutUint64(coordBuf, math.Float64bits(coord[1]))
			buf = append(buf, coordBuf...)
		}
	} else {
		buf = append(buf, 0) // big-endian
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, 2) // LineString type
		buf = append(buf, tmp...)
		binary.BigEndian.PutUint32(tmp, uint32(len(coords))) // point count
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

func Test_ExtractGeometryType(t *testing.T) {
	t.Run("point_little_endian", func(t *testing.T) {
		wkb := createWKBPoint(10.5, 20.3, true)
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(1), geoType) // Point = 1
	})

	t.Run("point_big_endian", func(t *testing.T) {
		wkb := createWKBPoint(10.5, 20.3, false)
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(1), geoType) // Point = 1
	})

	t.Run("linestring_little_endian", func(t *testing.T) {
		coords := [][2]float64{{0, 0}, {10, 10}}
		wkb := createWKBLineString(coords, true)
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(2), geoType) // LineString = 2
	})

	t.Run("linestring_big_endian", func(t *testing.T) {
		coords := [][2]float64{{0, 0}, {10, 10}}
		wkb := createWKBLineString(coords, false)
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(2), geoType) // LineString = 2
	})

	t.Run("empty_wkb", func(t *testing.T) {
		wkb := []byte{}
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(0), geoType)
	})

	t.Run("too_short_wkb", func(t *testing.T) {
		wkb := []byte{1, 2, 3} // Less than 5 bytes
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(0), geoType)
	})

	t.Run("polygon_type", func(t *testing.T) {
		// Create minimal polygon WKB (just header for testing type extraction)
		wkb := []byte{1, 3, 0, 0, 0} // little-endian, type 3 (Polygon)
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(3), geoType) // Polygon = 3
	})
}

func Test_ComputePageGeospatialStatistics(t *testing.T) {
	t.Run("single_point", func(t *testing.T) {
		wkb := createWKBPoint(10.5, 20.3, true)
		values := []any{string(wkb)}
		definitionLevels := []int32{1}
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		require.NotNil(t, bbox)
		require.Equal(t, 10.5, bbox.Xmin)
		require.Equal(t, 10.5, bbox.Xmax)
		require.Equal(t, 20.3, bbox.Ymin)
		require.Equal(t, 20.3, bbox.Ymax)
		require.Equal(t, []int32{1}, geoTypes) // Point type
	})

	t.Run("multiple_geometries", func(t *testing.T) {
		point1 := createWKBPoint(0, 0, true)
		point2 := createWKBPoint(10, 20, true)
		linestring := createWKBLineString([][2]float64{{5, 5}, {15, 25}}, true)

		values := []any{string(point1), string(point2), string(linestring)}
		definitionLevels := []int32{1, 1, 1}
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		require.NotNil(t, bbox)
		require.Equal(t, 0.0, bbox.Xmin)
		require.Equal(t, 15.0, bbox.Xmax) // From linestring endpoint
		require.Equal(t, 0.0, bbox.Ymin)
		require.Equal(t, 25.0, bbox.Ymax) // From linestring endpoint

		// Should contain both Point (1) and LineString (2) types
		require.Len(t, geoTypes, 2)
		require.Contains(t, geoTypes, int32(1)) // Point
		require.Contains(t, geoTypes, int32(2)) // LineString
	})

	t.Run("with_null_values", func(t *testing.T) {
		point1 := createWKBPoint(5, 10, true)

		values := []any{nil, string(point1), nil}
		definitionLevels := []int32{0, 1, 0} // Only middle value is non-null
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		require.NotNil(t, bbox)
		require.Equal(t, 5.0, bbox.Xmin)
		require.Equal(t, 5.0, bbox.Xmax)
		require.Equal(t, 10.0, bbox.Ymin)
		require.Equal(t, 10.0, bbox.Ymax)
		require.Equal(t, []int32{1}, geoTypes) // Point type
	})

	t.Run("byte_array_values", func(t *testing.T) {
		wkb := createWKBPoint(15.5, 25.7, true)
		values := []any{wkb} // []byte instead of string
		definitionLevels := []int32{1}
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		require.NotNil(t, bbox)
		require.Equal(t, 15.5, bbox.Xmin)
		require.Equal(t, 15.5, bbox.Xmax)
		require.Equal(t, 25.7, bbox.Ymin)
		require.Equal(t, 25.7, bbox.Ymax)
		require.Equal(t, []int32{1}, geoTypes)
	})

	t.Run("empty_values", func(t *testing.T) {
		values := []any{}
		definitionLevels := []int32{}
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		require.Nil(t, bbox)
		require.Nil(t, geoTypes)
	})

	t.Run("all_null_values", func(t *testing.T) {
		values := []any{nil, nil}
		definitionLevels := []int32{0, 0} // All null
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		require.Nil(t, bbox)
		require.Nil(t, geoTypes)
	})

	t.Run("invalid_wkb_data", func(t *testing.T) {
		// Mix valid and invalid WKB
		validWKB := createWKBPoint(10, 20, true)
		invalidWKB := []byte{1, 2} // Too short

		values := []any{string(validWKB), string(invalidWKB)}
		definitionLevels := []int32{1, 1}
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		// Should only process the valid WKB
		require.NotNil(t, bbox)
		require.Equal(t, 10.0, bbox.Xmin)
		require.Equal(t, 10.0, bbox.Xmax)
		require.Equal(t, 20.0, bbox.Ymin)
		require.Equal(t, 20.0, bbox.Ymax)
		require.Equal(t, []int32{1}, geoTypes)
	})

	t.Run("unsupported_value_types", func(t *testing.T) {
		values := []any{123, 45.6, true} // Non-binary types
		definitionLevels := []int32{1, 1, 1}
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		require.Nil(t, bbox)
		require.Nil(t, geoTypes)
	})

	t.Run("definition_levels_shorter_than_values", func(t *testing.T) {
		wkb1 := createWKBPoint(1, 2, true)
		wkb2 := createWKBPoint(3, 4, true)

		values := []any{string(wkb1), string(wkb2)}
		definitionLevels := []int32{1} // Shorter than values
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		// Should process both values: first one checks definition level, second one has no definition level so gets processed
		require.NotNil(t, bbox)
		require.Equal(t, 1.0, bbox.Xmin)
		require.Equal(t, 3.0, bbox.Xmax) // From second point
		require.Equal(t, 2.0, bbox.Ymin)
		require.Equal(t, 4.0, bbox.Ymax)       // From second point
		require.Equal(t, []int32{1}, geoTypes) // Both are points
	})

	t.Run("duplicate_geometry_types", func(t *testing.T) {
		// Multiple points should only record geometry type once
		point1 := createWKBPoint(0, 0, true)
		point2 := createWKBPoint(10, 10, true)
		point3 := createWKBPoint(5, 5, true)

		values := []any{string(point1), string(point2), string(point3)}
		definitionLevels := []int32{1, 1, 1}
		maxDefinitionLevel := int32(1)

		bbox, geoTypes := computePageGeospatialStatistics(values, definitionLevels, maxDefinitionLevel)

		require.NotNil(t, bbox)
		require.Equal(t, 0.0, bbox.Xmin)
		require.Equal(t, 10.0, bbox.Xmax)
		require.Equal(t, 0.0, bbox.Ymin)
		require.Equal(t, 10.0, bbox.Ymax)
		require.Equal(t, []int32{1}, geoTypes) // Only one Point type despite multiple points
	})
}
