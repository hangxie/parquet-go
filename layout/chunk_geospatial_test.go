package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_AggregateGeospatialStatistics(t *testing.T) {
	t.Run("single_page_with_stats", func(t *testing.T) {
		page := &Page{
			GeospatialBBox: &parquet.BoundingBox{
				Xmin: 0.0,
				Xmax: 10.0,
				Ymin: 5.0,
				Ymax: 15.0,
			},
			GeospatialTypes: []int32{1, 2}, // Point, LineString
		}
		pages := []*Page{page}

		bbox, geoTypes := aggregateGeospatialStatistics(pages)

		require.NotNil(t, bbox)
		require.Equal(t, 0.0, bbox.Xmin)
		require.Equal(t, 10.0, bbox.Xmax)
		require.Equal(t, 5.0, bbox.Ymin)
		require.Equal(t, 15.0, bbox.Ymax)
		require.Len(t, geoTypes, 2)
		require.Contains(t, geoTypes, int32(1)) // Point
		require.Contains(t, geoTypes, int32(2)) // LineString
	})

	t.Run("multiple_pages_with_stats", func(t *testing.T) {
		page1 := &Page{
			GeospatialBBox: &parquet.BoundingBox{
				Xmin: 0.0,
				Xmax: 10.0,
				Ymin: 0.0,
				Ymax: 10.0,
			},
			GeospatialTypes: []int32{1}, // Point
		}
		page2 := &Page{
			GeospatialBBox: &parquet.BoundingBox{
				Xmin: 5.0,
				Xmax: 20.0,
				Ymin: -5.0,
				Ymax: 5.0,
			},
			GeospatialTypes: []int32{2, 3}, // LineString, Polygon
		}
		pages := []*Page{page1, page2}

		bbox, geoTypes := aggregateGeospatialStatistics(pages)

		require.NotNil(t, bbox)
		require.Equal(t, 0.0, bbox.Xmin)  // Min from page1
		require.Equal(t, 20.0, bbox.Xmax) // Max from page2
		require.Equal(t, -5.0, bbox.Ymin) // Min from page2
		require.Equal(t, 10.0, bbox.Ymax) // Max from page1
		require.Len(t, geoTypes, 3)
		require.Contains(t, geoTypes, int32(1)) // Point from page1
		require.Contains(t, geoTypes, int32(2)) // LineString from page2
		require.Contains(t, geoTypes, int32(3)) // Polygon from page2
	})

	t.Run("pages_with_overlapping_geometry_types", func(t *testing.T) {
		page1 := &Page{
			GeospatialBBox: &parquet.BoundingBox{
				Xmin: 0.0,
				Xmax: 10.0,
				Ymin: 0.0,
				Ymax: 10.0,
			},
			GeospatialTypes: []int32{1, 2}, // Point, LineString
		}
		page2 := &Page{
			GeospatialBBox: &parquet.BoundingBox{
				Xmin: 5.0,
				Xmax: 15.0,
				Ymin: 5.0,
				Ymax: 15.0,
			},
			GeospatialTypes: []int32{1, 3}, // Point (duplicate), Polygon
		}
		pages := []*Page{page1, page2}

		bbox, geoTypes := aggregateGeospatialStatistics(pages)

		require.NotNil(t, bbox)
		require.Equal(t, 0.0, bbox.Xmin)
		require.Equal(t, 15.0, bbox.Xmax)
		require.Equal(t, 0.0, bbox.Ymin)
		require.Equal(t, 15.0, bbox.Ymax)
		require.Len(t, geoTypes, 3)             // Should deduplicate Point type
		require.Contains(t, geoTypes, int32(1)) // Point
		require.Contains(t, geoTypes, int32(2)) // LineString
		require.Contains(t, geoTypes, int32(3)) // Polygon
	})

	t.Run("empty_pages_list", func(t *testing.T) {
		pages := []*Page{}

		bbox, geoTypes := aggregateGeospatialStatistics(pages)

		require.Nil(t, bbox)
		require.Nil(t, geoTypes)
	})

	t.Run("pages_without_geospatial_stats", func(t *testing.T) {
		page1 := &Page{} // No geospatial stats
		page2 := &Page{
			GeospatialBBox: nil, // Explicitly nil bbox
		}
		pages := []*Page{page1, page2}

		bbox, geoTypes := aggregateGeospatialStatistics(pages)

		require.Nil(t, bbox)
		require.Nil(t, geoTypes)
	})

	t.Run("mixed_pages_some_with_stats", func(t *testing.T) {
		page1 := &Page{} // No geospatial stats
		page2 := &Page{
			GeospatialBBox: &parquet.BoundingBox{
				Xmin: 10.0,
				Xmax: 20.0,
				Ymin: 10.0,
				Ymax: 20.0,
			},
			GeospatialTypes: []int32{3}, // Polygon
		}
		page3 := &Page{
			GeospatialBBox: nil, // Nil bbox
		}
		pages := []*Page{page1, page2, page3}

		bbox, geoTypes := aggregateGeospatialStatistics(pages)

		require.NotNil(t, bbox)
		require.Equal(t, 10.0, bbox.Xmin)
		require.Equal(t, 20.0, bbox.Xmax)
		require.Equal(t, 10.0, bbox.Ymin)
		require.Equal(t, 20.0, bbox.Ymax)
		require.Equal(t, []int32{3}, geoTypes)
	})

	t.Run("pages_with_nil_pointers", func(t *testing.T) {
		page1 := &Page{
			GeospatialBBox: &parquet.BoundingBox{
				Xmin: 0.0,
				Xmax: 5.0,
				Ymin: 0.0,
				Ymax: 5.0,
			},
			GeospatialTypes: []int32{1},
		}
		pages := []*Page{nil, page1, nil}

		bbox, geoTypes := aggregateGeospatialStatistics(pages)

		require.NotNil(t, bbox)
		require.Equal(t, 0.0, bbox.Xmin)
		require.Equal(t, 5.0, bbox.Xmax)
		require.Equal(t, 0.0, bbox.Ymin)
		require.Equal(t, 5.0, bbox.Ymax)
		require.Equal(t, []int32{1}, geoTypes)
	})

	t.Run("negative_coordinates", func(t *testing.T) {
		page := &Page{
			GeospatialBBox: &parquet.BoundingBox{
				Xmin: -180.0,
				Xmax: -170.0,
				Ymin: -90.0,
				Ymax: -80.0,
			},
			GeospatialTypes: []int32{1},
		}
		pages := []*Page{page}

		bbox, geoTypes := aggregateGeospatialStatistics(pages)

		require.NotNil(t, bbox)
		require.Equal(t, -180.0, bbox.Xmin)
		require.Equal(t, -170.0, bbox.Xmax)
		require.Equal(t, -90.0, bbox.Ymin)
		require.Equal(t, -80.0, bbox.Ymax)
		require.Equal(t, []int32{1}, geoTypes)
	})
}
