package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

// Test the integration of geospatial statistics aggregation in PagesToChunk
func Test_PagesToChunk_GeospatialStatisticsAggregation(t *testing.T) {
	t.Run("geometry_logical_type_with_stats", func(t *testing.T) {
		// Create pages with geospatial statistics
		page1 := createTestPage(0, 10, 0, 10, []int32{1})
		page2 := createTestPage(5, 15, 5, 15, []int32{2})
		pages := []*Page{page1, page2}

		// Set up pages with GEOMETRY logical type
		geometryLogicalType := &parquet.LogicalType{
			GEOMETRY: &parquet.GeometryType{},
		}
		for _, page := range pages {
			page.Schema = &parquet.SchemaElement{
				Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: geometryLogicalType,
			}
			page.Info = &common.Tag{}
		}

		chunk, err := PagesToChunk(pages)

		require.NoError(t, err)
		require.NotNil(t, chunk)
		require.NotNil(t, chunk.ChunkHeader.MetaData)
		require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics)

		geoStats := chunk.ChunkHeader.MetaData.GeospatialStatistics
		require.NotNil(t, geoStats.Bbox)
		require.Equal(t, 0.0, geoStats.Bbox.Xmin)  // Min from page1
		require.Equal(t, 15.0, geoStats.Bbox.Xmax) // Max from page2
		require.Equal(t, 0.0, geoStats.Bbox.Ymin)  // Min from page1
		require.Equal(t, 15.0, geoStats.Bbox.Ymax) // Max from page2

		require.Len(t, geoStats.GeospatialTypes, 2)
		require.Contains(t, geoStats.GeospatialTypes, int32(1)) // Point from page1
		require.Contains(t, geoStats.GeospatialTypes, int32(2)) // LineString from page2
	})

	t.Run("geography_logical_type_with_stats", func(t *testing.T) {
		// Create pages with geospatial statistics
		page := createTestPage(-180, 180, -90, 90, []int32{1, 3})
		pages := []*Page{page}

		// Set up page with GEOGRAPHY logical type
		geographyLogicalType := &parquet.LogicalType{
			GEOGRAPHY: &parquet.GeographyType{},
		}
		page.Schema = &parquet.SchemaElement{
			Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			LogicalType: geographyLogicalType,
		}
		page.Info = &common.Tag{}

		chunk, err := PagesToChunk(pages)

		require.NoError(t, err)
		require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics)

		geoStats := chunk.ChunkHeader.MetaData.GeospatialStatistics
		require.NotNil(t, geoStats.Bbox)
		require.Equal(t, -180.0, geoStats.Bbox.Xmin)
		require.Equal(t, 180.0, geoStats.Bbox.Xmax)
		require.Equal(t, -90.0, geoStats.Bbox.Ymin)
		require.Equal(t, 90.0, geoStats.Bbox.Ymax)

		require.Len(t, geoStats.GeospatialTypes, 2)
		require.Contains(t, geoStats.GeospatialTypes, int32(1)) // Point
		require.Contains(t, geoStats.GeospatialTypes, int32(3)) // Polygon
	})

	t.Run("non_geospatial_logical_type", func(t *testing.T) {
		// Create page with geospatial statistics but non-geospatial logical type
		page := createTestPage(0, 10, 0, 10, []int32{1})
		pages := []*Page{page}

		// Set up page with STRING logical type (not geospatial)
		stringLogicalType := &parquet.LogicalType{
			STRING: &parquet.StringType{},
		}
		page.Schema = &parquet.SchemaElement{
			Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			LogicalType: stringLogicalType,
		}
		page.Info = &common.Tag{}

		chunk, err := PagesToChunk(pages)

		require.NoError(t, err)
		require.NotNil(t, chunk.ChunkHeader.MetaData)
		// Should not have geospatial statistics for non-geospatial logical type
		require.Nil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics)
	})

	t.Run("nil_logical_type", func(t *testing.T) {
		// Create page with geospatial statistics but nil logical type
		page := createTestPage(0, 10, 0, 10, []int32{1})
		pages := []*Page{page}

		// Set up page with nil logical type
		page.Schema = &parquet.SchemaElement{
			Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			LogicalType: nil,
		}
		page.Info = &common.Tag{}

		chunk, err := PagesToChunk(pages)

		require.NoError(t, err)
		require.NotNil(t, chunk.ChunkHeader.MetaData)
		// Should not have geospatial statistics when logical type is nil
		require.Nil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics)
	})

	t.Run("pages_without_geospatial_bbox", func(t *testing.T) {
		// Create pages without geospatial statistics
		page1 := &Page{
			Schema: &parquet.SchemaElement{
				Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{
					GEOMETRY: &parquet.GeometryType{},
				},
			},
			Info:            &common.Tag{},
			GeospatialBBox:  nil, // No geospatial stats
			GeospatialTypes: nil,
		}
		pages := []*Page{page1}

		chunk, err := PagesToChunk(pages)

		require.NoError(t, err)
		require.NotNil(t, chunk.ChunkHeader.MetaData)
		// Should not have geospatial statistics when pages have no geospatial data
		require.Nil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics)
	})

	t.Run("mixed_pages_some_with_stats", func(t *testing.T) {
		// Mix of pages with and without geospatial statistics
		page1 := &Page{
			Schema: &parquet.SchemaElement{
				Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{
					GEOMETRY: &parquet.GeometryType{},
				},
			},
			Info:            &common.Tag{},
			GeospatialBBox:  nil, // No stats
			GeospatialTypes: nil,
		}
		page2 := createTestPage(10, 20, 10, 20, []int32{3})
		page2.Schema = &parquet.SchemaElement{
			Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			LogicalType: &parquet.LogicalType{
				GEOMETRY: &parquet.GeometryType{},
			},
		}
		page2.Info = &common.Tag{}

		pages := []*Page{page1, page2}

		chunk, err := PagesToChunk(pages)

		require.NoError(t, err)
		require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics)

		geoStats := chunk.ChunkHeader.MetaData.GeospatialStatistics
		require.NotNil(t, geoStats.Bbox)
		require.Equal(t, 10.0, geoStats.Bbox.Xmin)
		require.Equal(t, 20.0, geoStats.Bbox.Xmax)
		require.Equal(t, 10.0, geoStats.Bbox.Ymin)
		require.Equal(t, 20.0, geoStats.Bbox.Ymax)
		require.Equal(t, []int32{3}, geoStats.GeospatialTypes)
	})
}

// Test the integration of geospatial statistics aggregation in PagesToDictChunk
func Test_PagesToDictChunk_GeospatialStatisticsAggregation(t *testing.T) {
	t.Run("dict_chunk_with_geometry_stats", func(t *testing.T) {
		// Create dictionary page (first page)
		dictPage := &Page{
			Header: &parquet.PageHeader{
				Type: parquet.PageType_DICTIONARY_PAGE,
			},
			Schema: &parquet.SchemaElement{
				Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{
					GEOMETRY: &parquet.GeometryType{},
				},
			},
			Info: &common.Tag{},
		}

		// Create data page with geospatial statistics (second page)
		dataPage := createTestPage(0, 15, 0, 25, []int32{1, 4})
		dataPage.Schema = &parquet.SchemaElement{
			Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			LogicalType: &parquet.LogicalType{
				GEOMETRY: &parquet.GeometryType{},
			},
		}
		dataPage.Info = &common.Tag{}

		pages := []*Page{dictPage, dataPage}

		chunk, err := PagesToDictChunk(pages)

		require.NoError(t, err)
		require.NotNil(t, chunk)
		require.NotNil(t, chunk.ChunkHeader.MetaData)
		require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics)

		geoStats := chunk.ChunkHeader.MetaData.GeospatialStatistics
		require.NotNil(t, geoStats.Bbox)
		require.Equal(t, 0.0, geoStats.Bbox.Xmin)
		require.Equal(t, 15.0, geoStats.Bbox.Xmax)
		require.Equal(t, 0.0, geoStats.Bbox.Ymin)
		require.Equal(t, 25.0, geoStats.Bbox.Ymax)

		require.Len(t, geoStats.GeospatialTypes, 2)
		require.Contains(t, geoStats.GeospatialTypes, int32(1)) // Point
		require.Contains(t, geoStats.GeospatialTypes, int32(4)) // MultiPoint
	})

	t.Run("dict_chunk_with_geography_stats", func(t *testing.T) {
		// Create dictionary page
		dictPage := &Page{
			Header: &parquet.PageHeader{
				Type: parquet.PageType_DICTIONARY_PAGE,
			},
			Schema: &parquet.SchemaElement{
				Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{
					GEOGRAPHY: &parquet.GeographyType{},
				},
			},
			Info: &common.Tag{},
		}

		// Create data page with geography statistics
		dataPage := createTestPage(-122.5, -122.0, 37.5, 38.0, []int32{2})
		dataPage.Schema = &parquet.SchemaElement{
			Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			LogicalType: &parquet.LogicalType{
				GEOGRAPHY: &parquet.GeographyType{},
			},
		}
		dataPage.Info = &common.Tag{}

		pages := []*Page{dictPage, dataPage}

		chunk, err := PagesToDictChunk(pages)

		require.NoError(t, err)
		require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics)

		geoStats := chunk.ChunkHeader.MetaData.GeospatialStatistics
		require.Equal(t, -122.5, geoStats.Bbox.Xmin)
		require.Equal(t, -122.0, geoStats.Bbox.Xmax)
		require.Equal(t, 37.5, geoStats.Bbox.Ymin)
		require.Equal(t, 38.0, geoStats.Bbox.Ymax)
		require.Equal(t, []int32{2}, geoStats.GeospatialTypes)
	})

	t.Run("dict_chunk_without_geospatial_logical_type", func(t *testing.T) {
		// Create dictionary page with non-geospatial logical type
		dictPage := &Page{
			Header: &parquet.PageHeader{
				Type: parquet.PageType_DICTIONARY_PAGE,
			},
			Schema: &parquet.SchemaElement{
				Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{
					STRING: &parquet.StringType{},
				},
			},
			Info: &common.Tag{},
		}

		// Create data page (even with geospatial stats, should be ignored)
		dataPage := createTestPage(0, 10, 0, 10, []int32{1})
		dataPage.Schema = &parquet.SchemaElement{
			Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			LogicalType: &parquet.LogicalType{
				STRING: &parquet.StringType{},
			},
		}
		dataPage.Info = &common.Tag{}

		pages := []*Page{dictPage, dataPage}

		chunk, err := PagesToDictChunk(pages)

		require.NoError(t, err)
		require.NotNil(t, chunk.ChunkHeader.MetaData)
		// Should not have geospatial statistics for non-geospatial logical type
		require.Nil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics)
	})

	t.Run("dict_chunk_empty_pages", func(t *testing.T) {
		// Test with less than 2 pages (should return nil)
		pages := []*Page{}

		chunk, err := PagesToDictChunk(pages)

		require.NoError(t, err)
		require.Nil(t, chunk) // PagesToDictChunk returns nil for < 2 pages
	})

	t.Run("dict_chunk_single_page", func(t *testing.T) {
		// Test with exactly 1 page (should return nil)
		page := createTestPage(0, 10, 0, 10, []int32{1})
		pages := []*Page{page}

		chunk, err := PagesToDictChunk(pages)

		require.NoError(t, err)
		require.Nil(t, chunk) // PagesToDictChunk returns nil for < 2 pages
	})
}

// Helper function to create a test page with geospatial statistics
func createTestPage(xmin, xmax, ymin, ymax float64, geoTypes []int32) *Page {
	return &Page{
		Header: &parquet.PageHeader{
			Type: parquet.PageType_DATA_PAGE,
		},
		GeospatialBBox: &parquet.BoundingBox{
			Xmin: xmin,
			Xmax: xmax,
			Ymin: ymin,
			Ymax: ymax,
		},
		GeospatialTypes: geoTypes,
	}
}
