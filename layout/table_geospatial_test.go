package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

// Helper function to create a LogicalType for GEOMETRY
func createGeometryLogicalType() *parquet.LogicalType {
	return &parquet.LogicalType{
		GEOMETRY: &parquet.GeometryType{},
	}
}

// Helper function to create a LogicalType for GEOGRAPHY
func createGeographyLogicalType() *parquet.LogicalType {
	return &parquet.LogicalType{
		GEOGRAPHY: &parquet.GeographyType{},
	}
}

// Helper function to create a table with geospatial data
func createGeospatialTable(logicalType *parquet.LogicalType, values []any, omitStats bool) *Table {
	table := &Table{
		RepetitionType:     parquet.FieldRepetitionType_REQUIRED,
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
		Path:               []string{"root", "geom"},
		Values:             values,
		DefinitionLevels:   make([]int32, len(values)),
		RepetitionLevels:   make([]int32, len(values)),
		Schema: &parquet.SchemaElement{
			Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			LogicalType: logicalType,
		},
		Info: &common.Tag{},
	}

	// Set OmitStats field
	table.Info.OmitStats = omitStats

	// Set all definition levels to max (non-null values)
	for i := range table.DefinitionLevels {
		table.DefinitionLevels[i] = 1
	}

	return table
}

func Test_TableToDataPages_GeospatialStatistics(t *testing.T) {
	t.Run("geometry_column_with_stats", func(t *testing.T) {
		// Create WKB data for testing
		point1 := createWKBPoint(0, 0, true)
		point2 := createWKBPoint(10, 20, true)
		linestring := createWKBLineString([][2]float64{{5, 5}, {15, 25}}, true)

		values := []any{string(point1), string(point2), string(linestring)}
		table := createGeospatialTable(createGeometryLogicalType(), values, false)

		pages, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Len(t, pages, 1) // All data should fit in one page

		page := pages[0]
		require.NotNil(t, page.GeospatialBBox)
		require.Equal(t, 0.0, page.GeospatialBBox.Xmin)
		require.Equal(t, 15.0, page.GeospatialBBox.Xmax) // From linestring endpoint
		require.Equal(t, 0.0, page.GeospatialBBox.Ymin)
		require.Equal(t, 25.0, page.GeospatialBBox.Ymax) // From linestring endpoint

		require.NotNil(t, page.GeospatialTypes)
		require.Len(t, page.GeospatialTypes, 2)
		require.Contains(t, page.GeospatialTypes, int32(1)) // Point
		require.Contains(t, page.GeospatialTypes, int32(2)) // LineString
	})

	t.Run("geography_column_with_stats", func(t *testing.T) {
		// Create WKB data for testing with geographic coordinates
		point1 := createWKBPoint(-122.4, 37.8, true) // San Francisco
		point2 := createWKBPoint(-74.0, 40.7, true)  // New York

		values := []any{string(point1), string(point2)}
		table := createGeospatialTable(createGeographyLogicalType(), values, false)

		pages, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Len(t, pages, 1)

		page := pages[0]
		require.NotNil(t, page.GeospatialBBox)
		require.Equal(t, -122.4, page.GeospatialBBox.Xmin)
		require.Equal(t, -74.0, page.GeospatialBBox.Xmax)
		require.Equal(t, 37.8, page.GeospatialBBox.Ymin)
		require.Equal(t, 40.7, page.GeospatialBBox.Ymax)

		require.NotNil(t, page.GeospatialTypes)
		require.Equal(t, []int32{1}, page.GeospatialTypes) // Only Point type
	})

	t.Run("geospatial_column_with_omit_stats", func(t *testing.T) {
		point := createWKBPoint(10, 20, true)
		values := []any{string(point)}
		table := createGeospatialTable(createGeometryLogicalType(), values, true) // omitStats = true

		pages, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Len(t, pages, 1)

		page := pages[0]
		// When omitStats is true, geospatial statistics should not be computed
		require.Nil(t, page.GeospatialBBox)
		require.Nil(t, page.GeospatialTypes)
	})

	t.Run("non_geospatial_column", func(t *testing.T) {
		values := []any{"regular_string", "another_string"}
		table := &Table{
			RepetitionType:     parquet.FieldRepetitionType_REQUIRED,
			MaxDefinitionLevel: 1,
			MaxRepetitionLevel: 0,
			Path:               []string{"root", "text"},
			Values:             values,
			DefinitionLevels:   []int32{1, 1},
			RepetitionLevels:   []int32{0, 0},
			Schema: &parquet.SchemaElement{
				Type: parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				// No LogicalType - regular string column
			},
			Info: &common.Tag{},
		}

		pages, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Len(t, pages, 1)

		page := pages[0]
		// Non-geospatial columns should not have geospatial statistics
		require.Nil(t, page.GeospatialBBox)
		require.Nil(t, page.GeospatialTypes)
	})

	t.Run("geospatial_column_with_null_values", func(t *testing.T) {
		point := createWKBPoint(5, 10, true)
		values := []any{nil, string(point), nil}
		table := createGeospatialTable(createGeometryLogicalType(), values, false)

		// Set definition levels: 0 for nulls, 1 for non-null
		table.DefinitionLevels = []int32{0, 1, 0}

		pages, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Len(t, pages, 1)

		page := pages[0]
		require.NotNil(t, page.GeospatialBBox)
		require.Equal(t, 5.0, page.GeospatialBBox.Xmin)
		require.Equal(t, 5.0, page.GeospatialBBox.Xmax)
		require.Equal(t, 10.0, page.GeospatialBBox.Ymin)
		require.Equal(t, 10.0, page.GeospatialBBox.Ymax)
		require.Equal(t, []int32{1}, page.GeospatialTypes)
	})

	t.Run("geospatial_column_all_null_values", func(t *testing.T) {
		values := []any{nil, nil, nil}
		table := createGeospatialTable(createGeometryLogicalType(), values, false)

		// All values are null
		table.DefinitionLevels = []int32{0, 0, 0}

		pages, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Len(t, pages, 1)

		page := pages[0]
		// All null values should result in no geospatial statistics
		require.Nil(t, page.GeospatialBBox)
		require.Nil(t, page.GeospatialTypes)
	})

	t.Run("geospatial_column_with_invalid_wkb", func(t *testing.T) {
		validPoint := createWKBPoint(10, 20, true)
		invalidWKB := []byte{1, 2} // Too short to be valid WKB

		values := []any{string(validPoint), string(invalidWKB)}
		table := createGeospatialTable(createGeometryLogicalType(), values, false)

		pages, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Len(t, pages, 1)

		page := pages[0]
		// Should only process the valid WKB data
		require.NotNil(t, page.GeospatialBBox)
		require.Equal(t, 10.0, page.GeospatialBBox.Xmin)
		require.Equal(t, 10.0, page.GeospatialBBox.Xmax)
		require.Equal(t, 20.0, page.GeospatialBBox.Ymin)
		require.Equal(t, 20.0, page.GeospatialBBox.Ymax)
		require.Equal(t, []int32{1}, page.GeospatialTypes)
	})

	t.Run("geospatial_column_multiple_pages", func(t *testing.T) {
		// Create enough data to span multiple pages
		var values []any
		for i := 0; i < 10; i++ {
			point := createWKBPoint(float64(i), float64(i*2), true)
			values = append(values, string(point))
		}

		table := createGeospatialTable(createGeometryLogicalType(), values, false)

		// Use small page size to force multiple pages
		pages, _, err := TableToDataPages(table, 100, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Greater(t, len(pages), 1) // Should create multiple pages

		// Each page should have its own geospatial statistics
		for _, page := range pages {
			require.NotNil(t, page.GeospatialBBox)
			require.NotNil(t, page.GeospatialTypes)
			require.Equal(t, []int32{1}, page.GeospatialTypes) // All are points

			// Each page should have valid bounding box coordinates
			require.GreaterOrEqual(t, page.GeospatialBBox.Xmax, page.GeospatialBBox.Xmin)
			require.GreaterOrEqual(t, page.GeospatialBBox.Ymax, page.GeospatialBBox.Ymin)
		}
	})

	t.Run("geometry_column_with_nil_logical_type", func(t *testing.T) {
		point := createWKBPoint(10, 20, true)
		values := []any{string(point)}
		table := createGeospatialTable(nil, values, false) // nil LogicalType

		pages, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Len(t, pages, 1)

		page := pages[0]
		// Without geospatial LogicalType, no geospatial statistics should be computed
		require.Nil(t, page.GeospatialBBox)
		require.Nil(t, page.GeospatialTypes)
	})

	t.Run("geometry_column_with_other_logical_type", func(t *testing.T) {
		point := createWKBPoint(10, 20, true)
		values := []any{string(point)}

		// Create table with a different logical type (UTF8)
		table := createGeospatialTable(&parquet.LogicalType{
			STRING: &parquet.StringType{},
		}, values, false)

		pages, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_SNAPPY)

		require.NoError(t, err)
		require.Len(t, pages, 1)

		page := pages[0]
		// Non-geospatial logical type should not trigger geospatial statistics computation
		require.Nil(t, page.GeospatialBBox)
		require.Nil(t, page.GeospatialTypes)
	})
}
