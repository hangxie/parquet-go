package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_DecodeDictChunk(t *testing.T) {
	// Create a chunk with dictionary and data pages
	dictPage := NewDictPage()
	dictPage.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	dictPage.Info = common.NewTag()
	dictPage.DataTable = &Table{
		Values: []any{int32(10), int32(20), int32(30)},
	}

	dataPage := NewDataPage()
	dataPage.Schema = dictPage.Schema
	dataPage.Info = common.NewTag()
	dataPage.DataTable = &Table{
		Values: []any{int64(0), int64(2), int64(1)}, // indices into dictionary
	}

	chunk := &Chunk{
		Pages: []*Page{dictPage, dataPage},
	}

	// This should decode the dictionary indices to actual values
	DecodeDictChunk(chunk)

	require.Equal(t, 1, len(chunk.Pages))
	require.Equal(t, 3, len(chunk.Pages[0].DataTable.Values))
	require.Equal(t, int32(10), chunk.Pages[0].DataTable.Values[0])
}

func Test_PagesToChunk(t *testing.T) {
	tests := []struct {
		name          string
		setupPages    func() []*Page
		expectError   bool
		expectedPages int
		checkChunk    func(t *testing.T, chunk *Chunk)
	}{
		{
			name: "multiple_data_pages",
			setupPages: func() []*Page {
				// Create test pages
				page1 := NewDataPage()
				page1.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
					Name: "test_col",
				}
				page1.Info = common.NewTag()
				page1.MaxVal = int32(10)
				page1.MinVal = int32(1)
				nullCount := int64(0)
				page1.NullCount = &nullCount

				// Set up page header for DataPage
				page1.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues: 5,
				}
				page1.Header.UncompressedPageSize = 100
				page1.Header.CompressedPageSize = 80
				page1.RawData = make([]byte, 80)

				page2 := NewDataPage()
				page2.Schema = page1.Schema
				page2.Info = common.NewTag()
				page2.MaxVal = int32(20)
				page2.MinVal = int32(5)
				nullCount2 := int64(1)
				page2.NullCount = &nullCount2

				page2.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues: 3,
				}
				page2.Header.UncompressedPageSize = 60
				page2.Header.CompressedPageSize = 50
				page2.RawData = make([]byte, 50)

				return []*Page{page1, page2}
			},
			expectedPages: 2,
			checkChunk: func(t *testing.T, chunk *Chunk) {
				require.NotNil(t, chunk.ChunkHeader)
			},
		},
		{
			name: "data_page_v2",
			setupPages: func() []*Page {
				// Create test page with DataPageV2 header
				page := NewDataPage()
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
					Name: "test_col",
				}
				page.Info = common.NewTag()
				page.MaxVal = int32(10)
				page.MinVal = int32(1)
				nullCount := int64(0)
				page.NullCount = &nullCount

				// Set up page header for DataPageV2
				page.Header.DataPageHeaderV2 = &parquet.DataPageHeaderV2{
					NumValues: 5,
				}
				page.Header.UncompressedPageSize = 100
				page.Header.CompressedPageSize = 80
				page.RawData = make([]byte, 80)

				return []*Page{page}
			},
			expectedPages: 1,
		},
		{
			name: "invalid_schema_no_type",
			setupPages: func() []*Page {
				// Create page with invalid schema (no type)
				page := NewDataPage()
				page.Schema = &parquet.SchemaElement{
					Name: "test_col",
					// No Type set
				}
				page.Info = common.NewTag()
				page.MaxVal = int32(10)
				page.MinVal = int32(1)
				nullCount := int64(0)
				page.NullCount = &nullCount

				page.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues: 5,
				}

				return []*Page{page}
			},
			expectError: true,
		},
		{
			name: "omit_stats_enabled",
			setupPages: func() []*Page {
				// Create page with omit stats enabled
				page := NewDataPage()
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
					Name: "test_col",
				}
				page.Info = common.NewTag()
				page.Info.OmitStats = true // Enable omit stats
				page.MaxVal = int32(10)
				page.MinVal = int32(1)
				nullCount := int64(0)
				page.NullCount = &nullCount

				page.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues: 5,
				}
				page.Header.UncompressedPageSize = 100
				page.Header.CompressedPageSize = 80
				page.RawData = make([]byte, 80)

				return []*Page{page}
			},
			expectedPages: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pages := tt.setupPages()
			chunk, err := PagesToChunk(pages)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			require.NotNil(t, chunk)

			require.Equal(t, tt.expectedPages, len(chunk.Pages))

			if tt.checkChunk != nil {
				tt.checkChunk(t, chunk)
			}
		})
	}
}

func Test_PagesToDictChunk(t *testing.T) {
	// Create dictionary page
	dictPage := NewDictPage()
	dictPage.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	dictPage.Info = common.NewTag()
	dictPage.MaxVal = int32(10)
	dictPage.MinVal = int32(1)
	nullCount := int64(0)
	dictPage.NullCount = &nullCount

	dictPage.Header.DictionaryPageHeader = &parquet.DictionaryPageHeader{
		NumValues: 3,
	}
	dictPage.Header.UncompressedPageSize = 50
	dictPage.Header.CompressedPageSize = 40
	dictPage.RawData = make([]byte, 40)

	// Create data page
	dataPage := NewDataPage()
	dataPage.Schema = dictPage.Schema
	dataPage.Info = common.NewTag()
	dataPage.MaxVal = int32(10)
	dataPage.MinVal = int32(1)
	nullCount2 := int64(0)
	dataPage.NullCount = &nullCount2

	dataPage.Header.DataPageHeader = &parquet.DataPageHeader{
		NumValues: 5,
	}
	dataPage.Header.UncompressedPageSize = 100
	dataPage.Header.CompressedPageSize = 80
	dataPage.RawData = make([]byte, 80)

	pages := []*Page{dictPage, dataPage}
	chunk, err := PagesToDictChunk(pages)
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.Equal(t, 2, len(chunk.Pages))
}

func Test_PagesToDictChunkWithInvalidSchema(t *testing.T) {
	// Create dictionary page
	dictPage := NewDictPage()
	dictPage.Schema = &parquet.SchemaElement{
		Name: "dict_col",
	}
	dictPage.Info = common.NewTag()

	// Create data page with invalid schema (no Type set)
	dataPage := NewDataPage()
	dataPage.Schema = &parquet.SchemaElement{
		Name: "test_col",
		// No Type set
	}
	dataPage.Info = common.NewTag()

	pages := []*Page{dictPage, dataPage}
	_, err := PagesToDictChunk(pages)
	require.Error(t, err)
}

func Test_ReadChunk_ErrorConditions(t *testing.T) {
	// Test ReadChunk with error conditions
	// Since ReadChunk is deprecated and involves complex thrift reader setup,
	// we'll test error paths with minimal mocking

	tests := []struct {
		name        string
		setupChunk  func() *parquet.ColumnChunk
		expectPanic bool
		description string
	}{
		{
			name: "nil_chunk_header",
			setupChunk: func() *parquet.ColumnChunk {
				return nil
			},
			expectPanic: true,
			description: "ReadChunk should panic when chunk header is nil",
		},
		{
			name: "chunk_header_with_nil_metadata",
			setupChunk: func() *parquet.ColumnChunk {
				return &parquet.ColumnChunk{
					// MetaData is nil
				}
			},
			expectPanic: true,
			description: "ReadChunk should panic when metadata is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunkHeader := tt.setupChunk()

			if tt.expectPanic {
				_, err := ReadChunk(nil, nil, chunkHeader)
				require.Error(t, err)
			}
		})
	}
}

func Test_PagesToChunk_NilChecks(t *testing.T) {
	tests := []struct {
		name        string
		pages       []*Page
		expectError bool
		errorMsg    string
	}{
		{
			name:        "empty_pages_slice",
			pages:       []*Page{},
			expectError: true,
			errorMsg:    "pages slice cannot be empty",
		},
		{
			name:        "nil_pages_slice",
			pages:       nil,
			expectError: true,
			errorMsg:    "pages slice cannot be empty",
		},
		{
			name:        "first_page_nil",
			pages:       []*Page{nil},
			expectError: true,
			errorMsg:    "first page cannot be nil",
		},
		{
			name: "first_page_schema_nil",
			pages: []*Page{
				{
					Schema: nil,
				},
			},
			expectError: true,
			errorMsg:    "first page schema cannot be nil",
		},
		{
			name: "first_page_schema_type_nil",
			pages: []*Page{
				{
					Schema: &parquet.SchemaElement{
						Type: nil,
					},
				},
			},
			expectError: true,
			errorMsg:    "first page schema type cannot be nil",
		},
		{
			name: "first_page_info_nil",
			pages: []*Page{
				{
					Schema: &parquet.SchemaElement{
						Type: &[]parquet.Type{parquet.Type_INT32}[0],
					},
					Info: nil,
				},
			},
			expectError: true,
			errorMsg:    "first page info cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := PagesToChunk(tt.pages)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_PagesToDictChunk_NilChecks(t *testing.T) {
	tests := []struct {
		name        string
		pages       []*Page
		expectError bool
		expectNil   bool
		errorMsg    string
	}{
		{
			name:      "less_than_two_pages",
			pages:     []*Page{{}},
			expectNil: true,
		},
		{
			name:        "second_page_nil",
			pages:       []*Page{{}, nil},
			expectError: true,
			errorMsg:    "second page cannot be nil",
		},
		{
			name: "second_page_schema_nil",
			pages: []*Page{
				{},
				{Schema: nil},
			},
			expectError: true,
			errorMsg:    "second page schema cannot be nil",
		},
		{
			name: "second_page_schema_type_nil",
			pages: []*Page{
				{},
				{
					Schema: &parquet.SchemaElement{
						Type: nil,
					},
				},
			},
			expectError: true,
			errorMsg:    "second page schema type cannot be nil",
		},
		{
			name: "second_page_info_nil",
			pages: []*Page{
				{},
				{
					Schema: &parquet.SchemaElement{
						Type: &[]parquet.Type{parquet.Type_INT32}[0],
					},
					Info: nil,
				},
			},
			expectError: true,
			errorMsg:    "second page info cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := PagesToDictChunk(tt.pages)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else if tt.expectNil {
				require.NoError(t, err)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_DecodeDictChunk_NilChecks(t *testing.T) {
	tests := []struct {
		name  string
		chunk *Chunk
	}{
		{
			name:  "nil_chunk",
			chunk: nil,
		},
		{
			name: "empty_pages",
			chunk: &Chunk{
				Pages: []*Page{},
			},
		},
		{
			name: "nil_pages",
			chunk: &Chunk{
				Pages: nil,
			},
		},
		{
			name: "nil_dict_page",
			chunk: &Chunk{
				Pages: []*Page{nil, {}},
			},
		},
		{
			name: "nil_dict_page_data_table",
			chunk: &Chunk{
				Pages: []*Page{
					{DataTable: nil},
					{},
				},
			},
		},
		{
			name: "nil_data_page",
			chunk: &Chunk{
				Pages: []*Page{
					{DataTable: &Table{}},
					nil,
				},
			},
		},
		{
			name: "nil_data_page_data_table",
			chunk: &Chunk{
				Pages: []*Page{
					{DataTable: &Table{}},
					{DataTable: nil},
				},
			},
		},
		{
			name: "valid_chunk_with_safe_indices",
			chunk: &Chunk{
				Pages: []*Page{
					{
						DataTable: &Table{
							Values: []any{"value1", "value2"},
						},
					},
					{
						DataTable: &Table{
							Values: []any{int64(0), int64(1), int64(2)}, // Index 2 is out of bounds
						},
					},
				},
			},
		},
		{
			name: "invalid_type_assertion",
			chunk: &Chunk{
				Pages: []*Page{
					{
						DataTable: &Table{
							Values: []any{"value1", "value2"},
						},
					},
					{
						DataTable: &Table{
							Values: []any{"not_an_int64"}, // Wrong type
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DecodeDictChunk(tt.chunk)
		})
	}
}

func Test_PagesToChunk_NullCountNilChecks(t *testing.T) {
	// Test the specific case where NullCount might be nil
	pageType := parquet.Type_INT32
	info := &common.Tag{}
	info.OmitStats = false

	tests := []struct {
		name  string
		pages []*Page
	}{
		{
			name: "nil_null_count",
			pages: []*Page{
				{
					Schema: &parquet.SchemaElement{
						Type: &pageType,
					},
					Info:      info,
					NullCount: nil, // This should be handled gracefully
				},
			},
		},
		{
			name: "valid_null_count",
			pages: []*Page{
				{
					Schema: &parquet.SchemaElement{
						Type: &pageType,
					},
					Info:      info,
					NullCount: &[]int64{5}[0],
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := PagesToChunk(tt.pages)
			// We expect this to work or fail gracefully, but not panic
			if err != nil {
				// It's okay if there are other errors, as long as it doesn't panic
				t.Logf("Function returned error (expected): %v", err)
			}
		})
	}
}

func Test_PagesToDictChunk_NullCountNilChecks(t *testing.T) {
	// Test the specific case where NullCount might be nil in dict chunks
	pageType := parquet.Type_INT32
	info := &common.Tag{}
	info.OmitStats = false

	pages := []*Page{
		{}, // Dict page
		{
			Schema: &parquet.SchemaElement{
				Type: &pageType,
			},
			Info:      info,
			NullCount: nil, // This should be handled gracefully
		},
	}

	_, err := PagesToDictChunk(pages)
	// We expect this to work or fail gracefully, but not panic
	if err != nil {
		// It's okay if there are other errors, as long as it doesn't panic
		t.Logf("Function returned error (expected): %v", err)
	}
}

func Test_aggregateGeospatialStatistics(t *testing.T) {
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

func Test_PagesToChunk_GeospatialStatistics(t *testing.T) {
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
func Test_PagesToDictChunk_GeospatialStatistics(t *testing.T) {
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

func Test_ChunkLevel_SkipMinMaxStatistics_ForGeospatialTypes(t *testing.T) {
	// Create WKB point data for testing
	wkbPoint := []byte{
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x40, 0xcd, 0xcc, 0xcc,
		0xcc, 0xcc, 0x4c, 0x34, 0x40,
	}

	tests := []struct {
		name              string
		logicalType       *parquet.LogicalType
		expectMinMaxStats bool
	}{
		{
			name:              "regular_byte_array_chunk",
			logicalType:       nil,
			expectMinMaxStats: true,
		},
		{
			name: "geometry_chunk",
			logicalType: &parquet.LogicalType{
				GEOMETRY: &parquet.GeometryType{},
			},
			expectMinMaxStats: false,
		},
		{
			name: "geography_chunk",
			logicalType: &parquet.LogicalType{
				GEOGRAPHY: &parquet.GeographyType{},
			},
			expectMinMaxStats: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("PagesToChunk", func(t *testing.T) {
				// Create test pages
				page1 := &Page{
					Header: &parquet.PageHeader{
						Type: parquet.PageType_DATA_PAGE,
						DataPageHeader: &parquet.DataPageHeader{
							NumValues: 1,
						},
					},
					Schema: &parquet.SchemaElement{
						Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						LogicalType: tt.logicalType,
					},
					Info:      &common.Tag{},
					NullCount: common.ToPtr(int64(0)),
					RawData:   []byte{0x01, 0x02, 0x03, 0x04}, // dummy data
				}

				// Only set min/max values for non-geospatial types
				if tt.logicalType == nil || (!tt.logicalType.IsSetGEOMETRY() && !tt.logicalType.IsSetGEOGRAPHY()) {
					page1.MaxVal = string(wkbPoint)
					page1.MinVal = string(wkbPoint)
				}

				page2 := &Page{
					Header: &parquet.PageHeader{
						Type: parquet.PageType_DATA_PAGE,
						DataPageHeader: &parquet.DataPageHeader{
							NumValues: 1,
						},
					},
					Schema: &parquet.SchemaElement{
						Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						LogicalType: tt.logicalType,
					},
					Info:      &common.Tag{},
					NullCount: common.ToPtr(int64(0)),
					RawData:   []byte{0x05, 0x06, 0x07, 0x08}, // dummy data
				}

				// Only set min/max values for non-geospatial types
				if tt.logicalType == nil || (!tt.logicalType.IsSetGEOMETRY() && !tt.logicalType.IsSetGEOGRAPHY()) {
					page2.MaxVal = string(wkbPoint)
					page2.MinVal = string(wkbPoint)
				}

				// Add geospatial statistics if it's a geospatial type
				if tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY()) {
					page1.GeospatialBBox = &parquet.BoundingBox{Xmin: 10.5, Xmax: 10.5, Ymin: 20.3, Ymax: 20.3}
					page1.GeospatialTypes = []int32{1}
					page2.GeospatialBBox = &parquet.BoundingBox{Xmin: 10.5, Xmax: 10.5, Ymin: 20.3, Ymax: 20.3}
					page2.GeospatialTypes = []int32{1}
				}

				pages := []*Page{page1, page2}

				chunk, err := PagesToChunk(pages)
				require.NoError(t, err)
				require.NotNil(t, chunk)
				require.NotNil(t, chunk.ChunkHeader)
				require.NotNil(t, chunk.ChunkHeader.MetaData)
				require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics)

				// Check min/max statistics based on expectation
				if tt.expectMinMaxStats {
					require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.Max, "Expected min/max statistics for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.Min, "Expected min/max statistics for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.MaxValue, "Expected MaxValue for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.MinValue, "Expected MinValue for %s", tt.name)
				} else {
					require.Nil(t, chunk.ChunkHeader.MetaData.Statistics.Max, "Expected no Max statistics for %s", tt.name)
					require.Nil(t, chunk.ChunkHeader.MetaData.Statistics.Min, "Expected no Min statistics for %s", tt.name)
					require.Nil(t, chunk.ChunkHeader.MetaData.Statistics.MaxValue, "Expected no MaxValue for %s", tt.name)
					require.Nil(t, chunk.ChunkHeader.MetaData.Statistics.MinValue, "Expected no MinValue for %s", tt.name)
				}

				// Null count should always be present
				require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.NullCount, "Expected NullCount for %s", tt.name)

				// Verify geospatial statistics are present for geospatial types
				if tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY()) {
					require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics, "Expected GeospatialStatistics for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics.Bbox, "Expected GeospatialStatistics.Bbox for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics.GeospatialTypes, "Expected GeospatialStatistics.GeospatialTypes for %s", tt.name)
				} else {
					require.Nil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics, "Expected no GeospatialStatistics for %s", tt.name)
				}
			})

			t.Run("PagesToDictChunk", func(t *testing.T) {
				// Create dictionary page (first page)
				dictPage := &Page{
					Header: &parquet.PageHeader{
						Type: parquet.PageType_DICTIONARY_PAGE,
					},
					Schema: &parquet.SchemaElement{
						Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						LogicalType: tt.logicalType,
					},
					Info:    &common.Tag{},
					RawData: []byte{0x01, 0x02, 0x03, 0x04}, // dummy data
				}

				// Create data page with geospatial statistics (second page)
				dataPage := &Page{
					Header: &parquet.PageHeader{
						Type: parquet.PageType_DATA_PAGE,
						DataPageHeader: &parquet.DataPageHeader{
							NumValues: 1,
						},
					},
					Schema: &parquet.SchemaElement{
						Type:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
						LogicalType: tt.logicalType,
					},
					Info:      &common.Tag{},
					NullCount: common.ToPtr(int64(0)),
					RawData:   []byte{0x05, 0x06, 0x07, 0x08}, // dummy data
				}

				// Only set min/max values for non-geospatial types
				if tt.logicalType == nil || (!tt.logicalType.IsSetGEOMETRY() && !tt.logicalType.IsSetGEOGRAPHY()) {
					dataPage.MaxVal = string(wkbPoint)
					dataPage.MinVal = string(wkbPoint)
				}

				// Add geospatial statistics if it's a geospatial type
				if tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY()) {
					dataPage.GeospatialBBox = &parquet.BoundingBox{Xmin: 10.5, Xmax: 10.5, Ymin: 20.3, Ymax: 20.3}
					dataPage.GeospatialTypes = []int32{1}
				}

				pages := []*Page{dictPage, dataPage}

				chunk, err := PagesToDictChunk(pages)
				require.NoError(t, err)
				require.NotNil(t, chunk)
				require.NotNil(t, chunk.ChunkHeader)
				require.NotNil(t, chunk.ChunkHeader.MetaData)
				require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics)

				// Check min/max statistics based on expectation
				if tt.expectMinMaxStats {
					require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.Max, "Expected min/max statistics for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.Min, "Expected min/max statistics for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.MaxValue, "Expected MaxValue for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.MinValue, "Expected MinValue for %s", tt.name)
				} else {
					require.Nil(t, chunk.ChunkHeader.MetaData.Statistics.Max, "Expected no Max statistics for %s", tt.name)
					require.Nil(t, chunk.ChunkHeader.MetaData.Statistics.Min, "Expected no Min statistics for %s", tt.name)
					require.Nil(t, chunk.ChunkHeader.MetaData.Statistics.MaxValue, "Expected no MaxValue for %s", tt.name)
					require.Nil(t, chunk.ChunkHeader.MetaData.Statistics.MinValue, "Expected no MinValue for %s", tt.name)
				}

				// Null count should always be present
				require.NotNil(t, chunk.ChunkHeader.MetaData.Statistics.NullCount, "Expected NullCount for %s", tt.name)

				// Verify geospatial statistics are present for geospatial types
				if tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY()) {
					require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics, "Expected GeospatialStatistics for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics.Bbox, "Expected GeospatialStatistics.Bbox for %s", tt.name)
					require.NotNil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics.GeospatialTypes, "Expected GeospatialStatistics.GeospatialTypes for %s", tt.name)
				} else {
					require.Nil(t, chunk.ChunkHeader.MetaData.GeospatialStatistics, "Expected no GeospatialStatistics for %s", tt.name)
				}
			})
		})
	}
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
