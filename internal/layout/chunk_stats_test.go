package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// TestPopulateStatistics_EncodeErrors exercises the wrapped error paths for
// max- and min-value encoding when the value type does not match the parquet
// type (WritePlain returns a type-assertion error).
func TestPopulateStatistics_EncodeErrors(t *testing.T) {
	schema := &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)}

	t.Run("max_encode_error", func(t *testing.T) {
		meta := parquet.NewColumnMetaData()
		// maxVal is a string but the type is INT32, so WritePlain fails.
		err := populateStatistics(meta, schema, int32(1), "not-an-int", 0, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "encode chunk max statistic")
	})

	t.Run("min_encode_error", func(t *testing.T) {
		meta := parquet.NewColumnMetaData()
		// maxVal is valid INT32, but minVal is a string, so the min encode fails.
		err := populateStatistics(meta, schema, "not-an-int", int32(1), 0, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "encode chunk min statistic")
	})
}

// TestAggregatePageMetrics_EncodingsSorted verifies that the aggregated
// Encodings slice is deterministically sorted. The encodings are collected via
// a map, whose iteration order Go randomizes, so without an explicit sort the
// order would vary between runs and produce byte-different files for identical
// input. Repeating the aggregation exercises the randomized iteration.
func TestAggregatePageMetrics_EncodingsSorted(t *testing.T) {
	makePages := func() []*Page {
		dictPage := NewDictPage()
		dictPage.Header.DictionaryPageHeader = &parquet.DictionaryPageHeader{
			Encoding: parquet.Encoding_PLAIN,
		}

		dataPage := NewDataPage()
		dataPage.Header.DataPageHeader = &parquet.DataPageHeader{
			NumValues:               1,
			Encoding:                parquet.Encoding_RLE_DICTIONARY,
			DefinitionLevelEncoding: parquet.Encoding_RLE,
			RepetitionLevelEncoding: parquet.Encoding_BIT_PACKED,
		}

		return []*Page{dictPage, dataPage}
	}

	want := []parquet.Encoding{
		parquet.Encoding_PLAIN,
		parquet.Encoding_RLE,
		parquet.Encoding_BIT_PACKED,
		parquet.Encoding_RLE_DICTIONARY,
	}

	// Run repeatedly so randomized map iteration order is exercised; the result
	// must be identical and sorted every time.
	for range 100 {
		_, _, _, _, _, _, encodings := aggregatePageMetrics(makePages(), 0, nil, true)
		require.Equal(t, want, encodings)
	}
}

func TestAggregateGeospatialStatistics(t *testing.T) {
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

func TestAggregateSizeStatistics(t *testing.T) {
	tests := map[string]struct {
		pages         []*Page
		statsStartIdx int
		wantNil       bool
		wantDefHist   []int64
		wantRepHist   []int64
		wantByteBytes *int64
	}{
		"all_nil_returns_nil": {
			pages:         []*Page{{}, {}},
			statsStartIdx: 0,
			wantNil:       true,
		},
		"nil_page_skipped": {
			pages: []*Page{
				nil,
				{DefinitionLevelHistogram: []int64{2, 3}},
			},
			statsStartIdx: 0,
			wantDefHist:   []int64{2, 3},
		},
		"definition_histogram_aggregated": {
			pages: []*Page{
				{DefinitionLevelHistogram: []int64{1, 4}},
				{DefinitionLevelHistogram: []int64{2, 3}},
			},
			statsStartIdx: 0,
			wantDefHist:   []int64{3, 7},
		},
		"repetition_histogram_aggregated": {
			pages: []*Page{
				{RepetitionLevelHistogram: []int64{5, 2}},
				{RepetitionLevelHistogram: []int64{3, 1}},
			},
			statsStartIdx: 0,
			wantRepHist:   []int64{8, 3},
		},
		"byte_array_bytes_aggregated": {
			pages: []*Page{
				{UnencodedByteArrayDataBytes: common.ToPtr(int64(100))},
				{UnencodedByteArrayDataBytes: common.ToPtr(int64(200))},
			},
			statsStartIdx: 0,
			wantByteBytes: common.ToPtr(int64(300)),
		},
		"stats_start_idx_skips_dict_page": {
			pages: []*Page{
				{DefinitionLevelHistogram: []int64{99, 99}}, // dict page, should be skipped
				{DefinitionLevelHistogram: []int64{1, 2}},
			},
			statsStartIdx: 1,
			wantDefHist:   []int64{1, 2},
		},
		"all_three_combined": {
			pages: []*Page{
				{
					DefinitionLevelHistogram:    []int64{1, 4},
					RepetitionLevelHistogram:    []int64{3, 2},
					UnencodedByteArrayDataBytes: common.ToPtr(int64(50)),
				},
				{
					DefinitionLevelHistogram:    []int64{2, 3},
					RepetitionLevelHistogram:    []int64{4, 1},
					UnencodedByteArrayDataBytes: common.ToPtr(int64(75)),
				},
			},
			statsStartIdx: 0,
			wantDefHist:   []int64{3, 7},
			wantRepHist:   []int64{7, 3},
			wantByteBytes: common.ToPtr(int64(125)),
		},
		"empty_pages_slice": {
			pages:         []*Page{},
			statsStartIdx: 0,
			wantNil:       true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result := aggregateSizeStatistics(tt.pages, tt.statsStartIdx)
			if tt.wantNil {
				require.Nil(t, result)
				return
			}
			require.NotNil(t, result)
			require.Equal(t, tt.wantDefHist, result.DefinitionLevelHistogram)
			require.Equal(t, tt.wantRepHist, result.RepetitionLevelHistogram)
			if tt.wantByteBytes == nil {
				require.Nil(t, result.UnencodedByteArrayDataBytes)
			} else {
				require.NotNil(t, result.UnencodedByteArrayDataBytes)
				require.Equal(t, *tt.wantByteBytes, *result.UnencodedByteArrayDataBytes)
			}
		})
	}
}

// TestAggregateGeospatialStatisticsUnknownBounds covers a chunk holding a page whose bounds
// could not be read. Combining the pages that were read would publish a box smaller than
// the chunk's data, and a reader pushing a spatial filter down to it would skip rows that
// match, so the chunk reports no box; the geometry types come from WKB headers and survive.
func TestAggregateGeospatialStatisticsUnknownBounds(t *testing.T) {
	measured := &Page{
		GeospatialBBox:  &parquet.BoundingBox{Xmin: 0, Xmax: 10, Ymin: 0, Ymax: 10},
		GeospatialTypes: []int32{1},
	}
	unreadable := &Page{GeospatialTypes: []int32{1001}, GeospatialBoundsUnknown: true}
	allNull := &Page{}

	bbox, geoTypes := aggregateGeospatialStatistics([]*Page{measured, unreadable, allNull})
	require.Nil(t, bbox)
	require.Equal(t, []int32{1, 1001}, geoTypes)

	// A page whose geometry types could not be read withholds the list as well: one
	// built from the rest would say the chunk holds only those types. The format spells
	// that as an empty list rather than a missing one.
	unknownTypes := &Page{
		GeospatialBBox:          &parquet.BoundingBox{Xmin: 0, Xmax: 1, Ymin: 0, Ymax: 1},
		GeospatialBoundsUnknown: true,
		GeospatialTypesUnknown:  true,
	}
	bbox, geoTypes = aggregateGeospatialStatistics([]*Page{measured, unknownTypes})
	require.Nil(t, bbox)
	require.Empty(t, geoTypes)
	require.NotNil(t, geoTypes)

	// Without that page the box comes back, and the page with nothing to measure does
	// not withhold it.
	bbox, geoTypes = aggregateGeospatialStatistics([]*Page{measured, allNull})
	require.NotNil(t, bbox)
	require.Equal(t, []float64{0, 10, 0, 10}, []float64{bbox.Xmin, bbox.Xmax, bbox.Ymin, bbox.Ymax})
	require.Equal(t, []int32{1}, geoTypes)
}

// TestAggregateSizeStatisticsWithholdsPartialByteArrayTotal pins that a chunk reports the
// byte-array total only when every page contributed one. Summing the pages that could be
// measured publishes an understated number that reads as exact, which is the failure the
// per-page count already refuses.
func TestAggregateSizeStatisticsWithholdsPartialByteArrayTotal(t *testing.T) {
	page := func(bytes *int64) *Page {
		p := NewDataPage()
		p.Schema = &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY), Name: "s"}
		p.UnencodedByteArrayDataBytes = bytes
		return p
	}
	n := func(v int64) *int64 { return &v }

	t.Run("every page measured", func(t *testing.T) {
		stats := aggregateSizeStatistics([]*Page{page(n(10)), page(n(20))}, 0)
		require.NotNil(t, stats.UnencodedByteArrayDataBytes)
		require.Equal(t, int64(30), *stats.UnencodedByteArrayDataBytes)
	})

	// These pages carry no histograms either, so withholding the total leaves nothing to
	// report and the whole SizeStatistics is omitted.
	t.Run("one page unmeasured", func(t *testing.T) {
		require.Nil(t, aggregateSizeStatistics([]*Page{page(n(10)), page(nil), page(n(20))}, 0))
	})

	// What a column of any other physical type looks like here: the aggregation never reads
	// the schema, so a type that has no byte-array total is simply every page reporting none.
	t.Run("no page measured", func(t *testing.T) {
		require.Nil(t, aggregateSizeStatistics([]*Page{page(nil), page(nil)}, 0))
	})

	t.Run("a nil page", func(t *testing.T) {
		require.Nil(t, aggregateSizeStatistics([]*Page{page(n(10)), nil, page(n(20))}, 0))
	})
}
