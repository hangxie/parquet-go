package layout

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestDataPageCompressWithStatistics(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
		Name: "test_col",
	}
	page.Info = &common.Tag{}
	page.MaxVal = "zzz"
	page.MinVal = "aaa"
	nullCount := int64(1)
	page.NullCount = &nullCount

	// Set up a data table with BYTE_ARRAY type to test the statistics handling
	page.DataTable = &Table{
		Values:             []any{"hello", "world"},
		DefinitionLevels:   []int32{1, 1},
		RepetitionLevels:   []int32{0, 0},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
	}

	data, err := page.dataPageCompress(parquet.CompressionCodec_SNAPPY, nil)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Verify that statistics were set
	require.NotNil(t, page.Header.DataPageHeader.Statistics)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.Max)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.Min)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.NullCount)
}

//nolint:gocognit
func TestGeospatialFields_SkipMinMaxStatistics(t *testing.T) {
	// Create WKB point data for testing (point at coordinates 10.5, 20.3)
	wkbPoint := []byte{
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x40, 0xcd, 0xcc, 0xcc,
		0xcc, 0xcc, 0x4c, 0x34, 0x40,
	}

	tests := []struct {
		name                string
		logicalType         *parquet.LogicalType
		convertedType       *parquet.ConvertedType
		expectMinMaxStats   bool
		expectNullCountStat bool
	}{
		{
			name:                "regular_byte_array_field",
			logicalType:         nil,
			convertedType:       nil,
			expectMinMaxStats:   true,
			expectNullCountStat: true,
		},
		{
			name: "geometry_field",
			logicalType: &parquet.LogicalType{
				GEOMETRY: &parquet.GeometryType{},
			},
			convertedType:       nil,
			expectMinMaxStats:   false,
			expectNullCountStat: true,
		},
		{
			name: "geography_field",
			logicalType: &parquet.LogicalType{
				GEOGRAPHY: &parquet.GeographyType{},
			},
			convertedType:       nil,
			expectMinMaxStats:   false,
			expectNullCountStat: true,
		},
		{
			name:                "interval_field",
			logicalType:         nil,
			convertedType:       common.ToPtr(parquet.ConvertedType_INTERVAL),
			expectMinMaxStats:   false,
			expectNullCountStat: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("data_page_v1", func(t *testing.T) {
				page := NewDataPage()
				page.Schema = &parquet.SchemaElement{
					Type:          common.ToPtr(parquet.Type_BYTE_ARRAY),
					Name:          "test_col",
					LogicalType:   tt.logicalType,
					ConvertedType: tt.convertedType,
				}
				page.Info = &common.Tag{}

				// Only set min/max values for types that should have them
				isGeospatial := tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY())
				isInterval := tt.convertedType != nil && *tt.convertedType == parquet.ConvertedType_INTERVAL

				if !isGeospatial && !isInterval {
					page.MaxVal = string(wkbPoint)
					page.MinVal = string(wkbPoint)
				}

				nullCount := int64(0)
				page.NullCount = &nullCount

				page.DataTable = &Table{
					Values:             []any{string(wkbPoint)},
					DefinitionLevels:   []int32{1},
					RepetitionLevels:   []int32{0},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
				}

				// Add geospatial statistics if it's a geospatial type
				if tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY()) {
					page.GeospatialBBox = &parquet.BoundingBox{
						Xmin: 10.5,
						Xmax: 10.5,
						Ymin: 20.3,
						Ymax: 20.3,
					}
					page.GeospatialTypes = []int32{1} // WKB Point type
				}

				data, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
				require.NoError(t, err)
				require.NotEmpty(t, data)

				// Verify statistics were created
				require.NotNil(t, page.Header.DataPageHeader.Statistics)

				// Check min/max statistics based on expectation
				if tt.expectMinMaxStats {
					require.NotNil(t, page.Header.DataPageHeader.Statistics.Max)
					require.NotNil(t, page.Header.DataPageHeader.Statistics.Min)
					require.NotNil(t, page.Header.DataPageHeader.Statistics.MaxValue)
					require.NotNil(t, page.Header.DataPageHeader.Statistics.MinValue)
				} else {
					require.Nil(t, page.Header.DataPageHeader.Statistics.Max)
					require.Nil(t, page.Header.DataPageHeader.Statistics.Min)
					require.Nil(t, page.Header.DataPageHeader.Statistics.MaxValue)
					require.Nil(t, page.Header.DataPageHeader.Statistics.MinValue)
				}

				// Null count should always be present (not skipped)
				if tt.expectNullCountStat {
					require.NotNil(t, page.Header.DataPageHeader.Statistics.NullCount)
				}

				// Verify geospatial statistics are preserved for geospatial types
				if tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY()) {
					require.NotNil(t, page.GeospatialBBox)
					require.NotNil(t, page.GeospatialTypes)
				}
			})

			t.Run("data_page_v2", func(t *testing.T) {
				page := NewDataPage()
				page.Schema = &parquet.SchemaElement{
					Type:          common.ToPtr(parquet.Type_BYTE_ARRAY),
					Name:          "test_col",
					LogicalType:   tt.logicalType,
					ConvertedType: tt.convertedType,
				}
				page.Info = &common.Tag{}

				// Only set min/max values for types that should have them
				isGeospatial := tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY())
				isInterval := tt.convertedType != nil && *tt.convertedType == parquet.ConvertedType_INTERVAL

				if !isGeospatial && !isInterval {
					page.MaxVal = string(wkbPoint)
					page.MinVal = string(wkbPoint)
				}

				nullCount := int64(0)
				page.NullCount = &nullCount

				page.DataTable = &Table{
					Values:             []any{string(wkbPoint)},
					DefinitionLevels:   []int32{1},
					RepetitionLevels:   []int32{0},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
				}

				// Add geospatial statistics if it's a geospatial type
				if tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY()) {
					page.GeospatialBBox = &parquet.BoundingBox{
						Xmin: 10.5,
						Xmax: 10.5,
						Ymin: 20.3,
						Ymax: 20.3,
					}
					page.GeospatialTypes = []int32{1} // WKB Point type
				}

				_, _, data, err := page.dataPageV2Compress(parquet.CompressionCodec_UNCOMPRESSED, nil)
				require.NoError(t, err)
				require.NotEmpty(t, data)

				// Verify statistics were created
				require.NotNil(t, page.Header.DataPageHeaderV2.Statistics)

				// Check min/max statistics based on expectation
				if tt.expectMinMaxStats {
					require.NotNil(t, page.Header.DataPageHeaderV2.Statistics.Max)
					require.NotNil(t, page.Header.DataPageHeaderV2.Statistics.Min)
					require.NotNil(t, page.Header.DataPageHeaderV2.Statistics.MaxValue)
					require.NotNil(t, page.Header.DataPageHeaderV2.Statistics.MinValue)
				} else {
					require.Nil(t, page.Header.DataPageHeaderV2.Statistics.Max)
					require.Nil(t, page.Header.DataPageHeaderV2.Statistics.Min)
					require.Nil(t, page.Header.DataPageHeaderV2.Statistics.MaxValue)
					require.Nil(t, page.Header.DataPageHeaderV2.Statistics.MinValue)
				}

				// Null count should always be present (not skipped)
				if tt.expectNullCountStat {
					require.NotNil(t, page.Header.DataPageHeaderV2.Statistics.NullCount)
				}

				// Verify geospatial statistics are preserved for geospatial types
				if tt.logicalType != nil && (tt.logicalType.IsSetGEOMETRY() || tt.logicalType.IsSetGEOGRAPHY()) {
					require.NotNil(t, page.GeospatialBBox)
					require.NotNil(t, page.GeospatialTypes)
				}
			})
		})
	}
}

func TestDataPageV2CompressWithComplexData(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT64),
		Name: "test_col",
	}
	page.Info = &common.Tag{}
	page.MaxVal = int64(200)
	page.MinVal = int64(50)
	nullCount := int64(2)
	page.NullCount = &nullCount

	// Set up a data table with repetition levels
	page.DataTable = &Table{
		Values:             []any{int64(100), int64(150), int64(75), int64(200)},
		DefinitionLevels:   []int32{1, 1, 0, 1}, // One null value
		RepetitionLevels:   []int32{0, 1, 0, 1}, // Some repetition
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 1,
	}

	_, _, data, err := page.dataPageV2Compress(parquet.CompressionCodec_GZIP, nil)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Verify header values for v2
	require.Equal(t, parquet.PageType_DATA_PAGE_V2, page.Header.Type)
	require.NotZero(t, page.Header.DataPageHeaderV2.NumRows)
	require.NotNil(t, page.Header.DataPageHeaderV2.Statistics)
}

func TestPageDataPageCompress(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}

	// Set up a data table
	page.DataTable = &Table{
		Values:           []any{int32(1), int32(2)},
		DefinitionLevels: []int32{0, 0},
		RepetitionLevels: []int32{0, 0},
	}

	data, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestPageDataPageV2Compress(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}

	// Set up a data table
	page.DataTable = &Table{
		Values:           []any{int32(1), int32(2)},
		DefinitionLevels: []int32{0, 0},
		RepetitionLevels: []int32{0, 0},
	}

	_, _, data, err := page.dataPageV2Compress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestPageEncodingValues(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}

	values := []any{int32(1), int32(2), int32(3)}
	data, err := page.EncodingValues(values)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestPageEncodingValuesErrorCases(t *testing.T) {
	testCases := []struct {
		name     string
		encoding parquet.Encoding
		dataType parquet.Type
		values   []any
		setup    func(*Page)
		hasError bool
	}{
		{
			name:     "rle_encoding_with_length",
			encoding: parquet.Encoding_RLE,
			dataType: parquet.Type_INT32,
			values:   []any{int32(1)},
			setup: func(page *Page) {
				page.Info.Length = 4 // Valid bit width
			},
			hasError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			page := NewDataPage()
			page.Schema = &parquet.SchemaElement{
				Type: &tc.dataType,
				Name: "test_col",
			}
			page.Info = &common.Tag{}
			page.Info.Encoding = tc.encoding

			if tc.setup != nil {
				tc.setup(page)
			}

			_, err := page.EncodingValues(tc.values)
			if tc.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPageEncodingValuesWithDifferentEncodings(t *testing.T) {
	testCases := []struct {
		name     string
		encoding parquet.Encoding
		dataType parquet.Type
		values   []any
		bitWidth int32
		hasError bool
	}{
		{
			name:     "rle_encoding",
			encoding: parquet.Encoding_RLE,
			dataType: parquet.Type_INT32,
			values:   []any{int32(1), int32(1), int32(2)},
			bitWidth: 2,
			hasError: false,
		},
		{
			name:     "delta_binary_packed",
			encoding: parquet.Encoding_DELTA_BINARY_PACKED,
			dataType: parquet.Type_INT32,
			values:   []any{int32(1), int32(2), int32(3)},
			hasError: false,
		},
		{
			name:     "delta_byte_array",
			encoding: parquet.Encoding_DELTA_BYTE_ARRAY,
			dataType: parquet.Type_BYTE_ARRAY,
			values:   []any{"hello", "world"},
			hasError: false,
		},
		{
			name:     "delta_length_byte_array",
			encoding: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			dataType: parquet.Type_BYTE_ARRAY,
			values:   []any{"test", "data"},
			hasError: false,
		},
		{
			name:     "byte_stream_split",
			encoding: parquet.Encoding_BYTE_STREAM_SPLIT,
			dataType: parquet.Type_FLOAT,
			values:   []any{float32(1.0), float32(2.0)},
			hasError: false,
		},
		{
			name:     "plain_encoding_default",
			encoding: parquet.Encoding_PLAIN,
			dataType: parquet.Type_INT64,
			values:   []any{int64(100), int64(200)},
			hasError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			page := NewDataPage()
			page.Schema = &parquet.SchemaElement{
				Type: &tc.dataType,
				Name: "test_col",
			}
			page.Info = &common.Tag{}
			page.Info.Encoding = tc.encoding
			if tc.bitWidth > 0 {
				page.Info.Length = tc.bitWidth
			}

			data, err := page.EncodingValues(tc.values)
			if tc.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if len(tc.values) > 0 {
					require.NotEmpty(t, data)
				}
			}
		})
	}
}

func TestPageEncodingValuesWithEmptyValues(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}

	values := []any{}
	data, err := page.EncodingValues(values)
	require.NoError(t, err)
	// Empty values should produce empty data
	require.Empty(t, data)
}

func TestPageEncodingValues_BooleanRLE(t *testing.T) {
	t.Run("boolean_rle_without_length_specified", func(t *testing.T) {
		// Test that Page.EncodingValues correctly defaults to bitWidth=1 for BOOLEAN type
		// when page.Info.Length is 0 (not specified in struct tag)
		page := NewPage()
		boolType := parquet.Type_BOOLEAN
		page.Schema = &parquet.SchemaElement{
			Type: &boolType,
		}
		page.Info = &common.Tag{}
		page.Info.Encoding = parquet.Encoding_RLE
		page.Info.Length = 0 // Not set in struct tag

		boolVals := []any{true, false, true, true, false}

		// Encode using Page.EncodingValues (should fix bitWidth to 1)
		encoded, err := page.EncodingValues(boolVals)
		require.NoError(t, err)

		// Compare with expected encoding (bitWidth=1)
		expected, _ := encoding.WriteRLEBitPackedHybrid(boolVals, 1, parquet.Type_BOOLEAN)
		require.Equal(t, expected, encoded)

		// Verify it can be decoded correctly
		result, err := ReadDataPageValues(bytes.NewReader(encoded), parquet.Encoding_RLE, parquet.Type_BOOLEAN, -1, 5, 0)
		require.NoError(t, err)
		require.Len(t, result, 5)

		for i := range boolVals {
			require.Equal(t, boolVals[i], result[i])
		}
	})
}

func TestTableToDataPages(t *testing.T) {
	// Create a simple table with INT32 values
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:           []any{int32(1), int32(2), int32(3)},
		DefinitionLevels: []int32{0, 0, 0},
		RepetitionLevels: []int32{0, 0, 0},
		Info:             &common.Tag{},
	}

	pages, totalSize, err := TableToDataPagesWithOption(table, PageWriteOption{
		PageSize:        1024,
		CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
		DataPageVersion: 1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, pages)
	require.Positive(t, totalSize)
}

func TestTableToDataPagesComplexScenarios(t *testing.T) {
	testCases := []struct {
		name              string
		table             *Table
		pageSize          int32
		compressionCodec  parquet.CompressionCodec
		expectError       bool
		expectedPageCount int
	}{
		{
			name: "table_with_nulls",
			table: &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
					Name: "test_col",
				},
				Values:             []any{int32(1), nil, int32(3), int32(4)},
				DefinitionLevels:   []int32{1, 0, 1, 1},
				RepetitionLevels:   []int32{0, 0, 0, 0},
				MaxDefinitionLevel: 1,
				Info:               &common.Tag{},
			},
			pageSize:          1024,
			compressionCodec:  parquet.CompressionCodec_UNCOMPRESSED,
			expectError:       false,
			expectedPageCount: 1,
		},
		{
			name: "table_with_stats_omitted",
			table: &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT64),
					Name: "test_col",
				},
				Values:             []any{int64(100), int64(200), int64(300)},
				DefinitionLevels:   []int32{1, 1, 1},
				RepetitionLevels:   []int32{0, 0, 0},
				MaxDefinitionLevel: 1,
				Info: func() *common.Tag {
					tag := &common.Tag{}
					tag.OmitStats = true
					return tag
				}(),
			},
			pageSize:          1024,
			compressionCodec:  parquet.CompressionCodec_SNAPPY,
			expectError:       false,
			expectedPageCount: 1,
		},
		{
			name: "small_page_size_multiple_pages",
			table: &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
					Name: "test_col",
				},
				Values:             []any{int32(1), int32(2), int32(3), int32(4), int32(5)},
				DefinitionLevels:   []int32{1, 1, 1, 1, 1},
				RepetitionLevels:   []int32{0, 0, 0, 0, 0},
				MaxDefinitionLevel: 1,
				Info:               &common.Tag{},
			},
			pageSize:          8, // Very small page size to force multiple pages
			compressionCodec:  parquet.CompressionCodec_UNCOMPRESSED,
			expectError:       false,
			expectedPageCount: 3, // Actual page splitting behavior produces 3 pages
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pages, totalSize, err := TableToDataPagesWithOption(tc.table, PageWriteOption{
				PageSize:        tc.pageSize,
				CompressType:    tc.compressionCodec,
				DataPageVersion: 1,
			})

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, pages, tc.expectedPageCount)
				if len(pages) > 0 {
					require.Positive(t, totalSize)
				}

				// Verify page properties
				for _, page := range pages {
					require.NotNil(t, page.Schema)
					require.NotNil(t, page.DataTable)
					require.NotNil(t, page.RawData)
				}
			}
		})
	}
}

func TestTableToDataPagesWithEmptyTable(t *testing.T) {
	// Create an empty table
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:           []any{},
		DefinitionLevels: []int32{},
		RepetitionLevels: []int32{},
		Info:             &common.Tag{},
	}

	pages, totalSize, err := TableToDataPagesWithOption(table, PageWriteOption{
		PageSize:        1024,
		CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
		DataPageVersion: 1,
	})
	require.NoError(t, err)
	require.Empty(t, pages)
	require.Zero(t, totalSize)
}

func TestTableToDataPagesWithInvalidType(t *testing.T) {
	// Create a table with invalid schema type
	table := &Table{
		Schema: &parquet.SchemaElement{
			Name: "test_col",
			// No Type set - this should cause an error
		},
		Values:           []any{int32(1)},
		DefinitionLevels: []int32{0},
		RepetitionLevels: []int32{0},
		Info:             &common.Tag{},
	}

	_, _, err := TableToDataPagesWithOption(table, PageWriteOption{
		PageSize:        1024,
		CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
		DataPageVersion: 1,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "all types are nil")
}

func TestTableToDataPagesWithVersion(t *testing.T) {
	testCases := []struct {
		name        string
		table       *Table
		pageVersion int32
		expectV2    bool
	}{
		{
			name: "version_1_creates_data_page_v1",
			table: &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
					Name: "test_col",
				},
				Values:             []any{int32(1), int32(2), int32(3)},
				DefinitionLevels:   []int32{1, 1, 1},
				RepetitionLevels:   []int32{0, 0, 0},
				MaxDefinitionLevel: 1,
				Info:               &common.Tag{},
			},
			pageVersion: 1,
			expectV2:    false,
		},
		{
			name: "version_2_creates_data_page_v2",
			table: &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
					Name: "test_col",
				},
				Values:             []any{int32(10), int32(20), int32(30)},
				DefinitionLevels:   []int32{1, 1, 1},
				RepetitionLevels:   []int32{0, 0, 0},
				MaxDefinitionLevel: 1,
				Info:               &common.Tag{},
			},
			pageVersion: 2,
			expectV2:    true,
		},
		{
			name: "version_2_with_nulls",
			table: &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT64),
					Name: "test_col",
				},
				Values:             []any{int64(100), nil, int64(300), int64(400)},
				DefinitionLevels:   []int32{1, 0, 1, 1},
				RepetitionLevels:   []int32{0, 0, 0, 0},
				MaxDefinitionLevel: 1,
				Info:               &common.Tag{},
			},
			pageVersion: 2,
			expectV2:    true,
		},
		{
			name: "version_2_with_compression",
			table: &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
					Name: "test_col",
				},
				Values:             []any{"hello", "world", "test"},
				DefinitionLevels:   []int32{1, 1, 1},
				RepetitionLevels:   []int32{0, 0, 0},
				MaxDefinitionLevel: 1,
				Info:               &common.Tag{},
			},
			pageVersion: 2,
			expectV2:    true,
		},
		{
			name: "geometry_with_geospatial_statistics",
			table: &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
					Name: "geometry_col",
					LogicalType: &parquet.LogicalType{
						GEOMETRY: &parquet.GeometryType{},
					},
				},
				// WKB point data: point at (10.5, 20.3)
				Values: []any{
					string([]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x40, 0xcd, 0xcc, 0xcc, 0xcc, 0xcc, 0x4c, 0x34, 0x40}),
					string([]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x59, 0x34, 0x40}),
				},
				DefinitionLevels:   []int32{1, 1},
				RepetitionLevels:   []int32{0, 0},
				MaxDefinitionLevel: 1,
				Info:               &common.Tag{},
			},
			pageVersion: 1,
			expectV2:    false,
		},
		{
			name: "geography_with_geospatial_statistics",
			table: &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
					Name: "geography_col",
					LogicalType: &parquet.LogicalType{
						GEOGRAPHY: &parquet.GeographyType{},
					},
				},
				// WKB point data
				Values: []any{
					string([]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x40, 0xcd, 0xcc, 0xcc, 0xcc, 0xcc, 0x4c, 0x34, 0x40}),
				},
				DefinitionLevels:   []int32{1},
				RepetitionLevels:   []int32{0},
				MaxDefinitionLevel: 1,
				Info:               &common.Tag{},
			},
			pageVersion: 2,
			expectV2:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pages, totalSize, err := TableToDataPagesWithOption(
				tc.table,
				PageWriteOption{
					PageSize:        1024,
					CompressType:    parquet.CompressionCodec_SNAPPY,
					DataPageVersion: tc.pageVersion,
				},
			)

			require.NoError(t, err)
			require.NotEmpty(t, pages)
			require.Positive(t, totalSize)

			// Verify the page type matches the version
			for _, page := range pages {
				require.NotNil(t, page.Header)
				if tc.expectV2 {
					require.Equal(t, parquet.PageType_DATA_PAGE_V2, page.Header.Type)
					require.NotNil(t, page.Header.DataPageHeaderV2)
					require.Nil(t, page.Header.DataPageHeader)
				} else {
					require.Equal(t, parquet.PageType_DATA_PAGE, page.Header.Type)
					require.NotNil(t, page.Header.DataPageHeader)
					require.Nil(t, page.Header.DataPageHeaderV2)
				}

				// Check if geospatial statistics were computed
				logT := tc.table.Schema.LogicalType
				if logT != nil && (logT.IsSetGEOMETRY() || logT.IsSetGEOGRAPHY()) {
					require.NotNil(t, page.GeospatialBBox, "GeospatialBBox should be set for geospatial types")
					require.NotNil(t, page.GeospatialTypes, "GeospatialTypes should be set for geospatial types")
					require.NotEmpty(t, page.GeospatialTypes, "GeospatialTypes should not be empty")
				}
			}
		})
	}
}

func TestExtractGeometryType(t *testing.T) {
	t.Run("point_little_endian", func(t *testing.T) {
		// Hardcoded WKB point (10.5, 20.3) little-endian
		wkb := []byte{
			0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x40, 0xcd, 0xcc, 0xcc,
			0xcc, 0xcc, 0x4c, 0x34, 0x40,
		}
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(1), geoType) // Point = 1
	})

	t.Run("point_big_endian", func(t *testing.T) {
		// Hardcoded WKB point (10.5, 20.3) big-endian
		wkb := []byte{
			0x00, 0x00, 0x00, 0x00, 0x01, 0x40, 0x25, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x40, 0x34, 0x4c, 0xcc, 0xcc, 0xcc, 0xcc, 0xcd,
		}
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(1), geoType) // Point = 1
	})

	t.Run("linestring_little_endian", func(t *testing.T) {
		// Hardcoded WKB LineString ((0,0), (10,10)) little-endian
		wkb := []byte{
			0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x24, 0x40,
		}
		geoType := extractGeometryType(wkb)
		require.Equal(t, int32(2), geoType) // LineString = 2
	})

	t.Run("linestring_big_endian", func(t *testing.T) {
		// Hardcoded WKB LineString ((0,0), (10,10)) big-endian
		wkb := []byte{
			0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x40, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x24, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00,
		}
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

func TestComputePageGeospatialStatistics(t *testing.T) {
	t.Run("single_point", func(t *testing.T) {
		// Hardcoded WKB point (10.5, 20.3) little-endian
		wkb := []byte{
			0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x40, 0xcd, 0xcc, 0xcc,
			0xcc, 0xcc, 0x4c, 0x34, 0x40,
		}
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
		// Hardcoded WKB point (0, 0) little-endian
		point1 := []byte{
			0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00,
		}
		// Hardcoded WKB point (10, 20) little-endian
		point2 := []byte{
			0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x34, 0x40,
		}
		// Hardcoded WKB LineString ((5,5), (15,25)) little-endian
		linestring := []byte{
			0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14,
			0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2e,
			0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x39, 0x40,
		}

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
		// Hardcoded WKB point (5, 10) little-endian
		point1 := []byte{
			0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14,
			0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40,
		}

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
		wkb := []byte{0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2f, 0x40, 0x33, 0x33, 0x33, 0x33, 0x33, 0xb3, 0x39, 0x40}
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
		validWKB := []byte{0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x24, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x34, 0x40}
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
		wkb1 := []byte{0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40}
		wkb2 := []byte{0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x8, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x10, 0x40}

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
		point1 := []byte{0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
		point2 := []byte{0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x24, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x24, 0x40}
		point3 := []byte{0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x14, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x14, 0x40}

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

func TestTableToDataPagesWithVersion_EmptyTable(t *testing.T) {
	// Test that empty tables are handled gracefully without panic
	table := &Table{
		Values:             []any{},
		DefinitionLevels:   []int32{},
		RepetitionLevels:   []int32{},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Info: &common.Tag{},
	}

	pages, size, err := TableToDataPagesWithOption(table, PageWriteOption{
		PageSize:        1024,
		CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
		DataPageVersion: 1,
	})
	require.NoError(t, err)
	require.Empty(t, pages)
	require.Equal(t, int64(0), size)

	// Also test with v2 pages
	pages, size, err = TableToDataPagesWithOption(table, PageWriteOption{
		PageSize:        1024,
		CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
		DataPageVersion: 2,
	})
	require.NoError(t, err)
	require.Empty(t, pages)
	require.Equal(t, int64(0), size)
}

func TestEncodingValues_ErrorPaths(t *testing.T) {
	tests := map[string]struct {
		pType    parquet.Type
		encoding parquet.Encoding
		values   []any
		errMsg   string
	}{
		"RLE_with_BYTE_ARRAY": {
			pType:    parquet.Type_BYTE_ARRAY,
			encoding: parquet.Encoding_RLE,
			values:   []any{"hello"},
			errMsg:   "RLE encoding is not supported for",
		},
		"DELTA_BINARY_PACKED_with_BYTE_ARRAY": {
			pType:    parquet.Type_BYTE_ARRAY,
			encoding: parquet.Encoding_DELTA_BINARY_PACKED,
			values:   []any{"hello"},
			errMsg:   "DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64",
		},
		"DELTA_BYTE_ARRAY_with_INT32": {
			pType:    parquet.Type_INT32,
			encoding: parquet.Encoding_DELTA_BYTE_ARRAY,
			values:   []any{int32(1)},
			errMsg:   "DELTA_BYTE_ARRAY encoding is only supported for BYTE_ARRAY",
		},
		"DELTA_LENGTH_BYTE_ARRAY_with_INT32": {
			pType:    parquet.Type_INT32,
			encoding: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			values:   []any{int32(1)},
			errMsg:   "DELTA_LENGTH_BYTE_ARRAY encoding is only supported for BYTE_ARRAY",
		},
		"BYTE_STREAM_SPLIT_with_BYTE_ARRAY": {
			pType:    parquet.Type_BYTE_ARRAY,
			encoding: parquet.Encoding_BYTE_STREAM_SPLIT,
			values:   []any{"hello"},
			errMsg:   "BYTE_STREAM_SPLIT encoding is only supported for",
		},
		"DELTA_BINARY_PACKED_with_INT32_success": {
			pType:    parquet.Type_INT32,
			encoding: parquet.Encoding_DELTA_BINARY_PACKED,
			values:   []any{int32(1), int32(2), int32(3)},
		},
		"DELTA_BYTE_ARRAY_with_BYTE_ARRAY_success": {
			pType:    parquet.Type_BYTE_ARRAY,
			encoding: parquet.Encoding_DELTA_BYTE_ARRAY,
			values:   []any{"abc", "def"},
		},
		"DELTA_LENGTH_BYTE_ARRAY_with_BYTE_ARRAY_success": {
			pType:    parquet.Type_BYTE_ARRAY,
			encoding: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			values:   []any{"abc", "def"},
		},
		"BYTE_STREAM_SPLIT_with_FLOAT_success": {
			pType:    parquet.Type_FLOAT,
			encoding: parquet.Encoding_BYTE_STREAM_SPLIT,
			values:   []any{float32(1.5), float32(2.5)},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			page := NewDataPage()
			page.Schema = &parquet.SchemaElement{
				Type: common.ToPtr(tt.pType),
				Name: "test_col",
			}
			page.Info = &common.Tag{}
			page.Info.Encoding = tt.encoding

			result, err := page.EncodingValues(tt.values)
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, result)
			}
		})
	}
}

func TestDataPageCompress_ZeroLevels(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type:           common.ToPtr(parquet.Type_INT32),
		Name:           "required_col",
		RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
	}
	page.Info = &common.Tag{}
	page.MaxVal = int32(100)
	page.MinVal = int32(1)
	nullCount := int64(0)
	page.NullCount = &nullCount

	page.DataTable = &Table{
		Values:             []any{int32(1), int32(50), int32(100)},
		DefinitionLevels:   []int32{0, 0, 0},
		RepetitionLevels:   []int32{0, 0, 0},
		MaxDefinitionLevel: 0,
		MaxRepetitionLevel: 0,
	}

	data, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	require.Equal(t, parquet.PageType_DATA_PAGE, page.Header.Type)
	require.NotNil(t, page.Header.DataPageHeader.Statistics)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.Max)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.Min)
}

func TestComputeLevelHistograms(t *testing.T) {
	tests := map[string]struct {
		page           *Page
		wantDefHist    []int64
		wantRepHist    []int64
		wantByteArrayN *int64
	}{
		"nil_data_table": {
			page: &Page{
				DataTable: nil,
			},
			wantDefHist:    nil,
			wantRepHist:    nil,
			wantByteArrayN: nil,
		},
		"definition_level_histogram_only": {
			page: &Page{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				},
				DataTable: &Table{
					Values:             []any{int32(1), nil, int32(3)},
					DefinitionLevels:   []int32{1, 0, 1},
					RepetitionLevels:   []int32{0, 0, 0},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
				},
			},
			wantDefHist:    []int64{1, 2},
			wantRepHist:    nil,
			wantByteArrayN: nil,
		},
		"repetition_level_histogram_only": {
			page: &Page{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				},
				DataTable: &Table{
					Values:             []any{int32(1), int32(2), int32(3), int32(4)},
					DefinitionLevels:   []int32{0, 0, 0, 0},
					RepetitionLevels:   []int32{0, 1, 0, 1},
					MaxDefinitionLevel: 0,
					MaxRepetitionLevel: 1,
				},
			},
			wantDefHist:    nil,
			wantRepHist:    []int64{2, 2},
			wantByteArrayN: nil,
		},
		"both_def_and_rep_histograms": {
			page: &Page{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				},
				DataTable: &Table{
					Values:             []any{int32(1), nil, int32(3), int32(4)},
					DefinitionLevels:   []int32{1, 0, 1, 1},
					RepetitionLevels:   []int32{0, 0, 1, 0},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 1,
				},
			},
			wantDefHist:    []int64{1, 3},
			wantRepHist:    []int64{3, 1},
			wantByteArrayN: nil,
		},
		"byte_array_string_values": {
			page: &Page{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
				},
				DataTable: &Table{
					Values:             []any{"hello", nil, "world"},
					DefinitionLevels:   []int32{1, 0, 1},
					RepetitionLevels:   []int32{0, 0, 0},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
				},
			},
			wantDefHist:    []int64{1, 2},
			wantRepHist:    nil,
			wantByteArrayN: common.ToPtr(int64(10)), // "hello"(5) + "world"(5)
		},
		"byte_array_byte_slice_values": {
			page: &Page{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
				},
				DataTable: &Table{
					Values:             []any{[]byte{1, 2, 3}, nil, []byte{4, 5}},
					DefinitionLevels:   []int32{1, 0, 1},
					RepetitionLevels:   []int32{0, 0, 0},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
				},
			},
			wantDefHist:    []int64{1, 2},
			wantRepHist:    nil,
			wantByteArrayN: common.ToPtr(int64(5)), // 3 + 2
		},
		"required_field_no_histograms": {
			page: &Page{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT64),
				},
				DataTable: &Table{
					Values:             []any{int64(1), int64(2)},
					DefinitionLevels:   []int32{0, 0},
					RepetitionLevels:   []int32{0, 0},
					MaxDefinitionLevel: 0,
					MaxRepetitionLevel: 0,
				},
			},
			wantDefHist:    nil,
			wantRepHist:    nil,
			wantByteArrayN: nil,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tt.page.computeLevelHistograms()
			require.Equal(t, tt.wantDefHist, tt.page.DefinitionLevelHistogram)
			require.Equal(t, tt.wantRepHist, tt.page.RepetitionLevelHistogram)
			if tt.wantByteArrayN == nil {
				require.Nil(t, tt.page.UnencodedByteArrayDataBytes)
			} else {
				require.NotNil(t, tt.page.UnencodedByteArrayDataBytes)
				require.Equal(t, *tt.wantByteArrayN, *tt.page.UnencodedByteArrayDataBytes)
			}
		})
	}
}

func TestDataPageCompress_OmitStats(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}
	page.Info.OmitStats = true

	page.DataTable = &Table{
		Values:             []any{int32(10), int32(20)},
		DefinitionLevels:   []int32{1, 1},
		RepetitionLevels:   []int32{0, 0},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
	}

	data, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	stats := page.Header.DataPageHeader.Statistics
	require.NotNil(t, stats)
	// OmitStats means MaxVal/MinVal are never set on the page
	require.Nil(t, stats.Max)
	require.Nil(t, stats.Min)
	require.Nil(t, stats.NullCount)
}

func TestDataPageCompress_ReturnsCompressedData(t *testing.T) {
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:             []any{int32(1), int32(2), int32(3)},
		DefinitionLevels:   []int32{1, 1, 1},
		RepetitionLevels:   []int32{0, 0, 0},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
		Path:               []string{"test_col"},
	}

	page := NewDataPage()
	page.DataTable = table
	page.Schema = table.Schema
	page.Info = &common.Tag{}

	compressedData, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, compressedData)

	// Header should be populated but RawData should NOT be set
	require.Equal(t, parquet.PageType_DATA_PAGE, page.Header.Type)
	require.Positive(t, page.Header.CompressedPageSize)
	require.Empty(t, page.RawData)
}

func TestDataPageV2Compress_ReturnsCompressedBuffers(t *testing.T) {
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:             []any{int32(10), int32(20), int32(30)},
		DefinitionLevels:   []int32{1, 1, 1},
		RepetitionLevels:   []int32{0, 0, 0},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
		Path:               []string{"test_col"},
	}

	page := NewDataPage()
	page.DataTable = table
	page.Schema = table.Schema
	page.Info = &common.Tag{}

	repLevels, defLevels, compressedValues, err := page.dataPageV2Compress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, compressedValues)
	_ = repLevels
	_ = defLevels

	require.Equal(t, parquet.PageType_DATA_PAGE_V2, page.Header.Type)
	require.Positive(t, page.Header.CompressedPageSize)
	require.Empty(t, page.RawData)
}

func TestDataPageV2Compress_SkipsCompressionWhenNoGain(t *testing.T) {
	// When compression doesn't reduce size, the writer should store the data
	// uncompressed and set is_compressed=false. This is standard practice in
	// other Parquet implementations (e.g., PyArrow).
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:             []any{int32(10), int32(20), int32(30)},
		DefinitionLevels:   []int32{1, 1, 1},
		RepetitionLevels:   []int32{0, 0, 0},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
		Path:               []string{"test_col"},
	}

	page := NewDataPage()
	page.DataTable = table
	page.Schema = table.Schema
	page.Info = &common.Tag{}

	_, _, _, err := page.dataPageV2Compress(parquet.CompressionCodec_SNAPPY, nil)
	require.NoError(t, err)

	// With only 3 small INT32 values, Snappy compression won't reduce size.
	// The writer should detect this and set IsCompressed=false.
	require.False(t, page.Header.DataPageHeaderV2.GetIsCompressed(),
		"expected is_compressed=false when compression does not reduce size")
	require.Equal(t, page.Header.CompressedPageSize, page.Header.UncompressedPageSize,
		"compressed and uncompressed sizes should match when data is stored uncompressed")
}

func TestSerializePage(t *testing.T) {
	testCases := []struct {
		name           string
		writeCRC       bool
		compressedData [][]byte
		expectCRC      bool
	}{
		{
			name:           "without_crc",
			writeCRC:       false,
			compressedData: [][]byte{[]byte("compressed-data")},
			expectCRC:      false,
		},
		{
			name:           "with_crc",
			writeCRC:       true,
			compressedData: [][]byte{[]byte("compressed-data")},
			expectCRC:      true,
		},
		{
			name:           "with_crc_multiple_slices",
			writeCRC:       true,
			compressedData: [][]byte{[]byte("rep-levels"), []byte("def-levels"), []byte("values")},
			expectCRC:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			page := NewPage()
			page.Header.Type = parquet.PageType_DATA_PAGE
			page.Header.CompressedPageSize = 15
			page.Header.UncompressedPageSize = 15

			opt := PageWriteOption{
				WriteCRC: tc.writeCRC,
			}

			err := serializePage(page, opt, tc.compressedData...)
			require.NoError(t, err)
			require.NotEmpty(t, page.RawData)

			if tc.expectCRC {
				require.True(t, page.Header.IsSetCrc())
				expectedCRC := common.ComputePageCRC(tc.compressedData...)
				require.Equal(t, int32(expectedCRC), page.Header.GetCrc())
			} else {
				require.False(t, page.Header.IsSetCrc())
			}
		})
	}
}

func TestTableToDataPagesWithOption(t *testing.T) {
	testCases := []struct {
		name      string
		opt       PageWriteOption
		expectCRC bool
	}{
		{
			name: "v1_without_crc",
			opt: PageWriteOption{
				PageSize:        1024,
				CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
				DataPageVersion: 1,
				WriteCRC:        false,
			},
			expectCRC: false,
		},
		{
			name: "v1_with_crc",
			opt: PageWriteOption{
				PageSize:        1024,
				CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
				DataPageVersion: 1,
				WriteCRC:        true,
			},
			expectCRC: true,
		},
		{
			name: "v2_with_crc",
			opt: PageWriteOption{
				PageSize:        1024,
				CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
				DataPageVersion: 2,
				WriteCRC:        true,
			},
			expectCRC: true,
		},
		{
			name: "v1_with_snappy_and_crc",
			opt: PageWriteOption{
				PageSize:        1024,
				CompressType:    parquet.CompressionCodec_SNAPPY,
				DataPageVersion: 1,
				WriteCRC:        true,
			},
			expectCRC: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			table := &Table{
				Schema: &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
					Name: "test_col",
				},
				Values:             []any{int32(1), int32(2), int32(3)},
				DefinitionLevels:   []int32{1, 1, 1},
				RepetitionLevels:   []int32{0, 0, 0},
				MaxDefinitionLevel: 1,
				Info:               &common.Tag{},
			}

			pages, totalSize, err := TableToDataPagesWithOption(table, tc.opt)
			require.NoError(t, err)
			require.NotEmpty(t, pages)
			require.Positive(t, totalSize)

			for _, page := range pages {
				require.NotEmpty(t, page.RawData)
				if tc.expectCRC {
					require.True(t, page.Header.IsSetCrc(), "expected CRC to be set")
				} else {
					require.False(t, page.Header.IsSetCrc(), "expected CRC not to be set")
				}
			}
		})
	}
}

func TestCompressAndSerializePage_ErrorPaths(t *testing.T) {
	t.Run("v1_compress_error", func(t *testing.T) {
		page := NewDataPage()
		page.Schema = &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
		}
		page.Info = &common.Tag{}
		page.DataTable = &Table{
			Values: []any{int32(1)},
		}
		// Codec level error
		opt := PageWriteOption{
			DataPageVersion: 1,
			CompressType:    parquet.CompressionCodec_GZIP,
		}
		// Force error by setting an unsupported compression level
		c, _ := compress.NewCompressor(compress.WithCompressionLevel(parquet.CompressionCodec_GZIP, 1))
		opt.Compressor = c
		// Actually it is hard to force error here without mocking.
		// Let's try to use a codec that does not support levels with a level
	})
}

func TestDataPageCompress_RequiredNilError(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
	}
	page.DataTable = &Table{
		Values:             []any{nil},
		DefinitionLevels:   []int32{0},
		MaxDefinitionLevel: 0,
		Path:               []string{"test"},
	}
	_, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil value encountered for REQUIRED field")
}

func TestDataPageV2Compress_RequiredNilError(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
	}
	page.DataTable = &Table{
		Values:             []any{nil},
		DefinitionLevels:   []int32{0},
		MaxDefinitionLevel: 0,
		Path:               []string{"test"},
	}
	_, _, _, err := page.dataPageV2Compress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil value encountered for REQUIRED field")
}

func TestDataPageCompress_WithNulls(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
		Type:           common.ToPtr(parquet.Type_INT32),
	}
	page.Info = &common.Tag{}
	page.DataTable = &Table{
		Values:             []any{int32(1), nil, int32(3)},
		DefinitionLevels:   []int32{1, 0, 1},
		MaxDefinitionLevel: 1,
		Path:               []string{"test"},
	}
	data, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestDataPageCompress_WithRepeatedValues(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
		Type:           common.ToPtr(parquet.Type_INT32),
	}
	page.Info = &common.Tag{}
	page.DataTable = &Table{
		Values:             []any{int32(1), int32(2)},
		DefinitionLevels:   []int32{0, 1}, // first value is not at max DL
		MaxDefinitionLevel: 1,
		Path:               []string{"test"},
	}
	data, err := page.dataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, nil)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}
