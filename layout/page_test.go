package layout

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/encoding"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
)

func Test_DataPageCompressWithStatistics(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
		Name: "test_col",
	}
	page.Info = common.NewTag()
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

	data, err := page.DataPageCompress(parquet.CompressionCodec_SNAPPY)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Verify that statistics were set
	require.NotNil(t, page.Header.DataPageHeader.Statistics)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.Max)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.Min)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.NullCount)
}

func Test_DataPageV2CompressWithComplexData(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT64),
		Name: "test_col",
	}
	page.Info = common.NewTag()
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

	data, err := page.DataPageV2Compress(parquet.CompressionCodec_GZIP)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Verify header values for v2
	require.Equal(t, parquet.PageType_DATA_PAGE_V2, page.Header.Type)
	require.NotZero(t, page.Header.DataPageHeaderV2.NumRows)
	require.NotNil(t, page.Header.DataPageHeaderV2.Statistics)
}

func Test_GetRLDLFromRawData(t *testing.T) {
	// Create schema handler
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(2)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "required_field",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "optional_field",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)

	tests := []struct {
		name           string
		setupPage      func() *Page
		expectError    bool
		expectedValues int64
		expectedRows   int64
		errorMessage   string
	}{
		{
			name: "data_page_v2_required_field",
			setupPage: func() *Page {
				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE_V2
				page.Header.DataPageHeaderV2 = &parquet.DataPageHeaderV2{
					NumValues:                  2,
					NumNulls:                   0,
					NumRows:                    2,
					Encoding:                   parquet.Encoding_PLAIN,
					DefinitionLevelsByteLength: 0,
					RepetitionLevelsByteLength: 0,
				}
				page.Path = []string{"parquet_go_root", "required_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				// Create raw data for DATA_PAGE_V2 (empty RL and DL, then data)
				page.RawData = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
				return page
			},
			expectError:    false,
			expectedValues: 2,
			expectedRows:   2,
		},
		{
			name: "data_page_v2_with_repetition_and_definition_levels",
			setupPage: func() *Page {
				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE_V2
				page.Header.DataPageHeaderV2 = &parquet.DataPageHeaderV2{
					NumValues:                  3,
					NumNulls:                   0,
					NumRows:                    3,
					Encoding:                   parquet.Encoding_PLAIN,
					DefinitionLevelsByteLength: 4,
					RepetitionLevelsByteLength: 4,
				}
				page.Path = []string{"parquet_go_root", "optional_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				// Create raw data: RL (4 bytes) + DL (4 bytes) + data (4 bytes)
				page.RawData = []byte{
					0x01, 0x02, 0x03, 0x04, // repetition levels
					0x05, 0x06, 0x07, 0x08, // definition levels
					0x09, 0x0A, 0x0B, 0x0C, // data
				}
				return page
			},
			expectError:    false,
			expectedValues: 3,
			expectedRows:   3,
		},
		{
			name: "data_page_v2_with_levels",
			setupPage: func() *Page {
				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE_V2
				page.Header.DataPageHeaderV2 = &parquet.DataPageHeaderV2{
					NumValues:                  1,
					NumNulls:                   0,
					NumRows:                    1,
					Encoding:                   parquet.Encoding_PLAIN,
					DefinitionLevelsByteLength: 1,
					RepetitionLevelsByteLength: 2,
				}
				page.Path = []string{"parquet_go_root", "required_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				// Provide exactly enough data: 2 bytes RL + 1 byte DL + 1 byte data = 4 bytes
				page.RawData = []byte{0x01, 0x02, 0x03, 0x04}
				return page
			},
			expectError:    false,
			expectedValues: 1,
			expectedRows:   1,
		},
		{
			name: "data_page_uncompressed",
			setupPage: func() *Page {
				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE
				page.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues:               2,
					Encoding:                parquet.Encoding_PLAIN,
					DefinitionLevelEncoding: parquet.Encoding_RLE,
					RepetitionLevelEncoding: parquet.Encoding_RLE,
				}
				page.Path = []string{"parquet_go_root", "required_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				// Simple data that should pass through uncompressed
				page.RawData = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
				return page
			},
			expectError:    false,
			expectedValues: 2,
			expectedRows:   2,
		},
		{
			name: "dictionary_page",
			setupPage: func() *Page {
				page := NewDictPage()
				page.Header.Type = parquet.PageType_DICTIONARY_PAGE
				page.Header.DictionaryPageHeader = &parquet.DictionaryPageHeader{
					NumValues: 2,
					Encoding:  parquet.Encoding_PLAIN,
				}
				page.Path = []string{"parquet_go_root", "required_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				page.RawData = []byte{0x01, 0x02, 0x03, 0x04}
				return page
			},
			expectError:    false,
			expectedValues: 0,
			expectedRows:   0,
		},
		{
			name: "unsupported_page_type",
			setupPage: func() *Page {
				page := NewPage()
				page.Header.Type = parquet.PageType_INDEX_PAGE
				page.Path = []string{"parquet_go_root", "required_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				page.RawData = []byte{0x01, 0x02, 0x03, 0x04}
				return page
			},
			expectError:  true,
			errorMessage: "unsupported page type",
		},
		{
			name: "data_page_compressed_invalid",
			setupPage: func() *Page {
				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE
				page.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues:               1,
					Encoding:                parquet.Encoding_PLAIN,
					DefinitionLevelEncoding: parquet.Encoding_RLE,
					RepetitionLevelEncoding: parquet.Encoding_RLE,
				}
				page.Path = []string{"parquet_go_root", "required_field"}
				page.CompressType = parquet.CompressionCodec_SNAPPY // Use compression with invalid data

				// Invalid compressed data
				page.RawData = []byte{0xFF, 0xFF, 0xFF, 0xFF}
				return page
			},
			expectError:  true,
			errorMessage: "unsupported compress method",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := tt.setupPage()
			numValues, numRows, err := page.GetRLDLFromRawData(schemaHandler)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMessage != "" {
					require.Contains(t, err.Error(), tt.errorMessage)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expectedValues, numValues)
			require.Equal(t, tt.expectedRows, numRows)

			// Verify page was properly modified
			require.NotNil(t, page.DataTable)
		})
	}
}

func Test_GetRLDLFromRawDataComplexScenarios(t *testing.T) {
	// Create schema handler with nested/repeated fields
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(3)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "required_field",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "optional_field",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
		},
		{
			Name:           "repeated_field",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)

	tests := []struct {
		name        string
		setupPage   func() *Page
		expectError bool
	}{
		{
			name: "optional_field_with_nulls",
			setupPage: func() *Page {
				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE
				page.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues:               3,
					Encoding:                parquet.Encoding_PLAIN,
					DefinitionLevelEncoding: parquet.Encoding_RLE,
					RepetitionLevelEncoding: parquet.Encoding_RLE,
				}
				page.Path = []string{"parquet_go_root", "optional_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				// Create RLE encoded data for definition levels (some nulls)
				// Format: bit_width(1) + length(4) + data
				page.RawData = []byte{
					0x01, 0x03, 0x00, 0x00, 0x00, // Definition levels: 3 values with bit width 1
					0x01, 0x03, 0x00, 0x00, 0x00, // Repetition levels: 3 values with bit width 1
					0x01, 0x02, 0x03, // Data values
				}
				return page
			},
			expectError: false,
		},
		{
			name: "repeated_field_with_multiple_repetition_levels",
			setupPage: func() *Page {
				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE
				page.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues:               4,
					Encoding:                parquet.Encoding_PLAIN,
					DefinitionLevelEncoding: parquet.Encoding_RLE,
					RepetitionLevelEncoding: parquet.Encoding_RLE,
				}
				page.Path = []string{"parquet_go_root", "repeated_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				// RLE encoded repetition and definition levels
				page.RawData = []byte{
					0x01, 0x04, 0x00, 0x00, 0x00, 0xAA, // Repetition levels: alternating 0,1,0,1
					0x01, 0x04, 0x00, 0x00, 0x00, 0xFF, // Definition levels: all 1s
					0x01, 0x02, 0x03, 0x04, // Data values
				}
				return page
			},
			expectError: false,
		},
		{
			name: "data_page_v2_with_empty_levels",
			setupPage: func() *Page {
				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE_V2
				page.Header.DataPageHeaderV2 = &parquet.DataPageHeaderV2{
					NumValues:                  2,
					NumNulls:                   0,
					NumRows:                    2,
					Encoding:                   parquet.Encoding_PLAIN,
					DefinitionLevelsByteLength: 0, // No definition levels
					RepetitionLevelsByteLength: 0, // No repetition levels
				}
				page.Path = []string{"parquet_go_root", "required_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				// Only data, no level data
				page.RawData = []byte{0x01, 0x02, 0x03, 0x04}
				return page
			},
			expectError: false,
		},
		{
			name: "data_page_with_truncated_levels",
			setupPage: func() *Page {
				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE
				page.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues:               10, // More values than actual data
					Encoding:                parquet.Encoding_PLAIN,
					DefinitionLevelEncoding: parquet.Encoding_RLE,
					RepetitionLevelEncoding: parquet.Encoding_RLE,
				}
				page.Path = []string{"parquet_go_root", "optional_field"}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED

				// RLE data that would provide more values than numValues
				page.RawData = []byte{
					0x01, 0x05, 0x00, 0x00, 0x00, 0xFF, // Definition levels: 5 values
					0x01, 0x05, 0x00, 0x00, 0x00, 0x00, // Repetition levels: 5 values
					0x01, 0x02, 0x03, 0x04, 0x05, // Data values
				}
				return page
			},
			expectError: false, // Should truncate to numValues
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := tt.setupPage()
			numValues, numRows, err := page.GetRLDLFromRawData(schemaHandler)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Basic validation
			require.GreaterOrEqual(t, numValues, int64(0))
			require.GreaterOrEqual(t, numRows, int64(0))

			// Verify DataTable was created
			require.NotNil(t, page.DataTable)

			// Verify table structure
			table := page.DataTable
			require.NotNil(t, table.Path)
			require.Len(t, table.DefinitionLevels, len(table.Values))
			require.Len(t, table.RepetitionLevels, len(table.Values))

			// Verify numRows calculation (count where RepetitionLevel == 0)
			actualNumRows := int64(0)
			for _, rl := range table.RepetitionLevels {
				if rl == 0 {
					actualNumRows++
				}
			}
			require.Equal(t, actualNumRows, numRows)
		})
	}
}

func Test_NewDataPage(t *testing.T) {
	page := NewDataPage()
	require.NotNil(t, page)
	require.NotNil(t, page.Header)
	require.Equal(t, parquet.PageType_DATA_PAGE, page.Header.Type)
	require.NotNil(t, page.Info)
}

func Test_NewDictPage(t *testing.T) {
	page := NewDictPage()
	require.NotNil(t, page)
	require.NotNil(t, page.Header)
	require.NotNil(t, page.Header.DictionaryPageHeader)
	require.NotNil(t, page.Info)
}

func Test_NewPage(t *testing.T) {
	page := NewPage()
	require.NotNil(t, page)
	require.NotNil(t, page.Header)
	require.NotNil(t, page.Info)
	require.Nil(t, page.DataTable)
}

func Test_PageDataPageCompress(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	// Set up a data table
	page.DataTable = &Table{
		Values:           []any{int32(1), int32(2)},
		DefinitionLevels: []int32{0, 0},
		RepetitionLevels: []int32{0, 0},
	}

	data, err := page.DataPageCompress(parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func Test_PageDataPageV2Compress(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	// Set up a data table
	page.DataTable = &Table{
		Values:           []any{int32(1), int32(2)},
		DefinitionLevels: []int32{0, 0},
		RepetitionLevels: []int32{0, 0},
	}

	data, err := page.DataPageV2Compress(parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func Test_PageDecode(t *testing.T) {
	testCases := []struct {
		name     string
		page     *Page
		dictPage *Page
		expected bool // whether decode should happen
	}{
		{
			name:     "nil_page",
			page:     nil,
			dictPage: NewDictPage(),
			expected: false,
		},
		{
			name:     "nil_dict_page",
			page:     NewDataPage(),
			dictPage: nil,
			expected: false,
		},
		{
			name: "page_without_headers",
			page: &Page{
				Header: &parquet.PageHeader{},
			},
			dictPage: NewDictPage(),
			expected: false,
		},
		{
			name: "data_page_with_rle_dictionary",
			page: &Page{
				Header: &parquet.PageHeader{
					DataPageHeader: &parquet.DataPageHeader{
						Encoding: parquet.Encoding_RLE_DICTIONARY,
					},
				},
				DataTable: &Table{
					Values: []any{int64(0), int64(1), int64(2)},
				},
			},
			dictPage: &Page{
				DataTable: &Table{
					Values: []any{"hello", "world", "test"},
				},
			},
			expected: true,
		},
		{
			name: "data_page_with_plain_dictionary",
			page: &Page{
				Header: &parquet.PageHeader{
					DataPageHeader: &parquet.DataPageHeader{
						Encoding: parquet.Encoding_PLAIN_DICTIONARY,
					},
				},
				DataTable: &Table{
					Values: []any{int64(0), int64(1)},
				},
			},
			dictPage: &Page{
				DataTable: &Table{
					Values: []any{"apple", "banana"},
				},
			},
			expected: true,
		},
		{
			name: "data_page_v2_with_rle_dictionary",
			page: &Page{
				Header: &parquet.PageHeader{
					DataPageHeaderV2: &parquet.DataPageHeaderV2{
						Encoding: parquet.Encoding_RLE_DICTIONARY,
					},
				},
				DataTable: &Table{
					Values: []any{int64(1), int64(0)},
				},
			},
			dictPage: &Page{
				DataTable: &Table{
					Values: []any{"first", "second"},
				},
			},
			expected: true,
		},
		{
			name: "data_page_with_plain_encoding",
			page: &Page{
				Header: &parquet.PageHeader{
					DataPageHeader: &parquet.DataPageHeader{
						Encoding: parquet.Encoding_PLAIN,
					},
				},
				DataTable: &Table{
					Values: []any{int32(1), int32(2)},
				},
			},
			dictPage: &Page{
				DataTable: &Table{
					Values: []any{"ignored"},
				},
			},
			expected: false,
		},
		{
			name: "page_with_null_values",
			page: &Page{
				Header: &parquet.PageHeader{
					DataPageHeader: &parquet.DataPageHeader{
						Encoding: parquet.Encoding_RLE_DICTIONARY,
					},
				},
				DataTable: &Table{
					Values: []any{int64(0), nil, int64(1)},
				},
			},
			dictPage: &Page{
				DataTable: &Table{
					Values: []any{"value1", "value2"},
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			originalValues := make([]any, 0)
			if tc.page != nil && tc.page.DataTable != nil {
				originalValues = make([]any, len(tc.page.DataTable.Values))
				copy(originalValues, tc.page.DataTable.Values)
			}

			tc.page.Decode(tc.dictPage)

			if tc.expected && tc.page != nil && tc.page.DataTable != nil {
				// Verify that dictionary lookup occurred for non-nil values
				for i, val := range tc.page.DataTable.Values {
					if originalValues[i] != nil {
						if index, ok := originalValues[i].(int64); ok {
							expectedVal := tc.dictPage.DataTable.Values[index]
							require.Equal(t, expectedVal, val)
						}
					} else {
						// Null values should remain null
						require.Nil(t, val)
					}
				}
			}
		})
	}
}

func Test_PageEncodingValues(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	values := []any{int32(1), int32(2), int32(3)}
	data, err := page.EncodingValues(values)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func Test_PageEncodingValuesErrorCases(t *testing.T) {
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
			page.Info = common.NewTag()
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

func Test_PageEncodingValuesWithDifferentEncodings(t *testing.T) {
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
			page.Info = common.NewTag()
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

func Test_PageEncodingValuesWithEmptyValues(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	values := []any{}
	data, err := page.EncodingValues(values)
	require.NoError(t, err)
	// Empty values should produce empty data
	require.Empty(t, data)
}

func Test_Page_GetValueFromRawData(t *testing.T) {
	tests := []struct {
		name        string
		setupPage   func() *Page
		setupSchema func() *schema.SchemaHandler
		expectError bool
	}{
		{
			name: "dictionary_page_valid",
			setupPage: func() *Page {
				page := NewPage()
				page.Header = &parquet.PageHeader{
					Type:                 parquet.PageType_DICTIONARY_PAGE,
					UncompressedPageSize: 20,
					CompressedPageSize:   20,
					DictionaryPageHeader: &parquet.DictionaryPageHeader{
						NumValues: int32(2),
						Encoding:  parquet.Encoding_PLAIN,
					},
				}
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				}
				page.DataTable = &Table{}
				// Two int32 values: 1, 2
				page.RawData = []byte{
					0x01, 0x00, 0x00, 0x00, // int32(1)
					0x02, 0x00, 0x00, 0x00, // int32(2)
				}
				return page
			},
			setupSchema: func() *schema.SchemaHandler {
				return &schema.SchemaHandler{}
			},
			expectError: false,
		},
		{
			name: "unsupported_page_type",
			setupPage: func() *Page {
				page := NewPage()
				page.Header = &parquet.PageHeader{
					Type: parquet.PageType_INDEX_PAGE, // Unsupported type
				}
				return page
			},
			setupSchema: func() *schema.SchemaHandler {
				return &schema.SchemaHandler{}
			},
			expectError: true,
		},
		{
			name: "dictionary_page_invalid_data",
			setupPage: func() *Page {
				page := NewPage()
				page.Header = &parquet.PageHeader{
					Type:                 parquet.PageType_DICTIONARY_PAGE,
					UncompressedPageSize: 20,
					CompressedPageSize:   20,
					DictionaryPageHeader: &parquet.DictionaryPageHeader{
						NumValues: int32(2),
						Encoding:  parquet.Encoding_PLAIN,
					},
				}
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				}
				page.DataTable = &Table{}
				// Invalid data - not enough bytes
				page.RawData = []byte{0x01, 0x00} // Incomplete int32
				return page
			},
			setupSchema: func() *schema.SchemaHandler {
				return &schema.SchemaHandler{}
			},
			expectError: true,
		},
		{
			name: "data_page_plain_encoding",
			setupPage: func() *Page {
				page := NewPage()
				page.Header = &parquet.PageHeader{
					Type:                 parquet.PageType_DATA_PAGE,
					UncompressedPageSize: 12,
					CompressedPageSize:   12,
					DataPageHeader: &parquet.DataPageHeader{
						NumValues:               int32(3),
						Encoding:                parquet.Encoding_PLAIN,
						DefinitionLevelEncoding: parquet.Encoding_RLE,
						RepetitionLevelEncoding: parquet.Encoding_RLE,
					},
				}
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				}
				page.DataTable = &Table{
					Path:               []string{"test_field"},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
					DefinitionLevels:   []int32{1, 1, 1},
					RepetitionLevels:   []int32{0, 0, 0},
					Values:             make([]any, 3),
				}
				// Three int32 values: 10, 20, 30
				page.RawData = []byte{
					0x0A, 0x00, 0x00, 0x00, // int32(10)
					0x14, 0x00, 0x00, 0x00, // int32(20)
					0x1E, 0x00, 0x00, 0x00, // int32(30)
				}
				return page
			},
			setupSchema: func() *schema.SchemaHandler {
				schemaHandler := &schema.SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name: "test_field",
							Type: common.ToPtr(parquet.Type_INT32),
						},
					},
					MapIndex: map[string]int32{
						"test_field": 0,
					},
				}
				return schemaHandler
			},
			expectError: false,
		},
		{
			name: "data_page_with_nulls",
			setupPage: func() *Page {
				page := NewPage()
				page.Header = &parquet.PageHeader{
					Type:                 parquet.PageType_DATA_PAGE,
					UncompressedPageSize: 8,
					CompressedPageSize:   8,
					DataPageHeader: &parquet.DataPageHeader{
						NumValues:               int32(3),
						Encoding:                parquet.Encoding_PLAIN,
						DefinitionLevelEncoding: parquet.Encoding_RLE,
						RepetitionLevelEncoding: parquet.Encoding_RLE,
					},
				}
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				}
				page.DataTable = &Table{
					Path:               []string{"nullable_field"},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
					DefinitionLevels:   []int32{1, 0, 1}, // Second value is null
					RepetitionLevels:   []int32{0, 0, 0},
					Values:             make([]any, 3),
				}
				// Two int32 values (third is null): 100, 200
				page.RawData = []byte{
					0x64, 0x00, 0x00, 0x00, // int32(100)
					0xC8, 0x00, 0x00, 0x00, // int32(200)
				}
				return page
			},
			setupSchema: func() *schema.SchemaHandler {
				schemaHandler := &schema.SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name: "nullable_field",
							Type: common.ToPtr(parquet.Type_INT32),
						},
					},
					MapIndex: map[string]int32{
						"nullable_field": 0,
					},
				}
				return schemaHandler
			},
			expectError: false,
		},
		{
			name: "data_page_with_converted_type",
			setupPage: func() *Page {
				page := NewPage()
				page.Header = &parquet.PageHeader{
					Type:                 parquet.PageType_DATA_PAGE,
					UncompressedPageSize: 8,
					CompressedPageSize:   8,
					DataPageHeader: &parquet.DataPageHeader{
						NumValues:               int32(2),
						Encoding:                parquet.Encoding_PLAIN,
						DefinitionLevelEncoding: parquet.Encoding_RLE,
						RepetitionLevelEncoding: parquet.Encoding_RLE,
					},
				}
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				}
				page.DataTable = &Table{
					Path:               []string{"timestamp_field"},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
					DefinitionLevels:   []int32{1, 1},
					RepetitionLevels:   []int32{0, 0},
					Values:             make([]any, 2),
				}
				// Two int32 timestamp values
				page.RawData = []byte{
					0x10, 0x27, 0x00, 0x00, // int32(10000)
					0x20, 0x4E, 0x00, 0x00, // int32(20000)
				}
				return page
			},
			setupSchema: func() *schema.SchemaHandler {
				schemaHandler := &schema.SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:          "timestamp_field",
							Type:          common.ToPtr(parquet.Type_INT32),
							ConvertedType: common.ToPtr(parquet.ConvertedType_TIME_MILLIS),
						},
					},
					MapIndex: map[string]int32{
						"timestamp_field": 0,
					},
				}
				return schemaHandler
			},
			expectError: false,
		},
		{
			name: "data_page_v2_compressed",
			setupPage: func() *Page {
				page := NewPage()
				page.Header = &parquet.PageHeader{
					Type:                 parquet.PageType_DATA_PAGE_V2,
					UncompressedPageSize: 8,
					CompressedPageSize:   8,
					DataPageHeaderV2: &parquet.DataPageHeaderV2{
						NumValues:                  int32(2),
						Encoding:                   parquet.Encoding_PLAIN,
						NumNulls:                   int32(0),
						NumRows:                    int32(2),
						DefinitionLevelsByteLength: 0,
						RepetitionLevelsByteLength: 0,
					},
				}
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				}
				page.DataTable = &Table{
					Path:               []string{"v2_field"},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
					DefinitionLevels:   []int32{1, 1},
					RepetitionLevels:   []int32{0, 0},
					Values:             make([]any, 2),
				}
				page.CompressType = parquet.CompressionCodec_UNCOMPRESSED
				// Two int32 values
				page.RawData = []byte{
					0x2A, 0x00, 0x00, 0x00, // int32(42)
					0x54, 0x00, 0x00, 0x00, // int32(84)
				}
				return page
			},
			setupSchema: func() *schema.SchemaHandler {
				schemaHandler := &schema.SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name: "v2_field",
							Type: common.ToPtr(parquet.Type_INT32),
						},
					},
					MapIndex: map[string]int32{
						"v2_field": 0,
					},
				}
				return schemaHandler
			},
			expectError: false,
		},
		{
			name: "data_page_string_type",
			setupPage: func() *Page {
				page := NewPage()
				page.Header = &parquet.PageHeader{
					Type:                 parquet.PageType_DATA_PAGE,
					UncompressedPageSize: 16,
					CompressedPageSize:   16,
					DataPageHeader: &parquet.DataPageHeader{
						NumValues:               int32(2),
						Encoding:                parquet.Encoding_PLAIN,
						DefinitionLevelEncoding: parquet.Encoding_RLE,
						RepetitionLevelEncoding: parquet.Encoding_RLE,
					},
				}
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
				}
				page.DataTable = &Table{
					Path:               []string{"string_field"},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
					DefinitionLevels:   []int32{1, 1},
					RepetitionLevels:   []int32{0, 0},
					Values:             make([]any, 2),
				}
				// Two byte arrays: "hi", "bye"
				page.RawData = []byte{
					0x02, 0x00, 0x00, 0x00, 'h', 'i', // "hi" (length=2)
					0x03, 0x00, 0x00, 0x00, 'b', 'y', 'e', // "bye" (length=3)
				}
				return page
			},
			setupSchema: func() *schema.SchemaHandler {
				schemaHandler := &schema.SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name: "string_field",
							Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
						},
					},
					MapIndex: map[string]int32{
						"string_field": 0,
					},
				}
				return schemaHandler
			},
			expectError: false,
		},
		{
			name: "data_page_invalid_encoding_error",
			setupPage: func() *Page {
				page := NewPage()
				page.Header = &parquet.PageHeader{
					Type:                 parquet.PageType_DATA_PAGE,
					UncompressedPageSize: 8,
					CompressedPageSize:   8,
					DataPageHeader: &parquet.DataPageHeader{
						NumValues:               int32(2),
						Encoding:                parquet.Encoding_PLAIN,
						DefinitionLevelEncoding: parquet.Encoding_RLE,
						RepetitionLevelEncoding: parquet.Encoding_RLE,
					},
				}
				page.Schema = &parquet.SchemaElement{
					Type: common.ToPtr(parquet.Type_INT32),
				}
				page.DataTable = &Table{
					Path:               []string{"error_field"},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
					DefinitionLevels:   []int32{1, 1},
					RepetitionLevels:   []int32{0, 0},
					Values:             make([]any, 2),
				}
				// Invalid data - not enough bytes for two int32s
				page.RawData = []byte{0x01, 0x02} // Only 2 bytes, need 8
				return page
			},
			setupSchema: func() *schema.SchemaHandler {
				schemaHandler := &schema.SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name: "error_field",
							Type: common.ToPtr(parquet.Type_INT32),
						},
					},
					MapIndex: map[string]int32{
						"error_field": 0,
					},
				}
				return schemaHandler
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := tt.setupPage()
			schemaHandler := tt.setupSchema()

			err := page.GetValueFromRawData(schemaHandler)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, page.DataTable.Values)

				// Additional verification for successful cases
				if page.Header.GetType() == parquet.PageType_DATA_PAGE ||
					page.Header.GetType() == parquet.PageType_DATA_PAGE_V2 {
					// Verify RawData is cleared after processing
					require.Empty(t, page.RawData)

					// Verify values are set correctly for non-null entries
					for i, defLevel := range page.DataTable.DefinitionLevels {
						if defLevel == page.DataTable.MaxDefinitionLevel {
							require.NotNil(t, page.DataTable.Values[i])
						}
					}
				}
			}
		})
	}
}

func Test_ReadDataPageValues(t *testing.T) {
	testCases := []struct {
		name           string
		encodingMethod parquet.Encoding
		dataType       parquet.Type
		convertedType  parquet.ConvertedType
		cnt            uint64
		bitWidth       uint64
		setupData      func() []byte
		expectError    bool
	}{
		{
			name:           "zero_count",
			encodingMethod: parquet.Encoding_PLAIN,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            0,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			expectError:    false,
		},
		{
			name:           "bit_packed_deprecated",
			encodingMethod: parquet.Encoding_BIT_PACKED,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       4,
			setupData:      func() []byte { return []byte{0x01} },
			expectError:    true,
		},
		{
			name:           "unknown_encoding",
			encodingMethod: parquet.Encoding(-1),
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			expectError:    true,
		},
		{
			name:           "delta_binary_packed_int64",
			encodingMethod: parquet.Encoding_DELTA_BINARY_PACKED,
			dataType:       parquet.Type_INT64,
			convertedType:  -1,
			cnt:            3,
			bitWidth:       0,
			setupData: func() []byte {
				// Create delta binary packed data for [1, 2, 3]
				values := []any{int64(1), int64(2), int64(3)}
				return encoding.WriteDeltaINT64(values)
			},
			expectError: false,
		},
		{
			name:           "delta_binary_packed_unsupported_type",
			encodingMethod: parquet.Encoding_DELTA_BINARY_PACKED,
			dataType:       parquet.Type_FLOAT,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			expectError:    true,
		},
		{
			name:           "byte_stream_split_unsupported_type",
			encodingMethod: parquet.Encoding_BYTE_STREAM_SPLIT,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			expectError:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			bytesReader := bytes.NewReader(data)

			result, err := ReadDataPageValues(bytesReader, tc.encodingMethod, tc.dataType, tc.convertedType, tc.cnt, tc.bitWidth)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.cnt == 0 {
					require.Empty(t, result)
				}
			}
		})
	}
}

func Test_ReadDataPageValuesMoreCases(t *testing.T) {
	testCases := []struct {
		name           string
		encodingMethod parquet.Encoding
		dataType       parquet.Type
		convertedType  parquet.ConvertedType
		cnt            uint64
		bitWidth       uint64
		setupData      func() []byte
		expectError    bool
	}{
		{
			name:           "plain_int32",
			encodingMethod: parquet.Encoding_PLAIN,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{int32(42), int32(100)}
				data, _ := encoding.WritePlainINT32(values)
				return data
			},
			expectError: false,
		},
		{
			name:           "plain_dictionary_encoding",
			encodingMethod: parquet.Encoding_PLAIN_DICTIONARY,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            3,
			bitWidth:       2,
			setupData: func() []byte {
				// Create RLE bit packed hybrid data with bit width prefix
				indices := []any{int64(0), int64(1), int64(0)}
				data, _ := encoding.WriteRLEBitPackedHybrid(indices, 2, parquet.Type_INT64)
				// Prepend bit width byte
				result := make([]byte, 1+len(data))
				result[0] = 2 // bit width
				copy(result[1:], data)
				return result
			},
			expectError: false,
		},
		{
			name:           "rle_dictionary_encoding",
			encodingMethod: parquet.Encoding_RLE_DICTIONARY,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       1,
			setupData: func() []byte {
				indices := []any{int64(0), int64(1)}
				data, _ := encoding.WriteRLEBitPackedHybrid(indices, 1, parquet.Type_INT64)
				result := make([]byte, 1+len(data))
				result[0] = 1 // bit width
				copy(result[1:], data)
				return result
			},
			expectError: false,
		},
		{
			name:           "rle_encoding_int32",
			encodingMethod: parquet.Encoding_RLE,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            3,
			bitWidth:       2,
			setupData: func() []byte {
				values := []any{int64(1), int64(1), int64(2)}
				data, _ := encoding.WriteRLEBitPackedHybrid(values, 2, parquet.Type_INT64)
				return data
			},
			expectError: false,
		},
		{
			name:           "delta_binary_packed_int32",
			encodingMethod: parquet.Encoding_DELTA_BINARY_PACKED,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            4,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{int32(10), int32(15), int32(20), int32(25)}
				return encoding.WriteDeltaINT32(values)
			},
			expectError: false,
		},
		{
			name:           "delta_length_byte_array",
			encodingMethod: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			dataType:       parquet.Type_BYTE_ARRAY,
			convertedType:  -1,
			cnt:            3,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{"hello", "world", "test"}
				return encoding.WriteDeltaLengthByteArray(values)
			},
			expectError: false,
		},
		{
			name:           "delta_length_byte_array_fixed_len",
			encodingMethod: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			dataType:       parquet.Type_FIXED_LEN_BYTE_ARRAY,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       5,
			setupData: func() []byte {
				values := []any{"hello", "world"}
				return encoding.WriteDeltaLengthByteArray(values)
			},
			expectError: false,
		},
		{
			name:           "delta_byte_array",
			encodingMethod: parquet.Encoding_DELTA_BYTE_ARRAY,
			dataType:       parquet.Type_BYTE_ARRAY,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{"apple", "application"}
				return encoding.WriteDeltaByteArray(values)
			},
			expectError: false,
		},
		{
			name:           "delta_byte_array_fixed_len",
			encodingMethod: parquet.Encoding_DELTA_BYTE_ARRAY,
			dataType:       parquet.Type_FIXED_LEN_BYTE_ARRAY,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       4,
			setupData: func() []byte {
				values := []any{"test", "data"}
				return encoding.WriteDeltaByteArray(values)
			},
			expectError: false,
		},
		{
			name:           "byte_stream_split_float",
			encodingMethod: parquet.Encoding_BYTE_STREAM_SPLIT,
			dataType:       parquet.Type_FLOAT,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{float32(1.5), float32(2.5)}
				return encoding.WriteByteStreamSplitFloat32(values)
			},
			expectError: false,
		},
		{
			name:           "byte_stream_split_double",
			encodingMethod: parquet.Encoding_BYTE_STREAM_SPLIT,
			dataType:       parquet.Type_DOUBLE,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{float64(3.14), float64(2.71)}
				return encoding.WriteByteStreamSplitFloat64(values)
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			bytesReader := bytes.NewReader(data)

			result, err := ReadDataPageValues(bytesReader, tc.encodingMethod, tc.dataType, tc.convertedType, tc.cnt, tc.bitWidth)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.cnt > 0 {
					require.NotEmpty(t, result)
				}
			}
		})
	}
}

func Test_ReadPage(t *testing.T) {
	// Create schema handler
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "test_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)

	// Create column metadata
	colMetaData := &parquet.ColumnMetaData{
		Type:         parquet.Type_INT32,
		Codec:        parquet.CompressionCodec_UNCOMPRESSED,
		PathInSchema: []string{"test_col"},
	}

	// Helper function to create a valid dictionary page
	createDictionaryPageData := func() []byte {
		header := &parquet.PageHeader{
			Type:                 parquet.PageType_DICTIONARY_PAGE,
			CompressedPageSize:   8,
			UncompressedPageSize: 8,
			DictionaryPageHeader: &parquet.DictionaryPageHeader{
				NumValues: 2,
				Encoding:  parquet.Encoding_PLAIN,
			},
		}

		var buf bytes.Buffer
		transport := thrift.NewTMemoryBufferLen(1024)
		protocol := thrift.NewTCompactProtocolConf(transport, nil)
		if err := header.Write(context.Background(), protocol); err != nil {
			require.NoError(t, err)
		}
		if err := protocol.Flush(context.Background()); err != nil {
			require.NoError(t, err)
		}
		headerData := transport.Buffer.Bytes()
		buf.Write(headerData)

		// Add dictionary values (2 INT32 values: 100, 200)
		dictData := []byte{0x64, 0x00, 0x00, 0x00, 0xC8, 0x00, 0x00, 0x00}
		buf.Write(dictData)

		return buf.Bytes()
	}

	// Helper function to create a valid data page
	createDataPageData := func() []byte {
		header := &parquet.PageHeader{
			Type:                 parquet.PageType_DATA_PAGE,
			CompressedPageSize:   12,
			UncompressedPageSize: 12,
			DataPageHeader: &parquet.DataPageHeader{
				NumValues:               2,
				Encoding:                parquet.Encoding_PLAIN,
				DefinitionLevelEncoding: parquet.Encoding_RLE,
				RepetitionLevelEncoding: parquet.Encoding_RLE,
			},
		}

		var buf bytes.Buffer
		transport := thrift.NewTMemoryBufferLen(1024)
		protocol := thrift.NewTCompactProtocolConf(transport, nil)
		if err := header.Write(context.Background(), protocol); err != nil {
			require.NoError(t, err)
		}
		if err := protocol.Flush(context.Background()); err != nil {
			require.NoError(t, err)
		}
		headerData := transport.Buffer.Bytes()
		buf.Write(headerData)

		// Add page data (2 INT32 values: 42, 84)
		pageData := []byte{0x2A, 0x00, 0x00, 0x00, 0x54, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		buf.Write(pageData)

		return buf.Bytes()
	}

	// Helper function to create a data page v2
	createDataPageV2Data := func() []byte {
		header := &parquet.PageHeader{
			Type:                 parquet.PageType_DATA_PAGE_V2,
			CompressedPageSize:   8,
			UncompressedPageSize: 8,
			DataPageHeaderV2: &parquet.DataPageHeaderV2{
				NumValues:                  2,
				NumNulls:                   0,
				NumRows:                    2,
				Encoding:                   parquet.Encoding_PLAIN,
				DefinitionLevelsByteLength: 0,
				RepetitionLevelsByteLength: 0,
				IsCompressed:               false,
			},
		}

		var buf bytes.Buffer
		transport := thrift.NewTMemoryBufferLen(1024)
		protocol := thrift.NewTCompactProtocolConf(transport, nil)
		if err := header.Write(context.Background(), protocol); err != nil {
			require.NoError(t, err)
		}
		if err := protocol.Flush(context.Background()); err != nil {
			require.NoError(t, err)
		}
		headerData := transport.Buffer.Bytes()
		buf.Write(headerData)

		// Add page data (2 INT32 values: 123, 456)
		pageData := []byte{0x7B, 0x00, 0x00, 0x00, 0xC8, 0x01, 0x00, 0x00}
		buf.Write(pageData)

		return buf.Bytes()
	}

	tests := []struct {
		name         string
		setupData    func() *thrift.TBufferedTransport
		expectError  bool
		expectedType string
	}{
		{
			name: "empty buffer",
			setupData: func() *thrift.TBufferedTransport {
				return thrift.NewTBufferedTransport(thrift.NewTMemoryBuffer(), 1024)
			},
			expectError: true,
		},
		{
			name: "invalid data",
			setupData: func() *thrift.TBufferedTransport {
				mem := thrift.NewTMemoryBuffer()
				mem.Write([]byte{0x01, 0x02, 0x03})
				return thrift.NewTBufferedTransport(mem, 1024)
			},
			expectError: true,
		},
		{
			name: "dictionary page",
			setupData: func() *thrift.TBufferedTransport {
				data := createDictionaryPageData()
				mem := thrift.NewTMemoryBuffer()
				mem.Write(data)
				return thrift.NewTBufferedTransport(mem, 1024)
			},
			expectError:  false,
			expectedType: "DICT",
		},
		{
			name: "data page",
			setupData: func() *thrift.TBufferedTransport {
				data := createDataPageData()
				mem := thrift.NewTMemoryBuffer()
				mem.Write(data)
				return thrift.NewTBufferedTransport(mem, 1024)
			},
			expectError:  false,
			expectedType: "DATA",
		},
		{
			name: "data page v2",
			setupData: func() *thrift.TBufferedTransport {
				data := createDataPageV2Data()
				mem := thrift.NewTMemoryBuffer()
				mem.Write(data)
				return thrift.NewTBufferedTransport(mem, 1024)
			},
			expectError:  false,
			expectedType: "DATA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			thriftReader := tt.setupData()
			page, _, _, err := ReadPage(thriftReader, schemaHandler, colMetaData)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, page)

			// Verify page type by checking header
			if tt.expectedType == "DICT" {
				require.Equal(t, parquet.PageType_DICTIONARY_PAGE, page.Header.GetType())
			}
			if tt.expectedType == "DATA" {
				require.Contains(t, []parquet.PageType{parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2}, page.Header.GetType())
			}

			// Basic validation
			require.NotNil(t, page.Header)
		})
	}
}

func Test_ReadPage2(t *testing.T) {
	// Create schema handler
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "test_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)

	testCases := []struct {
		name        string
		setupData   func() []byte
		colMetadata *parquet.ColumnMetaData
		expectError bool
		expectPanic bool
	}{
		{
			name: "dictionary_page",
			setupData: func() []byte {
				// Create dictionary page header
				header := &parquet.PageHeader{
					Type:                 parquet.PageType_DICTIONARY_PAGE,
					UncompressedPageSize: 8,
					CompressedPageSize:   8,
					DictionaryPageHeader: &parquet.DictionaryPageHeader{
						NumValues: int32(2),
						Encoding:  parquet.Encoding_PLAIN,
					},
				}

				// Serialize header
				transport := thrift.NewTMemoryBufferLen(1024)
				protocol := thrift.NewTCompactProtocolConf(transport, nil)
				if err := header.Write(context.Background(), protocol); err != nil {
					return nil
				}
				if err := protocol.Flush(context.Background()); err != nil {
					return nil
				}

				headerBytes := transport.Buffer.Bytes()

				// Create dictionary data
				dictValues := []any{int32(100), int32(200)}
				dictData, _ := encoding.WritePlainINT32(dictValues)

				// Combine header and data
				result := make([]byte, len(headerBytes)+len(dictData))
				copy(result, headerBytes)
				copy(result[len(headerBytes):], dictData)
				return result
			},
			colMetadata: &parquet.ColumnMetaData{
				Type:         parquet.Type_INT32,
				Encodings:    []parquet.Encoding{parquet.Encoding_PLAIN},
				PathInSchema: []string{"test_col"},
				Codec:        parquet.CompressionCodec_UNCOMPRESSED,
			},
			expectError: true,
		},
		{
			name: "empty_data",
			setupData: func() []byte {
				return []byte{}
			},
			colMetadata: &parquet.ColumnMetaData{
				Type:         parquet.Type_INT32,
				Encodings:    []parquet.Encoding{parquet.Encoding_PLAIN},
				PathInSchema: []string{"test_col"},
				Codec:        parquet.CompressionCodec_UNCOMPRESSED,
			},
			expectError: true,
			expectPanic: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			transport := thrift.NewTMemoryBufferLen(len(data))
			transport.Buffer.Write(data)
			bufferedTransport := thrift.NewTBufferedTransport(transport, 1024)

			page, numValues, numRows, err := ReadPage2(bufferedTransport, schemaHandler, tc.colMetadata)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, page)
				require.GreaterOrEqual(t, numValues, int64(0))
				require.GreaterOrEqual(t, numRows, int64(0))
			}
		})
	}
}

func Test_ReadPageErrorCases(t *testing.T) {
	// Create schema handler
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "test_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)

	// Create column metadata with compression
	colMetaDataCompressed := &parquet.ColumnMetaData{
		Type:         parquet.Type_INT32,
		Codec:        parquet.CompressionCodec_UNCOMPRESSED,
		PathInSchema: []string{"test_col"},
	}

	// Helper function to create invalid page with index type
	createIndexPageData := func() []byte {
		header := &parquet.PageHeader{
			Type:                 parquet.PageType_INDEX_PAGE,
			CompressedPageSize:   4,
			UncompressedPageSize: 4,
		}

		var buf bytes.Buffer
		transport := thrift.NewTMemoryBufferLen(1024)
		protocol := thrift.NewTCompactProtocolConf(transport, nil)
		if err := header.Write(context.Background(), protocol); err != nil {
			require.NoError(t, err)
		}
		if err := protocol.Flush(context.Background()); err != nil {
			require.NoError(t, err)
		}
		headerData := transport.Buffer.Bytes()
		buf.Write(headerData)

		buf.Write([]byte{0x00, 0x00, 0x00, 0x00})
		return buf.Bytes()
	}

	// Helper function to create malformed data page v2
	createMalformedDataPageV2 := func() []byte {
		header := &parquet.PageHeader{
			Type:                 parquet.PageType_DATA_PAGE_V2,
			CompressedPageSize:   20,
			UncompressedPageSize: 20,
			DataPageHeaderV2: &parquet.DataPageHeaderV2{
				NumValues:                  2,
				NumNulls:                   0,
				NumRows:                    2,
				Encoding:                   parquet.Encoding_PLAIN,
				DefinitionLevelsByteLength: 4,
				RepetitionLevelsByteLength: 4,
				IsCompressed:               false,
			},
		}

		var buf bytes.Buffer
		transport := thrift.NewTMemoryBufferLen(1024)
		protocol := thrift.NewTCompactProtocolConf(transport, nil)
		if err := header.Write(context.Background(), protocol); err != nil {
			require.NoError(t, err)
		}
		if err := protocol.Flush(context.Background()); err != nil {
			require.NoError(t, err)
		}
		headerData := transport.Buffer.Bytes()
		buf.Write(headerData)

		// Add insufficient data (should cause read error)
		buf.Write([]byte{0x01, 0x02})
		return buf.Bytes()
	}

	tests := []struct {
		name         string
		setupData    func() *thrift.TBufferedTransport
		colMetaData  *parquet.ColumnMetaData
		expectError  bool
		errorMessage string
	}{
		{
			name: "unsupported index page",
			setupData: func() *thrift.TBufferedTransport {
				data := createIndexPageData()
				mem := thrift.NewTMemoryBuffer()
				mem.Write(data)
				return thrift.NewTBufferedTransport(mem, 1024)
			},
			colMetaData:  colMetaDataCompressed,
			expectError:  true,
			errorMessage: "INDEX_PAGE",
		},
		{
			name: "malformed data page v2",
			setupData: func() *thrift.TBufferedTransport {
				data := createMalformedDataPageV2()
				mem := thrift.NewTMemoryBuffer()
				mem.Write(data)
				return thrift.NewTBufferedTransport(mem, 1024)
			},
			colMetaData: colMetaDataCompressed,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			thriftReader := tt.setupData()
			page, _, _, err := ReadPage(thriftReader, schemaHandler, tt.colMetaData)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMessage != "" {
					require.Contains(t, err.Error(), tt.errorMessage)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, page)
		})
	}
}

func Test_ReadPageHeader(t *testing.T) {
	testCases := []struct {
		name        string
		setupData   func() []byte
		expectError bool
	}{
		{
			name: "valid_data_page_header",
			setupData: func() []byte {
				header := &parquet.PageHeader{
					Type:                 parquet.PageType_DATA_PAGE,
					UncompressedPageSize: 100,
					CompressedPageSize:   80,
					DataPageHeader: &parquet.DataPageHeader{
						NumValues:               int32(10),
						Encoding:                parquet.Encoding_PLAIN,
						DefinitionLevelEncoding: parquet.Encoding_RLE,
						RepetitionLevelEncoding: parquet.Encoding_RLE,
					},
				}

				// Serialize the header using Thrift compact protocol
				transport := thrift.NewTMemoryBufferLen(1024)
				protocol := thrift.NewTCompactProtocolConf(transport, nil)
				if err := header.Write(context.Background(), protocol); err != nil {
					return nil
				}
				if err := protocol.Flush(context.Background()); err != nil {
					return nil
				}

				return transport.Buffer.Bytes()
			},
			expectError: false,
		},
		{
			name: "valid_dictionary_page_header",
			setupData: func() []byte {
				header := &parquet.PageHeader{
					Type:                 parquet.PageType_DICTIONARY_PAGE,
					UncompressedPageSize: 50,
					CompressedPageSize:   50,
					DictionaryPageHeader: &parquet.DictionaryPageHeader{
						NumValues: int32(5),
						Encoding:  parquet.Encoding_PLAIN,
					},
				}

				transport := thrift.NewTMemoryBufferLen(1024)
				protocol := thrift.NewTCompactProtocolConf(transport, nil)
				if err := header.Write(context.Background(), protocol); err != nil {
					return nil
				}
				if err := protocol.Flush(context.Background()); err != nil {
					return nil
				}

				return transport.Buffer.Bytes()
			},
			expectError: false,
		},
		{
			name: "empty_data",
			setupData: func() []byte {
				return []byte{}
			},
			expectError: true,
		},
		{
			name: "corrupted_data",
			setupData: func() []byte {
				return []byte{0xFF, 0xFF, 0xFF, 0xFF}
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			transport := thrift.NewTMemoryBufferLen(len(data))
			transport.Buffer.Write(data)
			bufferedTransport := thrift.NewTBufferedTransport(transport, 1024)

			header, err := ReadPageHeader(bufferedTransport)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, header)
			}
		})
	}
}

func Test_ReadPageRawData(t *testing.T) {
	// Create a simple schema handler
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "test_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)

	testCases := []struct {
		name        string
		setupData   func() []byte
		colMetadata *parquet.ColumnMetaData
		expectError bool
	}{
		{
			name: "valid_data_page",
			setupData: func() []byte {
				// Create a data page header
				header := &parquet.PageHeader{
					Type:                 parquet.PageType_DATA_PAGE,
					UncompressedPageSize: 16, // 4 int32 values = 16 bytes
					CompressedPageSize:   16,
					DataPageHeader: &parquet.DataPageHeader{
						NumValues:               int32(4),
						Encoding:                parquet.Encoding_PLAIN,
						DefinitionLevelEncoding: parquet.Encoding_RLE,
						RepetitionLevelEncoding: parquet.Encoding_RLE,
					},
				}

				// Serialize header
				transport := thrift.NewTMemoryBufferLen(1024)
				protocol := thrift.NewTCompactProtocolConf(transport, nil)
				if err := header.Write(context.Background(), protocol); err != nil {
					return nil
				}
				if err := protocol.Flush(context.Background()); err != nil {
					return nil
				}

				headerBytes := transport.Buffer.Bytes()

				// Create raw page data (simple int32 values)
				pageData := []byte{
					0x01, 0x00, 0x00, 0x00, // int32(1)
					0x02, 0x00, 0x00, 0x00, // int32(2)
					0x03, 0x00, 0x00, 0x00, // int32(3)
					0x04, 0x00, 0x00, 0x00, // int32(4)
				}

				// Combine header and data
				result := make([]byte, len(headerBytes)+len(pageData))
				copy(result, headerBytes)
				copy(result[len(headerBytes):], pageData)
				return result
			},
			colMetadata: &parquet.ColumnMetaData{
				Type:         parquet.Type_INT32,
				Encodings:    []parquet.Encoding{parquet.Encoding_PLAIN},
				PathInSchema: []string{"test_col"},
				Codec:        parquet.CompressionCodec_UNCOMPRESSED,
			},
			expectError: false,
		},
		{
			name: "empty_transport",
			setupData: func() []byte {
				return []byte{}
			},
			colMetadata: &parquet.ColumnMetaData{
				Type:         parquet.Type_INT32,
				Encodings:    []parquet.Encoding{parquet.Encoding_PLAIN},
				PathInSchema: []string{"test_col"},
				Codec:        parquet.CompressionCodec_UNCOMPRESSED,
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			transport := thrift.NewTMemoryBufferLen(len(data))
			transport.Buffer.Write(data)
			bufferedTransport := thrift.NewTBufferedTransport(transport, 1024)

			page, err := ReadPageRawData(bufferedTransport, schemaHandler, tc.colMetadata)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, page)
				require.NotNil(t, page.RawData)
			}
		})
	}
}

func Test_TableToDataPages(t *testing.T) {
	// Create a simple table with INT32 values
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:           []any{int32(1), int32(2), int32(3)},
		DefinitionLevels: []int32{0, 0, 0},
		RepetitionLevels: []int32{0, 0, 0},
		Info:             common.NewTag(),
	}

	pages, totalSize, err := TableToDataPages(table, 1024, parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.NotEmpty(t, pages)
	require.Positive(t, totalSize)
}

func Test_TableToDataPagesComplexScenarios(t *testing.T) {
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
				Info:               common.NewTag(),
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
					tag := common.NewTag()
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
				Info:               common.NewTag(),
			},
			pageSize:          8, // Very small page size to force multiple pages
			compressionCodec:  parquet.CompressionCodec_UNCOMPRESSED,
			expectError:       false,
			expectedPageCount: 3, // Actual page splitting behavior produces 3 pages
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pages, totalSize, err := TableToDataPages(tc.table, tc.pageSize, tc.compressionCodec)

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

func Test_TableToDataPagesWithEmptyTable(t *testing.T) {
	// Create an empty table
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:           []any{},
		DefinitionLevels: []int32{},
		RepetitionLevels: []int32{},
		Info:             common.NewTag(),
	}

	pages, totalSize, err := TableToDataPages(table, 1024, parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.Empty(t, pages)
	require.Zero(t, totalSize)
}

func Test_TableToDataPagesWithInvalidType(t *testing.T) {
	// Create a table with invalid schema type
	table := &Table{
		Schema: &parquet.SchemaElement{
			Name: "test_col",
			// No Type set - this should cause an error
		},
		Values:           []any{int32(1)},
		DefinitionLevels: []int32{0},
		RepetitionLevels: []int32{0},
		Info:             common.NewTag(),
	}

	_, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_UNCOMPRESSED)
	require.Error(t, err)
}

func Test_Page_Decode_BoundsChecking(t *testing.T) {
	tests := []struct {
		name       string
		setupPages func() (*Page, *Page)
	}{
		{
			name: "dictionary_index_out_of_bounds_negative",
			setupPages: func() (*Page, *Page) {
				dictPage := &Page{
					DataTable: &Table{
						Values: []any{"value1", "value2"},
					},
				}
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: &Table{
						Values: []any{int64(-1)}, // Negative index - should be ignored
					},
				}
				return page, dictPage
			},
		},
		{
			name: "dictionary_index_out_of_bounds_too_large",
			setupPages: func() (*Page, *Page) {
				dictPage := &Page{
					DataTable: &Table{
						Values: []any{"value1", "value2"},
					},
				}
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: &Table{
						Values: []any{int64(10)}, // Index 10 > dictionary size 2
					},
				}
				return page, dictPage
			},
		},
		{
			name: "dictionary_index_boundary_valid",
			setupPages: func() (*Page, *Page) {
				dictPage := &Page{
					DataTable: &Table{
						Values: []any{"value1", "value2", "value3"},
					},
				}
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: &Table{
						Values: []any{int64(2)}, // Index 2 is the last valid index
					},
				}
				return page, dictPage
			},
		},
		{
			name: "dictionary_index_invalid_type_assertion",
			setupPages: func() (*Page, *Page) {
				dictPage := &Page{
					DataTable: &Table{
						Values: []any{"value1", "value2"},
					},
				}
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: &Table{
						Values: []any{"not_an_int64"}, // Wrong type - should be ignored
					},
				}
				return page, dictPage
			},
		},
		{
			name: "dictionary_index_mixed_valid_invalid",
			setupPages: func() (*Page, *Page) {
				dictPage := &Page{
					DataTable: &Table{
						Values: []any{"value1", "value2", "value3"},
					},
				}
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: &Table{
						Values: []any{
							int64(1),  // Valid
							int64(-1), // Invalid - negative
							int64(10), // Invalid - too large
							"invalid", // Invalid - wrong type
							int64(0),  // Valid
						},
					},
				}
				return page, dictPage
			},
		},
		{
			name: "empty_dictionary",
			setupPages: func() (*Page, *Page) {
				dictPage := &Page{
					DataTable: &Table{
						Values: []any{}, // Empty dictionary
					},
				}
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: &Table{
						Values: []any{int64(0)}, // Any index in empty dict is invalid
					},
				}
				return page, dictPage
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, dictPage := tt.setupPages()

			page.Decode(dictPage)

			// Verify that the function completed without crashing
			// Invalid indices should remain unchanged or be skipped
		})
	}
}

func Test_Page_Decode_NilSafety(t *testing.T) {
	tests := []struct {
		name       string
		setupPages func() (*Page, *Page)
	}{
		{
			name: "nil_dict_page",
			setupPages: func() (*Page, *Page) {
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: &Table{
						Values: []any{int64(0)},
					},
				}
				return page, nil
			},
		},
		{
			name: "nil_dict_page_data_table",
			setupPages: func() (*Page, *Page) {
				dictPage := &Page{
					DataTable: nil, // Nil DataTable
				}
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: &Table{
						Values: []any{int64(0)},
					},
				}
				return page, dictPage
			},
		},
		{
			name: "nil_page_data_table",
			setupPages: func() (*Page, *Page) {
				dictPage := &Page{
					DataTable: &Table{
						Values: []any{"value1"},
					},
				}
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: nil, // Nil DataTable
				}
				return page, dictPage
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, dictPage := tt.setupPages()

			page.Decode(dictPage)
		})
	}
}

func Test_Table_Pop_ArrayConsistency(t *testing.T) {
	tests := []struct {
		name     string
		table    *Table
		numRows  int64
		expected bool // true if should return valid result, false if should return empty
	}{
		{
			name: "inconsistent_array_lengths_repetition_shorter",
			table: &Table{
				Values:           []any{"value1", "value2", "value3"},
				RepetitionLevels: []int32{0, 1}, // Shorter than Values
				DefinitionLevels: []int32{0, 1, 2},
			},
			numRows:  1,
			expected: false,
		},
		{
			name: "inconsistent_array_lengths_definition_shorter",
			table: &Table{
				Values:           []any{"value1", "value2", "value3"},
				RepetitionLevels: []int32{0, 1, 2},
				DefinitionLevels: []int32{0, 1}, // Shorter than Values
			},
			numRows:  1,
			expected: false,
		},
		{
			name: "consistent_array_lengths",
			table: &Table{
				Values:           []any{"value1", "value2", "value3"},
				RepetitionLevels: []int32{0, 1, 0},
				DefinitionLevels: []int32{1, 1, 1},
			},
			numRows:  1,
			expected: true,
		},
		{
			name: "empty_arrays_consistent",
			table: &Table{
				Values:           []any{},
				RepetitionLevels: []int32{},
				DefinitionLevels: []int32{},
			},
			numRows:  1,
			expected: true,
		},
		{
			name: "inconsistent_empty_arrays",
			table: &Table{
				Values:           []any{},
				RepetitionLevels: []int32{0}, // Non-empty while Values is empty
				DefinitionLevels: []int32{},
			},
			numRows:  1,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.table.Pop(tt.numRows)

			if tt.expected {
				// Should return a valid result (may be empty if input was empty)
				require.NotNil(t, result)
			} else {
				// Should return an empty table due to inconsistent arrays
				require.NotNil(t, result)
				require.Empty(t, result.Values)
				require.Empty(t, result.RepetitionLevels)
				require.Empty(t, result.DefinitionLevels)
			}
		})
	}
}

func Test_ExtractGeometryType(t *testing.T) {
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

func Test_ComputePageGeospatialStatistics(t *testing.T) {
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
