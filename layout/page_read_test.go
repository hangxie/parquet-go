package layout

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/compress"
	"github.com/hangxie/parquet-go/v3/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

func TestGetRLDLFromRawData(t *testing.T) {
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
			errorMessage: "unsupported page type",
		},
		{
			name: "data_page_compressed_valid_expected_size",
			setupPage: func() *Page {
				// Compress known data and set matching UncompressedPageSize
				rawData := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
				compressedData, err := compress.CompressWithError(rawData, parquet.CompressionCodec_SNAPPY)
				if err != nil {
					t.Fatalf("compress test data: %v", err)
				}

				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE
				page.Header.UncompressedPageSize = int32(len(rawData))
				page.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues:               2,
					Encoding:                parquet.Encoding_PLAIN,
					DefinitionLevelEncoding: parquet.Encoding_RLE,
					RepetitionLevelEncoding: parquet.Encoding_RLE,
				}
				page.Path = []string{"parquet_go_root", "required_field"}
				page.CompressType = parquet.CompressionCodec_SNAPPY
				page.RawData = compressedData
				return page
			},
			expectedValues: 2,
			expectedRows:   2,
		},
		{
			name: "data_page_compressed_size_mismatch",
			setupPage: func() *Page {
				// Compress data but set wrong UncompressedPageSize
				rawData := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
				compressedData, err := compress.CompressWithError(rawData, parquet.CompressionCodec_SNAPPY)
				if err != nil {
					t.Fatalf("compress test data: %v", err)
				}

				page := NewDataPage()
				page.Header.Type = parquet.PageType_DATA_PAGE
				page.Header.UncompressedPageSize = int32(len(rawData)) + 10 // wrong size
				page.Header.DataPageHeader = &parquet.DataPageHeader{
					NumValues:               2,
					Encoding:                parquet.Encoding_PLAIN,
					DefinitionLevelEncoding: parquet.Encoding_RLE,
					RepetitionLevelEncoding: parquet.Encoding_RLE,
				}
				page.Path = []string{"parquet_go_root", "required_field"}
				page.CompressType = parquet.CompressionCodec_SNAPPY
				page.RawData = compressedData
				return page
			},
			errorMessage: "uncompress data page",
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
			errorMessage: "uncompress data page",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := tt.setupPage()
			numValues, numRows, err := page.GetRLDLFromRawData(schemaHandler)

			if tt.errorMessage != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
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

func TestGetRLDLFromRawDataComplexScenarios(t *testing.T) {
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

func TestPageDecode(t *testing.T) {
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

func TestPage_GetValueFromRawData(t *testing.T) {
	tests := []struct {
		name        string
		setupPage   func() *Page
		setupSchema func() *schema.SchemaHandler
		errMsg      string
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
			errMsg: "unsupported page type",
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
			errMsg: "read plain values from dictionary page",
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
			errMsg: "read data page values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := tt.setupPage()
			schemaHandler := tt.setupSchema()

			err := page.GetValueFromRawData(schemaHandler)

			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
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

func TestReadDataPageValues(t *testing.T) {
	testCases := []struct {
		name           string
		encodingMethod parquet.Encoding
		dataType       parquet.Type
		convertedType  parquet.ConvertedType
		cnt            uint64
		bitWidth       uint64
		setupData      func() []byte
		errMsg         string
	}{
		{
			name:           "zero_count",
			encodingMethod: parquet.Encoding_PLAIN,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            0,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
		},
		{
			name:           "bit_packed_deprecated",
			encodingMethod: parquet.Encoding_BIT_PACKED,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            4,
			bitWidth:       3,
			setupData: func() []byte {
				// BIT_PACKED encoding for [1, 2, 3, 4] with bitWidth=3
				return []byte{0xD1, 0x08}
			},
		},
		{
			name:           "unknown_encoding",
			encodingMethod: parquet.Encoding(-1),
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			errMsg:         "unknown Encoding method",
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
		},
		{
			name:           "delta_binary_packed_unsupported_type",
			encodingMethod: parquet.Encoding_DELTA_BINARY_PACKED,
			dataType:       parquet.Type_FLOAT,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			errMsg:         "DELTA_BINARY_PACKED can only be used with int32 and int64",
		},
		{
			name:           "byte_stream_split_unsupported_type",
			encodingMethod: parquet.Encoding_BYTE_STREAM_SPLIT,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			errMsg:         "EOF",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			bytesReader := bytes.NewReader(data)

			result, err := ReadDataPageValues(bytesReader, tc.encodingMethod, tc.dataType, tc.convertedType, tc.cnt, tc.bitWidth)

			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				if tc.cnt == 0 {
					require.Empty(t, result)
				}
			}
		})
	}
}

func TestReadDataPageValuesMoreCases(t *testing.T) {
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
			name:           "rle_encoding_boolean",
			encodingMethod: parquet.Encoding_RLE,
			dataType:       parquet.Type_BOOLEAN,
			convertedType:  -1,
			cnt:            4,
			bitWidth:       1,
			setupData: func() []byte {
				// RLE encoding for booleans: [true, true, false, true]
				values := []any{int64(1), int64(1), int64(0), int64(1)}
				data, _ := encoding.WriteRLEBitPackedHybrid(values, 1, parquet.Type_INT64)
				return data
			},
			expectError: false,
		},
		{
			name:           "bit_packed_deprecated_boolean",
			encodingMethod: parquet.Encoding_BIT_PACKED,
			dataType:       parquet.Type_BOOLEAN,
			convertedType:  -1,
			cnt:            4,
			bitWidth:       1,
			setupData: func() []byte {
				// Deprecated bit packed encoding for booleans: [true, false, true, false]
				return []byte{0x05}
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

func TestReadDataPageValues_BooleanTypeConversion(t *testing.T) {
	t.Run("rle_encoding_boolean_values", func(t *testing.T) {
		// Test that RLE encoding correctly converts int64 to bool
		values := []any{int64(1), int64(1), int64(0), int64(1)}
		data, _ := encoding.WriteRLEBitPackedHybrid(values, 1, parquet.Type_INT64)
		bytesReader := bytes.NewReader(data)

		result, err := ReadDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_BOOLEAN, -1, 4, 1)

		require.NoError(t, err)
		require.Len(t, result, 4)

		// Verify types are bool, not int64
		require.IsType(t, true, result[0], "Expected bool type, got %T", result[0])
		require.IsType(t, true, result[1], "Expected bool type, got %T", result[1])
		require.IsType(t, true, result[2], "Expected bool type, got %T", result[2])
		require.IsType(t, true, result[3], "Expected bool type, got %T", result[3])

		// Verify values
		require.Equal(t, true, result[0])
		require.Equal(t, true, result[1])
		require.Equal(t, false, result[2])
		require.Equal(t, true, result[3])
	})

	t.Run("bit_packed_deprecated_boolean_values", func(t *testing.T) {
		// Test that BIT_PACKED encoding correctly converts int64 to bool
		data := []byte{0x05}
		bytesReader := bytes.NewReader(data)

		result, err := ReadDataPageValues(bytesReader, parquet.Encoding_BIT_PACKED, parquet.Type_BOOLEAN, -1, 4, 1)

		require.NoError(t, err)
		require.Len(t, result, 4)

		// Verify types are bool, not int64
		require.IsType(t, true, result[0], "Expected bool type, got %T", result[0])
		require.IsType(t, true, result[1], "Expected bool type, got %T", result[1])
		require.IsType(t, true, result[2], "Expected bool type, got %T", result[2])
		require.IsType(t, true, result[3], "Expected bool type, got %T", result[3])

		// Verify values
		require.Equal(t, true, result[0])
		require.Equal(t, false, result[1])
		require.Equal(t, true, result[2])
		require.Equal(t, false, result[3])
	})

	t.Run("rle_encoding_boolean_with_zero_bitwidth", func(t *testing.T) {
		// Test that RLE encoding with bitWidth=0 defaults to bitWidth=1 for BOOLEAN type
		// This simulates when struct tag doesn't specify length= for a boolean field
		values := []any{int64(1), int64(0), int64(1), int64(1), int64(0)}
		data, _ := encoding.WriteRLEBitPackedHybrid(values, 1, parquet.Type_INT64)
		bytesReader := bytes.NewReader(data)

		// Read with bitWidth=0 - should be fixed to 1 automatically
		result, err := ReadDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_BOOLEAN, -1, 5, 0)

		require.NoError(t, err)
		require.Len(t, result, 5)

		// Verify types are bool, not int64
		for i := range result {
			require.IsType(t, true, result[i], "Expected bool type at index %d, got %T", i, result[i])
		}

		// Verify values
		require.Equal(t, true, result[0])
		require.Equal(t, false, result[1])
		require.Equal(t, true, result[2])
		require.Equal(t, true, result[3])
		require.Equal(t, false, result[4])
	})
}

func TestReadPage(t *testing.T) {
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
		headerData := transport.Bytes()
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
		headerData := transport.Bytes()
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
		headerData := transport.Bytes()
		buf.Write(headerData)

		// Add page data (2 INT32 values: 123, 456)
		pageData := []byte{0x7B, 0x00, 0x00, 0x00, 0xC8, 0x01, 0x00, 0x00}
		buf.Write(pageData)

		return buf.Bytes()
	}

	tests := []struct {
		name         string
		setupData    func() *thrift.TBufferedTransport
		errMsg       string
		expectedType string
	}{
		{
			name: "empty buffer",
			setupData: func() *thrift.TBufferedTransport {
				return thrift.NewTBufferedTransport(thrift.NewTMemoryBuffer(), 1024)
			},
			errMsg: "EOF",
		},
		{
			name: "invalid data",
			setupData: func() *thrift.TBufferedTransport {
				mem := thrift.NewTMemoryBuffer()
				mem.Write([]byte{0x01, 0x02, 0x03})
				return thrift.NewTBufferedTransport(mem, 1024)
			},
			errMsg: "EOF",
		},
		{
			name: "dictionary page",
			setupData: func() *thrift.TBufferedTransport {
				data := createDictionaryPageData()
				mem := thrift.NewTMemoryBuffer()
				mem.Write(data)
				return thrift.NewTBufferedTransport(mem, 1024)
			},
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
			expectedType: "DATA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			thriftReader := tt.setupData()
			page, _, _, err := ReadPage(thriftReader, schemaHandler, colMetaData, nil)

			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
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

func TestReadPage_NilOptsUsesDefaults(t *testing.T) {
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
	colMetaData := &parquet.ColumnMetaData{
		Type:         parquet.Type_INT32,
		Codec:        parquet.CompressionCodec_UNCOMPRESSED,
		PathInSchema: []string{"test_col"},
	}

	// Build a small valid data page (CompressedPageSize=8)
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
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolConf(transport, nil)
	require.NoError(t, header.Write(context.Background(), protocol))
	require.NoError(t, protocol.Flush(context.Background()))
	headerBytes := transport.Bytes()

	pageData := []byte{0x7B, 0x00, 0x00, 0x00, 0xC8, 0x01, 0x00, 0x00}

	t.Run("nil opts succeeds with default MaxPageSize", func(t *testing.T) {
		mem := thrift.NewTMemoryBuffer()
		mem.Write(headerBytes)
		mem.Write(pageData)
		thriftReader := thrift.NewTBufferedTransport(mem, 1024)

		page, _, _, err := ReadPage(thriftReader, schemaHandler, colMetaData, nil)
		require.NoError(t, err)
		require.NotNil(t, page)
	})

	t.Run("explicit MaxPageSize rejects oversized page", func(t *testing.T) {
		mem := thrift.NewTMemoryBuffer()
		mem.Write(headerBytes)
		mem.Write(pageData)
		thriftReader := thrift.NewTBufferedTransport(mem, 1024)

		_, _, _, err := ReadPage(thriftReader, schemaHandler, colMetaData, &PageReadOptions{MaxPageSize: 1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "page size 8 exceeds limit 1")
	})

	t.Run("explicit MaxPageSize allows page within limit", func(t *testing.T) {
		mem := thrift.NewTMemoryBuffer()
		mem.Write(headerBytes)
		mem.Write(pageData)
		thriftReader := thrift.NewTBufferedTransport(mem, 1024)

		page, _, _, err := ReadPage(thriftReader, schemaHandler, colMetaData, &PageReadOptions{MaxPageSize: 100})
		require.NoError(t, err)
		require.NotNil(t, page)
	})
}

func TestReadPageRawData_NilOptsUsesDefaults(t *testing.T) {
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
	colMetaData := &parquet.ColumnMetaData{
		Type:         parquet.Type_INT32,
		Codec:        parquet.CompressionCodec_UNCOMPRESSED,
		PathInSchema: []string{"test_col"},
	}

	// Build a small valid data page (CompressedPageSize=8)
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
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolConf(transport, nil)
	require.NoError(t, header.Write(context.Background(), protocol))
	require.NoError(t, protocol.Flush(context.Background()))
	headerBytes := transport.Bytes()

	pageData := []byte{0x7B, 0x00, 0x00, 0x00, 0xC8, 0x01, 0x00, 0x00}

	t.Run("nil opts succeeds with default MaxPageSize", func(t *testing.T) {
		mem := thrift.NewTMemoryBuffer()
		mem.Write(headerBytes)
		mem.Write(pageData)
		thriftReader := thrift.NewTBufferedTransport(mem, 1024)

		page, err := ReadPageRawData(thriftReader, schemaHandler, colMetaData, nil)
		require.NoError(t, err)
		require.NotNil(t, page)
	})

	t.Run("explicit MaxPageSize rejects oversized page", func(t *testing.T) {
		mem := thrift.NewTMemoryBuffer()
		mem.Write(headerBytes)
		mem.Write(pageData)
		thriftReader := thrift.NewTBufferedTransport(mem, 1024)

		_, err := ReadPageRawData(thriftReader, schemaHandler, colMetaData, &PageReadOptions{MaxPageSize: 1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "page size 8 exceeds limit 1")
	})
}

func TestReadPageV2IsCompressedFalse(t *testing.T) {
	// Regression test: DATA_PAGE_V2 with is_compressed=false should not
	// attempt decompression, even when the column's codec is SNAPPY.
	// Previously, ReadPage unconditionally decompressed V2 data pages,
	// causing "corrupt input" errors for files written by other libraries
	// (e.g., PyArrow) that set is_compressed=false.
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

	colMetaData := &parquet.ColumnMetaData{
		Type:         parquet.Type_INT32,
		Codec:        parquet.CompressionCodec_SNAPPY,
		PathInSchema: []string{"test_col"},
	}

	// Create DATA_PAGE_V2 with is_compressed=false and uncompressed INT32 data
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

	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolConf(transport, nil)
	require.NoError(t, header.Write(context.Background(), protocol))
	require.NoError(t, protocol.Flush(context.Background()))
	headerData := transport.Bytes()

	var buf bytes.Buffer
	buf.Write(headerData)
	// 2 PLAIN INT32 values: 42, 84 (uncompressed)
	buf.Write([]byte{0x2A, 0x00, 0x00, 0x00, 0x54, 0x00, 0x00, 0x00})

	mem := thrift.NewTMemoryBufferLen(buf.Len())
	mem.Write(buf.Bytes())
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	page, _, _, err := ReadPage(thriftReader, schemaHandler, colMetaData, nil)
	require.NoError(t, err)
	require.NotNil(t, page)
	require.Equal(t, parquet.PageType_DATA_PAGE_V2, page.Header.GetType())
}

func TestProcessDataPageV2IsCompressedFalse(t *testing.T) {
	// Regression test: processDataPageV2 must not decompress when
	// is_compressed=false, even when CompressType is SNAPPY.
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

	page := NewDataPage()
	page.Header = &parquet.PageHeader{
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
	page.CompressType = parquet.CompressionCodec_SNAPPY
	page.Path = []string{"parquet_go_root", "test_col"}
	page.Schema = schemaElements[1]
	// 2 PLAIN INT32 values: 42, 84 (uncompressed)
	page.RawData = []byte{0x2A, 0x00, 0x00, 0x00, 0x54, 0x00, 0x00, 0x00}
	page.DataTable = &Table{
		Path:               []string{"parquet_go_root", "test_col"},
		MaxDefinitionLevel: 0,
		MaxRepetitionLevel: 0,
		Values:             make([]any, 2),
		DefinitionLevels:   []int32{0, 0},
		RepetitionLevels:   []int32{0, 0},
	}

	err := page.GetValueFromRawData(schemaHandler)
	require.NoError(t, err)
	require.Equal(t, int32(42), page.DataTable.Values[0])
	require.Equal(t, int32(84), page.DataTable.Values[1])
}

func TestReadPageErrorCases(t *testing.T) {
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
		headerData := transport.Bytes()
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
		headerData := transport.Bytes()
		buf.Write(headerData)

		// Add insufficient data (should cause read error)
		buf.Write([]byte{0x01, 0x02})
		return buf.Bytes()
	}

	tests := []struct {
		name         string
		setupData    func() *thrift.TBufferedTransport
		colMetaData  *parquet.ColumnMetaData
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
			colMetaData:  colMetaDataCompressed,
			errorMessage: "unexpected EOF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			thriftReader := tt.setupData()
			page, _, _, err := ReadPage(thriftReader, schemaHandler, tt.colMetaData, nil)

			if tt.errorMessage != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, page)
		})
	}
}

func TestReadPageHeader(t *testing.T) {
	testCases := []struct {
		name      string
		setupData func() []byte
		errMsg    string
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

				return transport.Bytes()
			},
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

				return transport.Bytes()
			},
		},
		{
			name: "empty_data",
			setupData: func() []byte {
				return []byte{}
			},
			errMsg: "EOF",
		},
		{
			name: "corrupted_data",
			setupData: func() []byte {
				return []byte{0xFF, 0xFF, 0xFF, 0xFF}
			},
			errMsg: "read error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			transport := thrift.NewTMemoryBufferLen(len(data))
			transport.Write(data)
			bufferedTransport := thrift.NewTBufferedTransport(transport, 1024)

			header, err := ReadPageHeader(bufferedTransport)

			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, header)
			}
		})
	}
}

func TestReadPageRawData(t *testing.T) {
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
		errMsg      string
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

				headerBytes := transport.Bytes()

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
			errMsg: "EOF",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			transport := thrift.NewTMemoryBufferLen(len(data))
			transport.Write(data)
			bufferedTransport := thrift.NewTBufferedTransport(transport, 1024)

			page, err := ReadPageRawData(bufferedTransport, schemaHandler, tc.colMetadata, nil)

			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, page)
				require.NotNil(t, page.RawData)
			}
		})
	}
}

func TestPage_Decode_BoundsChecking(t *testing.T) {
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

func TestPage_Decode_NilSafety(t *testing.T) {
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

func TestReadDataPageValues_BoundsChecking(t *testing.T) {
	testCases := []struct {
		name           string
		encodingMethod parquet.Encoding
		dataType       parquet.Type
		cnt            uint64
		bitWidth       uint64
		setupData      func() []byte
		expectedError  string
	}{
		{
			name:           "rle_dictionary_fewer_values_than_expected",
			encodingMethod: parquet.Encoding_RLE_DICTIONARY,
			dataType:       parquet.Type_INT32,
			cnt:            21, // Request 21 values
			bitWidth:       4,
			setupData: func() []byte {
				// Create RLE data that only contains 1 value (repeated once)
				// Bit width byte
				data := []byte{4} // bitWidth = 4
				// Write RLE/bit-packed hybrid data for just 1 value
				rleBuf, _ := encoding.WriteRLEBitPackedHybrid([]any{int64(0)}, 4, parquet.Type_INT64)
				data = append(data, rleBuf...)
				return data
			},
			expectedError: "expected 21 values but got 2 from RLE/bit-packed hybrid decoder",
		},
		{
			name:           "rle_fewer_values_than_expected",
			encodingMethod: parquet.Encoding_RLE,
			dataType:       parquet.Type_INT32,
			cnt:            21, // Request 21 values
			bitWidth:       4,
			setupData: func() []byte {
				// Create RLE data that only contains 1 value
				rleBuf, _ := encoding.WriteRLEBitPackedHybrid([]any{int64(0)}, 4, parquet.Type_INT64)
				return rleBuf
			},
			expectedError: "expected 21 values but got 1 from RLE/bit-packed hybrid decoder",
		},
		{
			name:           "plain_dictionary_fewer_values_than_expected",
			encodingMethod: parquet.Encoding_PLAIN_DICTIONARY,
			dataType:       parquet.Type_INT32,
			cnt:            10,
			bitWidth:       3,
			setupData: func() []byte {
				// Create dictionary-encoded data with fewer values than requested
				data := []byte{3} // bitWidth = 3
				// RLE data with only 3 values - create minimal data
				// We manually construct this to have fewer values than cnt
				rleBuf, _ := encoding.WriteRLEBitPackedHybrid(
					[]any{int64(0), int64(1), int64(0)},
					3,
					parquet.Type_INT64,
				)
				data = append(data, rleBuf...)
				return data
			},
			expectedError: "expected 10 values but got",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			bytesReader := bytes.NewReader(data)

			result, err := ReadDataPageValues(
				bytesReader,
				tc.encodingMethod,
				tc.dataType,
				-1, // convertedType
				tc.cnt,
				tc.bitWidth,
			)

			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedError)
			require.Empty(t, result)
		})
	}
}
