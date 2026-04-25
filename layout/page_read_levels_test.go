package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/compress"
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
