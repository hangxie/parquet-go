package layout

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

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

func TestProcessDictionaryPage_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		setupPage func() *Page
		errMsg    string
	}{
		{
			name: "nil_schema",
			setupPage: func() *Page {
				return &Page{}
			},
			errMsg: "page schema is nil",
		},
		{
			name: "nil_schema_type",
			setupPage: func() *Page {
				return &Page{Schema: &parquet.SchemaElement{}}
			},
			errMsg: "page schema type is nil",
		},
		{
			name: "nil_header",
			setupPage: func() *Page {
				return &Page{Schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)}}
			},
			errMsg: "page header is nil",
		},
		{
			name: "nil_dict_header",
			setupPage: func() *Page {
				return &Page{
					Schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)},
					Header: &parquet.PageHeader{},
				}
			},
			errMsg: "page dictionary header is nil",
		},
		{
			name: "nil_data_table",
			setupPage: func() *Page {
				return &Page{
					Schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)},
					Header: &parquet.PageHeader{DictionaryPageHeader: &parquet.DictionaryPageHeader{}},
				}
			},
			errMsg: "page data table is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := tt.setupPage()
			err := page.processDictionaryPage()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestReadPageV2Data_InvalidLevels(t *testing.T) {
	header := &parquet.PageHeader{
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			DefinitionLevelsByteLength: -1,
		},
	}
	_, err := readPageV2Data(nil, header, nil, nil, PageReadOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid level byte lengths")

	header.DataPageHeaderV2.DefinitionLevelsByteLength = 10
	header.DataPageHeaderV2.RepetitionLevelsByteLength = 10
	header.CompressedPageSize = 15
	_, err = readPageV2Data(nil, header, nil, nil, PageReadOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "level byte lengths exceed page size")
}

func TestAssembleLevelPrefixedBuf_WithRll(t *testing.T) {
	repBuf := []byte{0x01}
	defBuf := []byte{0x02}
	dataBuf := []byte{0x03}
	res, err := assembleLevelPrefixedBuf(1, 1, repBuf, defBuf, dataBuf)
	require.NoError(t, err)
	require.NotEmpty(t, res)
}

func TestReadPageV1Data_Compressed(t *testing.T) {
	data := []byte("some data to be compressed")
	compressed, _ := compress.DefaultCompressor().Compress(data, parquet.CompressionCodec_SNAPPY)

	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		CompressedPageSize:   int32(len(compressed)),
		UncompressedPageSize: int32(len(data)),
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 1,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}

	colMetaData := &parquet.ColumnMetaData{
		Codec: parquet.CompressionCodec_SNAPPY,
	}

	mem := thrift.NewTMemoryBuffer()
	mem.Write(compressed)
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	res, err := readPageV1Data(thriftReader, header, colMetaData, nil, PageReadOptions{})
	require.NoError(t, err)
	require.Equal(t, data, res)
}

func TestProcessDataPageV2_Compressed(t *testing.T) {
	schemaElements := []*parquet.SchemaElement{
		{
			Name: "test_col",
			Type: common.ToPtr(parquet.Type_INT32),
		},
	}
	schemaHandler := &schema.SchemaHandler{
		SchemaElements: schemaElements,
		MapIndex:       map[string]int32{"test_col": 0},
	}

	data := []byte{0x01, 0x00, 0x00, 0x00} // int32(1)
	compressed, _ := compress.DefaultCompressor().Compress(data, parquet.CompressionCodec_SNAPPY)

	page := NewDataPage()
	page.Header = &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE_V2,
		CompressedPageSize:   int32(len(compressed)),
		UncompressedPageSize: int32(len(data)),
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  1,
			Encoding:                   parquet.Encoding_PLAIN,
			IsCompressed:               true,
			DefinitionLevelsByteLength: 0,
			RepetitionLevelsByteLength: 0,
		},
	}
	page.CompressType = parquet.CompressionCodec_SNAPPY
	page.RawData = compressed
	page.Schema = schemaElements[0]
	page.DataTable = &Table{
		Path:               []string{"test_col"},
		MaxDefinitionLevel: 0,
		Values:             make([]any, 1),
		DefinitionLevels:   []int32{0},
	}

	err := page.processDataPageV2(schemaHandler)
	require.NoError(t, err)
	require.Equal(t, int32(1), page.DataTable.Values[0])
}

func TestReadPageV2Data_Compressed(t *testing.T) {
	data := []byte{0x01, 0x00, 0x00, 0x00}
	compressed, _ := compress.DefaultCompressor().Compress(data, parquet.CompressionCodec_SNAPPY)

	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE_V2,
		CompressedPageSize:   int32(len(compressed)),
		UncompressedPageSize: int32(len(data)),
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  1,
			Encoding:                   parquet.Encoding_PLAIN,
			IsCompressed:               true,
			DefinitionLevelsByteLength: 0,
			RepetitionLevelsByteLength: 0,
		},
	}

	colMetaData := &parquet.ColumnMetaData{
		Codec: parquet.CompressionCodec_SNAPPY,
	}

	mem := thrift.NewTMemoryBuffer()
	mem.Write(compressed)
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	res, err := readPageV2Data(thriftReader, header, colMetaData, nil, PageReadOptions{})
	require.NoError(t, err)
	// res should be prefixed by 0, 0 (rll, dll) as they are 0
	require.Equal(t, data, res)
}

func TestReadPageV1Data_WithCRC(t *testing.T) {
	data := []byte("some data")
	crc := common.ComputePageCRC(data)

	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		CompressedPageSize:   int32(len(data)),
		UncompressedPageSize: int32(len(data)),
		Crc:                  common.ToPtr(int32(crc)),
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 1,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}

	colMetaData := &parquet.ColumnMetaData{
		Codec: parquet.CompressionCodec_UNCOMPRESSED,
	}

	mem := thrift.NewTMemoryBuffer()
	mem.Write(data)
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	res, err := readPageV1Data(thriftReader, header, colMetaData, nil, PageReadOptions{CRCMode: common.CRCAuto})
	require.NoError(t, err)
	require.Equal(t, data, res)
}

func TestReadPageV1Data_ReadError(t *testing.T) {
	header := &parquet.PageHeader{
		CompressedPageSize: 10,
	}
	mem := thrift.NewTMemoryBuffer()
	mem.Write([]byte{1, 2, 3}) // less than 10
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	_, err := readPageV1Data(thriftReader, header, nil, nil, PageReadOptions{})
	require.Error(t, err)
}
