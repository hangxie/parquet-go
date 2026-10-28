package layout

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
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

			require.NoError(t, tc.page.Decode(tc.dictPage))

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

func bufferedTransportFrom(data ...[]byte) *thrift.TBufferedTransport {
	mem := thrift.NewTMemoryBuffer()
	for _, d := range data {
		mem.Write(d)
	}
	return thrift.NewTBufferedTransport(mem, 1024)
}

func simpleInt32Schema(t *testing.T) (*schema.SchemaHandler, *parquet.ColumnMetaData) {
	t.Helper()
	sh := schema.NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
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
	})
	cmd := &parquet.ColumnMetaData{
		Type:         parquet.Type_INT32,
		Codec:        parquet.CompressionCodec_UNCOMPRESSED,
		PathInSchema: []string{"Test_col"},
	}
	return sh, cmd
}

func TestPageDecode_V2NonDictionaryEncoding(t *testing.T) {
	page := &Page{
		Header: &parquet.PageHeader{
			DataPageHeaderV2: &parquet.DataPageHeaderV2{
				Encoding: parquet.Encoding_PLAIN, // not a dictionary encoding
			},
		},
		DataTable: &Table{Values: []any{int64(0)}},
	}
	dictPage := &Page{DataTable: &Table{Values: []any{"unused"}}}
	require.NoError(t, page.Decode(dictPage))
	// Non-dictionary V2 page must be left untouched.
	require.Equal(t, int64(0), page.DataTable.Values[0])
}

func TestReadPageRawData_UnsupportedPageType(t *testing.T) {
	sh, cmd := simpleInt32Schema(t)
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_INDEX_PAGE,
		CompressedPageSize:   4,
		UncompressedPageSize: 4,
	}
	reader := bufferedTransportFrom(serializePageHeader(t, header), []byte{0, 0, 0, 0})
	_, err := ReadPageRawData(reader, sh, cmd, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported page type")
}

func TestReadPageRawData_ReadBodyError(t *testing.T) {
	sh, cmd := simpleInt32Schema(t)
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		CompressedPageSize:   16, // declares more than is available
		UncompressedPageSize: 16,
		DataPageHeader:       &parquet.DataPageHeader{NumValues: 4, Encoding: parquet.Encoding_PLAIN},
	}
	reader := bufferedTransportFrom(serializePageHeader(t, header), []byte{0x01, 0x02})
	_, err := ReadPageRawData(reader, sh, cmd, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read page body")
}

func TestReadPageRawData_CRCFailure(t *testing.T) {
	sh, cmd := simpleInt32Schema(t)
	data := []byte{0x01, 0x00, 0x00, 0x00}
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		CompressedPageSize:   int32(len(data)),
		UncompressedPageSize: int32(len(data)),
		Crc:                  common.ToPtr(int32(0x12345678)),
		DataPageHeader:       &parquet.DataPageHeader{NumValues: 1, Encoding: parquet.Encoding_PLAIN},
	}
	reader := bufferedTransportFrom(serializePageHeader(t, header), data)
	_, err := ReadPageRawData(reader, sh, cmd, &PageReadOptions{CRCMode: common.CRCStrict})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CRC validation failed")
}

func TestReadPage_DictionaryBodyError(t *testing.T) {
	sh, cmd := simpleInt32Schema(t)
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DICTIONARY_PAGE,
		CompressedPageSize:   2,
		UncompressedPageSize: 2,
		DictionaryPageHeader: &parquet.DictionaryPageHeader{NumValues: 2, Encoding: parquet.Encoding_PLAIN},
	}
	// Only 2 bytes for 2 INT32 dictionary values -> decode error.
	reader := bufferedTransportFrom(serializePageHeader(t, header), []byte{0x01, 0x02})
	_, _, _, err := ReadPage(reader, sh, cmd, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dictionary page body")
}

func TestReadPage_DataBodyError(t *testing.T) {
	sh, cmd := simpleInt32Schema(t)
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		CompressedPageSize:   4,
		UncompressedPageSize: 4,
		DataPageHeader:       &parquet.DataPageHeader{NumValues: 2, Encoding: parquet.Encoding_PLAIN},
	}
	// 2 values requested but only one INT32 of data -> body decode error.
	reader := bufferedTransportFrom(serializePageHeader(t, header), []byte{0x01, 0x00, 0x00, 0x00})
	_, _, _, err := ReadPage(reader, sh, cmd, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data page body")
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
	// A corrupt page must not have an invalid index read back as a value.
	tests := []struct {
		name       string
		setupPages func() (*Page, *Page)
		wantValues []any
		wantErr    string
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
						Values: []any{int64(-1)},
					},
				}
				return page, dictPage
			},
			wantErr: "dictionary index -1 out of range [0, 2)",
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
						Values: []any{int64(10)},
					},
				}
				return page, dictPage
			},
			wantErr: "dictionary index 10 out of range [0, 2)",
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
			wantValues: []any{"value3"},
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
						Values: []any{"not_an_int64"},
					},
				}
				return page, dictPage
			},
			wantErr: "dictionary index has type string",
		},
		{
			name: "dictionary_index_valid_with_null",
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
						Values: []any{int64(1), nil, int64(0)},
					},
				}
				return page, dictPage
			},
			wantValues: []any{"value2", nil, "value1"},
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
			wantErr: "dictionary index 0 out of range [0, 0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, dictPage := tt.setupPages()

			err := page.Decode(dictPage)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantValues, page.DataTable.Values)
		})
	}
}

func TestPage_Decode_NilSafety(t *testing.T) {
	tests := []struct {
		name       string
		setupPages func() (*Page, *Page)
		wantErr    string
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
			wantErr: "dictionary-encoded page has no dictionary",
		},
		{
			name: "nil_dict_page_all_null",
			setupPages: func() (*Page, *Page) {
				page := &Page{
					Header: &parquet.PageHeader{
						DataPageHeader: &parquet.DataPageHeader{
							Encoding: parquet.Encoding_RLE_DICTIONARY,
						},
					},
					DataTable: &Table{
						Values: []any{nil, nil},
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
			wantErr: "dictionary-encoded page has no dictionary",
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

			err := page.Decode(dictPage)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
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

func TestReadPageHugeBooleanRunUsesBoundedBatch(t *testing.T) {
	for _, count := range []int32{1<<20 - 1, 1<<20 + 1, 1<<31 - 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			body := append(encoding.WriteUnsignedVarInt(uint64(count)<<1), byte(1))
			data := append([]byte{byte(len(body)), 0, 0, 0}, body...)
			header := &parquet.PageHeader{
				Type: parquet.PageType_DATA_PAGE, CompressedPageSize: int32(len(data)), UncompressedPageSize: int32(len(data)),
				DataPageHeader: &parquet.DataPageHeader{NumValues: count, Encoding: parquet.Encoding_RLE},
			}
			sh := testSchemaHandler()
			sh.SchemaElements[1].Type = common.ToPtr(parquet.Type_BOOLEAN)
			meta := testColumnMetaData()
			meta.Type, meta.NumValues = parquet.Type_BOOLEAN, int64(count)
			meta.PathInSchema = []string{sh.GetInName(1)}
			mem := thrift.NewTMemoryBuffer()
			require.NoError(t, header.Write(context.Background(), thrift.NewTCompactProtocolConf(mem, nil)))
			_, err := mem.Write(data)
			require.NoError(t, err)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			page, n, rows, err := ReadPage(thrift.NewTBufferedTransport(mem, 1024), sh, meta, nil)
			runtime.ReadMemStats(&after)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
			require.NoError(t, err)
			require.Positive(t, n)
			require.LessOrEqual(t, n, int64(1024))
			require.Equal(t, n, rows)
			require.Len(t, page.DataTable.Values, int(n))
			for _, value := range page.DataTable.Values {
				require.Equal(t, true, value)
			}
		})
	}
}

func TestReadPageRawDataValueCountExceedsChunk(t *testing.T) {
	for _, pageType := range []parquet.PageType{parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2} {
		t.Run(pageType.String(), func(t *testing.T) {
			sh := testSchemaHandler()
			meta := testColumnMetaData()
			meta.PathInSchema = []string{sh.GetInName(1)}
			meta.NumValues = 1
			header := &parquet.PageHeader{
				Type: pageType, CompressedPageSize: 8, UncompressedPageSize: 8,
				DataPageHeader:   &parquet.DataPageHeader{NumValues: 2, Encoding: parquet.Encoding_PLAIN},
				DataPageHeaderV2: &parquet.DataPageHeaderV2{NumValues: 2, Encoding: parquet.Encoding_PLAIN},
			}
			mem := thrift.NewTMemoryBuffer()
			require.NoError(t, header.Write(context.Background(), thrift.NewTCompactProtocolConf(mem, nil)))
			_, err := mem.Write([]byte{1, 0, 0, 0, 2, 0, 0, 0})
			require.NoError(t, err)
			page, err := ReadPageRawData(thrift.NewTBufferedTransport(mem, 1024), sh, meta, nil)
			require.NoError(t, err)
			_, _, err = page.GetRLDLFromRawData(sh)
			require.ErrorContains(t, err, "data page value count 2 exceeds column chunk total 1")
			require.Nil(t, page.decoder)
		})
	}
}

func TestGetValueFromRawDataRequiresDecoder(t *testing.T) {
	for _, pageType := range []parquet.PageType{parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2} {
		page := &Page{Header: &parquet.PageHeader{Type: pageType}}
		require.ErrorContains(t, page.GetValueFromRawData(), "page decoder is not initialized")
	}
	page := &Page{Header: &parquet.PageHeader{Type: parquet.PageType_INDEX_PAGE}}
	require.ErrorContains(t, page.GetValueFromRawData(), "unsupported page type")
	data, err := encoding.WritePlainINT32([]any{int32(7)})
	require.NoError(t, err)
	page = &Page{Header: &parquet.PageHeader{Type: parquet.PageType_DICTIONARY_PAGE, DictionaryPageHeader: &parquet.DictionaryPageHeader{NumValues: 1}}, Schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)}, DataTable: &Table{}, RawData: data}
	require.NoError(t, page.GetValueFromRawData())
	require.Equal(t, []any{int32(7)}, page.DataTable.Values)
}

func TestReadPagePaddedDefinitionLevels(t *testing.T) {
	sh := schema.NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
		{Name: "root", NumChildren: common.ToPtr(int32(1))},
		{Name: "a", NumChildren: common.ToPtr(int32(1)), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
		{Name: "b", NumChildren: common.ToPtr(int32(1)), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
		{Name: "c", NumChildren: common.ToPtr(int32(1)), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
		{Name: "value", Type: common.ToPtr(parquet.Type_INT32), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
	})
	path := common.StrToPath(sh.IndexMap[4])
	levels := make([]any, 16)
	for i := range levels {
		levels[i] = int64(4)
		if i >= 9 {
			levels[i] = int64(7)
		}
	}
	packed := encoding.WriteBitPacked(levels, 3, true)
	want := []any{int32(1), int32(2), int32(3), int32(4), int32(5), int32(6), int32(7), int32(8), int32(9)}
	values, err := encoding.WritePlainINT32(want)
	require.NoError(t, err)
	meta := &parquet.ColumnMetaData{Type: parquet.Type_INT32, Codec: parquet.CompressionCodec_UNCOMPRESSED, NumValues: 9, PathInSchema: path[1:]}
	for _, v2 := range []bool{false, true} {
		for _, raw := range []bool{false, true} {
			t.Run(fmt.Sprintf("v2=%t/raw=%t", v2, raw), func(t *testing.T) {
				header := &parquet.PageHeader{Type: parquet.PageType_DATA_PAGE, DataPageHeader: &parquet.DataPageHeader{NumValues: 9, Encoding: parquet.Encoding_PLAIN}}
				data := binary.LittleEndian.AppendUint32(nil, uint32(len(packed)))
				if v2 {
					header.Type = parquet.PageType_DATA_PAGE_V2
					header.DataPageHeader = nil
					header.DataPageHeaderV2 = &parquet.DataPageHeaderV2{NumValues: 9, NumRows: 9, Encoding: parquet.Encoding_PLAIN, DefinitionLevelsByteLength: int32(len(packed)), IsCompressed: false}
					data = nil
				}
				data = append(data, packed...)
				data = append(data, values...)
				header.CompressedPageSize, header.UncompressedPageSize = int32(len(data)), int32(len(data))
				mem := thrift.NewTMemoryBuffer()
				require.NoError(t, header.Write(context.Background(), thrift.NewTCompactProtocolConf(mem, nil)))
				_, err := mem.Write(data)
				require.NoError(t, err)
				transport := thrift.NewTBufferedTransport(mem, 1024)
				var page *Page
				if raw {
					page, err = ReadPageRawData(transport, sh, meta, nil)
					require.NoError(t, err)
					_, _, err = page.GetRLDLFromRawData(sh)
					require.NoError(t, err)
					require.NoError(t, page.GetValueFromRawData())
				} else {
					page, _, _, err = ReadPage(transport, sh, meta, nil)
					require.NoError(t, err)
				}
				require.Equal(t, want, page.DataTable.Values)
				require.Equal(t, []int32{4, 4, 4, 4, 4, 4, 4, 4, 4}, page.DataTable.DefinitionLevels)
			})
		}
	}
}
