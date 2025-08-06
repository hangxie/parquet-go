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
