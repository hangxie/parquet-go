package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

func TestDictDataPageCompress(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}

	// Set up DataTable with proper definition and repetition levels
	page.DataTable = &Table{
		DefinitionLevels:   []int32{0, 0, 0, 0},
		RepetitionLevels:   []int32{0, 0, 0, 0},
		MaxDefinitionLevel: 0,
		MaxRepetitionLevel: 0,
	}

	// Test values representing dictionary indices
	values := []int32{0, 1, 0, 2}

	data, err := page.DictDataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, 2, values)
	require.NoError(t, err)
	require.NotZero(t, len(data))
}

func TestDictDataPageCompressWithEmptyValues(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}

	// Set up DataTable
	page.DataTable = &Table{
		DefinitionLevels:   []int32{},
		RepetitionLevels:   []int32{},
		MaxDefinitionLevel: 0,
		MaxRepetitionLevel: 0,
	}

	// Test with empty values slice
	values := []int32{}

	data, err := page.DictDataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, 1, values)
	require.NoError(t, err)
	require.NotZero(t, len(data))
}

func TestDictPageCompress(t *testing.T) {
	page := NewDictPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}

	// Set up some data for the dictionary page
	page.DataTable = &Table{
		Values: []any{int32(1), int32(2), int32(3)},
	}

	data, err := page.DictPageCompress(parquet.CompressionCodec_UNCOMPRESSED, parquet.Type_INT32)
	require.NoError(t, err)
	require.NotZero(t, len(data))
}

func TestDictPageCompressWithEmptyDataTable(t *testing.T) {
	page := NewDictPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}
	page.DataTable = &Table{
		Values: []any{}, // empty values slice
	}

	data, err := page.DictPageCompress(parquet.CompressionCodec_UNCOMPRESSED, parquet.Type_INT32)
	require.NoError(t, err)
	require.NotZero(t, len(data))
}

func TestDictRecToDictPage(t *testing.T) {
	tests := []struct {
		name         string
		setupDictRec func() *DictRecType
		pageSize     int32
		compression  parquet.CompressionCodec
		checkPage    func(t *testing.T, page *Page, totalSize int64)
		expectError  bool
	}{
		{
			name: "populated_dictionary",
			setupDictRec: func() *DictRecType {
				dictRec := NewDictRec(parquet.Type_INT32)
				// Add some test values to the dictionary
				dictRec.DictSlice = append(dictRec.DictSlice, int32(1))
				dictRec.DictSlice = append(dictRec.DictSlice, int32(2))
				dictRec.DictSlice = append(dictRec.DictSlice, int32(3))
				dictRec.DictMap[int32(1)] = 0
				dictRec.DictMap[int32(2)] = 1
				dictRec.DictMap[int32(3)] = 2
				return dictRec
			},
			pageSize:    1024,
			compression: parquet.CompressionCodec_UNCOMPRESSED,
			checkPage: func(t *testing.T, page *Page, totalSize int64) {
				require.Equal(t, parquet.PageType_DICTIONARY_PAGE, page.Header.Type)
				require.Positive(t, totalSize)
			},
		},
		{
			name: "empty_dictionary",
			setupDictRec: func() *DictRecType {
				dictRec := NewDictRec(parquet.Type_INT32)
				// Don't add any values - test empty dictionary
				return dictRec
			},
			pageSize:    1024,
			compression: parquet.CompressionCodec_UNCOMPRESSED,
			checkPage: func(t *testing.T, page *Page, totalSize int64) {
				require.GreaterOrEqual(t, totalSize, int64(0))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dictRec := tt.setupDictRec()

			page, totalSize, err := DictRecToDictPage(dictRec, tt.pageSize, tt.compression)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			require.NotNil(t, page)

			if tt.checkPage != nil {
				tt.checkPage(t, page, totalSize)
			}
		})
	}
}

func TestNewDictRec(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)
	require.NotNil(t, dictRec)
	require.NotNil(t, dictRec.DictMap)
	// DictSlice is not initialized by NewDictRec - it's nil initially
	require.Equal(t, parquet.Type_INT32, dictRec.Type)
}

func TestTableToDictDataPages(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

	// Add some dictionary values
	dictRec.DictSlice = append(dictRec.DictSlice, int32(10))
	dictRec.DictSlice = append(dictRec.DictSlice, int32(20))
	dictRec.DictMap[int32(10)] = 0
	dictRec.DictMap[int32(20)] = 1

	// Create a table with values that reference the dictionary
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:           []any{int32(10), int32(20), int32(10)},
		DefinitionLevels: []int32{0, 0, 0},
		RepetitionLevels: []int32{0, 0, 0},
		Info:             &common.Tag{},
	}

	pages, totalSize, err := TableToDictDataPages(dictRec, table, 1024, 2, parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.NotZero(t, len(pages))
	require.Positive(t, totalSize)
}

func TestTableToDictDataPagesWithEmptyTable(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

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

	pages, totalSize, err := TableToDictDataPages(dictRec, table, 1024, 1, parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.Len(t, pages, 0)
	require.Equal(t, int64(0), totalSize)
}

func TestTableToDictDataPagesWithInvalidType(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

	// Create a table with invalid schema
	table := &Table{
		Schema: &parquet.SchemaElement{
			Name: "test_col",
			// No Type set
		},
		Values:           []any{int32(1)},
		DefinitionLevels: []int32{0},
		RepetitionLevels: []int32{0},
		Info:             &common.Tag{},
	}

	_, _, err := TableToDictDataPages(dictRec, table, 1024, 1, parquet.CompressionCodec_UNCOMPRESSED)
	require.Error(t, err)
}
