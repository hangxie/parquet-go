package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
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

func TestDictPageCompress_ReturnsCompressedData(t *testing.T) {
	page := NewDataPage()
	page.DataTable = &Table{
		Values: []any{int32(1), int32(2), int32(3)},
	}
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
	}

	compressedData, err := page.dictPageCompress(parquet.CompressionCodec_UNCOMPRESSED, parquet.Type_INT32, nil)
	require.NoError(t, err)
	require.NotEmpty(t, compressedData)

	// Header populated, RawData NOT set
	require.Equal(t, parquet.PageType_DICTIONARY_PAGE, page.Header.Type)
	require.Positive(t, page.Header.CompressedPageSize)
	require.Empty(t, page.RawData)
}

func TestDictDataPageCompress_ReturnsCompressedData(t *testing.T) {
	page := NewDataPage()
	page.DataTable = &Table{
		DefinitionLevels:   []int32{1, 1, 1},
		RepetitionLevels:   []int32{0, 0, 0},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
	}
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
	}
	page.MaxVal = int32(3)
	page.MinVal = int32(1)

	compressedData, err := page.dictDataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, 2, []int32{0, 1, 2}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, compressedData)

	// Header populated, RawData NOT set
	require.Equal(t, parquet.PageType_DATA_PAGE, page.Header.Type)
	require.Positive(t, page.Header.CompressedPageSize)
	require.Empty(t, page.RawData)
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

func TestDictDataPageCompress_WithLevelsAndStatistics(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = &common.Tag{}
	page.MaxVal = int32(100)
	page.MinVal = int32(10)
	nullCount := int64(1)
	page.NullCount = &nullCount

	page.DataTable = &Table{
		DefinitionLevels:   []int32{1, 0, 1, 1},
		RepetitionLevels:   []int32{0, 0, 1, 0},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 1,
	}

	values := []int32{0, 1, 2}

	data, err := page.DictDataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, 2, values)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	require.NotNil(t, page.Header.DataPageHeader.Statistics)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.Max)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.Min)
	require.NotNil(t, page.Header.DataPageHeader.Statistics.NullCount)
	require.Equal(t, int64(1), *page.Header.DataPageHeader.Statistics.NullCount)
}

func TestTableToDictDataPages_WithNulls(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

	dictRec.DictSlice = append(dictRec.DictSlice, int32(10))
	dictRec.DictSlice = append(dictRec.DictSlice, int32(20))
	dictRec.DictMap[int32(10)] = 0
	dictRec.DictMap[int32(20)] = 1

	table := &Table{
		Schema: &parquet.SchemaElement{
			Type:           common.ToPtr(parquet.Type_INT32),
			Name:           "test_col",
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
		},
		Values:             []any{int32(10), nil, int32(20)},
		DefinitionLevels:   []int32{1, 0, 1},
		RepetitionLevels:   []int32{0, 0, 0},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
		Info:               &common.Tag{},
	}

	pages, totalSize, err := TableToDictDataPages(dictRec, table, 1024, 2, parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.NotEmpty(t, pages)
	require.Positive(t, totalSize)

	// Verify null count was tracked
	require.NotNil(t, pages[0].NullCount)
	require.Equal(t, int64(1), *pages[0].NullCount)
}

func TestTableToDictDataPages_MultiplePages(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

	dictRec.DictSlice = append(dictRec.DictSlice, int32(1))
	dictRec.DictSlice = append(dictRec.DictSlice, int32(2))
	dictRec.DictMap[int32(1)] = 0
	dictRec.DictMap[int32(2)] = 1

	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:             []any{int32(1), int32(2), int32(1), int32(2)},
		DefinitionLevels:   []int32{0, 0, 0, 0},
		RepetitionLevels:   []int32{0, 0, 0, 0},
		MaxDefinitionLevel: 0,
		MaxRepetitionLevel: 0,
		Info:               &common.Tag{},
	}

	// Use tiny pageSize (4 bytes) to force multiple pages
	pages, totalSize, err := TableToDictDataPages(dictRec, table, 4, 2, parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.Greater(t, len(pages), 1)
	require.Positive(t, totalSize)
}

func TestTableToDictDataPages_OmitStats(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

	dictRec.DictSlice = append(dictRec.DictSlice, int32(10))
	dictRec.DictMap[int32(10)] = 0

	info := &common.Tag{}
	info.OmitStats = true

	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:             []any{int32(10), int32(10)},
		DefinitionLevels:   []int32{0, 0},
		RepetitionLevels:   []int32{0, 0},
		MaxDefinitionLevel: 0,
		MaxRepetitionLevel: 0,
		Info:               info,
	}

	pages, totalSize, err := TableToDictDataPages(dictRec, table, 1024, 2, parquet.CompressionCodec_UNCOMPRESSED)
	require.NoError(t, err)
	require.NotEmpty(t, pages)
	require.Positive(t, totalSize)

	// OmitStats: MaxVal, MinVal, NullCount should not be set on the page
	require.Nil(t, pages[0].MaxVal)
	require.Nil(t, pages[0].MinVal)
	require.Nil(t, pages[0].NullCount)
}

func TestTableToDictDataPagesWithOption(t *testing.T) {
	testCases := []struct {
		name      string
		writeCRC  bool
		expectCRC bool
	}{
		{
			name:      "without_crc",
			writeCRC:  false,
			expectCRC: false,
		},
		{
			name:      "with_crc",
			writeCRC:  true,
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
			dictRec := NewDictRec(parquet.Type_INT32)

			opt := PageWriteOption{
				PageSize:     1024,
				CompressType: parquet.CompressionCodec_UNCOMPRESSED,
				WriteCRC:     tc.writeCRC,
			}

			pages, totalSize, err := TableToDictDataPagesWithOption(dictRec, table, 2, opt)
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

func TestDictRecToDictPageWithOption(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)
	dictRec.DictSlice = []any{int32(1), int32(2), int32(3)}
	dictRec.DictMap = map[any]int32{int32(1): 0, int32(2): 1, int32(3): 2}

	testCases := []struct {
		name      string
		writeCRC  bool
		expectCRC bool
	}{
		{"without_crc", false, false},
		{"with_crc", true, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opt := PageWriteOption{
				PageSize:     1024,
				CompressType: parquet.CompressionCodec_UNCOMPRESSED,
				WriteCRC:     tc.writeCRC,
			}
			page, totalSize, err := DictRecToDictPageWithOption(dictRec, opt)
			require.NoError(t, err)
			require.NotNil(t, page)
			require.Positive(t, totalSize)
			require.NotEmpty(t, page.RawData)

			if tc.expectCRC {
				require.True(t, page.Header.IsSetCrc())
			} else {
				require.False(t, page.Header.IsSetCrc())
			}
		})
	}
}
