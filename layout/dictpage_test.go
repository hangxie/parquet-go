package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

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

func TestNewDictRec(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)
	require.NotNil(t, dictRec)
	require.NotNil(t, dictRec.DictMap)
	// DictSlice is not initialized by NewDictRec - it's nil initially
	require.Equal(t, parquet.Type_INT32, dictRec.Type)
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
