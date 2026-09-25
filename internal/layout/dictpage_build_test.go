package layout

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

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

func TestTableToDictDataPagesWithOption(t *testing.T) {
	testCases := []struct {
		name            string
		dataPageVersion int32
		writeCRC        bool
		expectCRC       bool
		expectedType    parquet.PageType
	}{
		{
			name:         "v1_without_crc",
			expectedType: parquet.PageType_DATA_PAGE,
		},
		{
			name:         "v1_with_crc",
			writeCRC:     true,
			expectCRC:    true,
			expectedType: parquet.PageType_DATA_PAGE,
		},
		{
			name:            "v2_without_crc",
			dataPageVersion: 2,
			expectedType:    parquet.PageType_DATA_PAGE_V2,
		},
		{
			name:            "v2_with_crc",
			dataPageVersion: 2,
			writeCRC:        true,
			expectCRC:       true,
			expectedType:    parquet.PageType_DATA_PAGE_V2,
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
				PageSize:        1024,
				CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
				DataPageVersion: tc.dataPageVersion,
				WriteCRC:        tc.writeCRC,
			}

			pages, totalSize, err := TableToDictDataPagesWithOption(dictRec, table, opt)
			require.NoError(t, err)
			require.NotEmpty(t, pages)
			require.Positive(t, totalSize)

			for _, page := range pages {
				require.NotEmpty(t, page.RawData)
				require.Equal(t, tc.expectedType, page.Header.Type)
				if tc.dataPageVersion == 2 {
					require.Equal(t, parquet.Encoding_RLE_DICTIONARY, page.Header.DataPageHeaderV2.Encoding)
					require.Equal(t, int32(3), page.Header.DataPageHeaderV2.NumValues)
					require.Zero(t, page.Header.DataPageHeaderV2.NumNulls)
					require.Equal(t, int32(3), page.Header.DataPageHeaderV2.NumRows)
					require.Positive(t, page.Header.DataPageHeaderV2.DefinitionLevelsByteLength)
				} else {
					require.Equal(t, parquet.Encoding_RLE_DICTIONARY, page.Header.DataPageHeader.Encoding)
				}
				if tc.expectCRC {
					require.True(t, page.Header.IsSetCrc(), "expected CRC to be set")
				} else {
					require.False(t, page.Header.IsSetCrc(), "expected CRC not to be set")
				}
			}
		})
	}
}

func TestTableToDictDataPagesWithOption_EmptyTable(t *testing.T) {
	table := &Table{
		Schema: &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)},
		Values: []any{},
	}
	pages, totalSize, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_INT32), table, PageWriteOption{PageSize: 1024})
	require.NoError(t, err)
	require.Empty(t, pages)
	require.Zero(t, totalSize)
}

func TestTableToDictDataPagesWithOption_ScanError(t *testing.T) {
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		Values:             []any{nil}, // nil for a REQUIRED field triggers a scan error
		DefinitionLevels:   []int32{0},
		MaxDefinitionLevel: 0,
		Info:               &common.Tag{},
	}
	_, _, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_INT32), table, PageWriteOption{
		PageSize:     1024,
		CompressType: parquet.CompressionCodec_UNCOMPRESSED,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "scan dict page values")
}

func TestTableToDictDataPagesWithOption_PlainFallback(t *testing.T) {
	dictTag, err := common.StringToTag("name=value, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY")
	require.NoError(t, err)
	newTable := func() *Table {
		return &Table{
			Schema: &parquet.SchemaElement{
				Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
				RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
			},
			Values:             []any{"alpha", "bravo", "charlie"},
			DefinitionLevels:   []int32{0, 0, 0},
			RepetitionLevels:   []int32{0, 0, 0},
			MaxDefinitionLevel: 0,
			Info:               dictTag,
		}
	}

	tests := []struct {
		name          string
		pageSize      int32
		wantDictSize  int
		wantEncodings []parquet.Encoding
	}{
		{
			name:          "fallback_at_page_boundary",
			pageSize:      5,
			wantDictSize:  1,
			wantEncodings: []parquet.Encoding{parquet.Encoding_RLE_DICTIONARY, parquet.Encoding_PLAIN, parquet.Encoding_PLAIN},
		},
		{
			name:          "rollback_partial_dictionary_page",
			pageSize:      1024,
			wantDictSize:  0,
			wantEncodings: []parquet.Encoding{parquet.Encoding_PLAIN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dictRec := NewDictRecWithLimit(parquet.Type_BYTE_ARRAY, 10)
			pages, totalSize, err := TableToDictDataPagesWithOption(dictRec, newTable(), PageWriteOption{
				PageSize:     tt.pageSize,
				CompressType: parquet.CompressionCodec_UNCOMPRESSED,
			})
			require.NoError(t, err)
			require.Positive(t, totalSize)
			require.Len(t, dictRec.DictSlice, tt.wantDictSize)
			require.Len(t, pages, len(tt.wantEncodings))
			for i, encoding := range tt.wantEncodings {
				require.Equal(t, encoding, pages[i].Header.DataPageHeader.Encoding)
			}

			morePages, _, err := TableToDictDataPagesWithOption(dictRec, newTable(), PageWriteOption{
				PageSize:     tt.pageSize,
				CompressType: parquet.CompressionCodec_UNCOMPRESSED,
			})
			require.NoError(t, err)
			for _, page := range morePages {
				require.Equal(t, parquet.Encoding_PLAIN, page.Header.DataPageHeader.Encoding)
			}

			_, _, err = TableToDictDataPagesWithOption(dictRec, newTable(), PageWriteOption{
				PageSize:     tt.pageSize,
				CompressType: parquet.CompressionCodec(9999),
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "build plain fallback pages")
		})
	}
}

func TestFinalizeDictDataPagesWithOption(t *testing.T) {
	dictTag, err := common.StringToTag("name=value, type=INT32, encoding=PLAIN_DICTIONARY")
	require.NoError(t, err)
	tests := []struct {
		name   string
		values []any
		defs   []int32
		maxDef int32
		width  int32
	}{
		{
			name:   "values",
			values: []any{int32(1), int32(2)},
			defs:   []int32{0, 0},
			width:  1,
		},
		{
			name:   "all_null",
			values: []any{nil},
			defs:   []int32{0},
			maxDef: 1,
			width:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := &Table{
				Schema: &parquet.SchemaElement{
					Type:           common.ToPtr(parquet.Type_INT32),
					RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
				},
				Values:             tt.values,
				DefinitionLevels:   tt.defs,
				RepetitionLevels:   make([]int32, len(tt.values)),
				MaxDefinitionLevel: tt.maxDef,
				Info:               dictTag,
			}
			pages, _, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_INT32), table, PageWriteOption{
				PageSize:     1024,
				CompressType: parquet.CompressionCodec_UNCOMPRESSED,
			})
			require.NoError(t, err)
			require.NotNil(t, pages[0].DataTable)

			err = FinalizeDictDataPagesWithOption(
				append([]*Page{nil}, pages...),
				tt.width,
				PageWriteOption{CompressType: parquet.CompressionCodec_UNCOMPRESSED},
			)
			require.NoError(t, err)
			require.Nil(t, pages[0].DataTable)
			require.Nil(t, pages[0].dictionaryIndices)
		})
	}
}

func TestFinalizeDictDataPagesWithOption_CompressError(t *testing.T) {
	for _, version := range []int32{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			table := &Table{
				Schema:             &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)},
				Values:             []any{int32(1)},
				DefinitionLevels:   []int32{0},
				RepetitionLevels:   []int32{0},
				MaxDefinitionLevel: 0,
				MaxRepetitionLevel: 0,
				Info:               &common.Tag{},
				RepetitionType:     parquet.FieldRepetitionType_REQUIRED,
			}
			pages, _, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_INT32), table, PageWriteOption{
				PageSize:        1024,
				CompressType:    parquet.CompressionCodec_UNCOMPRESSED,
				DataPageVersion: version,
			})
			require.NoError(t, err)

			err = FinalizeDictDataPagesWithOption(pages, 1, PageWriteOption{
				CompressType:    parquet.CompressionCodec(9999),
				DataPageVersion: version,
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "compress dict data page")
		})
	}
}

func TestDictDataPageCompress_RepetitionLevels(t *testing.T) {
	page := NewDataPage()
	page.DataTable = &Table{
		DefinitionLevels:   []int32{1, 1},
		RepetitionLevels:   []int32{0, 1},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 1,
	}
	page.Schema = &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)}

	compressedData, err := page.dictDataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, 2, []int32{0, 1}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, compressedData)
}

func TestDictDataPageCompress_ByteArrayStats(t *testing.T) {
	page := NewDataPage()
	page.DataTable = &Table{
		DefinitionLevels:   []int32{1, 1},
		RepetitionLevels:   []int32{0, 0},
		MaxDefinitionLevel: 1,
	}
	page.Schema = &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY)}
	page.MaxVal = "zzz"
	page.MinVal = "aaa"

	compressedData, err := page.dictDataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, 1, []int32{0, 1}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, compressedData)
	// BYTE_ARRAY stats strip the 4-byte length prefix. Its sort order is
	// UNSIGNED, so only MinValue/MaxValue are written; the deprecated (signed)
	// Min/Max fields are omitted (PARQUET-251).
	require.Equal(t, []byte("zzz"), page.Header.DataPageHeader.Statistics.MaxValue)
	require.Equal(t, []byte("aaa"), page.Header.DataPageHeader.Statistics.MinValue)
	require.Nil(t, page.Header.DataPageHeader.Statistics.Max)
	require.Nil(t, page.Header.DataPageHeader.Statistics.Min)
}

func TestDictDataPageCompress_CompressError(t *testing.T) {
	page := NewDataPage()
	page.DataTable = &Table{
		DefinitionLevels:   []int32{1},
		RepetitionLevels:   []int32{0},
		MaxDefinitionLevel: 1,
	}
	page.Schema = &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)}

	_, err := page.dictDataPageCompress(parquet.CompressionCodec(9999), 1, []int32{0}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "compress dict data")
}

func TestTableToDictDataPagesWithOption_FixedLenByteArrayWidth(t *testing.T) {
	testCases := map[string]struct {
		values []any
		errMsg string
	}{
		"exact":     {[]any{"abcd", "efgh"}, ""},
		"too-short": {[]any{"abcd", "ef"}, "value of length 2 does not match column length 4"},
		"too-long":  {[]any{"abcdef", "abcd"}, "value of length 6 does not match column length 4"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			table := &Table{
				Schema: &parquet.SchemaElement{
					Type:       common.ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
					TypeLength: common.ToPtr(int32(4)),
					Name:       "flba_col",
				},
				Path:             []string{"root", "flba_col"},
				Values:           tc.values,
				DefinitionLevels: []int32{0, 0},
				RepetitionLevels: []int32{0, 0},
				Info:             &common.Tag{},
			}
			_, _, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_FIXED_LEN_BYTE_ARRAY), table, PageWriteOption{
				PageSize:     1024,
				CompressType: parquet.CompressionCodec_UNCOMPRESSED,
			})
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}
