package layout

import (
	"encoding/binary"
	"fmt"
	"math"
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

func TestDictRecToDictPageWithOption_EncodeError(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)
	dictRec.DictSlice = []any{"wrong type"}

	_, _, err := DictRecToDictPageWithOption(dictRec, PageWriteOption{
		CompressType: parquet.CompressionCodec_UNCOMPRESSED,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "compress dictionary page")
}

func TestScanDictPageValues_RequiredNil(t *testing.T) {
	table := &Table{
		Schema: &parquet.SchemaElement{
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		Values:             []any{nil},
		DefinitionLevels:   []int32{0},
		MaxDefinitionLevel: 0,
	}
	dictRec := NewDictRec(parquet.Type_INT32)
	funcTable, _ := common.FindFuncTable(common.ToPtr(parquet.Type_INT32), nil, nil)

	_, err := scanDictPageValues(table, dictRec, 0, 1024, false, funcTable)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil value encountered for REQUIRED field")
}

func TestLookupOrInsert_ExistingValue(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)
	idx1 := dictRec.lookupOrInsert(int32(42))
	idx2 := dictRec.lookupOrInsert(int32(42)) // already present
	require.Equal(t, idx1, idx2)
	require.Len(t, dictRec.DictSlice, 1)

	idx3, ok, err := dictRec.tryLookupOrInsert(int32(42))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, idx1, idx3)

	_, ok, err = NewDictRec(parquet.Type_INT32).tryLookupOrInsert("wrong type")
	require.Error(t, err)
	require.False(t, ok)
}

func TestScanDictPageValues_OptionalNull(t *testing.T) {
	table := &Table{
		Schema: &parquet.SchemaElement{
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
		},
		Values:             []any{int32(1), nil, int32(1)},
		DefinitionLevels:   []int32{1, 0, 1},
		MaxDefinitionLevel: 1,
	}
	dictRec := NewDictRec(parquet.Type_INT32)
	funcTable, _ := common.FindFuncTable(common.ToPtr(parquet.Type_INT32), nil, nil)

	res, err := scanDictPageValues(table, dictRec, 0, 1024, false, funcTable)
	require.NoError(t, err)
	require.Equal(t, int32(2), res.numValues)
	require.Equal(t, int64(1), res.nullCount)
	// The repeated value 1 must reuse the same dictionary index.
	require.Len(t, dictRec.DictSlice, 1)
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

func TestDictPageCompress_CompressError(t *testing.T) {
	page := NewDataPage()
	page.DataTable = &Table{Values: []any{int32(1)}}
	page.Schema = &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)}

	_, err := page.dictPageCompress(parquet.CompressionCodec(9999), parquet.Type_INT32, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "compress dictionary buffer")
}

func TestScanDictPageValues_OmitStats(t *testing.T) {
	table := &Table{
		Schema: &parquet.SchemaElement{
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
		},
		Values:             []any{int32(1), int32(2)},
		DefinitionLevels:   []int32{1, 1},
		MaxDefinitionLevel: 1,
	}
	dictRec := NewDictRec(parquet.Type_INT32)
	funcTable, _ := common.FindFuncTable(common.ToPtr(parquet.Type_INT32), nil, nil)

	res, err := scanDictPageValues(table, dictRec, 0, 1024, true, funcTable)
	require.NoError(t, err)
	require.Equal(t, int32(2), res.numValues)
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

// wkbPointAt spells a little-endian 2D WKB point.
func wkbPointAt(x, y float64) string {
	b := []byte{0x01}
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(x))
	return string(binary.LittleEndian.AppendUint64(b, math.Float64bits(y)))
}

func geospatialTable(values []any) *Table {
	defLevels := make([]int32, len(values))
	repLevels := make([]int32, len(values))
	for i := range defLevels {
		defLevels[i] = 1
	}
	return &Table{
		Schema: &parquet.SchemaElement{
			Type:        common.ToPtr(parquet.Type_BYTE_ARRAY),
			Name:        "geo",
			LogicalType: &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()},
		},
		Values:             values,
		DefinitionLevels:   defLevels,
		RepetitionLevels:   repLevels,
		MaxDefinitionLevel: 1,
		Info:               &common.Tag{},
	}
}

// TestDictPageGeospatialStatisticsMatchPlain pins that statistics do not depend on encoding.
func TestDictPageGeospatialStatisticsMatchPlain(t *testing.T) {
	// A dictionary page left DataTable.Values nil and never measured them, so this column
	// carried no box and no type list, and min/max the plain path withholds.
	values := []any{wkbPointAt(-170, -80), wkbPointAt(10.5, 20.3), wkbPointAt(30, 40)}
	opt := PageWriteOption{PageSize: 1024, CompressType: parquet.CompressionCodec_UNCOMPRESSED}

	dictPages, _, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_BYTE_ARRAY), geospatialTable(values), opt)
	require.NoError(t, err)
	require.NotEmpty(t, dictPages)

	plainPages, _, err := TableToDataPagesWithOption(geospatialTable(values), opt)
	require.NoError(t, err)
	require.NotEmpty(t, plainPages)

	dictBBox, dictTypes := aggregateGeospatialStatistics(dictPages)
	plainBBox, plainTypes := aggregateGeospatialStatistics(plainPages)

	require.NotNil(t, dictBBox)
	require.Equal(t, plainBBox, dictBBox)
	require.Equal(t, plainTypes, dictTypes)
	require.Equal(t, -170.0, dictBBox.Xmin)
	require.Equal(t, 30.0, dictBBox.Xmax)

	// A geospatial column carries no min/max, whichever encoding wrote it.
	for _, page := range dictPages {
		require.Nil(t, page.MinVal)
		require.Nil(t, page.MaxVal)
	}
}

// TestDictPageGeospatialUnreadableWithdrawsBounds covers a dictionary page holding a value
// the walk cannot read. The rule #437 set for plain pages applies here too: the box is
// withdrawn rather than drawn around the values that were readable.
func TestDictPageGeospatialUnreadableWithdrawsBounds(t *testing.T) {
	values := []any{wkbPointAt(-170, -80), "not-wkb-at-all"}
	opt := PageWriteOption{PageSize: 1024, CompressType: parquet.CompressionCodec_UNCOMPRESSED}

	pages, _, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_BYTE_ARRAY), geospatialTable(values), opt)
	require.NoError(t, err)
	require.NotEmpty(t, pages)

	bbox, _ := aggregateGeospatialStatistics(pages)
	require.Nil(t, bbox)
	for _, page := range pages {
		require.True(t, page.GeospatialBoundsUnknown)
	}
}

// TestDictPageGeospatialAcrossDictionaryFallback covers the transition #438 describes.
func TestDictPageGeospatialAcrossDictionaryFallback(t *testing.T) {
	// The dictionary fills part way through, so the chunk holds dictionary pages followed by
	// the plain pages that took over, and only the plain ones used to measure their values.
	// A far-away first point, so a box built from the rest would visibly exclude it.
	values := []any{wkbPointAt(-170, -80), wkbPointAt(10.5, 20.3), wkbPointAt(30, 40)}

	// One encoded value is 25 bytes, so the limit admits the first and refuses the second,
	// and a one-byte page budget ends the page before that happens.
	dictRec := NewDictRecWithLimit(parquet.Type_BYTE_ARRAY, 30)
	opt := PageWriteOption{PageSize: 1, CompressType: parquet.CompressionCodec_UNCOMPRESSED}

	pages, _, err := TableToDictDataPagesWithOption(dictRec, geospatialTable(values), opt)
	require.NoError(t, err)
	require.Greater(t, len(pages), 1, "expected a dictionary page and plain fallback pages")
	require.True(t, dictRec.full, "the dictionary should have filled part way through")

	bbox, geoTypes := aggregateGeospatialStatistics(pages)
	require.NotNil(t, bbox)
	require.Equal(t, []int32{1}, geoTypes)
	require.Equal(t, -170.0, bbox.Xmin, "the box must cover the value on the dictionary page")
	require.Equal(t, -80.0, bbox.Ymin)
	require.Equal(t, 30.0, bbox.Xmax)
	require.Equal(t, 40.0, bbox.Ymax)
}

// TestDictPageIntervalHasNoMinMax covers the other annotation setPageStats withholds min/max
// for. The specification leaves INTERVAL's sort order undefined, so neither encoding writes
// bounds for it; the dictionary path used to write them from the raw bytes.
func TestDictPageIntervalHasNoMinMax(t *testing.T) {
	interval := func(b ...byte) string {
		v := make([]byte, 12)
		copy(v, b)
		return string(v)
	}
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type:          common.ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			TypeLength:    common.ToPtr(int32(12)),
			Name:          "iv",
			ConvertedType: common.ToPtr(parquet.ConvertedType_INTERVAL),
		},
		Values:             []any{interval(1), interval(2), interval(3)},
		DefinitionLevels:   []int32{1, 1, 1},
		RepetitionLevels:   []int32{0, 0, 0},
		MaxDefinitionLevel: 1,
		Info:               &common.Tag{},
	}
	opt := PageWriteOption{PageSize: 1024, CompressType: parquet.CompressionCodec_UNCOMPRESSED}

	dictPages, _, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_FIXED_LEN_BYTE_ARRAY), table, opt)
	require.NoError(t, err)
	require.NotEmpty(t, dictPages)
	for _, page := range dictPages {
		require.Nil(t, page.MinVal)
		require.Nil(t, page.MaxVal)
		require.NotNil(t, page.NullCount)
	}
}

// byteArrayTable builds a flat BYTE_ARRAY column over values.
func dictByteArrayTable(values []any) *Table {
	defLevels := make([]int32, len(values))
	for i, v := range values {
		if v != nil {
			defLevels[i] = 1
		}
	}
	return &Table{
		Schema:             &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY), Name: "s"},
		Values:             values,
		DefinitionLevels:   defLevels,
		RepetitionLevels:   make([]int32, len(values)),
		MaxDefinitionLevel: 1,
		Info:               &common.Tag{},
	}
}

func sumPageByteArrayBytes(t *testing.T, pages []*Page) int64 {
	t.Helper()
	var sum int64
	for _, page := range pages {
		require.NotNil(t, page.UnencodedByteArrayDataBytes, "every data page reports its own total")
		sum += *page.UnencodedByteArrayDataBytes
	}
	return sum
}

// TestDictPageUnencodedByteArrayDataBytes covers the statistic a dictionary page used to
// measure over a nil DataTable.Values and publish as a confident zero.
func TestDictPageUnencodedByteArrayDataBytes(t *testing.T) {
	tests := []struct {
		name      string
		values    []any
		pageSize  int32
		pageVer   int32
		wantTotal int64
	}{
		// The spec's own example: the count is per occurrence, not per dictionary entry,
		// so a repeated value is counted each time it appears.
		{"repeated value", []any{"a", "a", "bc", "cde"}, 1024, 0, 7},
		{"distinct values", []any{"aaaa", "bbbb", "cccc"}, 1024, 0, 12},
		{"nulls are not counted", []any{"aaaa", nil, "bb"}, 1024, 0, 6},
		// A one-byte budget ends every page after a value, so the slice offsets that
		// attribute bytes to a page are exercised rather than assumed.
		{"one value per page", []any{"a", "a", "bc", "cde"}, 1, 0, 7},
		{"data page v2", []any{"a", "a", "bc", "cde"}, 1, 2, 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := PageWriteOption{PageSize: tt.pageSize, CompressType: parquet.CompressionCodec_UNCOMPRESSED, DataPageVersion: tt.pageVer}

			dictPages, _, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_BYTE_ARRAY), dictByteArrayTable(tt.values), opt)
			require.NoError(t, err)
			plainPages, _, err := TableToDataPagesWithOption(dictByteArrayTable(tt.values), opt)
			require.NoError(t, err)

			require.Equal(t, tt.wantTotal, sumPageByteArrayBytes(t, plainPages))
			require.Equal(t, tt.wantTotal, sumPageByteArrayBytes(t, dictPages))
			// Only the chunk total reaches the file, but agreeing page by page is what
			// shows the bytes are attributed to the page that holds them.
			require.Equal(t, len(plainPages), len(dictPages))
			for i := range dictPages {
				require.Equal(t, *plainPages[i].UnencodedByteArrayDataBytes, *dictPages[i].UnencodedByteArrayDataBytes, "page %d", i)
			}
		})
	}
}

// TestDictPageUnencodedByteArrayDataBytesInChunk asserts the field the reader actually sees,
// which aggregateSizeStatistics builds a step after the per-page totals.
func TestDictPageUnencodedByteArrayDataBytesInChunk(t *testing.T) {
	values := []any{"a", "a", "bc", "cde"}
	opt := PageWriteOption{PageSize: 1, CompressType: parquet.CompressionCodec_UNCOMPRESSED}

	dictRec := NewDictRec(parquet.Type_BYTE_ARRAY)
	dataPages, _, err := TableToDictDataPagesWithOption(dictRec, dictByteArrayTable(values), opt)
	require.NoError(t, err)
	require.Greater(t, len(dataPages), 1, "the page budget should have split the column")

	dictPage, _, err := DictRecToDictPageWithOption(dictRec, opt)
	require.NoError(t, err)

	// The dictionary page carries no total of its own and sits outside the window the
	// aggregation reads, so the chunk counts each value's bytes once, not the entries too.
	require.Nil(t, dictPage.UnencodedByteArrayDataBytes)

	chunk, err := PagesToDictChunk(append([]*Page{dictPage}, dataPages...))
	require.NoError(t, err)

	size := chunk.ChunkHeader.MetaData.SizeStatistics
	require.NotNil(t, size)
	require.NotNil(t, size.UnencodedByteArrayDataBytes)
	require.Equal(t, int64(7), *size.UnencodedByteArrayDataBytes)
}

// TestDictPageUnencodedByteArrayDataBytesAcrossFallback covers the seam where the dictionary
// fills part way through, so the chunk holds dictionary pages and then plain ones.
func TestDictPageUnencodedByteArrayDataBytesAcrossFallback(t *testing.T) {
	values := []any{"aaaa", "bbbb", "cccc"}
	opt := PageWriteOption{PageSize: 1, CompressType: parquet.CompressionCodec_UNCOMPRESSED}

	// One encoded value is 8 bytes, so the limit admits the first and refuses the second.
	dictRec := NewDictRecWithLimit(parquet.Type_BYTE_ARRAY, 10)
	pages, _, err := TableToDictDataPagesWithOption(dictRec, dictByteArrayTable(values), opt)
	require.NoError(t, err)
	require.True(t, dictRec.full, "the dictionary should have filled part way through")

	require.Equal(t, int64(12), sumPageByteArrayBytes(t, pages))
}

// TestDictPageUnencodedByteArrayDataBytesRepeated covers a repeated column, where a page
// runs on past its size budget to the next row boundary, so page ranges come out uneven and
// the slice handed to the histogram has to follow them.
func TestDictPageUnencodedByteArrayDataBytesRepeated(t *testing.T) {
	// Two rows: ["a", "bc"] and ["cde"].
	table := func() *Table {
		return &Table{
			Schema:             &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY), Name: "s"},
			Values:             []any{"a", "bc", "cde"},
			DefinitionLevels:   []int32{1, 1, 1},
			RepetitionLevels:   []int32{0, 1, 0},
			MaxDefinitionLevel: 1,
			MaxRepetitionLevel: 1,
			Info:               &common.Tag{},
		}
	}
	opt := PageWriteOption{PageSize: 1, CompressType: parquet.CompressionCodec_UNCOMPRESSED}

	dictPages, _, err := TableToDictDataPagesWithOption(NewDictRec(parquet.Type_BYTE_ARRAY), table(), opt)
	require.NoError(t, err)
	plainPages, _, err := TableToDataPagesWithOption(table(), opt)
	require.NoError(t, err)

	require.Equal(t, int64(6), sumPageByteArrayBytes(t, plainPages))
	require.Equal(t, int64(6), sumPageByteArrayBytes(t, dictPages))
	require.Equal(t, len(plainPages), len(dictPages))
	for i := range dictPages {
		require.Equal(t, *plainPages[i].UnencodedByteArrayDataBytes, *dictPages[i].UnencodedByteArrayDataBytes, "page %d", i)
		require.Equal(t, plainPages[i].NumRows, dictPages[i].NumRows, "page %d rows", i)
	}
}
