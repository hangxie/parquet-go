package layout

import (
	"math"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestTruncateBinaryBounds(t *testing.T) {
	tests := []struct {
		name     string
		min      []byte
		max      []byte
		length   int
		wantMin  []byte
		wantMax  []byte
		minExact bool
		maxExact bool
	}{
		{"within limit", []byte("abc"), []byte("xyz"), 3, []byte("abc"), []byte("xyz"), true, true},
		{"truncate and round upper bound", []byte("abcdef"), []byte("xyz123"), 3, []byte("abc"), []byte("xy{"), false, false},
		{"disabled", []byte("abcdef"), []byte("xyz123"), 0, []byte("abcdef"), []byte("xyz123"), true, true},
		{"first rune exceeds limit", []byte("ár"), []byte("ár"), 1, []byte("ár"), []byte("ár"), true, true},
		{
			"largest UTF-8 rune cannot be incremented",
			[]byte("\U0010ffffx"),
			[]byte("\U0010ffffx"),
			4,
			[]byte("\U0010ffff"),
			[]byte("\U0010ffffx"),
			false,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max, minExact, maxExact := truncateBinaryBounds(tt.min, tt.max, tt.length)
			require.Equal(t, tt.wantMin, min)
			require.Equal(t, tt.wantMax, max)
			require.Equal(t, tt.minExact, minExact)
			require.Equal(t, tt.maxExact, maxExact)
		})
	}
}

func TestPublicTruncateBinaryBounds(t *testing.T) {
	byteArray := &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY)}
	min, max := TruncateBinaryBounds(byteArray, []byte("abcdef"), []byte("xyz123"), 3)
	require.Equal(t, []byte("abc"), min)
	require.Equal(t, []byte("xy{"), max)

	int32Type := &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)}
	min, max = TruncateBinaryBounds(int32Type, []byte("abcdef"), []byte("xyz123"), 3)
	require.Equal(t, []byte("abcdef"), min)
	require.Equal(t, []byte("xyz123"), max)

	min, max = TruncateBinaryBounds(byteArray, []byte("abcdef"), []byte("xyz123"), 0)
	require.Equal(t, []byte("abcdef"), min)
	require.Equal(t, []byte("xyz123"), max)

	min, max = TruncateBinaryBounds(byteArray, []byte("abc"), []byte("xyz"), 3)
	require.Equal(t, []byte("abc"), min)
	require.Equal(t, []byte("xyz"), max)
}

func TestTruncateBinaryStatistics(t *testing.T) {
	tests := []struct {
		name         string
		schema       *parquet.SchemaElement
		wantMin      []byte
		wantMax      []byte
		wantMinExact bool
		wantMaxExact bool
	}{
		{
			name: "raw byte array",
			schema: &parquet.SchemaElement{
				Type: common.ToPtr(parquet.Type_BYTE_ARRAY),
			},
			wantMin:      []byte("abc"),
			wantMax:      []byte("xy{"),
			wantMinExact: false,
			wantMaxExact: false,
		},
		{
			name: "converted UTF8",
			schema: &parquet.SchemaElement{
				Type:          common.ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: common.ToPtr(parquet.ConvertedType_UTF8),
			},
			wantMin:      []byte("abc"),
			wantMax:      []byte("xy{"),
			wantMinExact: false,
			wantMaxExact: false,
		},
		{
			name: "raw fixed length byte array",
			schema: &parquet.SchemaElement{
				Type: common.ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			},
			wantMin:      []byte("abc"),
			wantMax:      []byte("xy{"),
			wantMinExact: false,
			wantMaxExact: false,
		},
		{
			name: "decimal keeps signed-order bytes",
			schema: &parquet.SchemaElement{
				Type:          common.ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: common.ToPtr(parquet.ConvertedType_DECIMAL),
			},
			wantMin:      []byte("abcdef"),
			wantMax:      []byte("xyz123"),
			wantMinExact: true,
			wantMaxExact: true,
		},
		{
			name: "uuid keeps fixed-width value",
			schema: &parquet.SchemaElement{
				Type:        common.ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			},
			wantMin:      []byte("abcdef"),
			wantMax:      []byte("xyz123"),
			wantMinExact: true,
			wantMaxExact: true,
		},
		{
			name: "json remains a valid document",
			schema: &parquet.SchemaElement{
				Type:        common.ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{JSON: &parquet.JsonType{}},
			},
			wantMin:      []byte("abcdef"),
			wantMax:      []byte("xyz123"),
			wantMinExact: true,
			wantMaxExact: true,
		},
		{
			name: "logical JSON overrides inconsistent UTF8 converted type",
			schema: &parquet.SchemaElement{
				Type:          common.ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: common.ToPtr(parquet.ConvertedType_UTF8),
				LogicalType:   &parquet.LogicalType{JSON: &parquet.JsonType{}},
			},
			wantMin:      []byte("abcdef"),
			wantMax:      []byte("xyz123"),
			wantMinExact: true,
			wantMaxExact: true,
		},
		{
			name: "bson remains a valid document",
			schema: &parquet.SchemaElement{
				Type:          common.ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: common.ToPtr(parquet.ConvertedType_BSON),
			},
			wantMin:      []byte("abcdef"),
			wantMax:      []byte("xyz123"),
			wantMinExact: true,
			wantMaxExact: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := &parquet.Statistics{
				MinValue: []byte("abcdef"),
				MaxValue: []byte("xyz123"),
				Min:      []byte("abcdef"),
				Max:      []byte("xyz123"),
			}
			TruncateBinaryStatistics(stats, tt.schema, 3)
			require.Equal(t, tt.wantMin, stats.MinValue)
			require.Equal(t, tt.wantMax, stats.MaxValue)
			require.Equal(t, tt.wantMinExact, stats.GetIsMinValueExact())
			require.Equal(t, tt.wantMaxExact, stats.GetIsMaxValueExact())
			require.Equal(t, tt.wantMin, stats.Min)
			require.Equal(t, tt.wantMax, stats.Max)
		})
	}

	TruncateBinaryStatistics(nil, nil, 3)
	empty := parquet.NewStatistics()
	TruncateBinaryStatistics(empty, &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY)}, 3)
	require.False(t, empty.IsSetIsMinValueExact())
	require.False(t, empty.IsSetIsMaxValueExact())
}

func TestTruncateUTF8Bounds(t *testing.T) {
	tests := []struct {
		name   string
		schema *parquet.SchemaElement
	}{
		{
			name: "converted UTF8",
			schema: &parquet.SchemaElement{
				Type:          common.ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: common.ToPtr(parquet.ConvertedType_UTF8),
			},
		},
		{
			name: "logical STRING",
			schema: &parquet.SchemaElement{
				Type:        common.ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{STRING: &parquet.StringType{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max := TruncateBinaryBounds(tt.schema, []byte("árvíztűrő"), []byte("árvíztűrő"), 9)
			require.Equal(t, []byte("árvízt"), min)
			require.Equal(t, []byte("árvízu"), max)
			require.True(t, utf8.Valid(min))
			require.True(t, utf8.Valid(max))
		})
	}
}

func TestInvalidUTF8BoundsRemainUntruncated(t *testing.T) {
	schema := &parquet.SchemaElement{
		Type:          common.ToPtr(parquet.Type_BYTE_ARRAY),
		ConvertedType: common.ToPtr(parquet.ConvertedType_UTF8),
	}
	value := []byte{0xff, 0x01, 0x02}
	min, max := TruncateBinaryBounds(schema, value, value, 2)
	require.Equal(t, value, min)
	require.Equal(t, value, max)
}

func TestTruncateRawMalformedUTF8Bounds(t *testing.T) {
	tests := []struct {
		name         string
		physicalType parquet.Type
		value        []byte
		wantMin      []byte
		wantMax      []byte
	}{
		{"increment", parquet.Type_BYTE_ARRAY, []byte{0xff, 0x01, 0x02}, []byte{0xff, 0x01}, []byte{0xff, 0x02}},
		{"carry", parquet.Type_BYTE_ARRAY, []byte{0x01, 0xff, 0x03}, []byte{0x01, 0xff}, []byte{0x02, 0x00}},
		{"byte array overflow", parquet.Type_BYTE_ARRAY, []byte{0xff, 0xff, 0x03}, []byte{0xff, 0xff}, []byte{0xff, 0xff, 0x03}},
		{"fixed length overflow", parquet.Type_FIXED_LEN_BYTE_ARRAY, []byte{0xff, 0xff, 0x03}, []byte{0xff, 0xff}, []byte{0xff, 0xff, 0x03}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &parquet.SchemaElement{Type: common.ToPtr(tt.physicalType)}
			min, max := TruncateBinaryBounds(schema, tt.value, tt.value, 2)
			require.Equal(t, tt.wantMin, min)
			require.Equal(t, tt.wantMax, max)
		})
	}
}

func TestTruncateRawOverflowStatistics(t *testing.T) {
	for _, physicalType := range []parquet.Type{parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY} {
		t.Run(physicalType.String(), func(t *testing.T) {
			value := []byte{0xff, 0xff, 0x03}
			stats := &parquet.Statistics{MinValue: value, MaxValue: value}
			TruncateBinaryStatistics(stats, &parquet.SchemaElement{Type: common.ToPtr(physicalType)}, 2)
			require.Equal(t, []byte{0xff, 0xff}, stats.MinValue)
			require.Equal(t, value, stats.MaxValue)
			require.False(t, stats.GetIsMinValueExact())
			require.True(t, stats.GetIsMaxValueExact())
		})
	}
}

func TestTruncateRawByteArrayDoesNotApplyUTF8Rules(t *testing.T) {
	schema := &parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY)}
	min, max := TruncateBinaryBounds(schema, []byte("ár"), []byte("ár"), 1)
	require.Equal(t, []byte{0xc3}, min)
	require.Equal(t, []byte{0xc4}, max)
}

func TestDictionaryDistinctCount(t *testing.T) {
	dictPage := func(numValues int32) *Page {
		page := NewDictPage()
		page.Header.DictionaryPageHeader = &parquet.DictionaryPageHeader{NumValues: numValues}
		return page
	}
	floatDictPage := func(values ...any) *Page {
		page := dictPage(int32(len(values)))
		page.DataTable = &Table{Values: values}
		return page
	}
	dataPage := func(encoding parquet.Encoding) *Page {
		page := NewDataPage()
		page.Header.DataPageHeader = &parquet.DataPageHeader{Encoding: encoding}
		return page
	}
	dataPageV2 := func(encoding parquet.Encoding) *Page {
		page := NewDataPage()
		page.Header.DataPageHeader = nil
		page.Header.DataPageHeaderV2 = &parquet.DataPageHeaderV2{Encoding: encoding}
		return page
	}

	tests := []struct {
		name          string
		pages         []*Page
		physicalType  parquet.Type
		convertedType *parquet.ConvertedType
		logicalType   *parquet.LogicalType
		expected      *int64
	}{
		{
			name:     "all-dictionary-encoded-pages",
			pages:    []*Page{dictPage(3), dataPage(parquet.Encoding_RLE_DICTIONARY), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			expected: common.ToPtr(int64(3)),
		},
		{
			name:     "all-dictionary-encoded-v2-pages",
			pages:    []*Page{dictPage(7), dataPageV2(parquet.Encoding_RLE_DICTIONARY)},
			expected: common.ToPtr(int64(7)),
		},
		{
			name:     "empty-dictionary",
			pages:    []*Page{dictPage(0)},
			expected: common.ToPtr(int64(0)),
		},
		{
			name:     "plain-fallback-page",
			pages:    []*Page{dictPage(3), dataPage(parquet.Encoding_RLE_DICTIONARY), dataPage(parquet.Encoding_PLAIN)},
			expected: nil,
		},
		{
			name:     "plain-fallback-v2-page",
			pages:    []*Page{dictPage(3), dataPageV2(parquet.Encoding_PLAIN)},
			expected: nil,
		},
		{
			name:     "no-pages",
			pages:    nil,
			expected: nil,
		},
		{
			name:     "nil-dictionary-page",
			pages:    []*Page{nil, dataPage(parquet.Encoding_RLE_DICTIONARY)},
			expected: nil,
		},
		{
			name:     "missing-dictionary-page-header",
			pages:    []*Page{{Header: parquet.NewPageHeader()}, dataPage(parquet.Encoding_RLE_DICTIONARY)},
			expected: nil,
		},
		{
			name:     "nil-data-page",
			pages:    []*Page{dictPage(3), nil},
			expected: nil,
		},
		{
			name:     "data-page-without-header",
			pages:    []*Page{dictPage(3), {}},
			expected: nil,
		},
		{
			name:     "data-page-without-data-page-header",
			pages:    []*Page{dictPage(3), {Header: parquet.NewPageHeader()}},
			expected: nil,
		},
		{
			name:         "float-dictionary-without-nan",
			pages:        []*Page{floatDictPage(float32(1), float32(2)), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType: parquet.Type_FLOAT,
			expected:     common.ToPtr(int64(2)),
		},
		{
			name:         "float-dictionary-with-nan",
			pages:        []*Page{floatDictPage(float32(1), float32(math.NaN()), float32(math.NaN())), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType: parquet.Type_FLOAT,
			expected:     nil,
		},
		{
			name:         "double-dictionary-with-nan",
			pages:        []*Page{floatDictPage(1.0, math.NaN()), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType: parquet.Type_DOUBLE,
			expected:     nil,
		},
		{
			name:         "double-dictionary-without-values",
			pages:        []*Page{dictPage(2), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType: parquet.Type_DOUBLE,
			expected:     nil,
		},
		{
			name:         "nan-bits-in-a-non-float-dictionary",
			pages:        []*Page{floatDictPage(math.NaN()), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType: parquet.Type_INT64,
			expected:     common.ToPtr(int64(1)),
		},
		{
			name:          "byte-array-decimal-dictionary",
			pages:         []*Page{dictPage(3), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType:  parquet.Type_BYTE_ARRAY,
			convertedType: common.ToPtr(parquet.ConvertedType_DECIMAL),
			expected:      nil,
		},
		{
			name:         "byte-array-decimal-logical-dictionary",
			pages:        []*Page{dictPage(3), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType: parquet.Type_BYTE_ARRAY,
			logicalType:  &parquet.LogicalType{DECIMAL: parquet.NewDecimalType()},
			expected:     nil,
		},
		{
			name:          "byte-array-utf8-dictionary",
			pages:         []*Page{dictPage(3), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType:  parquet.Type_BYTE_ARRAY,
			convertedType: common.ToPtr(parquet.ConvertedType_UTF8),
			expected:      common.ToPtr(int64(3)),
		},
		{
			name:          "fixed-len-byte-array-decimal-dictionary",
			pages:         []*Page{dictPage(3), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType:  parquet.Type_FIXED_LEN_BYTE_ARRAY,
			convertedType: common.ToPtr(parquet.ConvertedType_DECIMAL),
			expected:      common.ToPtr(int64(3)),
		},
		{
			name:         "float16-dictionary",
			pages:        []*Page{dictPage(2), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType: parquet.Type_FIXED_LEN_BYTE_ARRAY,
			logicalType:  &parquet.LogicalType{FLOAT16: parquet.NewFloat16Type()},
			expected:     nil,
		},
		{
			name:         "geometry-dictionary",
			pages:        []*Page{dictPage(2), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType: parquet.Type_BYTE_ARRAY,
			logicalType:  &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()},
			expected:     nil,
		},
		{
			name:         "geography-dictionary",
			pages:        []*Page{dictPage(2), dataPage(parquet.Encoding_RLE_DICTIONARY)},
			physicalType: parquet.Type_BYTE_ARRAY,
			logicalType:  &parquet.LogicalType{GEOGRAPHY: parquet.NewGeographyType()},
			expected:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			physicalType := tt.physicalType
			if physicalType == 0 {
				physicalType = parquet.Type_INT32
			}
			require.Equal(t, tt.expected, dictionaryDistinctCount(tt.pages, &physicalType, tt.convertedType, tt.logicalType))
		})
	}
}
