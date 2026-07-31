package layout

import (
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
