package schema

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func typePtr(t parquet.Type) *parquet.Type { return &t }
func int32Ptr(v int32) *int32              { return &v }
func convertedTypePtr(t parquet.ConvertedType) *parquet.ConvertedType {
	return &t
}

func int32Bytes(v int32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))
	return b
}

func int64Bytes(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func float32Bytes(v float32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, math.Float32bits(v))
	return b
}

func float64Bytes(v float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(v))
	return b
}

func TestDecodeStatisticsMinMax(t *testing.T) {
	tests := []struct {
		name    string
		se      *parquet.SchemaElement
		stats   *parquet.Statistics
		wantMin any
		wantMax any
		wantErr bool
	}{
		{
			name:    "nil statistics",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_INT32)},
			stats:   nil,
			wantMin: nil,
			wantMax: nil,
		},
		{
			name:    "nil schema element",
			se:      nil,
			stats:   &parquet.Statistics{MinValue: int32Bytes(1), MaxValue: int32Bytes(9)},
			wantMin: nil,
			wantMax: nil,
		},
		{
			name:    "nil type in schema element",
			se:      &parquet.SchemaElement{},
			stats:   &parquet.Statistics{MinValue: int32Bytes(1), MaxValue: int32Bytes(9)},
			wantMin: nil,
			wantMax: nil,
		},
		{
			name:    "empty min and max",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_INT32)},
			stats:   &parquet.Statistics{},
			wantMin: nil,
			wantMax: nil,
		},
		{
			name:    "absent minimum",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_INT32)},
			stats:   &parquet.Statistics{MinValue: nil, MaxValue: int32Bytes(9)},
			wantMin: nil,
			wantMax: int32(9),
		},
		{
			name:    "INT32",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_INT32)},
			stats:   &parquet.Statistics{MinValue: int32Bytes(-5), MaxValue: int32Bytes(42)},
			wantMin: int32(-5),
			wantMax: int32(42),
		},
		{
			name:    "INT64",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_INT64)},
			stats:   &parquet.Statistics{MinValue: int64Bytes(-100), MaxValue: int64Bytes(9999)},
			wantMin: int64(-100),
			wantMax: int64(9999),
		},
		{
			name:    "FLOAT",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_FLOAT)},
			stats:   &parquet.Statistics{MinValue: float32Bytes(1.5), MaxValue: float32Bytes(3.14)},
			wantMin: float32(1.5),
			wantMax: float32(3.14),
		},
		{
			name:    "DOUBLE",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_DOUBLE)},
			stats:   &parquet.Statistics{MinValue: float64Bytes(1.5), MaxValue: float64Bytes(3.14)},
			wantMin: float64(1.5),
			wantMax: float64(3.14),
		},
		{
			name: "BYTE_ARRAY",
			se:   &parquet.SchemaElement{Type: typePtr(parquet.Type_BYTE_ARRAY)},
			stats: &parquet.Statistics{
				MinValue: []byte("apple"),
				MaxValue: []byte("zebra"),
			},
			wantMin: "apple",
			wantMax: "zebra",
		},
		{
			name: "BYTE_ARRAY empty minimum",
			se:   &parquet.SchemaElement{Type: typePtr(parquet.Type_BYTE_ARRAY)},
			stats: &parquet.Statistics{
				MinValue: []byte{},
				MaxValue: []byte("zebra"),
			},
			wantMin: "",
			wantMax: "zebra",
		},
		{
			name: "STRING empty minimum and maximum",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{STRING: parquet.NewStringType()},
			},
			stats: &parquet.Statistics{
				MinValue: []byte{},
				MaxValue: []byte{},
			},
			wantMin: "",
			wantMax: "",
		},
		{
			name: "FIXED_LEN_BYTE_ARRAY",
			se:   &parquet.SchemaElement{Type: typePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), TypeLength: int32Ptr(4)},
			stats: &parquet.Statistics{
				MinValue: []byte{0x00, 0x01, 0x02, 0x03},
				MaxValue: []byte{0xaa, 0xbb, 0xcc, 0xdd},
			},
			wantMin: "\x00\x01\x02\x03",
			wantMax: "\xaa\xbb\xcc\xdd",
		},
		{
			name: "BOOLEAN",
			se:   &parquet.SchemaElement{Type: typePtr(parquet.Type_BOOLEAN)},
			stats: &parquet.Statistics{
				MinValue: []byte{0x00},
				MaxValue: []byte{0x01},
			},
			wantMin: false,
			wantMax: true,
		},
		{
			name: "INT96",
			se:   &parquet.SchemaElement{Type: typePtr(parquet.Type_INT96)},
			stats: &parquet.Statistics{
				MinValue: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				MaxValue: []byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 35, 0, 0},
			},
			wantMin: string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			wantMax: string([]byte{1, 0, 0, 0, 0, 0, 0, 0, 100, 35, 0, 0}),
		},
		{
			name:    "invalid physical minimum",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_INT32)},
			stats:   &parquet.Statistics{MinValue: []byte{1, 2, 3}, MaxValue: int32Bytes(9)},
			wantMin: nil,
			wantMax: int32(9),
		},
		{
			name:    "invalid physical maximum",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_DOUBLE)},
			stats:   &parquet.Statistics{MinValue: float64Bytes(1.5), MaxValue: []byte{1, 2, 3}},
			wantMin: float64(1.5),
			wantMax: nil,
		},
		{
			name: "compact unannotated BYTE_ARRAY",
			se:   &parquet.SchemaElement{Type: typePtr(parquet.Type_BYTE_ARRAY)},
			stats: &parquet.Statistics{
				MinValue: []byte{0xff},
				MaxValue: []byte{0xff, 0xff, 0x01},
			},
			wantMin: "\xff",
			wantMax: "\xff\xff\x01",
		},
		{
			name: "compact unannotated FIXED_LEN_BYTE_ARRAY",
			se:   &parquet.SchemaElement{Type: typePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), TypeLength: int32Ptr(4)},
			stats: &parquet.Statistics{
				MinValue: []byte{0x01},
				MaxValue: []byte{0xfe, 0xff},
			},
			wantMin: "\x01",
			wantMax: "\xfe\xff",
		},
		{
			name:    "invalid oversized FIXED_LEN_BYTE_ARRAY minimum",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), TypeLength: int32Ptr(4)},
			stats:   &parquet.Statistics{MinValue: make([]byte, 5), MaxValue: make([]byte, 4)},
			wantMin: nil,
			wantMax: string(make([]byte, 4)),
		},
		{
			name:    "unknown physical type",
			se:      &parquet.SchemaElement{Type: typePtr(parquet.Type(99))},
			stats:   &parquet.Statistics{MinValue: []byte{1}, MaxValue: []byte{2}},
			wantMin: nil,
			wantMax: nil,
		},
		{
			name: "invalid UTF8 minimum",
			se: &parquet.SchemaElement{
				Type:          typePtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: convertedTypePtr(parquet.ConvertedType_UTF8),
			},
			stats:   &parquet.Statistics{MinValue: []byte{0xff}, MaxValue: []byte("zebra")},
			wantMin: nil,
			wantMax: "zebra",
		},
		{
			name: "invalid JSON maximum",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{JSON: parquet.NewJsonType()},
			},
			stats:   &parquet.Statistics{MinValue: []byte(`{"a":1}`), MaxValue: []byte(`{"z":`)},
			wantMin: `{"a":1}`,
			wantMax: nil,
		},
		{
			name: "invalid BSON minimum",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{BSON: parquet.NewBsonType()},
			},
			stats:   &parquet.Statistics{MinValue: []byte{5, 0, 0}, MaxValue: []byte{5, 0, 0, 0, 0}},
			wantMin: nil,
			wantMax: string([]byte{5, 0, 0, 0, 0}),
		},
		{
			name: "invalid UUID maximum",
			se: &parquet.SchemaElement{
				Type:          typePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:    int32Ptr(16),
				LogicalType:   &parquet.LogicalType{UUID: parquet.NewUUIDType()},
				ConvertedType: nil,
			},
			stats:   &parquet.Statistics{MinValue: make([]byte, 16), MaxValue: make([]byte, 15)},
			wantMin: string(make([]byte, 16)),
			wantMax: nil,
		},
		{
			name: "invalid DECIMAL minimum",
			se: &parquet.SchemaElement{
				Type:          typePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:    int32Ptr(4),
				ConvertedType: convertedTypePtr(parquet.ConvertedType_DECIMAL),
			},
			stats:   &parquet.Statistics{MinValue: []byte{1, 2, 3}, MaxValue: []byte{1, 2, 3, 4}},
			wantMin: nil,
			wantMax: string([]byte{1, 2, 3, 4}),
		},
		{
			name: "invalid empty binary DECIMAL minimum",
			se: &parquet.SchemaElement{
				Type:          typePtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: convertedTypePtr(parquet.ConvertedType_DECIMAL),
			},
			stats:   &parquet.Statistics{MinValue: []byte{}, MaxValue: []byte{0}},
			wantMin: nil,
			wantMax: "\x00",
		},
		{
			name: "logical DECIMAL",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{DECIMAL: &parquet.DecimalType{Precision: 4, Scale: 2}},
			},
			stats:   &parquet.Statistics{MinValue: []byte{0}, MaxValue: []byte{1}},
			wantMin: "\x00",
			wantMax: "\x01",
		},
		{
			name: "invalid FLOAT16 maximum",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:  int32Ptr(2),
				LogicalType: &parquet.LogicalType{FLOAT16: parquet.NewFloat16Type()},
			},
			stats:   &parquet.Statistics{MinValue: []byte{0, 0}, MaxValue: []byte{0}},
			wantMin: string([]byte{0, 0}),
			wantMax: nil,
		},
		{
			name: "unsupported GEOMETRY bounds",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()},
			},
			stats:   &parquet.Statistics{MinValue: []byte{1, 2}, MaxValue: []byte{3, 4}},
			wantMin: nil,
			wantMax: nil,
		},
		{
			name: "unsupported GEOGRAPHY bounds",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{GEOGRAPHY: parquet.NewGeographyType()},
			},
			stats:   &parquet.Statistics{MinValue: []byte{1, 2}, MaxValue: []byte{3, 4}},
			wantMin: nil,
			wantMax: nil,
		},
		{
			name: "unsupported unknown logical bounds",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_BYTE_ARRAY),
				LogicalType: parquet.NewLogicalType(),
			},
			stats:   &parquet.Statistics{MinValue: []byte("a"), MaxValue: []byte("z")},
			wantMin: nil,
			wantMax: nil,
		},
		{
			name: "unsupported INTERVAL bounds",
			se: &parquet.SchemaElement{
				Type:          typePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:    int32Ptr(12),
				ConvertedType: convertedTypePtr(parquet.ConvertedType_INTERVAL),
			},
			stats:   &parquet.Statistics{MinValue: make([]byte, 12), MaxValue: make([]byte, 12)},
			wantMin: nil,
			wantMax: nil,
		},
		{
			name: "DATE uses physical validation",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{DATE: parquet.NewDateType()},
			},
			stats:   &parquet.Statistics{MinValue: int32Bytes(1), MaxValue: int32Bytes(9)},
			wantMin: int32(1),
			wantMax: int32(9),
		},
		{
			name: "INTEGER uses physical validation",
			se: &parquet.SchemaElement{
				Type:        typePtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: true}},
			},
			stats:   &parquet.Statistics{MinValue: int32Bytes(1), MaxValue: int32Bytes(9)},
			wantMin: int32(1),
			wantMax: int32(9),
		},
		{
			name: "converted DATE uses physical validation",
			se: &parquet.SchemaElement{
				Type:          typePtr(parquet.Type_INT32),
				ConvertedType: convertedTypePtr(parquet.ConvertedType_DATE),
			},
			stats:   &parquet.Statistics{MinValue: int32Bytes(1), MaxValue: int32Bytes(9)},
			wantMin: int32(1),
			wantMax: int32(9),
		},
		{
			name: "converted JSON",
			se: &parquet.SchemaElement{
				Type:          typePtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: convertedTypePtr(parquet.ConvertedType_JSON),
			},
			stats:   &parquet.Statistics{MinValue: []byte(`{"a":1}`), MaxValue: []byte(`{"z":9}`)},
			wantMin: `{"a":1}`,
			wantMax: `{"z":9}`,
		},
		{
			name: "converted BSON",
			se: &parquet.SchemaElement{
				Type:          typePtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: convertedTypePtr(parquet.ConvertedType_BSON),
			},
			stats:   &parquet.Statistics{MinValue: []byte{5, 0, 0, 0, 0}, MaxValue: []byte{5, 0, 0, 0, 0}},
			wantMin: string([]byte{5, 0, 0, 0, 0}),
			wantMax: string([]byte{5, 0, 0, 0, 0}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			min, max, err := DecodeStatisticsMinMax(tc.se, tc.stats)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantMin, min)
			require.Equal(t, tc.wantMax, max)
		})
	}
}
