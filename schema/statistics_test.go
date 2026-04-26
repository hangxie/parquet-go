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
