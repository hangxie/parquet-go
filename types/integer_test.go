package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestConvertIntegerLogicalValue(t *testing.T) {
	pT32 := parquet.TypePtr(parquet.Type_INT32)
	pT64 := parquet.TypePtr(parquet.Type_INT64)
	mkInt := func(width int8, signed bool) *parquet.IntType {
		it := parquet.NewIntType()
		it.BitWidth = width
		it.IsSigned = signed
		return it
	}

	tests := []struct {
		name string
		val  any
		pT   *parquet.Type
		it   *parquet.IntType
		want any
	}{
		{"int8_from_int32", int32(-5), pT32, mkInt(8, true), int8(-5)},
		{"uint8_from_int32", int32(250), pT32, mkInt(8, false), uint8(250)},
		{"int16_from_int32", int32(-200), pT32, mkInt(16, true), int16(-200)},
		{"uint16_from_int32", int32(50000), pT32, mkInt(16, false), uint16(50000)},
		{"int32_from_int32", int32(-100000), pT32, mkInt(32, true), int32(-100000)},
		{"int64_signed_64", int64(-7), pT64, mkInt(64, true), int64(-7)},
		{"int64_unsigned_64", int64(7), pT64, mkInt(64, false), uint64(7)},
		{"int64_signed_32", int64(-8), pT64, mkInt(32, true), int32(-8)},
		{"int64_unsigned_32", int64(8), pT64, mkInt(32, false), uint32(8)},
		{"nil_val", nil, pT32, mkInt(8, true), nil},
		{"nil_it", int32(1), pT32, nil, int32(1)},
		{"unsupported_val_type", "x", pT32, mkInt(8, true), "x"},
		{"int32_signed_default_width", int32(5), pT32, mkInt(24, true), int32(5)},
		{"int32_unsigned_default_width", int32(5), pT32, mkInt(24, false), uint32(5)},
		{"int32_signed_no_bitwidth", int32(12345), pT32, &parquet.IntType{IsSigned: true}, int32(12345)},
		{"int32_unsigned_no_bitwidth", int32(-1), pT32, &parquet.IntType{IsSigned: false}, uint32(4294967295)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ConvertIntegerLogicalValue(tc.val, tc.pT, tc.it)
			require.Equal(t, tc.want, got)
		})
	}
}
