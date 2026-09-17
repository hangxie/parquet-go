package types

import (
	"fmt"
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

func TestStrToIntegerLogical(t *testing.T) {
	testCases := map[string]struct {
		inputStr string
		pT       parquet.Type
		bitWidth int8
		signed   bool
		expected any
		errMsg   string
	}{
		"uint64-above-int64-max": {
			"18446744073709551615", parquet.Type_INT64, 64, false, int64(-1), "",
		},
		"uint32-above-int32-max": {
			"4294967295", parquet.Type_INT32, 32, false, int32(-1), "",
		},
		"uint8-max": {
			"255", parquet.Type_INT32, 8, false, int32(255), "",
		},
		"uint16-max": {
			"65535", parquet.Type_INT32, 16, false, int32(65535), "",
		},
		"int8-min": {
			"-128", parquet.Type_INT32, 8, true, int32(-128), "",
		},
		"int64-min": {
			"-9223372036854775808", parquet.Type_INT64, 64, true, int64(-9223372036854775808), "",
		},
		"uint8-out-of-range": {
			"256", parquet.Type_INT32, 8, false, int32(0),
			`parse UINT_8 "256": strconv.ParseUint: parsing "256": value out of range`,
		},
		"int16-out-of-range": {
			"32768", parquet.Type_INT32, 16, true, int32(0),
			`parse INT_16 "32768": strconv.ParseInt: parsing "32768": value out of range`,
		},
		"unsigned-rejects-negative": {
			"-1", parquet.Type_INT32, 32, false, int32(0),
			`parse UINT_32 "-1": strconv.ParseUint: parsing "-1": invalid syntax`,
		},
		"not-a-number": {
			"abc", parquet.Type_INT64, 64, true, int64(0),
			`parse INT_64 "abc": strconv.ParseInt: parsing "abc": invalid syntax`,
		},
		"odd-bit-width-falls-back-to-column-width": {
			// Not a width the format defines; the column decides the range instead.
			"4294967295", parquet.Type_INT32, 7, false, int32(-1), "",
		},
		"odd-bit-width-error-names-declared-width": {
			"abc", parquet.Type_INT32, 7, false, int32(0),
			`parse UINT_7 "abc": strconv.ParseUint: parsing "abc": invalid syntax`,
		},
		"leading-whitespace": {
			" 42", parquet.Type_INT32, 32, false, int32(42), "",
		},
		"trailing-whitespace": {
			"42\t\n", parquet.Type_INT64, 64, true, int64(42), "",
		},
		// Sscanf read these as 42 and 1 up to v3.8.3, the same input strToDecimal has
		// always rejected. See the StrToParquetTypeWithLogical doc comment.
		"trailing-garbage-rejected": {
			"42abc", parquet.Type_INT32, 32, false, int32(0),
			`parse UINT_32 "42abc": strconv.ParseUint: parsing "42abc": invalid syntax`,
		},
		"digit-separator-rejected": {
			"1_0", parquet.Type_INT32, 32, true, int32(0),
			`parse INT_32 "1_0": strconv.ParseInt: parsing "1_0": invalid syntax`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			res, err := StrToParquetTypeWithLogical(tc.inputStr, parquet.TypePtr(tc.pT), nil,
				createIntegerLogicalType(tc.bitWidth, tc.signed), 0, 0)
			if tc.errMsg != "" {
				require.EqualError(t, err, tc.errMsg)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expected, res)
		})
	}
}

// TestStrToIntegerLogical_MismatchedColumn covers INTEGER annotations the
// physical type cannot hold, which stay with the physical scan rather than being truncated.
func TestStrToIntegerLogical_MismatchedColumn(t *testing.T) {
	// 64-bit annotation on a 32-bit column: the physical INT32 scan reports the overflow.
	_, err := StrToParquetTypeWithLogical("4294967295", parquet.TypePtr(parquet.Type_INT32), nil,
		createIntegerLogicalType(64, false), 0, 0)
	require.ErrorContains(t, err, "parse INT32")

	// The mirror image: a narrow annotation on a 64-bit column. ConvertIntegerLogicalValue
	// renders such a value without truncating it, so the physical scan takes it back rather
	// than rejecting what the read path emits.
	res64, err := StrToParquetTypeWithLogical("300", parquet.TypePtr(parquet.Type_INT64), nil,
		createIntegerLogicalType(8, false), 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(300), res64)
	require.Equal(t, uint32(300), ConvertIntegerLogicalValue(int64(300),
		parquet.TypePtr(parquet.Type_INT64), createIntegerLogicalType(8, false).GetINTEGER()))

	// INTEGER on a byte-backed column is not a schema this can read, so the value falls to
	// the physical BYTE_ARRAY scan, which reads its input as base64.
	res, err := StrToParquetTypeWithLogical("NDI=", parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil,
		createIntegerLogicalType(32, true), 0, 0)
	require.NoError(t, err)
	require.Equal(t, "42", res)
}

// TestNarrowIntegerOnMismatchedColumn covers a narrow annotation the physical type cannot
// hold. strToIntegerLogical declines it, so the column's own range decides and the value
// stays the width the column stores; the narrow check has no int32 to look at.
func TestNarrowIntegerOnMismatchedColumn(t *testing.T) {
	int64T := parquet.Type_INT64
	int8CT := parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)

	for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw} {
		got, err := StrToParquetTypeWithLogical("1000", &int64T, int8CT, nil, 0, 0, WithValueMode(mode))
		require.NoError(t, err, "%s mode", mode)
		require.Equal(t, int64(1000), got, "%s mode", mode)
	}
}

// TestNarrowIntegerMalformedWidth covers an INTEGER width the format does not define, which
// both modes leave to the physical scan rather than invent a range for. A signed 0 panicked.
func TestNarrowIntegerMalformedWidth(t *testing.T) {
	int32T := parquet.Type_INT32

	for _, width := range []int8{0, 1, 12, 17, 33} {
		t.Run(fmt.Sprintf("width %d", width), func(t *testing.T) {
			lT := parquet.NewLogicalType()
			lT.INTEGER = &parquet.IntType{BitWidth: width, IsSigned: true}

			for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw} {
				got, err := StrToParquetTypeWithLogical("100", &int32T, nil, lT, 0, 0, WithValueMode(mode))
				require.NoError(t, err, "%s mode", mode)
				require.Equal(t, int32(100), got, "%s mode", mode)
			}
		})
	}
}
