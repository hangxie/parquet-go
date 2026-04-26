package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoolFuncTable_LessThan(t *testing.T) {
	table := boolFuncTable{}
	testCases := map[string]struct {
		a, b     any
		expected bool
	}{
		"false_lt_false": {false, false, false},
		"false_lt_true":  {false, true, true},
		"true_lt_false":  {true, false, false},
		"true_lt_true":   {true, true, false},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, table.LessThan(tc.a, tc.b))
		})
	}
}

func TestUint32FuncTable_UnsignedBehavior(t *testing.T) {
	// int32(-1) wraps to MaxUint32 when reinterpreted as uint32,
	// so it is NOT less than int32(0) or int32(1).
	require.False(t, uint32FuncTable{}.LessThan(int32(-1), int32(0)))
	require.False(t, uint32FuncTable{}.LessThan(int32(-1), int32(1)))

	// Signed int32FuncTable treats -1 as less than 0.
	require.True(t, int32FuncTable{}.LessThan(int32(-1), int32(0)))
	require.True(t, int32FuncTable{}.LessThan(int32(-1), int32(1)))
}

func TestUint64FuncTable_UnsignedBehavior(t *testing.T) {
	// int64(-1) wraps to MaxUint64 when reinterpreted as uint64,
	// so it is NOT less than int64(0) or int64(1).
	require.False(t, uint64FuncTable{}.LessThan(int64(-1), int64(0)))
	require.False(t, uint64FuncTable{}.LessThan(int64(-1), int64(1)))

	// Signed int64FuncTable treats -1 as less than 0.
	require.True(t, int64FuncTable{}.LessThan(int64(-1), int64(0)))
	require.True(t, int64FuncTable{}.LessThan(int64(-1), int64(1)))
}

func TestDecimalStringFuncTable_LessThan(t *testing.T) {
	table := decimalStringFuncTable{}
	testCases := map[string]struct {
		a, b     any
		expected bool
	}{
		"equal":                 {"\x00\x01", "\x00\x01", false},
		"positive_lt_positive":  {"\x00\x01", "\x00\x02", true},
		"positive_gt_positive":  {"\x00\x02", "\x00\x01", false},
		"negative_lt_positive":  {"\xff\xff", "\x00\x01", true}, // -1 < 1 in BigEndian signed
		"negative_lt_zero":      {"\xff\xff", "\x00\x00", true}, // -1 < 0
		"zero_lt_positive":      {"\x00\x00", "\x00\x01", true},
		"negative_gt_negative":  {"\xff\xff", "\xff\xfe", false}, // -1 > -2
		"more_negative_lt_less": {"\xff\xfe", "\xff\xff", true},  // -2 < -1
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, table.LessThan(tc.a, tc.b))
		})
	}
}

func TestIntervalFuncTable_NoSignBit(t *testing.T) {
	// intervalFuncTable compares bytes unsigned from byte 11 down to 0,
	// with no sign-bit special case (unlike int96FuncTable).
	// A value with 0xff in byte 11 is GREATER than one with 0x00, regardless of sign.
	table := intervalFuncTable{}

	// Build two 12-byte strings differing only in byte 11.
	base := string(make([]byte, 12))
	withHighByte := string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff})

	// withHighByte has byte[11] = 0xff > 0x00, so withHighByte is NOT less than base.
	require.False(t, table.LessThan(withHighByte, base))
	// base has byte[11] = 0x00 < 0xff, so base IS less than withHighByte.
	require.True(t, table.LessThan(base, withHighByte))
}
