package common

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFloat16FuncTable(t *testing.T) {
	// 0x3C00 -> 1.0; 0x3800 -> 0.5; 0xC000 -> -2.0 (little-endian)
	le := func(u16 uint16) string { return string([]byte{byte(u16), byte(u16 >> 8)}) }

	ftab := float16FuncTable{}
	require.True(t, ftab.LessThan(le(0x3800), le(0x3C00)))  // 0.5 < 1.0
	require.False(t, ftab.LessThan(le(0x3C00), le(0x3800))) // 1.0 < 0.5 is false
	require.True(t, ftab.LessThan(le(0xC000), le(0x3800)))  // -2.0 < 0.5

	// MinMaxSize should propagate min/max and size=2
	min, max, sz := ftab.MinMaxSize(le(0x3C00), le(0x3800), le(0xC000))
	// min(1.0, -2.0) => -2.0; max(0.5, -2.0) => 0.5
	require.Equal(t, le(0xC000), min)
	require.Equal(t, le(0x3800), max)
	require.Equal(t, int32(2), sz)
}

func TestFloat16FuncTableTotalOrder(t *testing.T) {
	le := func(u16 uint16) []byte { return []byte{byte(u16), byte(u16 >> 8)} }
	ordered := []struct {
		name string
		bits uint16
	}{
		{name: "negative_nan", bits: 0xFE00},
		{name: "negative_inf", bits: 0xFC00},
		{name: "negative_normal", bits: 0xC000},
		{name: "negative_subnormal", bits: 0x8001},
		{name: "negative_zero", bits: 0x8000},
		{name: "positive_zero", bits: 0x0000},
		{name: "positive_subnormal", bits: 0x0001},
		{name: "positive_normal", bits: 0x3C00},
		{name: "positive_inf", bits: 0x7C00},
		{name: "positive_nan", bits: 0x7E00},
	}

	ftab := float16FuncTable{}
	for i := 0; i < len(ordered)-1; i++ {
		t.Run(ordered[i].name+"_less_than_"+ordered[i+1].name, func(t *testing.T) {
			require.True(t, ftab.LessThan(le(ordered[i].bits), le(ordered[i+1].bits)))
			require.False(t, ftab.LessThan(le(ordered[i+1].bits), le(ordered[i].bits)))
		})
	}

	// The total order above places NaN at both extremes, but the Parquet spec requires
	// min/max statistics to be computed from non-NaN values only, so a NaN must not
	// displace an existing bound in either direction.
	min, max, sz := ftab.MinMaxSize(le(0x8000), le(0x0000), le(0xFE00))
	require.Equal(t, le(0x8000), min)
	require.Equal(t, le(0x0000), max)
	require.Equal(t, int32(2), sz)

	min, max, _ = ftab.MinMaxSize(le(0x8000), le(0x0000), le(0x7E00))
	require.Equal(t, le(0x8000), min)
	require.Equal(t, le(0x0000), max)

	// A NaN seed is cleared rather than carried, so the first non-NaN value becomes both bounds.
	min, max, _ = ftab.MinMaxSize(le(0x7E00), le(0x7E00), le(0x3C00))
	require.Equal(t, le(0x3C00), min)
	require.Equal(t, le(0x3C00), max)

	// An all-NaN run leaves no bounds at all, so none are written.
	min, max, _ = ftab.MinMaxSize(le(0xFE00), le(0xFE00), le(0x7E00))
	require.Nil(t, min)
	require.Nil(t, max)

	// Infinities are ordinary ordered values and remain valid bounds.
	min, max, _ = ftab.MinMaxSize(le(0xFC00), le(0x7C00), le(0x3C00))
	require.Equal(t, le(0xFC00), min)
	require.Equal(t, le(0x7C00), max)

	// Values that are not decodable as binary16 cannot be NaN and must pass through.
	require.False(t, isFloat16NaN("abc"))
	require.False(t, isFloat16NaN(nil))
	require.False(t, isFloat16NaN(le(0x7C00)))
}

func TestFloat16FuncTable_FallbackLexicographic(t *testing.T) {
	ftab := float16FuncTable{}

	// Inputs not decodable as float16 (len != 2) use lexicographic order on raw bytes
	require.True(t, ftab.LessThan("a", "b"))  // "a" < "b"
	require.False(t, ftab.LessThan("b", "a")) // "b" < "a" is false
	require.True(t, ftab.LessThan([]byte{1}, []byte{2}))

	// If either side cannot convert to bytes, LessThan returns false
	require.False(t, ftab.LessThan(123, "a"))
	require.False(t, ftab.LessThan("a", 123))
}

func TestHalfToFloat32(t *testing.T) {
	le := func(u16 uint16) string { return string([]byte{byte(u16), byte(u16 >> 8)}) }

	// expected for smallest positive subnormal: (1/1024) * 2^-14
	subnorm := float32(1.0/1024.0) / float32(1<<14)
	f32 := func(v float32) *float32 { return &v }

	testCases := map[string]struct {
		in      any
		ok      bool
		expect  *float32 // used when ok && not inf; nil when checking inf
		approx  bool     // compare with epsilon when true
		infSign int      // 0 none, +1 +Inf, -1 -Inf
	}{
		"one":         {in: le(0x3C00), ok: true, expect: f32(1.0)},
		"half":        {in: le(0x3800), ok: true, expect: f32(0.5)},
		"neg-half":    {in: le(0xB800), ok: true, expect: f32(-0.5)},
		"neg-two":     {in: le(0xC000), ok: true, expect: f32(-2.0)},
		"subnormal+":  {in: le(0x0001), ok: true, expect: &subnorm, approx: true},
		"subnormal-":  {in: le(0x8001), ok: true, expect: f32(-subnorm), approx: true},
		"zero":        {in: le(0x0000), ok: true, expect: f32(0.0)},
		"+inf":        {in: le(0x7C00), ok: true, infSign: +1},
		"-inf":        {in: le(0xFC00), ok: true, infSign: -1},
		"nan":         {in: le(0x7E00), ok: false},
		"wrong-len":   {in: string([]byte{0x00}), ok: false},
		"unsupported": {in: 42, ok: false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			v, ok := halfToFloat32(tc.in)
			require.Equal(t, tc.ok, ok)
			if !ok {
				return
			}
			if tc.infSign != 0 {
				require.True(t, math.IsInf(float64(v), tc.infSign))
				return
			}
			require.NotNil(t, tc.expect)
			if tc.approx {
				require.InEpsilon(t, *tc.expect, v, 1e-6)
			} else {
				require.Equal(t, *tc.expect, v)
			}
		})
	}
}

func TestToBytes(t *testing.T) {
	// Non-empty string
	b := toBytes("ab")
	require.Equal(t, []byte("ab"), b)

	// Empty string -> empty slice, not nil
	b = toBytes("")
	require.NotNil(t, b)
	require.Equal(t, 0, len(b))

	// []byte input
	src := []byte{0, 1, 2}
	b = toBytes(src)
	require.Equal(t, src, b)

	// Unsupported type -> nil
	require.Nil(t, toBytes(123))
}
