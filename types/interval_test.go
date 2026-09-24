package types

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
)

func TestParseIntervalString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
		errMsg   string
	}{
		{
			name:     "complex_interval",
			input:    "2 mon 15 day 7200.000 sec",
			expected: []byte{2, 0, 0, 0, 15, 0, 0, 0, 0, 221, 109, 0},
		},
		{
			name:     "months_only",
			input:    "3 mon",
			expected: []byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "days_only",
			input:    "7 day",
			expected: []byte{0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "seconds_only",
			input:    "1.500 sec",
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 220, 5, 0, 0},
		},
		{
			name:     "empty_string",
			input:    "",
			expected: make([]byte, 12),
		},
		{
			name:   "invalid_unit",
			input:  "5 weeks",
			errMsg: "unknown interval unit",
		},
		{
			name:   "invalid_months_value",
			input:  "abc mon",
			errMsg: "invalid months value",
		},
		{
			name:   "invalid_days_value",
			input:  "xyz day",
			errMsg: "invalid days value",
		},
		{
			name:   "invalid_seconds_value",
			input:  "bad sec",
			errMsg: "invalid seconds value",
		},
		{
			name:   "incomplete_pair",
			input:  "5",
			errMsg: "invalid interval format",
		},
		{
			name:     "seconds_max",
			input:    "4294967.295 sec",
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255},
		},
		{
			name:   "seconds_negative",
			input:  "-1 sec",
			errMsg: "seconds out of range",
		},
		{
			name:   "seconds_small_negative",
			input:  "-0.0004 sec",
			errMsg: "seconds out of range",
		},
		{
			name:   "seconds_overflow",
			input:  "5000000 sec",
			errMsg: "seconds out of range",
		},
		{
			name:   "seconds_just_over_max",
			input:  "4294967.296 sec",
			errMsg: "seconds out of range",
		},
		{
			name:   "seconds_inf",
			input:  "Inf sec",
			errMsg: "seconds out of range",
		},
		{
			name:   "seconds_nan",
			input:  "NaN sec",
			errMsg: "seconds out of range",
		},
		{
			name:   "months_overflow",
			input:  "99999999999 mon",
			errMsg: "invalid months value",
		},
		{
			name:   "days_overflow",
			input:  "99999999999 day",
			errMsg: "invalid days value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseIntervalString(tt.input)
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, []byte(result))
			}
		})
	}
}

func TestIntervalToString(t *testing.T) {
	tests := []struct {
		name     string
		interval []byte
		expected string
	}{
		{
			name:     "zero_interval",
			interval: make([]byte, 12), // All zeros
			expected: "0.000 sec",
		},
		{
			name:     "one_month_interval",
			interval: []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expected: "1 mon",
		},
		{
			name:     "one_day_interval",
			interval: []byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
			expected: "1 day",
		},
		{
			name:     "one_hour_interval",
			interval: []byte{0, 0, 0, 0, 0, 0, 0, 0, 128, 238, 54, 0},
			expected: "3600.000 sec",
		},
		{
			name:     "complex_interval",
			interval: []byte{2, 0, 0, 0, 15, 0, 0, 0, 0, 221, 109, 0},
			expected: "2 mon 15 day 7200.000 sec",
		},
		{
			name:     "months_and_seconds_only",
			interval: []byte{3, 0, 0, 0, 0, 0, 0, 0, 220, 5, 0, 0},
			expected: "3 mon 1.500 sec",
		},
		{
			name:     "days_and_seconds_only",
			interval: []byte{0, 0, 0, 0, 7, 0, 0, 0, 244, 1, 0, 0},
			expected: "7 day 0.500 sec",
		},
		{
			name:     "fractional_seconds",
			interval: []byte{0, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0},
			expected: "0.025 sec",
		},
		{
			name:     "invalid_length_short",
			interval: []byte{1, 2, 3},
			expected: "",
		},
		{
			name:     "invalid_length_long",
			interval: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			expected: "",
		},
		{
			name:     "nil_interval",
			interval: nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IntervalToString(tt.interval)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIntervalRoundTrip(t *testing.T) {
	pack := func(months, days, millis uint32) []byte {
		b := make([]byte, common.IntervalByteLen)
		binary.LittleEndian.PutUint32(b[0:4], months)
		binary.LittleEndian.PutUint32(b[4:8], days)
		binary.LittleEndian.PutUint32(b[8:12], millis)
		return b
	}

	// Values whose second form lands just below an exact millisecond once scaled back,
	// which truncation used to round down to the previous millisecond.
	cases := [][3]uint32{
		{0, 0, 0},
		{0, 0, 1},
		{0, 0, 999},
		{2, 3, 4500},
		{0, 0, 7200000},
		{0, 0, 524763700},
		{474677728, 319928379, 1071954545},
		{1357569782, 469226274, 2141262303},
		{1188101442, 3058434634, 4219504804},
		{0, 0, math.MaxUint32},
		{math.MaxUint32, math.MaxUint32, math.MaxUint32},
	}
	rnd := rand.New(rand.NewPCG(1, 2))
	for range 10000 {
		cases = append(cases, [3]uint32{rnd.Uint32(), rnd.Uint32(), rnd.Uint32()})
	}

	for _, c := range cases {
		want := pack(c[0], c[1], c[2])
		got, err := ParseIntervalString(IntervalToString(want))
		require.NoError(t, err)
		require.Equal(t, want, []byte(got), "months=%d days=%d millis=%d", c[0], c[1], c[2])
	}
}

// TestParseIntervalString_WholeComponent pins that each component is read over its whole field.
func TestParseIntervalString_WholeComponent(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		errMsg string
	}{
		{name: "months with a remainder", input: "2abc mon", errMsg: "invalid months value: 2abc"},
		{name: "days with a remainder", input: "3xyz day", errMsg: "invalid days value: 3xyz"},
		{name: "seconds with a remainder", input: "4.5abc sec", errMsg: "invalid seconds value: 4.5abc"},
		{name: "a later component with a remainder", input: "2 mon 3zzz day", errMsg: "invalid days value: 3zzz"},
		{name: "months in hex", input: "0x10 mon", errMsg: "invalid months value: 0x10"},
		{name: "months with a sign", input: "+2 mon", errMsg: "invalid months value: +2"},
		{name: "months that overflow", input: "4294967296 mon", errMsg: "invalid months value: 4294967296"},
		{name: "nothing but a unit", input: "mon", errMsg: "invalid interval format"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseIntervalString(tc.input)
			require.ErrorContains(t, err, tc.errMsg)
		})
	}

	// The forms the renderer emits, and the ones around them, still parse.
	for _, valid := range []string{"2 mon 3 day 4.500 sec", "0.000 sec", "1e3 sec", " 2  mon ", ""} {
		_, err := ParseIntervalString(valid)
		require.NoError(t, err, valid)
	}
}
