package types

import (
	"testing"

	"github.com/stretchr/testify/require"
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
