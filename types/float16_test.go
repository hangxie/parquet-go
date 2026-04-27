package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFloat16String(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		errMsg string
	}{
		{
			name:  "positive_float",
			input: "1.5",
		},
		{
			name:  "negative_float",
			input: "-2.25",
		},
		{
			name:  "zero",
			input: "0",
		},
		{
			name:  "small_number",
			input: "0.0001",
		},
		{
			name:  "very_small_subnormal",
			input: "0.00001",
		},
		{
			name:  "very_large_overflow",
			input: "100000",
		},
		{
			name:  "infinity",
			input: "Inf",
		},
		{
			name:  "negative_infinity",
			input: "-Inf",
		},
		{
			name:  "nan",
			input: "NaN",
		},
		{
			name:   "invalid",
			input:  "abc",
			errMsg: "invalid float16 value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseFloat16String(tt.input)
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				require.Len(t, result, 2)
			}
		})
	}
}
