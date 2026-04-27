package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertDateLogicalValue_Nil(t *testing.T) {
	result := ConvertDateLogicalValue(nil)
	require.Nil(t, result)
}

func TestConvertDateLogicalValue_Int32(t *testing.T) {
	result := ConvertDateLogicalValue(int32(0))
	require.Equal(t, "1970-01-01", result)
}

func TestConvertDateLogicalValue_Int32Positive(t *testing.T) {
	result := ConvertDateLogicalValue(int32(1))
	require.Equal(t, "1970-01-02", result)
}

func TestConvertDateLogicalValue_Int32Negative(t *testing.T) {
	result := ConvertDateLogicalValue(int32(-1))
	require.Equal(t, "1969-12-31", result)
}

func TestConvertDateLogicalValue_NonInt32(t *testing.T) {
	result := ConvertDateLogicalValue(int64(365))
	require.Equal(t, int64(365), result)
}

func TestParseDateString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int32
		errMsg   string
	}{
		{
			name:     "valid_date_epoch",
			input:    "1970-01-01",
			expected: 0,
		},
		{
			name:     "valid_date_2024",
			input:    "2024-01-15",
			expected: 19737,
		},
		{
			name:     "valid_date_past",
			input:    "1969-12-31",
			expected: -1,
		},
		{
			name:   "invalid_format",
			input:  "01/15/2024",
			errMsg: "cannot parse",
		},
		{
			name:   "empty_string",
			input:  "",
			errMsg: "cannot parse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseDateString(tt.input)
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}
