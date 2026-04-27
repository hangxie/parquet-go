package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestINT96(t *testing.T) {
	t1 := time.Now().Truncate(time.Microsecond).UTC()
	s := TimeToINT96(t1)
	t2, err := INT96ToTime(s)
	require.NoError(t, err)
	require.True(t, t1.Equal(t2), "INT96 error: expected %v, got %v", t1, t2)
}

func TestINT96ToTime_InvalidInput(t *testing.T) {
	// Test that INT96ToTime returns error for invalid input
	_, err := INT96ToTime("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")

	_, err = INT96ToTime("short")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")
}

func TestINT96ToTime(t *testing.T) {
	// Test valid input
	t1 := time.Now().Truncate(time.Microsecond).UTC()
	s := TimeToINT96(t1)
	t2, err := INT96ToTime(s)
	require.NoError(t, err)
	require.True(t, t1.Equal(t2), "INT96 error: expected %v, got %v", t1, t2)

	// Test with empty string
	_, err = INT96ToTime("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")

	// Test with string too short
	_, err = INT96ToTime("short")
	require.Error(t, err)
	require.Contains(t, err.Error(), "too short")

	// Test with exactly 11 bytes (one byte short)
	_, err = INT96ToTime("12345678901")
	require.Error(t, err)
	require.Contains(t, err.Error(), "got 11 bytes")

	// Test with exactly 12 bytes (valid)
	_, err = INT96ToTime("123456789012")
	require.NoError(t, err)
}

func TestParseINT96String(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		errMsg string
	}{
		{
			name:  "valid_timestamp",
			input: "2024-01-15T14:30:45.123Z",
		},
		{
			name:  "epoch",
			input: "1970-01-01T00:00:00Z",
		},
		{
			name:   "invalid_format",
			input:  "not a timestamp",
			errMsg: "cannot parse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseINT96String(tt.input)
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				require.Len(t, result, 12)
			}
		})
	}
}
