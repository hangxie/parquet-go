package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertBSONLogicalValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name:     "byte_slice_input",
			val:      []byte{0x0c, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
			expected: map[string]any{"i": int32(1)},
		},
		{
			name:     "string_input",
			val:      string([]byte{0x0c, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x02, 0x00, 0x00, 0x00, 0x00}),
			expected: map[string]any{"i": int32(2)},
		},
		{
			name:     "empty_byte_slice",
			val:      []byte{},
			expected: map[string]any{},
		},
		{
			name:     "empty_string",
			val:      "",
			expected: map[string]any{},
		},
		{
			name:     "non_binary_value",
			val:      123,
			expected: 123,
		},
		{
			name:     "complex_bson_document",
			val:      []byte{29, 0, 0, 0, 2, 107, 101, 121, 0, 6, 0, 0, 0, 118, 97, 108, 117, 101, 0, 16, 110, 117, 109, 0, 42, 0, 0, 0, 0},
			expected: map[string]any{"key": "value", "num": int32(42)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertBSONLogicalValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}
