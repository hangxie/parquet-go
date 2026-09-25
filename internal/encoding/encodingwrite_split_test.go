package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteByteStreamSplit(t *testing.T) {
	t.Run("generic", func(t *testing.T) {
		testCases := []struct {
			name     string
			src      []any
			expected int // expected byte length (0 means empty)
		}{
			{
				name:     "float32_type",
				src:      []any{float32(1.1), float32(2.2)},
				expected: 8, // 2 * 4 bytes
			},
			{
				name:     "float64_type",
				src:      []any{float64(1.1), float64(2.2)},
				expected: 16, // 2 * 8 bytes
			},
			{
				name:     "int32_type",
				src:      []any{int32(1), int32(2)},
				expected: 8, // 2 * 4 bytes
			},
			{
				name:     "int64_type",
				src:      []any{int64(1), int64(2)},
				expected: 16, // 2 * 8 bytes
			},
			{
				name:     "fixed_len_byte_array_type",
				src:      []any{"ab", "cd"},
				expected: 4, // 2 * 2 bytes
			},
			{
				name:     "fixed_len_byte_array_bytes_type",
				src:      []any{[]byte("ab"), []byte("cd")},
				expected: 4, // 2 * 2 bytes
			},
			{
				name:     "unsupported_type",
				src:      []any{uint32(1), uint32(2)},
				expected: 0,
			},
			{
				name:     "empty_input",
				src:      []any{},
				expected: 0,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := WriteByteStreamSplit(tc.src)
				require.Len(t, result, tc.expected)
			})
		}
	})

	t.Run("float32", func(t *testing.T) {
		testCases := []struct {
			name string
			src  []any
		}{
			{
				name: "single_value",
				src:  []any{float32(1.0)},
			},
			{
				name: "multiple_values",
				src:  []any{float32(1.1), float32(2.2), float32(3.3)},
			},
			{
				name: "empty_input",
				src:  []any{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := WriteByteStreamSplitFloat32(tc.src)
				expectedLen := len(tc.src) * 4
				require.Equal(t, expectedLen, len(result))
			})
		}
	})

	t.Run("fixed_len_byte_array", func(t *testing.T) {
		testCases := []struct {
			name     string
			src      []any
			expected int
		}{
			{name: "string_values", src: []any{"ab", "cd"}, expected: 4},
			{name: "byte_values", src: []any{[]byte("ab"), []byte("cd")}, expected: 4},
			{name: "empty_input", src: []any{}, expected: 0},
			{name: "first_value_unsupported_type", src: []any{42}, expected: 0},
			{name: "first_value_zero_length", src: []any{""}, expected: 0},
			{name: "mid_stream_unsupported_type", src: []any{"ab", 42}, expected: 0},
			{name: "mid_stream_wrong_size", src: []any{"ab", "abc"}, expected: 0},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := WriteByteStreamSplitFixedLenByteArray(tc.src)
				require.Len(t, result, tc.expected)
			})
		}
	})

	t.Run("float64", func(t *testing.T) {
		testCases := []struct {
			name string
			src  []any
		}{
			{
				name: "single_value",
				src:  []any{float64(1.0)},
			},
			{
				name: "multiple_values",
				src:  []any{float64(1.1), float64(2.2), float64(3.3)},
			},
			{
				name: "empty_input",
				src:  []any{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := WriteByteStreamSplitFloat64(tc.src)
				expectedLen := len(tc.src) * 8
				require.Equal(t, expectedLen, len(result))
			})
		}
	})
}
