package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteDelta(t *testing.T) {
	t.Run("generic", func(t *testing.T) {
		testCases := []struct {
			name string
			src  []any
		}{
			{
				name: "int32_delta",
				src:  []any{int32(1), int32(2), int32(3)},
			},
			{
				name: "int64_delta",
				src:  []any{int64(100), int64(200), int64(300)},
			},
			{
				name: "unsupported_type",
				src:  []any{true, false},
			},
			{
				name: "empty_input",
				src:  []any{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := WriteDelta(tc.src)
				if tc.name == "unsupported_type" {
					require.Error(t, err)
					require.Contains(t, err.Error(), "unsupported type")
					return
				}
				require.NoError(t, err)
				if tc.name == "empty_input" {
					require.Equal(t, []byte{128, 1, 4, 0, 0}, result)
					return
				}
				require.NotZero(t, len(result))
			})
		}
	})

	t.Run("byte_array", func(t *testing.T) {
		testData := []struct {
			nums     []any
			expected []byte
		}{
			{[]any{"Hello", "World", "Foobar", "ABCDEF"}, []byte{128, 1, 4, 4, 0, 0, 0, 0, 0, 0, 128, 1, 4, 4, 10, 0, 1, 0, 0, 0, 2, 0, 0, 0, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 70, 111, 111, 98, 97, 114, 65, 66, 67, 68, 69, 70}},
		}

		for _, data := range testData {
			res := WriteDeltaByteArray(data.nums)
			require.Equal(t, string(data.expected), string(res))
		}
	})

	t.Run("int32", func(t *testing.T) {
		testData := []struct {
			nums     []any
			expected []byte
		}{
			{[]any{int32(1), int32(2), int32(3), int32(4), int32(5)}, []byte{128, 1, 4, 5, 2, 2, 0, 0, 0, 0}},
			{
				[]any{int32(7), int32(5), int32(3), int32(1), int32(2), int32(3), int32(4), int32(5)},
				[]byte{128, 1, 4, 8, 14, 3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0},
			},
		}

		for _, data := range testData {
			res := WriteDeltaINT32(data.nums)
			require.Equal(t, string(data.expected), string(res))
		}
	})

	t.Run("int64", func(t *testing.T) {
		testData := []struct {
			nums     []any
			expected []byte
		}{
			{[]any{int64(1), int64(2), int64(3), int64(4), int64(5)}, []byte{128, 1, 4, 5, 2, 2, 0, 0, 0, 0}},
			{
				[]any{int64(7), int64(5), int64(3), int64(1), int64(2), int64(3), int64(4), int64(5)},
				[]byte{128, 1, 4, 8, 14, 3, 2, 0, 0, 0, 192, 63, 0, 0, 0, 0, 0, 0},
			},
		}

		for _, data := range testData {
			res := WriteDeltaINT64(data.nums)
			require.Equal(t, string(data.expected), string(res))
		}
	})

	t.Run("length_byte_array", func(t *testing.T) {
		testData := []struct {
			nums     []any
			expected []byte
		}{
			{[]any{"Hello", "World", "Foobar", "ABCDEF"}, []byte{128, 1, 4, 4, 10, 0, 1, 0, 0, 0, 2, 0, 0, 0, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100, 70, 111, 111, 98, 97, 114, 65, 66, 67, 68, 69, 70}},
		}

		for _, data := range testData {
			res := WriteDeltaLengthByteArray(data.nums)
			require.Equal(t, string(data.expected), string(res))
		}
	})
}

func TestWriteDeltaEmpty(t *testing.T) {
	t.Run("WriteDeltaINT32", func(t *testing.T) {
		res := WriteDeltaINT32([]any{})
		require.Equal(t, []byte{128, 1, 4, 0, 0}, res)
	})

	t.Run("WriteDeltaINT64", func(t *testing.T) {
		res := WriteDeltaINT64([]any{})
		require.Equal(t, []byte{128, 1, 4, 0, 0}, res)
	})

	t.Run("WriteDeltaLengthByteArray", func(t *testing.T) {
		res := WriteDeltaLengthByteArray([]any{})
		// Expect header for lengths (INT32)
		require.Equal(t, []byte{128, 1, 4, 0, 0}, res)
	})

	t.Run("WriteDeltaByteArray", func(t *testing.T) {
		res := WriteDeltaByteArray([]any{})
		// Expect header for prefixes (INT32) + header for suffixes (LengthByteArray -> INT32)
		expected := []byte{128, 1, 4, 0, 0, 128, 1, 4, 0, 0}
		require.Equal(t, expected, res)
	})
}
