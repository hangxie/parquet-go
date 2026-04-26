package encoding

import (
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestWriteBitPacked(t *testing.T) {
	t.Run("standard", func(t *testing.T) {
		testData := []struct {
			nums     []any
			expected []byte
		}{
			{[]any{0, 0, 0, 0, 0, 0, 0, 0}, []byte{3}},
			{[]any{0, 1, 2, 3, 4, 5, 6, 7}, []byte{3, 0x88, 0xC6, 0xFA}},
		}

		for _, data := range testData {
			res := WriteBitPacked(data.nums, int64(bits.Len64(uint64(data.nums[len(data.nums)-1].(int)))), true)
			require.Equal(t, string(data.expected), string(res))
		}
	})
}

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

func TestWriteRLE(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		testData := []struct {
			nums     []any
			expected []byte
		}{
			{[]any{int64(0), int64(0), int64(0)}, []byte{byte(3 << 1)}},
			{[]any{int64(3)}, []byte{byte(1 << 1), byte(3)}},
			{[]any{int64(1), int64(2), int64(3), int64(3)}, []byte{byte(1 << 1), byte(1), byte(1 << 1), byte(2), byte(2 << 1), byte(3)}},
		}

		for _, data := range testData {
			res, err := WriteRLE(data.nums, int32(bits.Len64(uint64(data.nums[len(data.nums)-1].(int64)))), parquet.Type_INT64)
			require.NoError(t, err)
			require.Equal(t, string(data.expected), string(res))
		}
	})

	t.Run("bit_packed_hybrid", func(t *testing.T) {
		testCases := []struct {
			name      string
			vals      []any
			bitWidths int32
			pt        parquet.Type
		}{
			{
				name:      "valid_int64",
				vals:      []any{int64(1), int64(2), int64(3)},
				bitWidths: 2,
				pt:        parquet.Type_INT64,
			},
			{
				name:      "valid_int32",
				vals:      []any{int32(1), int32(2), int32(3)},
				bitWidths: 2,
				pt:        parquet.Type_INT32,
			},
			{
				name:      "empty_input",
				vals:      []any{},
				bitWidths: 1,
				pt:        parquet.Type_INT64,
			},
			{
				name:      "unsupported_type",
				vals:      []any{"string1", "string2"},
				bitWidths: 2,
				pt:        parquet.Type_BYTE_ARRAY,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := WriteRLEBitPackedHybrid(tc.vals, tc.bitWidths, tc.pt)
				require.NoError(t, err)

				// Result should have at least 4 bytes for the length header
				require.GreaterOrEqual(t, len(result), 4)
			})
		}
	})

	t.Run("bit_packed_hybrid_int32", func(t *testing.T) {
		testCases := []struct {
			name      string
			vals      []int32
			bitWidths int32
		}{
			{
				name:      "single_value",
				vals:      []int32{1},
				bitWidths: 1,
			},
			{
				name:      "multiple_values",
				vals:      []int32{1, 2, 3, 4},
				bitWidths: 3,
			},
			{
				name:      "empty_vals",
				vals:      []int32{},
				bitWidths: 1,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := WriteRLEBitPackedHybridInt32(tc.vals, tc.bitWidths)
				require.NoError(t, err)
				if len(tc.vals) == 0 {
					require.Equal(t, 4, len(result))
				}
				if len(tc.vals) > 0 {
					require.NotZero(t, len(result))
				}
			})
		}
	})

	t.Run("int32", func(t *testing.T) {
		testCases := []struct {
			name     string
			vals     []int32
			bitWidth int32
		}{
			{
				name:     "single_value",
				vals:     []int32{1},
				bitWidth: 1,
			},
			{
				name:     "repeated_values",
				vals:     []int32{1, 1, 1, 2, 2},
				bitWidth: 2,
			},
			{
				name:     "all_same",
				vals:     []int32{5, 5, 5, 5},
				bitWidth: 3,
			},
			{
				name:     "empty_vals",
				vals:     []int32{},
				bitWidth: 1,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := WriteRLEInt32(tc.vals, tc.bitWidth)
				if len(tc.vals) == 0 {
					require.Equal(t, 0, len(result))
				}
				if len(tc.vals) > 0 {
					require.NotZero(t, len(result))
				}
			})
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
