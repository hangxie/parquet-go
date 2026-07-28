package encoding

import (
	"bytes"
	"fmt"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestWriteBitPacked(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require.Nil(t, WriteBitPacked(nil, 1, true))
	})

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

	t.Run("round_trip_bit_widths", func(t *testing.T) {
		testCases := []struct {
			name     string
			bitWidth uint64
			vals     []int64
		}{
			{name: "zero", bitWidth: 0, vals: []int64{0, 0, 0, 0, 0, 0, 0, 0}},
			{name: "one", bitWidth: 1, vals: []int64{0, 1, 0, 1, 1, 0, 1, 0}},
			{name: "seven", bitWidth: 7, vals: []int64{0, 1, 63, 64, 65, 126, 127, 42}},
			{name: "eight", bitWidth: 8, vals: []int64{0, 1, 127, 128, 129, 254, 255, 42}},
			{name: "nine", bitWidth: 9, vals: []int64{0, 1, 255, 256, 257, 510, 511, 42}},
			{name: "thirty_one", bitWidth: 31, vals: []int64{0, 1, 1<<30 - 1, 1 << 30, 1<<30 + 1, 1<<31 - 2, 1<<31 - 1, 42}},
			{name: "thirty_two", bitWidth: 32, vals: []int64{0, 1, 1<<31 - 1, 1 << 31, 1<<31 + 1, 1<<32 - 2, 1<<32 - 1, 42}},
			{name: "sixty_three", bitWidth: 63, vals: []int64{0, 1, 1<<62 - 1, 1 << 62, 1<<62 + 1, 1<<63 - 2, 1<<63 - 1, 42}},
			{name: "sixty_four", bitWidth: 64, vals: []int64{0, 1, 1<<62 - 1, 1 << 62, 1<<62 + 1, 1<<63 - 2, 1<<63 - 1, 42}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				vals := make([]any, len(tc.vals))
				for i := range tc.vals {
					vals[i] = tc.vals[i]
				}
				encoded := WriteBitPacked(vals, int64(tc.bitWidth), true)
				decoded, err := ReadBitPacked(bytes.NewReader(encoded[1:]), uint64(encoded[0]), tc.bitWidth)
				require.NoError(t, err)
				require.Equal(t, vals, decoded)
			})
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

func TestWriteRLEHybridRunSelection(t *testing.T) {
	testCases := []struct {
		name       string
		vals       []int32
		bitWidth   int32
		expected   []byte
		valueCount uint64
	}{
		{
			name:       "bit_packed_literals",
			vals:       []int32{0, 1, 2, 3, 4, 5, 6, 7},
			bitWidth:   3,
			expected:   []byte{3, 0x88, 0xC6, 0xFA},
			valueCount: 8,
		},
		{
			name:       "rle_at_eight_repeats",
			vals:       []int32{5, 5, 5, 5, 5, 5, 5, 5},
			bitWidth:   3,
			expected:   []byte{16, 5},
			valueCount: 8,
		},
		{
			name:       "literal_alignment_before_rle",
			vals:       []int32{0, 1, 2, 3, 4, 5, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7},
			bitWidth:   3,
			expected:   []byte{3, 0x88, 0xC6, 0xFA, 16, 7},
			valueCount: 16,
		},
		{
			name:       "padded_literal_tail",
			vals:       []int32{1, 2, 3},
			bitWidth:   2,
			expected:   []byte{3, 0x39, 0},
			valueCount: 3,
		},
		{
			name:       "zero_bit_width",
			vals:       []int32{0, 0, 0},
			bitWidth:   0,
			expected:   []byte{3},
			valueCount: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vals := make([]any, len(tc.vals))
			for i := range tc.vals {
				vals[i] = int32(tc.vals[i])
			}

			res, err := WriteRLE(vals, tc.bitWidth, parquet.Type_INT32)
			require.NoError(t, err)
			require.Equal(t, tc.expected, res)
			require.Equal(t, tc.expected, WriteRLEInt32(tc.vals, tc.bitWidth))

			decoded, err := ReadRLEBitPackedHybrid(bytes.NewReader(res), uint64(tc.bitWidth), uint64(len(res)), tc.valueCount)
			require.NoError(t, err)
			expectedDecoded := make([]any, len(tc.vals))
			for i := range tc.vals {
				expectedDecoded[i] = int64(tc.vals[i])
			}
			require.Equal(t, expectedDecoded, decoded[:tc.valueCount])
		})
	}
}

func TestWriteRLEHybridRoundTripRandomized(t *testing.T) {
	const (
		bitWidth       = 5
		casesPerOffset = 64
	)

	random := rand.New(rand.NewSource(0))
	for alignment := range bitPackedGroup {
		t.Run(fmt.Sprintf("literal_alignment_%d", alignment), func(t *testing.T) {
			for testCase := range casesPerOffset {
				vals := make([]int32, 0, 128)
				vals = appendRepeatedInt32(vals, int32(random.Intn(32)), 8+random.Intn(24))
				vals = appendLiteralInt32(vals, alignment, random)
				vals = appendRepeatedInt32(vals, differentInt32(vals[len(vals)-1], random), 8+random.Intn(24))

				for range 2 + random.Intn(6) {
					if random.Intn(2) == 0 {
						vals = appendRepeatedInt32(vals, differentInt32(vals[len(vals)-1], random), 8+random.Intn(24))
					} else {
						vals = appendLiteralInt32(vals, 1+random.Intn(23), random)
					}
				}

				expected := make([]any, len(vals))
				genericVals := make([]any, len(vals))
				for i := range vals {
					expected[i] = int64(vals[i])
					genericVals[i] = vals[i]
				}

				genericEncoded, err := WriteRLE(genericVals, bitWidth, parquet.Type_INT32)
				require.NoError(t, err)
				int32Encoded := WriteRLEInt32(vals, bitWidth)
				require.Equal(t, genericEncoded, int32Encoded)

				decoded, err := ReadRLEBitPackedHybrid(
					bytes.NewReader(genericEncoded),
					bitWidth,
					uint64(len(genericEncoded)),
					uint64(len(vals)),
				)
				require.NoError(t, err, "case %d", testCase)
				require.Equal(t, expected, decoded, "case %d", testCase)
			}
		})
	}
}

func appendRepeatedInt32(dst []int32, val int32, count int) []int32 {
	for range count {
		dst = append(dst, val)
	}
	return dst
}

func appendLiteralInt32(dst []int32, count int, random *rand.Rand) []int32 {
	for range count {
		dst = append(dst, differentInt32(dst[len(dst)-1], random))
	}
	return dst
}

func differentInt32(previous int32, random *rand.Rand) int32 {
	val := int32(random.Intn(31))
	if val >= previous {
		val++
	}
	return val
}

func TestWriteRLE(t *testing.T) {
	t.Run("bit_packed_hybrid", func(t *testing.T) {
		testCases := []struct {
			name        string
			vals        []any
			bitWidths   int32
			pt          parquet.Type
			expectError bool
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
				name:      "valid_boolean",
				vals:      []any{false, true, false},
				bitWidths: 1,
				pt:        parquet.Type_BOOLEAN,
			},
			{
				name:      "empty_input",
				vals:      []any{},
				bitWidths: 1,
				pt:        parquet.Type_INT64,
			},
			{
				name:        "invalid_boolean_value",
				vals:        []any{int32(1)},
				bitWidths:   1,
				pt:          parquet.Type_BOOLEAN,
				expectError: true,
			},
			{
				name:        "invalid_int32_value",
				vals:        []any{int64(1)},
				bitWidths:   1,
				pt:          parquet.Type_INT32,
				expectError: true,
			},
			{
				name:        "invalid_int64_value",
				vals:        []any{int32(1)},
				bitWidths:   1,
				pt:          parquet.Type_INT64,
				expectError: true,
			},
			{
				name:        "unsupported_type",
				vals:        []any{"string1", "string2"},
				bitWidths:   2,
				pt:          parquet.Type_BYTE_ARRAY,
				expectError: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := WriteRLEBitPackedHybrid(tc.vals, tc.bitWidths, tc.pt)
				if tc.expectError {
					require.Error(t, err)
					return
				}
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
