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
