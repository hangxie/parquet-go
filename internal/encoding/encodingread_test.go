package encoding

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestReadBitPacked(t *testing.T) {
	t.Run("round_trip", func(t *testing.T) {
		testData := [][]any{
			{1, 2, 3, 4, 5, 6, 7, 8},
			{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}
		for _, data := range testData {
			ln := len(data)
			header := ((ln/8)<<1 | 1)
			bitWidth := uint64(bits.Len(uint(data[ln-1].(int))))
			res, _ := ReadBitPacked(bytes.NewReader(WriteBitPacked(data, int64(bitWidth), false)), uint64(header), bitWidth)
			require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res), "ReadBitPacked err, expect %v, get %v", data, res)
		}
	})
}

//nolint:gocognit
func TestReadByteStreamSplit(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		testCases := []struct {
			name     string
			input    []byte
			count    uint64
			expected []any
		}{
			{
				name:     "single_value",
				input:    []byte{0x00, 0x00, 0x80, 0x3F}, // 1.0 in IEEE 754, byte-stream split format
				count:    1,
				expected: []any{math.Float32frombits(0x3F800000)}, // 1.0
			},
			{
				name:     "empty_count",
				input:    []byte{},
				count:    0,
				expected: []any{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				reader := bytes.NewReader(tc.input)
				result, err := ReadByteStreamSplitFloat32(reader, tc.count)
				if tc.count > 0 && len(tc.input) >= int(tc.count*4) {
					require.NoError(t, err)
				}

				require.Len(t, result, len(tc.expected))

				for i, expected := range tc.expected {
					if i < len(result) {
						require.Equal(t, expected, result[i], "Value mismatch at index %d", i)
					}
				}
			})
		}
	})

	t.Run("float32_error", func(t *testing.T) {
		// Test insufficient data
		reader := bytes.NewReader([]byte{0x00, 0x00}) // Only 2 bytes, need 4
		_, err := ReadByteStreamSplitFloat32(reader, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected EOF")
	})

	t.Run("float64", func(t *testing.T) {
		testCases := []struct {
			name     string
			input    []byte
			count    uint64
			expected []any
		}{
			{
				name:     "single_value",
				input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}, // 1.0 in IEEE 754, byte-stream split format
				count:    1,
				expected: []any{math.Float64frombits(0x3FF0000000000000)}, // 1.0
			},
			{
				name:     "empty_count",
				input:    []byte{},
				count:    0,
				expected: []any{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				reader := bytes.NewReader(tc.input)
				result, err := ReadByteStreamSplitFloat64(reader, tc.count)
				if tc.count > 0 && len(tc.input) >= int(tc.count*8) {
					require.NoError(t, err)
				}

				require.Len(t, result, len(tc.expected))

				for i, expected := range tc.expected {
					if i < len(result) {
						require.Equal(t, expected, result[i], "Value mismatch at index %d", i)
					}
				}
			})
		}
	})

	t.Run("float64_error", func(t *testing.T) {
		// Test insufficient data
		reader := bytes.NewReader([]byte{0x00, 0x00, 0x00, 0x00}) // Only 4 bytes, need 8
		_, err := ReadByteStreamSplitFloat64(reader, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected EOF")
	})

	t.Run("int32", func(t *testing.T) {
		testCases := []struct {
			name     string
			input    []any
			count    uint64
			expected []any
		}{
			{
				name:     "single_value",
				input:    []any{int32(12345)},
				count:    1,
				expected: []any{int32(12345)},
			},
			{
				name:     "multiple_values",
				input:    []any{int32(1), int32(2), int32(-3)},
				count:    3,
				expected: []any{int32(1), int32(2), int32(-3)},
			},
			{
				name:     "empty_count",
				input:    []any{},
				count:    0,
				expected: []any{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Write then read
				buf := WriteByteStreamSplitINT32(tc.input)
				reader := bytes.NewReader(buf)
				result, err := ReadByteStreamSplitINT32(reader, tc.count)
				require.NoError(t, err)
				require.Equal(t, len(tc.expected), len(result))
				for i := range tc.expected {
					require.Equal(t, tc.expected[i], result[i])
				}
			})
		}
	})

	t.Run("int32_error", func(t *testing.T) {
		// Test insufficient data
		reader := bytes.NewReader([]byte{0x00, 0x00}) // Only 2 bytes, need 4
		_, err := ReadByteStreamSplitINT32(reader, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected EOF")
	})

	t.Run("int64", func(t *testing.T) {
		testCases := []struct {
			name     string
			input    []any
			count    uint64
			expected []any
		}{
			{
				name:     "single_value",
				input:    []any{int64(1234567890123)},
				count:    1,
				expected: []any{int64(1234567890123)},
			},
			{
				name:     "multiple_values",
				input:    []any{int64(1), int64(2), int64(-3)},
				count:    3,
				expected: []any{int64(1), int64(2), int64(-3)},
			},
			{
				name:     "empty_count",
				input:    []any{},
				count:    0,
				expected: []any{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Write then read
				buf := WriteByteStreamSplitINT64(tc.input)
				reader := bytes.NewReader(buf)
				result, err := ReadByteStreamSplitINT64(reader, tc.count)
				require.NoError(t, err)
				require.Equal(t, len(tc.expected), len(result))
				for i := range tc.expected {
					require.Equal(t, tc.expected[i], result[i])
				}
			})
		}
	})

	t.Run("int64_error", func(t *testing.T) {
		// Test insufficient data
		reader := bytes.NewReader([]byte{0x00, 0x00, 0x00, 0x00}) // Only 4 bytes, need 8
		_, err := ReadByteStreamSplitINT64(reader, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected EOF")
	})

	t.Run("fixed_len_byte_array", func(t *testing.T) {
		testCases := []struct {
			name     string
			input    []any
			count    uint64
			elemSize uint64
			expected []any
		}{
			{
				name:     "single_value",
				input:    []any{"abcd"},
				count:    1,
				elemSize: 4,
				expected: []any{"abcd"},
			},
			{
				name:     "multiple_values",
				input:    []any{"ab", "cd", "ef"},
				count:    3,
				elemSize: 2,
				expected: []any{"ab", "cd", "ef"},
			},
			{
				name:     "empty_count",
				input:    []any{},
				count:    0,
				elemSize: 2,
				expected: []any{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Write then read
				buf := WriteByteStreamSplitFixedLenByteArray(tc.input)
				reader := bytes.NewReader(buf)
				result, err := ReadByteStreamSplitFixedLenByteArray(reader, tc.count, tc.elemSize)
				require.NoError(t, err)
				require.Equal(t, len(tc.expected), len(result))
				for i := range tc.expected {
					require.Equal(t, tc.expected[i], result[i])
				}
			})
		}
	})

	t.Run("fixed_len_byte_array_error", func(t *testing.T) {
		// Test insufficient data
		reader := bytes.NewReader([]byte{0x00, 0x00}) // Only 2 bytes, need 4
		_, err := ReadByteStreamSplitFixedLenByteArray(reader, 1, 4)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected EOF")

		// Test zero element size
		reader2 := bytes.NewReader([]byte{0x00, 0x00, 0x00, 0x00})
		_, err = ReadByteStreamSplitFixedLenByteArray(reader2, 1, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "element size must be > 0")
	})

	t.Run("invalid_count", func(t *testing.T) {
		tests := []struct {
			name      string
			count     uint64
			expectErr bool
		}{
			{
				name:      "float32_invalid_count",
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
			{
				name:      "float64_invalid_count",
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reader := bytes.NewReader([]byte{})

				if tt.name == "float32_invalid_count" {
					_, err := ReadByteStreamSplitFloat32(reader, tt.count)
					if tt.expectErr {
						require.Error(t, err)
						require.Contains(t, err.Error(), "invalid count")
					}
				} else {
					_, err := ReadByteStreamSplitFloat64(reader, tt.count)
					if tt.expectErr {
						require.Error(t, err)
						require.Contains(t, err.Error(), "invalid count")
					}
				}
			})
		}
	})
}

func TestReadDelta(t *testing.T) {
	t.Run("binary_packed_int64", func(t *testing.T) {
		testData := [][]any{
			{int64(1), int64(2), int64(3), int64(4)},
			{int64(math.MaxInt64), int64(math.MinInt64), int64(-15654523568543623), int64(4354365463543632), int64(0)},
		}

		for _, data := range testData {
			res, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(WriteDeltaINT64(data)))
			require.NoError(t, err)

			require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
		}
	})

	t.Run("binary_packed_int32", func(t *testing.T) {
		testData := [][]any{
			{int32(1), int32(2), int32(3), int32(4)},
			{int32(-1570499385), int32(-1570499385), int32(-1570499386), int32(-1570499388), int32(-1570499385)},
		}

		for _, data := range testData {

			res, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(WriteDeltaINT32(data)))
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
		}
	})

	t.Run("byte_array", func(t *testing.T) {
		testData := [][]any{
			{"Hello", "world"},
		}
		for _, data := range testData {
			res, _ := ReadDeltaByteArray(bytes.NewReader(WriteDeltaByteArray(data)))
			require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
		}
	})

	t.Run("int32_zigzag", func(t *testing.T) {
		testData := []int32{1, -1570499385, 3, -11, 1570499385, 111, 222, 333, 0}
		for _, data := range testData {
			u64 := uint64((data >> 31) ^ (data << 1))
			resZigZag, err := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(u64)))
			require.NoError(t, err)
			res32 := int32(resZigZag)
			res := int32(uint32(res32)>>1) ^ -(res32 & 1)
			require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res), "ReadUnsignedVarInt mismatch for %v", data)
		}
	})

	t.Run("length_byte_array", func(t *testing.T) {
		testData := [][]any{
			{"Hello", "world"},
		}
		for _, data := range testData {
			res, _ := ReadDeltaLengthByteArray(bytes.NewReader(WriteDeltaLengthByteArray(data)))
			require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
		}
	})
}

func TestReadRLEBitPackedHybrid(t *testing.T) {
	t.Run("normal_cases", func(t *testing.T) {
		testData := [][]any{
			{int64(1), int64(2), int64(3), int64(4)},
			{int64(0), int64(0), int64(0), int64(0), int64(0)},
		}
		for _, data := range testData {
			maxVal := uint64(data[len(data)-1].(int64))
			buf, err := WriteRLEBitPackedHybrid(data, int32(bits.Len64(maxVal)), parquet.Type_INT64)
			require.NoError(t, err)
			res, err := ReadRLEBitPackedHybrid(bytes.NewReader(buf), uint64(bits.Len64(maxVal)), 0)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
		}
	})

	t.Run("very_large_length", func(t *testing.T) {
		// Test with a very large length that would cause memory issues
		data := []byte{0x01, 0x02, 0x03, 0x04}
		reader := bytes.NewReader(data)

		// This should fail because we don't have enough data
		_, err := ReadRLEBitPackedHybrid(reader, 1, 1000000)
		require.Error(t, err)
		require.Contains(t, err.Error(), "EOF")
	})

	t.Run("coverage", func(t *testing.T) {
		tests := []struct {
			name        string
			setupData   func() *bytes.Reader
			bitWidth    uint64
			length      uint64
			expectError bool
			expectedLen int
		}{
			{
				name: "valid_data_with_positive_length",
				setupData: func() *bytes.Reader {
					// Create valid RLE bit-packed hybrid data
					data := []byte{
						0x02, 0x00, 0x00, 0x00, // length = 2
						0x08, 0x01, // RLE run with value 1, count 4 (encoded as 0x08 << 1 | 0x01)
					}
					return bytes.NewReader(data)
				},
				bitWidth:    1,
				length:      6, // 4 bytes for length + 2 bytes for data
				expectError: false,
				expectedLen: 0, // Expected to return some values but exact count depends on implementation
			},
			{
				name: "zero_length_reads_from_stream",
				setupData: func() *bytes.Reader {
					// When length=0, function reads length from stream as INT32
					data := []byte{
						0x04, 0x00, 0x00, 0x00, // length = 4 (read as INT32)
						0x08, 0x01, 0x02, 0x03, // 4 bytes of data
					}
					return bytes.NewReader(data)
				},
				bitWidth:    2,
				length:      0, // This triggers reading length from stream
				expectError: false,
				expectedLen: 0,
			},
			{
				name: "empty_reader",
				setupData: func() *bytes.Reader {
					return bytes.NewReader([]byte{})
				},
				bitWidth:    1,
				length:      0,
				expectError: true, // Should fail when trying to read length
			},
			{
				name: "insufficient_data_for_length",
				setupData: func() *bytes.Reader {
					// Only 2 bytes when we need 4 for INT32 length
					data := []byte{0x02, 0x00}
					return bytes.NewReader(data)
				},
				bitWidth:    1,
				length:      0,
				expectError: true,
			},
			{
				name: "valid_length_but_insufficient_data",
				setupData: func() *bytes.Reader {
					// Says it has 10 bytes but only provides 2
					data := []byte{0x01, 0x02}
					return bytes.NewReader(data)
				},
				bitWidth:    1,
				length:      10, // Claims 10 bytes but only has 2
				expectError: true,
			},
			{
				name: "bitwidth_zero",
				setupData: func() *bytes.Reader {
					data := []byte{
						0x02, 0x00, 0x00, 0x00, // length = 2
						0x01, 0x02, // 2 bytes of data
					}
					return bytes.NewReader(data)
				},
				bitWidth:    0, // Edge case: zero bit width
				length:      6,
				expectError: false,
				expectedLen: 0,
			},
			{
				name: "invalid_rle_data_format",
				setupData: func() *bytes.Reader {
					data := []byte{
						0x02, 0x00, 0x00, 0x00, // length = 2
						0x01, 0x02, // 2 bytes of invalid RLE data
					}
					return bytes.NewReader(data)
				},
				bitWidth:    4,    // 4 bits
				length:      6,    // 4 bytes for length + 2 bytes for data
				expectError: true, // Expect error due to invalid RLE format
				expectedLen: 0,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reader := tt.setupData()

				result, err := ReadRLEBitPackedHybrid(reader, tt.bitWidth, tt.length)

				if tt.expectError {
					require.Error(t, err)
					require.Contains(t, err.Error(), "EOF")
				} else {
					require.NoError(t, err)
					require.NotNil(t, result)
				}
			})
		}
	})
}

func TestReadUnsignedVarInt(t *testing.T) {
	i32 := int32(-1570499385)

	testData := []uint64{1, 2, 3, 11, 1570499385, uint64(i32), 111, 222, 333, 0}
	for _, data := range testData {
		res, _ := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(data)))
		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res), "ReadUnsignedVarInt mismatch for %v", data)
	}
}

func TestValidateCount(t *testing.T) {
	tests := []struct {
		name        string
		count       uint64
		expectError bool
		errorMsg    string
	}{
		{
			name:        "zero_count",
			count:       0,
			expectError: false,
		},
		{
			name:        "small_count",
			count:       100,
			expectError: false,
		},
		{
			name:        "medium_count",
			count:       1000000,
			expectError: false,
		},
		{
			name:        "large_valid_count",
			count:       maxAllowedCount, // max int
			expectError: false,
		},
		{
			name:        "count_exceeds_max_int",
			count:       maxAllowedCount + 1,
			expectError: true,
			errorMsg:    "invalid count",
		},
		{
			name:        "max_uint64",
			count:       ^uint64(0),
			expectError: true,
			errorMsg:    "invalid count",
		},
		{
			name:        "very_large_count",
			count:       uint64(1) << 63,
			expectError: true,
			errorMsg:    "invalid count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCount(tt.count)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReadDeltaEmpty(t *testing.T) {
	t.Run("ReadDeltaBinaryPackedINT32", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader([]byte{}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "EOF")
	})

	t.Run("ReadDeltaBinaryPackedINT64", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader([]byte{}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "EOF")
	})

	t.Run("ReadDeltaLengthByteArray", func(t *testing.T) {
		_, err := ReadDeltaLengthByteArray(bytes.NewReader([]byte{}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "EOF")
	})

	t.Run("ReadDeltaByteArray", func(t *testing.T) {
		_, err := ReadDeltaByteArray(bytes.NewReader([]byte{}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "EOF")
	})
}

func TestReadDeltaLengthByteArray_NegativeLength(t *testing.T) {
	// WriteDeltaINT32 with a negative value produces a valid delta-encoded stream
	// that ReadDeltaBinaryPackedINT64 decodes to a negative int64 length.
	lengths := WriteDeltaINT32([]any{int32(-5)})
	_, err := ReadDeltaLengthByteArray(bytes.NewReader(lengths))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid negative length")
}

func TestReadDeltaByteArrayWithEmptyPrefixLengths(t *testing.T) {
	var buf []byte
	buf = append(buf, WriteUnsignedVarInt(128)...)
	buf = append(buf, WriteUnsignedVarInt(4)...)
	buf = append(buf, WriteUnsignedVarInt(0)...) // numValues = 0
	buf = append(buf, WriteUnsignedVarInt(0)...) // firstValue = 0

	fullBuf := append(buf, buf...) // Two such streams

	res, err := ReadDeltaByteArray(bytes.NewReader(fullBuf))
	require.NoError(t, err)
	require.Empty(t, res)
}
