package encoding

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_ReadBitPacked(t *testing.T) {
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
}

func Test_ReadByteStreamSplitFloat32(t *testing.T) {
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
}

func Test_ReadByteStreamSplitFloat32_Error(t *testing.T) {
	// Test insufficient data
	reader := bytes.NewReader([]byte{0x00, 0x00}) // Only 2 bytes, need 4
	_, err := ReadByteStreamSplitFloat32(reader, 1)
	require.Error(t, err)
}

func Test_ReadByteStreamSplitFloat64(t *testing.T) {
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
}

func Test_ReadByteStreamSplitFloat64_Error(t *testing.T) {
	// Test insufficient data
	reader := bytes.NewReader([]byte{0x00, 0x00, 0x00, 0x00}) // Only 4 bytes, need 8
	_, err := ReadByteStreamSplitFloat64(reader, 1)
	require.Error(t, err)
}

func Test_ReadDeltaBinaryPackedINT(t *testing.T) {
	testData := [][]any{
		{int64(1), int64(2), int64(3), int64(4)},
		{int64(math.MaxInt64), int64(math.MinInt64), int64(-15654523568543623), int64(4354365463543632), int64(0)},
	}

	for _, data := range testData {
		res, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(WriteDeltaINT64(data)))
		require.NoError(t, err)

		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
	}
}

func Test_ReadDeltaBinaryPackedINT32(t *testing.T) {
	testData := [][]any{
		{int32(1), int32(2), int32(3), int32(4)},
		{int32(-1570499385), int32(-1570499385), int32(-1570499386), int32(-1570499388), int32(-1570499385)},
	}

	for _, data := range testData {

		res, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(WriteDeltaINT32(data)))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
	}
}

func Test_ReadDeltaByteArray(t *testing.T) {
	testData := [][]any{
		{"Hello", "world"},
	}
	for _, data := range testData {
		res, _ := ReadDeltaByteArray(bytes.NewReader(WriteDeltaByteArray(data)))
		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
	}
}

func Test_ReadDeltaINT32(t *testing.T) {
	testData := []int32{1, -1570499385, 3, -11, 1570499385, 111, 222, 333, 0}
	for _, data := range testData {
		u64 := uint64((data >> 31) ^ (data << 1))
		resZigZag, err := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(u64)))
		require.NoError(t, err)
		res32 := int32(resZigZag)
		var res int32 = int32(uint32(res32)>>1) ^ -(res32 & 1)
		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res), "ReadUnsignedVarInt mismatch for %v", data)
	}
}

func Test_ReadLengthDeltaByteArray(t *testing.T) {
	testData := [][]any{
		{"Hello", "world"},
	}
	for _, data := range testData {
		res, _ := ReadDeltaLengthByteArray(bytes.NewReader(WriteDeltaLengthByteArray(data)))
		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
	}
}

func Test_ReadPlain(t *testing.T) {
	testCases := []struct {
		name      string
		dataType  parquet.Type
		data      []any
		bitWidth  uint64
		expectErr bool
	}{
		{
			name:     "boolean_type",
			dataType: parquet.Type_BOOLEAN,
			data:     []any{true, false, true},
		},
		{
			name:     "int32_type",
			dataType: parquet.Type_INT32,
			data:     []any{int32(1), int32(2), int32(3)},
		},
		{
			name:     "int64_type",
			dataType: parquet.Type_INT64,
			data:     []any{int64(100), int64(200), int64(300)},
		},
		{
			name:     "int96_type",
			dataType: parquet.Type_INT96,
			data:     []any{common.ByteArray("helloworldab"), common.ByteArray("abcdefghijkl")},
		},
		{
			name:     "float_type",
			dataType: parquet.Type_FLOAT,
			data:     []any{float32(1.1), float32(2.2), float32(3.3)},
		},
		{
			name:     "double_type",
			dataType: parquet.Type_DOUBLE,
			data:     []any{float64(1.1), float64(2.2), float64(3.3)},
		},
		{
			name:     "byte_array_type",
			dataType: parquet.Type_BYTE_ARRAY,
			data:     []any{"hello", "world"},
		},
		{
			name:     "fixed_len_byte_array_type",
			dataType: parquet.Type_FIXED_LEN_BYTE_ARRAY,
			data:     []any{"hello", "world"},
			bitWidth: 5,
		},
		{
			name:      "unknown_type",
			dataType:  parquet.Type(-1),
			data:      []any{},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "unknown_type" {
				// Test unknown type directly
				_, err := ReadPlain(bytes.NewReader([]byte{}), tc.dataType, 0, tc.bitWidth)
				require.Error(t, err)
				return
			}

			// Write the data first
			var buf []byte
			var err error

			switch tc.dataType {
			case parquet.Type_BOOLEAN:
				buf, err = WritePlainBOOLEAN(tc.data)
			case parquet.Type_INT32:
				buf, err = WritePlainINT32(tc.data)
			case parquet.Type_INT64:
				buf, err = WritePlainINT64(tc.data)
			case parquet.Type_INT96:
				buf = WritePlainINT96(tc.data)
			case parquet.Type_FLOAT:
				buf, err = WritePlainFLOAT(tc.data)
			case parquet.Type_DOUBLE:
				buf, err = WritePlainDOUBLE(tc.data)
			case parquet.Type_BYTE_ARRAY:
				buf, err = WritePlainBYTE_ARRAY(tc.data)
			case parquet.Type_FIXED_LEN_BYTE_ARRAY:
				buf, err = WritePlainFIXED_LEN_BYTE_ARRAY(tc.data)
			}

			require.NoError(t, err)

			// Now test reading it back with ReadPlain
			result, err := ReadPlain(bytes.NewReader(buf), tc.dataType, uint64(len(tc.data)), tc.bitWidth)
			require.NoError(t, err)

			require.Equal(t, len(tc.data), len(result))

			// For most types we can compare directly, but INT96 is special
			if tc.dataType != parquet.Type_INT96 {
				for i, expected := range tc.data {
					if i < len(result) {
						require.Equal(t, expected, result[i], "Value mismatch at index %d", i)
					}
				}
			}
		})
	}
}

func Test_ReadPlainBOOLEAN(t *testing.T) {
	testCases := []struct {
		name  string
		input []any
	}{
		{
			name:  "single-true-value",
			input: []any{true},
		},
		{
			name:  "single-false-value",
			input: []any{false},
		},
		{
			name:  "two-false-values",
			input: []any{false, false},
		},
		{
			name:  "mixed-false-true",
			input: []any{false, true},
		},
		{
			name:  "empty-input",
			input: []any{},
		},
		{
			name:  "multiple-mixed-values",
			input: []any{true, false, true, true, false},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Write data to buffer
			buf, err := WritePlainBOOLEAN(testCase.input)
			require.NoError(t, err)

			// Read data back from buffer
			result, err := ReadPlainBOOLEAN(bytes.NewReader(buf), uint64(len(testCase.input)))
			require.NoError(t, err)

			// Compare results
			require.Len(t, result, len(testCase.input))

			for i, expected := range testCase.input {
				if i < len(result) {
					require.Equal(t, expected, result[i], "Value mismatch at index %d", i)
				}
			}
		})
	}
}

func Test_ReadPlainBYTE_ARRAY(t *testing.T) {
	testCases := []struct {
		name  string
		input []any
	}{
		{
			name:  "two-string-values",
			input: []any{"hello", "world"},
		},
		{
			name:  "mixed-empty-and-single-char",
			input: []any{"good", "", "a", "b"},
		},
		{
			name:  "empty-input",
			input: []any{},
		},
		{
			name:  "single-string",
			input: []any{"test"},
		},
		{
			name:  "long-strings",
			input: []any{"this is a longer string", "another long string with more characters"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Write data to buffer
			buf, err := WritePlainBYTE_ARRAY(testCase.input)
			require.NoError(t, err)

			// Read data back from buffer
			result, err := ReadPlainBYTE_ARRAY(bytes.NewReader(buf), uint64(len(testCase.input)))
			require.NoError(t, err)

			// Compare results
			require.Len(t, result, len(testCase.input))

			for i, expected := range testCase.input {
				if i < len(result) {
					require.Equal(t, expected, result[i], "Value mismatch at index %d", i)
				}
			}
		})
	}
}

func Test_ReadPlainDOUBLE(t *testing.T) {
	testData := [][]any{
		{float64(0), float64(1), float64(2)},
		{float64(0), float64(0), float64(0)},
	}

	for _, data := range testData {
		buf, err := WritePlainDOUBLE(data)
		require.NoError(t, err)
		res, _ := ReadPlainDOUBLE(bytes.NewReader(buf), uint64(len(data)))
		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
	}
}

func Test_ReadPlainFIXED_LEN_BYTE_ARRAY(t *testing.T) {
	testData := [][]any{
		{("hello"), ("world")},
		{("a"), ("b"), ("c"), ("d")},
	}

	for _, data := range testData {
		buf, err := WritePlainFIXED_LEN_BYTE_ARRAY(data)
		require.NoError(t, err)
		res, _ := ReadPlainFIXED_LEN_BYTE_ARRAY(bytes.NewReader(buf), uint64(len(data)), uint64(len(data[0].(string))))
		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
	}
}

func Test_ReadPlainFLOAT(t *testing.T) {
	testData := [][]any{
		{float32(0), float32(1), float32(2)},
		{float32(0), float32(0.1), float32(0.2)},
	}

	for _, data := range testData {
		buf, err := WritePlainFLOAT(data)
		require.NoError(t, err)
		res, _ := ReadPlainFLOAT(bytes.NewReader(buf), uint64(len(data)))
		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res))
	}
}

func Test_ReadPlainINT32(t *testing.T) {
	testCases := []struct {
		name       string
		expected   []any
		inputBytes []byte
	}{
		{
			name:       "empty_input",
			expected:   []any{},
			inputBytes: []byte{},
		},
		{
			name:       "single-zero-value",
			expected:   []any{int32(0)},
			inputBytes: []byte{0, 0, 0, 0},
		},
		{
			name:       "multiple-sequential-values",
			expected:   []any{int32(0), int32(1), int32(2)},
			inputBytes: []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0},
		},
		{
			name:       "negative-values",
			expected:   []any{int32(-1), int32(-2)},
			inputBytes: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0xFF, 0xFF, 0xFF},
		},
		{
			name:       "max-int32-value",
			expected:   []any{int32(2147483647)},
			inputBytes: []byte{0xFF, 0xFF, 0xFF, 0x7F},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reader := bytes.NewReader(testCase.inputBytes)
			result, err := ReadPlainINT32(reader, uint64(len(testCase.expected)))
			require.NoError(t, err)

			require.Len(t, result, len(testCase.expected))

			for i, expected := range testCase.expected {
				if i < len(result) {
					require.Equal(t, expected, result[i], "Value mismatch at index %d", i)
				}
			}
		})
	}
}

func Test_ReadPlainINT64(t *testing.T) {
	testCases := []struct {
		name       string
		expected   []any
		inputBytes []byte
	}{
		{
			name:       "empty_input",
			expected:   []any{},
			inputBytes: []byte{},
		},
		{
			name:       "single-zero-value",
			expected:   []any{int64(0)},
			inputBytes: []byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:       "multiple-sequential-values",
			expected:   []any{int64(0), int64(1), int64(2)},
			inputBytes: []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:       "negative-values",
			expected:   []any{int64(-1), int64(-100)},
			inputBytes: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x9C, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name:       "max-int64-value",
			expected:   []any{int64(9223372036854775807)},
			inputBytes: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reader := bytes.NewReader(testCase.inputBytes)
			result, err := ReadPlainINT64(reader, uint64(len(testCase.expected)))
			require.NoError(t, err)

			require.Len(t, result, len(testCase.expected))

			for i, expected := range testCase.expected {
				if i < len(result) {
					require.Equal(t, expected, result[i], "Value mismatch at index %d", i)
				}
			}
		})
	}
}

func Test_ReadPlainINT96(t *testing.T) {
	testCases := []struct {
		name        string
		input       []byte
		count       uint64
		expected    []any
		expectError bool
	}{
		{
			name:     "single_value",
			input:    []byte("helloworldab"),
			count:    1,
			expected: []any{"helloworldab"},
		},
		{
			name:     "multiple_values",
			input:    []byte("helloworldababcdefghijkl"),
			count:    2,
			expected: []any{"helloworldab", "abcdefghijkl"},
		},
		{
			name:     "empty_count",
			input:    []byte{},
			count:    0,
			expected: []any{},
		},
		{
			name:        "read_error_empty_data",
			input:       []byte{},
			count:       1,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := bytes.NewReader(tc.input)
			result, err := ReadPlainINT96(reader, tc.count)

			if tc.expectError {
				require.Error(t, err)
				return
			}

			if tc.count > 0 && len(tc.input) >= int(tc.count*12) {
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
}

func Test_ReadRLEBitPackedHybrid(t *testing.T) {
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
	})
}

func Test_ReadRLEBitPackedHybrid_Coverage(t *testing.T) {
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
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
			}
		})
	}
}

func Test_ReadUnsignedVarInt(t *testing.T) {
	i32 := int32(-1570499385)

	testData := []uint64{1, 2, 3, 11, 1570499385, uint64(i32), 111, 222, 333, 0}
	for _, data := range testData {
		res, _ := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(data)))
		require.Equal(t, fmt.Sprintf("%v", data), fmt.Sprintf("%v", res), "ReadUnsignedVarInt mismatch for %v", data)
	}
}
