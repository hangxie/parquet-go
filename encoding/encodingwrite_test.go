package encoding

import (
	"encoding/json"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_ToInt64(t *testing.T) {
	testData := []struct {
		nums     []any
		expected []int64
	}{
		{nums: []any{int(1), int(2), int(3)}, expected: []int64{int64(1), int64(2), int64(3)}},
		{nums: []any{true, false, true}, expected: []int64{int64(1), int64(0), int64(1)}},
		{nums: []any{}, expected: []int64{}},
	}

	for _, data := range testData {
		res := ToInt64(data.nums)
		sb1, _ := json.Marshal(res)
		sb2, _ := json.Marshal(data.expected)
		s1, s2 := string(sb1), string(sb2)
		require.Equal(t, s1, s2)

	}
}

func Test_WriteBitPacked(t *testing.T) {
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
}

func Test_WriteBitPackedDeprecated(t *testing.T) {
	testData := []struct {
		nums     []any
		expected []byte
	}{
		// [1,2,3,4] with bitWidth=3, LSB-first: 001|010|011|100 = 11010001|00001000 = [0xD1, 0x08] = [209, 8]
		{[]any{1, 2, 3, 4}, []byte{209, 8}},
	}

	for _, data := range testData {
		res := WriteBitPackedDeprecated(data.nums, 3)
		require.Equal(t, string(data.expected), string(res))
	}
}

func Test_WriteByteStreamSplit(t *testing.T) {
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
}

func Test_WriteByteStreamSplitFloat32(t *testing.T) {
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
}

func Test_WriteByteStreamSplitFloat64(t *testing.T) {
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
}

func Test_WriteDelta(t *testing.T) {
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
			result := WriteDelta(tc.src)
			if tc.name == "unsupported_type" || tc.name == "empty_input" {
				require.Len(t, result, 0)
				return
			}
			require.NotZero(t, len(result))
		})
	}
}

func Test_WriteDeltaByteArray(t *testing.T) {
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
}

func Test_WriteDeltaINT32(t *testing.T) {
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
}

func Test_WriteDeltaINT64(t *testing.T) {
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
}

func Test_WriteDeltaLengthByteArray(t *testing.T) {
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
}

func Test_WritePlain(t *testing.T) {
	testCases := []struct {
		name      string
		src       []any
		dataType  parquet.Type
		expectErr bool
	}{
		{
			name:     "empty_data",
			src:      []any{},
			dataType: parquet.Type_INT32,
		},
		{
			name:     "boolean_type",
			src:      []any{true, false, true},
			dataType: parquet.Type_BOOLEAN,
		},
		{
			name:     "int32_type",
			src:      []any{int32(1), int32(2), int32(3)},
			dataType: parquet.Type_INT32,
		},
		{
			name:     "int64_type",
			src:      []any{int64(100), int64(200), int64(300)},
			dataType: parquet.Type_INT64,
		},
		{
			name:     "int96_type",
			src:      []any{"helloworldab", "abcdefghijkl"},
			dataType: parquet.Type_INT96,
		},
		{
			name:     "float_type",
			src:      []any{float32(1.1), float32(2.2), float32(3.3)},
			dataType: parquet.Type_FLOAT,
		},
		{
			name:     "double_type",
			src:      []any{float64(1.1), float64(2.2), float64(3.3)},
			dataType: parquet.Type_DOUBLE,
		},
		{
			name:     "byte_array_type",
			src:      []any{"hello", "world"},
			dataType: parquet.Type_BYTE_ARRAY,
		},
		{
			name:     "fixed_len_byte_array_type",
			src:      []any{"hello", "world"},
			dataType: parquet.Type_FIXED_LEN_BYTE_ARRAY,
		},
		{
			name:     "unknown_type",
			src:      []any{1, 2, 3},
			dataType: parquet.Type(-1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := WritePlain(tc.src, tc.dataType)
			if !tc.expectErr {
				require.NoError(t, err)
			}

			if tc.name == "empty_data" {
				require.Equal(t, 0, len(result))
				return
			}

			if tc.name == "unknown_type" {
				require.Equal(t, 0, len(result))
				return
			}

			// For valid types, result should not be empty (except for empty input)
			if len(tc.src) > 0 {
				require.NotZero(t, len(result))
			}
		})
	}
}

func Test_WritePlainBOOLEAN(t *testing.T) {
	testData := []struct {
		nums     []any
		expected []byte
	}{
		{[]any{}, []byte{}},
		{[]any{(true)}, []byte{1}},
		{[]any{(true), (false)}, []byte{1}},
		{[]any{(true), (false), (false), (true), (false)}, []byte{9}},
	}

	for _, data := range testData {
		res, err := WritePlainBOOLEAN(data.nums)
		require.NoError(t, err)
		require.Equal(t, string(data.expected), string(res))
	}
}

func Test_WritePlainBYTE_ARRAY(t *testing.T) {
	testData := []struct {
		nums     []any
		expected []byte
	}{
		{[]any{}, []byte{}},
		{[]any{("a"), ("abc")}, []byte{1, 0, 0, 0, 97, 3, 0, 0, 0, 97, 98, 99}},
	}

	for _, data := range testData {
		res, err := WritePlainBYTE_ARRAY(data.nums)
		require.NoError(t, err)
		require.Equal(t, string(data.expected), string(res))
	}
}

func Test_WritePlainDOUBLE(t *testing.T) {
	testCases := []struct {
		name      string
		nums      []any
		expectErr bool
	}{
		{
			name: "empty_input",
			nums: []any{},
		},
		{
			name: "single_double",
			nums: []any{float64(1.5)},
		},
		{
			name: "multiple_doubles",
			nums: []any{float64(1.1), float64(2.2), float64(3.3)},
		},
		{
			name:      "invalid_type",
			nums:      []any{"invalid"},
			expectErr: true,
		},
		{
			name:      "mixed_types",
			nums:      []any{float64(1.1), "invalid"},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := WritePlainDOUBLE(tc.nums)

			if tc.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			expectedLen := len(tc.nums) * 8
			require.Equal(t, expectedLen, len(result))
		})
	}
}

func Test_WritePlainFIXED_LEN_BYTE_ARRAY(t *testing.T) {
	testData := []struct {
		nums     []any
		expected []byte
	}{
		{[]any{}, []byte{}},
		{[]any{("bca"), ("abc")}, []byte{98, 99, 97, 97, 98, 99}},
	}

	for _, data := range testData {
		res, err := WritePlainFIXED_LEN_BYTE_ARRAY(data.nums)
		require.NoError(t, err)
		require.Equal(t, string(data.expected), string(res))
	}
}

func Test_WritePlainFLOAT(t *testing.T) {
	testCases := []struct {
		name      string
		nums      []any
		expectErr bool
	}{
		{
			name: "empty_input",
			nums: []any{},
		},
		{
			name: "single_float",
			nums: []any{float32(1.5)},
		},
		{
			name: "multiple_floats",
			nums: []any{float32(1.1), float32(2.2), float32(3.3)},
		},
		{
			name:      "invalid_type",
			nums:      []any{"invalid"},
			expectErr: true,
		},
		{
			name:      "mixed_types",
			nums:      []any{float32(1.1), "invalid"},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := WritePlainFLOAT(tc.nums)

			if tc.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			expectedLen := len(tc.nums) * 4
			require.Equal(t, expectedLen, len(result))
		})
	}
}

func Test_WritePlainINT32(t *testing.T) {
	testData := []struct {
		name      string
		nums      []any
		expected  []byte
		expectErr bool
	}{
		{
			name:     "empty_input",
			nums:     []any{},
			expected: []byte{},
		},
		{
			name:     "single_int32",
			nums:     []any{int32(0)},
			expected: []byte{0, 0, 0, 0},
		},
		{
			name:     "multiple_int32",
			nums:     []any{int32(0), int32(1), int32(2)},
			expected: []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0},
		},
		{
			name:      "invalid_type",
			nums:      []any{"invalid"},
			expectErr: true,
		},
		{
			name:      "mixed_types",
			nums:      []any{int32(1), "invalid"},
			expectErr: true,
		},
	}

	for _, data := range testData {
		t.Run(data.name, func(t *testing.T) {
			res, err := WritePlainINT32(data.nums)

			if data.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			require.Equal(t, string(data.expected), string(res))
		})
	}
}

func Test_WritePlainINT64(t *testing.T) {
	testData := []struct {
		name      string
		nums      []any
		expected  []byte
		expectErr bool
	}{
		{
			name:     "empty_input",
			nums:     []any{},
			expected: []byte{},
		},
		{
			name:     "single_int64",
			nums:     []any{int64(0)},
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "multiple_int64",
			nums:     []any{int64(0), int64(1), int64(2)},
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:      "invalid_type",
			nums:      []any{"invalid"},
			expectErr: true,
		},
		{
			name:      "mixed_types",
			nums:      []any{int64(1), "invalid"},
			expectErr: true,
		},
	}

	for _, data := range testData {
		t.Run(data.name, func(t *testing.T) {
			res, err := WritePlainINT64(data.nums)

			if data.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			require.Equal(t, string(data.expected), string(res))
		})
	}
}

func Test_WritePlainINT96(t *testing.T) {
	testData := []struct {
		nums     []any
		expected []byte
	}{
		{[]any{}, []byte{}},
		{[]any{string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{
			[]any{
				string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
				string([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
				string([]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			},

			[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}

	for _, data := range testData {
		res := WritePlainINT96(data.nums)
		require.Equal(t, string(data.expected), string(res))
	}
}

func Test_WriteRLE(t *testing.T) {
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
}

func Test_WriteRLEBitPackedHybrid(t *testing.T) {
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
}

func Test_WriteRLEBitPackedHybridInt32(t *testing.T) {
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
}

func Test_WriteRLEInt32(t *testing.T) {
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
}

func Test_WriteUnsignedVarInt(t *testing.T) {
	resBuf := make([]byte, 0)
	resBuf = append(resBuf, byte(0x00))
	resBuf = append(resBuf, byte(0x7F))
	resBuf = append(resBuf, byte(0x80), byte(0x01))
	resBuf = append(resBuf, byte(0x80), byte(0x40))
	resBuf = append(resBuf, byte(0xFF), byte(0x7F))
	resBuf = append(resBuf, byte(0x80), byte(0x80), byte(0x01))
	resBuf = append(resBuf, byte(0xFF), byte(0xFF), byte(0x7F))
	resBuf = append(resBuf, byte(0x80), byte(0x80), byte(0x80), byte(0x01))
	resBuf = append(resBuf, byte(0x80), byte(0x80), byte(0x80), byte(0x40))
	resBuf = append(resBuf, byte(0xFF), byte(0xFF), byte(0xFF), byte(0x7F))

	testNum := make([]uint32, 10)
	testNum[0] = 0x0
	testNum[1] = 0x7F
	testNum[2] = 0x80
	testNum[3] = 0x2000
	testNum[4] = 0x3FFF
	testNum[5] = 0x4000
	testNum[6] = 0x1FFFFF
	testNum[7] = 0x200000
	testNum[8] = 0x8000000
	testNum[9] = 0xFFFFFFF

	testRes := make([]byte, 0)
	for i := range testNum {
		tmpBuf := WriteUnsignedVarInt(uint64(testNum[i]))
		testRes = append(testRes, tmpBuf...)
	}

	require.Equal(t, string(resBuf), string(testRes))
}
