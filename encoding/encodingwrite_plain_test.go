package encoding

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestToInt64(t *testing.T) {
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

//nolint:gocognit
func TestWritePlain(t *testing.T) {
	t.Run("generic", func(t *testing.T) {
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
				name:      "unknown_type",
				src:       []any{1, 2, 3},
				dataType:  parquet.Type(-1),
				expectErr: true,
			},
			{
				name:      "unknown_type_empty_src",
				src:       []any{},
				dataType:  parquet.Type(-1),
				expectErr: true,
			},
			{
				name:      "unknown_type_nil_src",
				src:       nil,
				dataType:  parquet.Type(-1),
				expectErr: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := WritePlain(tc.src, tc.dataType)
				if tc.expectErr {
					require.Error(t, err)
					require.Contains(t, err.Error(), "unsupported parquet type")
					return
				}
				require.NoError(t, err)

				if tc.name == "empty_data" {
					require.Equal(t, 0, len(result))
					return
				}

				// For valid types, result should not be empty (except for empty input)
				if len(tc.src) > 0 {
					require.NotZero(t, len(result))
				}
			})
		}
	})

	t.Run("boolean", func(t *testing.T) {
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
	})

	t.Run("byte_array", func(t *testing.T) {
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
	})

	t.Run("double", func(t *testing.T) {
		testCases := []struct {
			name      string
			nums      []any
			expectErr bool
			errMsg    string
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
				errMsg:    "is not float",
			},
			{
				name:      "mixed_types",
				nums:      []any{float64(1.1), "invalid"},
				expectErr: true,
				errMsg:    "is not float",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := WritePlainDOUBLE(tc.nums)

				if tc.expectErr {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errMsg)
					return
				}

				require.NoError(t, err)

				expectedLen := len(tc.nums) * 8
				require.Equal(t, expectedLen, len(result))
			})
		}
	})

	t.Run("fixed_len_byte_array", func(t *testing.T) {
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
	})

	t.Run("float", func(t *testing.T) {
		testCases := []struct {
			name      string
			nums      []any
			expectErr bool
			errMsg    string
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
				errMsg:    "is not float32",
			},
			{
				name:      "mixed_types",
				nums:      []any{float32(1.1), "invalid"},
				expectErr: true,
				errMsg:    "is not float32",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := WritePlainFLOAT(tc.nums)

				if tc.expectErr {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errMsg)
					return
				}

				require.NoError(t, err)

				expectedLen := len(tc.nums) * 4
				require.Equal(t, expectedLen, len(result))
			})
		}
	})

	t.Run("int32", func(t *testing.T) {
		testData := []struct {
			name      string
			nums      []any
			expected  []byte
			expectErr bool
			errMsg    string
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
				errMsg:    "is not int32",
			},
			{
				name:      "mixed_types",
				nums:      []any{int32(1), "invalid"},
				expectErr: true,
				errMsg:    "is not int32",
			},
		}

		for _, data := range testData {
			t.Run(data.name, func(t *testing.T) {
				res, err := WritePlainINT32(data.nums)

				if data.expectErr {
					require.Error(t, err)
					require.Contains(t, err.Error(), data.errMsg)
					return
				}

				require.NoError(t, err)

				require.Equal(t, string(data.expected), string(res))
			})
		}
	})

	t.Run("int64", func(t *testing.T) {
		testData := []struct {
			name      string
			nums      []any
			expected  []byte
			expectErr bool
			errMsg    string
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
				errMsg:    "is not int64",
			},
			{
				name:      "mixed_types",
				nums:      []any{int64(1), "invalid"},
				expectErr: true,
				errMsg:    "is not int64",
			},
		}

		for _, data := range testData {
			t.Run(data.name, func(t *testing.T) {
				res, err := WritePlainINT64(data.nums)

				if data.expectErr {
					require.Error(t, err)
					require.Contains(t, err.Error(), data.errMsg)
					return
				}

				require.NoError(t, err)

				require.Equal(t, string(data.expected), string(res))
			})
		}
	})

	t.Run("int96", func(t *testing.T) {
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
	})
}

func TestWriteUnsignedVarInt(t *testing.T) {
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
