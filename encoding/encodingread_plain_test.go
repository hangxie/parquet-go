package encoding

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

//nolint:gocognit
func TestReadPlain(t *testing.T) {
	t.Run("generic", func(t *testing.T) {
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
				data:     []any{"helloworldab", "abcdefghijkl"},
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
					require.Contains(t, err.Error(), "unknown parquet type")
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
	})

	t.Run("boolean", func(t *testing.T) {
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
	})

	t.Run("byte_array", func(t *testing.T) {
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
			{
				name:  "empty-string-at-end",
				input: []any{"a", "b", ""},
			},
			{
				name:  "only-empty-string",
				input: []any{""},
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
	})

	t.Run("byte_array_length_prefix_exceeds_buffer", func(t *testing.T) {
		// Craft a buffer with a length prefix that far exceeds remaining data.
		buf := []byte{
			0xFF, 0xFF, 0xFF, 0xFF, // length prefix: ~4GB
			0x01, 0x02, 0x03, // only 3 bytes of actual data
		}
		_, err := ReadPlainBYTE_ARRAY(bytes.NewReader(buf), 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "length prefix")
		require.Contains(t, err.Error(), "exceeds remaining data size")
	})

	t.Run("byte_array_boundary_cases", func(t *testing.T) {
		tests := []struct {
			name      string
			buf       []byte
			cnt       uint64
			expectErr bool
		}{
			{
				name: "length_equals_remaining_succeeds",
				// length prefix = 3, remaining = 3 bytes
				buf:       []byte{0x03, 0x00, 0x00, 0x00, 0x41, 0x42, 0x43},
				cnt:       1,
				expectErr: false,
			},
			{
				name: "length_exceeds_remaining_by_one",
				// length prefix = 4, remaining = 3 bytes
				buf:       []byte{0x04, 0x00, 0x00, 0x00, 0x41, 0x42, 0x43},
				cnt:       1,
				expectErr: true,
			},
			{
				name: "zero_length_value",
				// length prefix = 0
				buf:       []byte{0x00, 0x00, 0x00, 0x00},
				cnt:       1,
				expectErr: false,
			},
			{
				name: "multiple_values_second_oversized",
				// first value: length=1, data="A"; second value: length=0xFF, only 1 byte left
				buf:       []byte{0x01, 0x00, 0x00, 0x00, 0x41, 0xFF, 0x00, 0x00, 0x00, 0x42},
				cnt:       2,
				expectErr: true,
			},
			{
				name: "truncated_length_prefix",
				// only 2 bytes instead of 4 for length prefix
				buf:       []byte{0x01, 0x00},
				cnt:       1,
				expectErr: true,
			},
			{
				name:      "cnt_zero",
				buf:       []byte{},
				cnt:       0,
				expectErr: false,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := ReadPlainBYTE_ARRAY(bytes.NewReader(tt.buf), tt.cnt)
				if tt.expectErr {
					require.Error(t, err)
					require.Contains(t, err.Error(), "ReadPlainBYTE_ARRAY")
				} else {
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("fixed_len_byte_array_exceeds_buffer", func(t *testing.T) {
		// 3 bytes of data but fixedLength=100
		buf := []byte{0x01, 0x02, 0x03}
		_, err := ReadPlainFIXED_LEN_BYTE_ARRAY(bytes.NewReader(buf), 1, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "fixed length")
		require.Contains(t, err.Error(), "exceeds remaining data size")
	})

	t.Run("double", func(t *testing.T) {
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
	})

	t.Run("fixed_len_byte_array", func(t *testing.T) {
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
	})

	t.Run("float", func(t *testing.T) {
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
	})

	t.Run("int32", func(t *testing.T) {
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
	})

	t.Run("int64", func(t *testing.T) {
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
	})

	t.Run("int96", func(t *testing.T) {
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
					require.Contains(t, err.Error(), "EOF")
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
	})

	t.Run("invalid_count", func(t *testing.T) {
		tests := []struct {
			name      string
			dataType  parquet.Type
			count     uint64
			expectErr bool
		}{
			{
				name:      "boolean_invalid_count",
				dataType:  parquet.Type_BOOLEAN,
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
			{
				name:      "int32_invalid_count",
				dataType:  parquet.Type_INT32,
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
			{
				name:      "int64_invalid_count",
				dataType:  parquet.Type_INT64,
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
			{
				name:      "int96_invalid_count",
				dataType:  parquet.Type_INT96,
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
			{
				name:      "float_invalid_count",
				dataType:  parquet.Type_FLOAT,
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
			{
				name:      "double_invalid_count",
				dataType:  parquet.Type_DOUBLE,
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
			{
				name:      "byte_array_invalid_count",
				dataType:  parquet.Type_BYTE_ARRAY,
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
			{
				name:      "fixed_len_byte_array_invalid_count",
				dataType:  parquet.Type_FIXED_LEN_BYTE_ARRAY,
				count:     maxAllowedCount + 1,
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reader := bytes.NewReader([]byte{})
				_, err := ReadPlain(reader, tt.dataType, tt.count, 0)

				if tt.expectErr {
					require.Error(t, err)
					require.Contains(t, err.Error(), "invalid count")
				} else {
					require.NoError(t, err)
				}
			})
		}
	})
}
