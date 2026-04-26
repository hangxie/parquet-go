package layout

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestReadDataPageValues(t *testing.T) {
	testCases := []struct {
		name           string
		encodingMethod parquet.Encoding
		dataType       parquet.Type
		convertedType  parquet.ConvertedType
		cnt            uint64
		bitWidth       uint64
		setupData      func() []byte
		errMsg         string
	}{
		{
			name:           "zero_count",
			encodingMethod: parquet.Encoding_PLAIN,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            0,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
		},
		{
			name:           "bit_packed_deprecated",
			encodingMethod: parquet.Encoding_BIT_PACKED,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            4,
			bitWidth:       3,
			setupData: func() []byte {
				// BIT_PACKED encoding for [1, 2, 3, 4] with bitWidth=3
				return []byte{0xD1, 0x08}
			},
		},
		{
			name:           "unknown_encoding",
			encodingMethod: parquet.Encoding(-1),
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			errMsg:         "unknown Encoding method",
		},
		{
			name:           "delta_binary_packed_int64",
			encodingMethod: parquet.Encoding_DELTA_BINARY_PACKED,
			dataType:       parquet.Type_INT64,
			convertedType:  -1,
			cnt:            3,
			bitWidth:       0,
			setupData: func() []byte {
				// Create delta binary packed data for [1, 2, 3]
				values := []any{int64(1), int64(2), int64(3)}
				return encoding.WriteDeltaINT64(values)
			},
		},
		{
			name:           "delta_binary_packed_unsupported_type",
			encodingMethod: parquet.Encoding_DELTA_BINARY_PACKED,
			dataType:       parquet.Type_FLOAT,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			errMsg:         "DELTA_BINARY_PACKED can only be used with int32 and int64",
		},
		{
			name:           "byte_stream_split_unsupported_type",
			encodingMethod: parquet.Encoding_BYTE_STREAM_SPLIT,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            1,
			bitWidth:       0,
			setupData:      func() []byte { return []byte{} },
			errMsg:         "EOF",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			bytesReader := bytes.NewReader(data)

			result, err := ReadDataPageValues(bytesReader, tc.encodingMethod, tc.dataType, tc.convertedType, tc.cnt, tc.bitWidth)

			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.NoError(t, err)
				if tc.cnt == 0 {
					require.Empty(t, result)
				}
			}
		})
	}
}

func TestReadDataPageValuesMoreCases(t *testing.T) {
	testCases := []struct {
		name           string
		encodingMethod parquet.Encoding
		dataType       parquet.Type
		convertedType  parquet.ConvertedType
		cnt            uint64
		bitWidth       uint64
		setupData      func() []byte
		expectError    bool
	}{
		{
			name:           "plain_int32",
			encodingMethod: parquet.Encoding_PLAIN,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{int32(42), int32(100)}
				data, _ := encoding.WritePlainINT32(values)
				return data
			},
			expectError: false,
		},
		{
			name:           "plain_dictionary_encoding",
			encodingMethod: parquet.Encoding_PLAIN_DICTIONARY,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            3,
			bitWidth:       2,
			setupData: func() []byte {
				// Create RLE bit packed hybrid data with bit width prefix
				indices := []any{int64(0), int64(1), int64(0)}
				data, _ := encoding.WriteRLEBitPackedHybrid(indices, 2, parquet.Type_INT64)
				// Prepend bit width byte
				result := make([]byte, 1+len(data))
				result[0] = 2 // bit width
				copy(result[1:], data)
				return result
			},
			expectError: false,
		},
		{
			name:           "rle_dictionary_encoding",
			encodingMethod: parquet.Encoding_RLE_DICTIONARY,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       1,
			setupData: func() []byte {
				indices := []any{int64(0), int64(1)}
				data, _ := encoding.WriteRLEBitPackedHybrid(indices, 1, parquet.Type_INT64)
				result := make([]byte, 1+len(data))
				result[0] = 1 // bit width
				copy(result[1:], data)
				return result
			},
			expectError: false,
		},
		{
			name:           "rle_encoding_int32",
			encodingMethod: parquet.Encoding_RLE,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            3,
			bitWidth:       2,
			setupData: func() []byte {
				values := []any{int64(1), int64(1), int64(2)}
				data, _ := encoding.WriteRLEBitPackedHybrid(values, 2, parquet.Type_INT64)
				return data
			},
			expectError: false,
		},
		{
			name:           "rle_encoding_boolean",
			encodingMethod: parquet.Encoding_RLE,
			dataType:       parquet.Type_BOOLEAN,
			convertedType:  -1,
			cnt:            4,
			bitWidth:       1,
			setupData: func() []byte {
				// RLE encoding for booleans: [true, true, false, true]
				values := []any{int64(1), int64(1), int64(0), int64(1)}
				data, _ := encoding.WriteRLEBitPackedHybrid(values, 1, parquet.Type_INT64)
				return data
			},
			expectError: false,
		},
		{
			name:           "bit_packed_deprecated_boolean",
			encodingMethod: parquet.Encoding_BIT_PACKED,
			dataType:       parquet.Type_BOOLEAN,
			convertedType:  -1,
			cnt:            4,
			bitWidth:       1,
			setupData: func() []byte {
				// Deprecated bit packed encoding for booleans: [true, false, true, false]
				return []byte{0x05}
			},
			expectError: false,
		},
		{
			name:           "delta_binary_packed_int32",
			encodingMethod: parquet.Encoding_DELTA_BINARY_PACKED,
			dataType:       parquet.Type_INT32,
			convertedType:  -1,
			cnt:            4,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{int32(10), int32(15), int32(20), int32(25)}
				return encoding.WriteDeltaINT32(values)
			},
			expectError: false,
		},
		{
			name:           "delta_length_byte_array",
			encodingMethod: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			dataType:       parquet.Type_BYTE_ARRAY,
			convertedType:  -1,
			cnt:            3,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{"hello", "world", "test"}
				return encoding.WriteDeltaLengthByteArray(values)
			},
			expectError: false,
		},
		{
			name:           "delta_length_byte_array_fixed_len",
			encodingMethod: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			dataType:       parquet.Type_FIXED_LEN_BYTE_ARRAY,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       5,
			setupData: func() []byte {
				values := []any{"hello", "world"}
				return encoding.WriteDeltaLengthByteArray(values)
			},
			expectError: false,
		},
		{
			name:           "delta_byte_array",
			encodingMethod: parquet.Encoding_DELTA_BYTE_ARRAY,
			dataType:       parquet.Type_BYTE_ARRAY,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{"apple", "application"}
				return encoding.WriteDeltaByteArray(values)
			},
			expectError: false,
		},
		{
			name:           "delta_byte_array_fixed_len",
			encodingMethod: parquet.Encoding_DELTA_BYTE_ARRAY,
			dataType:       parquet.Type_FIXED_LEN_BYTE_ARRAY,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       4,
			setupData: func() []byte {
				values := []any{"test", "data"}
				return encoding.WriteDeltaByteArray(values)
			},
			expectError: false,
		},
		{
			name:           "byte_stream_split_float",
			encodingMethod: parquet.Encoding_BYTE_STREAM_SPLIT,
			dataType:       parquet.Type_FLOAT,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{float32(1.5), float32(2.5)}
				return encoding.WriteByteStreamSplitFloat32(values)
			},
			expectError: false,
		},
		{
			name:           "byte_stream_split_double",
			encodingMethod: parquet.Encoding_BYTE_STREAM_SPLIT,
			dataType:       parquet.Type_DOUBLE,
			convertedType:  -1,
			cnt:            2,
			bitWidth:       0,
			setupData: func() []byte {
				values := []any{float64(3.14), float64(2.71)}
				return encoding.WriteByteStreamSplitFloat64(values)
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			bytesReader := bytes.NewReader(data)

			result, err := ReadDataPageValues(bytesReader, tc.encodingMethod, tc.dataType, tc.convertedType, tc.cnt, tc.bitWidth)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.cnt > 0 {
					require.NotEmpty(t, result)
				}
			}
		})
	}
}

func TestReadDataPageValues_BooleanTypeConversion(t *testing.T) {
	t.Run("rle_encoding_boolean_values", func(t *testing.T) {
		// Test that RLE encoding correctly converts int64 to bool
		values := []any{int64(1), int64(1), int64(0), int64(1)}
		data, _ := encoding.WriteRLEBitPackedHybrid(values, 1, parquet.Type_INT64)
		bytesReader := bytes.NewReader(data)

		result, err := ReadDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_BOOLEAN, -1, 4, 1)

		require.NoError(t, err)
		require.Len(t, result, 4)

		// Verify types are bool, not int64
		require.IsType(t, true, result[0], "Expected bool type, got %T", result[0])
		require.IsType(t, true, result[1], "Expected bool type, got %T", result[1])
		require.IsType(t, true, result[2], "Expected bool type, got %T", result[2])
		require.IsType(t, true, result[3], "Expected bool type, got %T", result[3])

		// Verify values
		require.Equal(t, true, result[0])
		require.Equal(t, true, result[1])
		require.Equal(t, false, result[2])
		require.Equal(t, true, result[3])
	})

	t.Run("bit_packed_deprecated_boolean_values", func(t *testing.T) {
		// Test that BIT_PACKED encoding correctly converts int64 to bool
		data := []byte{0x05}
		bytesReader := bytes.NewReader(data)

		result, err := ReadDataPageValues(bytesReader, parquet.Encoding_BIT_PACKED, parquet.Type_BOOLEAN, -1, 4, 1)

		require.NoError(t, err)
		require.Len(t, result, 4)

		// Verify types are bool, not int64
		require.IsType(t, true, result[0], "Expected bool type, got %T", result[0])
		require.IsType(t, true, result[1], "Expected bool type, got %T", result[1])
		require.IsType(t, true, result[2], "Expected bool type, got %T", result[2])
		require.IsType(t, true, result[3], "Expected bool type, got %T", result[3])

		// Verify values
		require.Equal(t, true, result[0])
		require.Equal(t, false, result[1])
		require.Equal(t, true, result[2])
		require.Equal(t, false, result[3])
	})

	t.Run("rle_encoding_boolean_with_zero_bitwidth", func(t *testing.T) {
		// Test that RLE encoding with bitWidth=0 defaults to bitWidth=1 for BOOLEAN type
		// This simulates when struct tag doesn't specify length= for a boolean field
		values := []any{int64(1), int64(0), int64(1), int64(1), int64(0)}
		data, _ := encoding.WriteRLEBitPackedHybrid(values, 1, parquet.Type_INT64)
		bytesReader := bytes.NewReader(data)

		// Read with bitWidth=0 - should be fixed to 1 automatically
		result, err := ReadDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_BOOLEAN, -1, 5, 0)

		require.NoError(t, err)
		require.Len(t, result, 5)

		// Verify types are bool, not int64
		for i := range result {
			require.IsType(t, true, result[i], "Expected bool type at index %d, got %T", i, result[i])
		}

		// Verify values
		require.Equal(t, true, result[0])
		require.Equal(t, false, result[1])
		require.Equal(t, true, result[2])
		require.Equal(t, true, result[3])
		require.Equal(t, false, result[4])
	})
}

func TestReadDataPageValues_BoundsChecking(t *testing.T) {
	testCases := []struct {
		name           string
		encodingMethod parquet.Encoding
		dataType       parquet.Type
		cnt            uint64
		bitWidth       uint64
		setupData      func() []byte
		expectedError  string
	}{
		{
			name:           "rle_dictionary_fewer_values_than_expected",
			encodingMethod: parquet.Encoding_RLE_DICTIONARY,
			dataType:       parquet.Type_INT32,
			cnt:            21, // Request 21 values
			bitWidth:       4,
			setupData: func() []byte {
				// Create RLE data that only contains 1 value (repeated once)
				// Bit width byte
				data := []byte{4} // bitWidth = 4
				// Write RLE/bit-packed hybrid data for just 1 value
				rleBuf, _ := encoding.WriteRLEBitPackedHybrid([]any{int64(0)}, 4, parquet.Type_INT64)
				data = append(data, rleBuf...)
				return data
			},
			expectedError: "expected 21 values but got 2 from RLE/bit-packed hybrid decoder",
		},
		{
			name:           "rle_fewer_values_than_expected",
			encodingMethod: parquet.Encoding_RLE,
			dataType:       parquet.Type_INT32,
			cnt:            21, // Request 21 values
			bitWidth:       4,
			setupData: func() []byte {
				// Create RLE data that only contains 1 value
				rleBuf, _ := encoding.WriteRLEBitPackedHybrid([]any{int64(0)}, 4, parquet.Type_INT64)
				return rleBuf
			},
			expectedError: "expected 21 values but got 1 from RLE/bit-packed hybrid decoder",
		},
		{
			name:           "plain_dictionary_fewer_values_than_expected",
			encodingMethod: parquet.Encoding_PLAIN_DICTIONARY,
			dataType:       parquet.Type_INT32,
			cnt:            10,
			bitWidth:       3,
			setupData: func() []byte {
				// Create dictionary-encoded data with fewer values than requested
				data := []byte{3} // bitWidth = 3
				// RLE data with only 3 values - create minimal data
				// We manually construct this to have fewer values than cnt
				rleBuf, _ := encoding.WriteRLEBitPackedHybrid(
					[]any{int64(0), int64(1), int64(0)},
					3,
					parquet.Type_INT64,
				)
				data = append(data, rleBuf...)
				return data
			},
			expectedError: "expected 10 values but got",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := tc.setupData()
			bytesReader := bytes.NewReader(data)

			result, err := ReadDataPageValues(
				bytesReader,
				tc.encodingMethod,
				tc.dataType,
				-1, // convertedType
				tc.cnt,
				tc.bitWidth,
			)

			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedError)
			require.Empty(t, result)
		})
	}
}

func TestReadByteStreamSplit_Types(t *testing.T) {
	tests := []struct {
		name     string
		dataType parquet.Type
		setup    func() []byte
		cnt      uint64
		bitWidth uint64
	}{
		{
			name:     "double",
			dataType: parquet.Type_DOUBLE,
			cnt:      1,
			setup: func() []byte {
				data := encoding.WriteByteStreamSplitFloat64([]any{float64(1.0)})
				return data
			},
		},
		{
			name:     "int32",
			dataType: parquet.Type_INT32,
			cnt:      1,
			setup: func() []byte {
				data := encoding.WriteByteStreamSplitINT32([]any{int32(1)})
				return data
			},
		},
		{
			name:     "int64",
			dataType: parquet.Type_INT64,
			cnt:      1,
			setup: func() []byte {
				data := encoding.WriteByteStreamSplitINT64([]any{int64(1)})
				return data
			},
		},
		{
			name:     "fixed_len_byte_array",
			dataType: parquet.Type_FIXED_LEN_BYTE_ARRAY,
			cnt:      1,
			bitWidth: 4,
			setup: func() []byte {
				data := encoding.WriteByteStreamSplitFixedLenByteArray([]any{"test"})
				return data
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.setup()
			reader := bytes.NewReader(data)
			res, err := readByteStreamSplit(reader, tt.dataType, tt.cnt, tt.bitWidth)
			require.NoError(t, err)
			require.Len(t, res, int(tt.cnt))
		})
	}

	t.Run("unsupported_type", func(t *testing.T) {
		_, err := readByteStreamSplit(nil, parquet.Type_BYTE_ARRAY, 1, 0)
		require.Error(t, err)
	})
}

func TestDefaultBitWidth(t *testing.T) {
	testCases := []struct {
		name     string
		dataType parquet.Type
		expected uint64
	}{
		{
			name:     "boolean",
			dataType: parquet.Type_BOOLEAN,
			expected: 1,
		},
		{
			name:     "int32",
			dataType: parquet.Type_INT32,
			expected: 32,
		},
		{
			name:     "int64",
			dataType: parquet.Type_INT64,
			expected: 64,
		},
		{
			name:     "float",
			dataType: parquet.Type_FLOAT,
			expected: 0,
		},
		{
			name:     "double",
			dataType: parquet.Type_DOUBLE,
			expected: 0,
		},
		{
			name:     "byte_array",
			dataType: parquet.Type_BYTE_ARRAY,
			expected: 0,
		},
		{
			name:     "fixed_len_byte_array",
			dataType: parquet.Type_FIXED_LEN_BYTE_ARRAY,
			expected: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := defaultBitWidth(tc.dataType)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestConvertRLEValuesForType(t *testing.T) {
	t.Run("int32_conversion", func(t *testing.T) {
		values := []any{int64(1), int64(2), int64(-5), int64(100)}
		convertRLEValuesForType(values, parquet.Type_INT32)
		require.Equal(t, int32(1), values[0])
		require.Equal(t, int32(2), values[1])
		require.Equal(t, int32(-5), values[2])
		require.Equal(t, int32(100), values[3])
	})

	t.Run("boolean_conversion", func(t *testing.T) {
		values := []any{int64(1), int64(0), int64(5), int64(0)}
		convertRLEValuesForType(values, parquet.Type_BOOLEAN)
		require.Equal(t, true, values[0])  // int64 > 0 → true
		require.Equal(t, false, values[1]) // int64 == 0 → false
		require.Equal(t, true, values[2])  // int64 > 0 → true
		require.Equal(t, false, values[3]) // int64 == 0 → false
	})

	t.Run("other_type_no_change", func(t *testing.T) {
		values := []any{int64(10), int64(20), int64(30)}
		original := []any{int64(10), int64(20), int64(30)}
		convertRLEValuesForType(values, parquet.Type_INT64)
		// Values should be unchanged for INT64
		require.Equal(t, original[0], values[0])
		require.Equal(t, original[1], values[1])
		require.Equal(t, original[2], values[2])
	})

	t.Run("float_type_no_change", func(t *testing.T) {
		values := []any{int64(1), int64(2)}
		convertRLEValuesForType(values, parquet.Type_FLOAT)
		// Values should remain int64 for FLOAT type
		require.IsType(t, int64(0), values[0])
		require.IsType(t, int64(0), values[1])
	})
}

func TestReadDeltaByteArrayValues_ErrorPropagation(t *testing.T) {
	t.Run("error_from_parent_is_returned", func(t *testing.T) {
		// When err is non-nil, the error should be returned immediately
		someErr := fmt.Errorf("upstream decode error")
		result, err := readDeltaByteArrayValues(nil, someErr, parquet.Type_BYTE_ARRAY, 3)
		require.Error(t, err)
		require.Equal(t, someErr, err)
		require.Nil(t, result)
	})

	t.Run("fixed_len_byte_array_converts_to_strings", func(t *testing.T) {
		// For FIXED_LEN_BYTE_ARRAY, values should be converted to strings
		values := []any{"hello", "world", "test"}
		result, err := readDeltaByteArrayValues(values, nil, parquet.Type_FIXED_LEN_BYTE_ARRAY, 3)
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.IsType(t, "", result[0])
		require.IsType(t, "", result[1])
		require.IsType(t, "", result[2])
	})

	t.Run("cnt_truncation", func(t *testing.T) {
		// cnt should truncate the result to the first cnt values
		values := []any{"a", "b", "c", "d", "e"}
		result, err := readDeltaByteArrayValues(values, nil, parquet.Type_BYTE_ARRAY, 3)
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.Equal(t, "a", result[0])
		require.Equal(t, "b", result[1])
		require.Equal(t, "c", result[2])
	})

	t.Run("byte_array_no_conversion", func(t *testing.T) {
		// For BYTE_ARRAY, values should not be converted
		values := []any{"hello", "world"}
		result, err := readDeltaByteArrayValues(values, nil, parquet.Type_BYTE_ARRAY, 2)
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Equal(t, "hello", result[0])
		require.Equal(t, "world", result[1])
	})
}
