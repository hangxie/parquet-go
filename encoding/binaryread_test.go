package encoding

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockReader simulates various read scenarios for testing error cases
type mockReader struct {
	data        []byte
	pos         int
	failOnRead  bool
	partialRead bool
	bytesToRead int
}

func (m *mockReader) Read(p []byte) (n int, err error) {
	if m.failOnRead {
		return 0, errors.New("mock read error")
	}
	if m.pos >= len(m.data) {
		return 0, io.EOF
	}

	available := len(m.data) - m.pos
	toRead := len(p)

	if m.partialRead && m.bytesToRead > 0 {
		toRead = m.bytesToRead
	}

	if toRead > available {
		toRead = available
	}

	copy(p, m.data[m.pos:m.pos+toRead])
	m.pos += toRead

	return toRead, nil
}

func Test_BinaryReadFLOAT32(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		nums        []any
		expected    []any
		expectError bool
		errorType   error
		isSpecial   bool // for infinity values that need special comparison
	}{
		{
			name:     "zero",
			input:    []byte{0x00, 0x00, 0x00, 0x00},
			nums:     make([]any, 1),
			expected: []any{float32(0.0)},
		},
		{
			name:     "positive_float",
			input:    []byte{0x00, 0x00, 0x80, 0x3F}, // 1.0 in IEEE 754 little endian
			nums:     make([]any, 1),
			expected: []any{float32(1.0)},
		},
		{
			name:     "negative_float",
			input:    []byte{0x00, 0x00, 0x80, 0xBF}, // -1.0 in IEEE 754 little endian
			nums:     make([]any, 1),
			expected: []any{float32(-1.0)},
		},
		{
			name:     "pi_approximation",
			input:    []byte{0xDB, 0x0F, 0x49, 0x40}, // approximately 3.14159 in little endian
			nums:     make([]any, 1),
			expected: []any{float32(3.1415927)},
		},
		{
			name: "multiple_values",
			input: []byte{
				0x00, 0x00, 0x80, 0x3F, // 1.0
				0x00, 0x00, 0x00, 0x40, // 2.0
				0x00, 0x00, 0x40, 0x40, // 3.0
			},
			nums:     make([]any, 3),
			expected: []any{float32(1.0), float32(2.0), float32(3.0)},
		},
		{
			name:     "empty_array",
			input:    []byte{},
			nums:     []any{},
			expected: []any{},
		},
		{
			name:        "insufficient_data",
			input:       []byte{0x00, 0x00, 0x00}, // Only 3 bytes for float32
			nums:        make([]any, 1),
			expectError: true,
			errorType:   io.ErrUnexpectedEOF,
		},
		{
			name:        "partial_second_value",
			input:       []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00}, // 4 bytes + 2 bytes
			nums:        make([]any, 2),
			expectError: true,
			errorType:   io.ErrUnexpectedEOF,
		},
		{
			name:        "read_error",
			input:       []byte{0x00, 0x00, 0x00, 0x00},
			nums:        make([]any, 1),
			expectError: true,
		},
		{
			name:      "positive_infinity",
			input:     []byte{0x00, 0x00, 0x80, 0x7F}, // +Inf in IEEE 754 little endian
			nums:      make([]any, 1),
			expected:  []any{float32(math.Inf(1))},
			isSpecial: true,
		},
		{
			name:      "negative_infinity",
			input:     []byte{0x00, 0x00, 0x80, 0xFF}, // -Inf in IEEE 754 little endian
			nums:      make([]any, 1),
			expected:  []any{float32(math.Inf(-1))},
			isSpecial: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reader io.Reader
			if tt.name == "read_error" {
				reader = &mockReader{
					data:       tt.input,
					failOnRead: true,
				}
			} else {
				reader = bytes.NewReader(tt.input)
			}
			err := BinaryReadFLOAT32(reader, tt.nums)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != nil {
					require.ErrorIs(t, err, tt.errorType)
				}
				return
			}

			require.NoError(t, err)

			require.Len(t, tt.nums, len(tt.expected))

			for i, expected := range tt.expected {
				if tt.isSpecial {
					// Special handling for infinity values
					result := tt.nums[i].(float32)
					expectedFloat := expected.(float32)
					if math.IsInf(float64(expectedFloat), 1) {
						require.True(t, math.IsInf(float64(result), 1), "Expected +Inf, got %v", result)
					} else if math.IsInf(float64(expectedFloat), -1) {
						require.True(t, math.IsInf(float64(result), -1), "Expected -Inf, got %v", result)
					}
				} else {
					require.Equal(t, expected, tt.nums[i], "Value mismatch at index %d", i)
				}
			}
		})
	}
}

func Test_BinaryReadFLOAT64(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		nums        []any
		expected    []any
		expectError bool
		errorType   error
		isSpecial   bool // for infinity values that need special comparison
	}{
		{
			name:     "zero",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			nums:     make([]any, 1),
			expected: []any{float64(0.0)},
		},
		{
			name:     "positive_float",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}, // 1.0 in IEEE 754 little endian
			nums:     make([]any, 1),
			expected: []any{float64(1.0)},
		},
		{
			name:     "negative_float",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF}, // -1.0 in IEEE 754 little endian
			nums:     make([]any, 1),
			expected: []any{float64(-1.0)},
		},
		{
			name:     "max_float64",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0x7F}, // math.MaxFloat64 in little endian
			nums:     make([]any, 1),
			expected: []any{math.MaxFloat64},
		},
		{
			name:     "smallest_positive_float64",
			input:    []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // math.SmallestNonzeroFloat64 in little endian
			nums:     make([]any, 1),
			expected: []any{math.SmallestNonzeroFloat64},
		},
		{
			name: "multiple_values",
			input: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // 1.0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // 2.0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // 3.0
			},
			nums:     make([]any, 3),
			expected: []any{float64(1.0), float64(2.0), float64(3.0)},
		},
		{
			name:     "empty_array",
			input:    []byte{},
			nums:     []any{},
			expected: []any{},
		},
		{
			name:        "insufficient_data",
			input:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // Only 7 bytes for float64
			nums:        make([]any, 1),
			expectError: true,
			errorType:   io.ErrUnexpectedEOF,
		},
		{
			name: "partial_second_value",
			input: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Complete first value
				0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // Incomplete second value (6 bytes)
			},
			nums:        make([]any, 2),
			expectError: true,
			errorType:   io.ErrUnexpectedEOF,
		},
		{
			name:        "read_error",
			input:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			nums:        make([]any, 1),
			expectError: true,
		},
		{
			name:      "positive_infinity",
			input:     []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x7F}, // +Inf in IEEE 754 little endian
			nums:      make([]any, 1),
			expected:  []any{math.Inf(1)},
			isSpecial: true,
		},
		{
			name:      "negative_infinity",
			input:     []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xFF}, // -Inf in IEEE 754 little endian
			nums:      make([]any, 1),
			expected:  []any{math.Inf(-1)},
			isSpecial: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reader io.Reader
			if tt.name == "read_error" {
				reader = &mockReader{
					data:       tt.input,
					failOnRead: true,
				}
			} else {
				reader = bytes.NewReader(tt.input)
			}
			err := BinaryReadFLOAT64(reader, tt.nums)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != nil {
					require.ErrorIs(t, err, tt.errorType)
				}
				return
			}

			require.NoError(t, err)

			require.Len(t, tt.nums, len(tt.expected))

			for i, expected := range tt.expected {
				if tt.isSpecial {
					// Special handling for infinity values
					result := tt.nums[i].(float64)
					expectedFloat := expected.(float64)
					if math.IsInf(expectedFloat, 1) {
						require.True(t, math.IsInf(result, 1), "Expected +Inf, got %v", result)
					} else if math.IsInf(expectedFloat, -1) {
						require.True(t, math.IsInf(result, -1), "Expected -Inf, got %v", result)
					}
				} else {
					require.Equal(t, expected, tt.nums[i], "Value mismatch at index %d", i)
				}
			}
		})
	}
}

func Test_BinaryReadINT32(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		nums        []any
		expected    []any
		expectError bool
		errorType   error
	}{
		{
			name:     "single_zero",
			input:    []byte{0x00, 0x00, 0x00, 0x00},
			nums:     make([]any, 1),
			expected: []any{int32(0)},
		},
		{
			name:     "single_positive",
			input:    []byte{0x7B, 0x00, 0x00, 0x00}, // 123 in little endian
			nums:     make([]any, 1),
			expected: []any{int32(123)},
		},
		{
			name:     "single_negative",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF}, // -1 in little endian
			nums:     make([]any, 1),
			expected: []any{int32(-1)},
		},
		{
			name:     "max_int32",
			input:    []byte{0xFF, 0xFF, 0xFF, 0x7F}, // 2147483647 in little endian
			nums:     make([]any, 1),
			expected: []any{int32(2147483647)},
		},
		{
			name:     "min_int32",
			input:    []byte{0x00, 0x00, 0x00, 0x80}, // -2147483648 in little endian
			nums:     make([]any, 1),
			expected: []any{int32(-2147483648)},
		},
		{
			name: "multiple_values",
			input: []byte{
				0x01, 0x00, 0x00, 0x00, // 1
				0x02, 0x00, 0x00, 0x00, // 2
				0xFF, 0xFF, 0xFF, 0xFF, // -1
			},
			nums:     make([]any, 3),
			expected: []any{int32(1), int32(2), int32(-1)},
		},
		{
			name:     "empty_array",
			input:    []byte{},
			nums:     []any{},
			expected: []any{},
		},
		{
			name:        "insufficient_data",
			input:       []byte{0x00, 0x00, 0x00}, // Only 3 bytes for int32
			nums:        make([]any, 1),
			expectError: true,
			errorType:   io.ErrUnexpectedEOF,
		},
		{
			name:        "partial_second_value",
			input:       []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00}, // 4 bytes + 2 bytes
			nums:        make([]any, 2),
			expectError: true,
			errorType:   io.ErrUnexpectedEOF,
		},
		{
			name:        "read_error",
			input:       []byte{0x00, 0x00, 0x00, 0x00},
			nums:        make([]any, 1),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reader io.Reader
			if tt.name == "read_error" {
				reader = &mockReader{
					data:       tt.input,
					failOnRead: true,
				}
			} else {
				reader = bytes.NewReader(tt.input)
			}
			err := BinaryReadINT32(reader, tt.nums)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != nil {
					require.ErrorIs(t, err, tt.errorType)
				}
				return
			}

			require.NoError(t, err)

			require.Len(t, tt.nums, len(tt.expected))

			for i, expected := range tt.expected {
				require.Equal(t, expected, tt.nums[i], "Value mismatch at index %d", i)
			}
		})
	}
}

func Test_BinaryReadINT64(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		nums        []any
		expected    []any
		expectError bool
		errorType   error
	}{
		{
			name:     "single_zero",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			nums:     make([]any, 1),
			expected: []any{int64(0)},
		},
		{
			name:     "single_positive",
			input:    []byte{0x7B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // 123 in little endian
			nums:     make([]any, 1),
			expected: []any{int64(123)},
		},
		{
			name:     "single_negative",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, // -1 in little endian
			nums:     make([]any, 1),
			expected: []any{int64(-1)},
		},
		{
			name:     "max_int64",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, // 9223372036854775807 in little endian
			nums:     make([]any, 1),
			expected: []any{int64(9223372036854775807)},
		},
		{
			name:     "min_int64",
			input:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}, // -9223372036854775808 in little endian
			nums:     make([]any, 1),
			expected: []any{int64(-9223372036854775808)},
		},
		{
			name: "multiple_values",
			input: []byte{
				0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1
				0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 2
				0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // -1
			},
			nums:     make([]any, 3),
			expected: []any{int64(1), int64(2), int64(-1)},
		},
		{
			name:     "empty_array",
			input:    []byte{},
			nums:     []any{},
			expected: []any{},
		},
		{
			name:        "insufficient_data",
			input:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // Only 7 bytes for int64
			nums:        make([]any, 1),
			expectError: true,
			errorType:   io.ErrUnexpectedEOF,
		},
		{
			name: "partial_second_value",
			input: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Complete first value
				0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // Incomplete second value (6 bytes)
			},
			nums:        make([]any, 2),
			expectError: true,
			errorType:   io.ErrUnexpectedEOF,
		},
		{
			name:        "read_error",
			input:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			nums:        make([]any, 1),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reader io.Reader
			if tt.name == "read_error" {
				reader = &mockReader{
					data:       tt.input,
					failOnRead: true,
				}
			} else {
				reader = bytes.NewReader(tt.input)
			}
			err := BinaryReadINT64(reader, tt.nums)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != nil {
					require.ErrorIs(t, err, tt.errorType)
				}
				return
			}

			require.NoError(t, err)

			require.Len(t, tt.nums, len(tt.expected))

			for i, expected := range tt.expected {
				require.Equal(t, expected, tt.nums[i], "Value mismatch at index %d", i)
			}
		})
	}
}

func Test_BinaryRead_EdgeCases(t *testing.T) {
	t.Run("ReadFull_ByteCountMismatch_INT32", func(t *testing.T) {
		// Test case where there are fewer bytes available than expected
		mockReader := &mockReader{
			data: []byte{0x00, 0x00, 0x00}, // Only 3 bytes available for 4-byte int32
		}
		nums := make([]any, 1)
		err := BinaryReadINT32(mockReader, nums)
		require.Error(t, err)
	})

	t.Run("ReadFull_ByteCountMismatch_INT64", func(t *testing.T) {
		mockReader := &mockReader{
			data: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // Only 7 bytes for 8-byte int64
		}
		nums := make([]any, 1)
		err := BinaryReadINT64(mockReader, nums)
		require.Error(t, err)
	})

	t.Run("ReadFull_ByteCountMismatch_FLOAT32", func(t *testing.T) {
		mockReader := &mockReader{
			data: []byte{0x00, 0x00, 0x00}, // Only 3 bytes for 4-byte float32
		}
		nums := make([]any, 1)
		err := BinaryReadFLOAT32(mockReader, nums)
		require.Error(t, err)
	})

	t.Run("ReadFull_ByteCountMismatch_FLOAT64", func(t *testing.T) {
		mockReader := &mockReader{
			data: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // Only 7 bytes for 8-byte float64
		}
		nums := make([]any, 1)
		err := BinaryReadFLOAT64(mockReader, nums)
		require.Error(t, err)
	})
}
