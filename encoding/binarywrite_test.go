package encoding

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockWriter simulates various write scenarios for testing error cases
type mockWriter struct {
	data         []byte
	failOnWrite  bool
	bytesToWrite int
	writeCount   int
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	if m.failOnWrite {
		return 0, errors.New("mock write error")
	}

	// If bytesToWrite is set, limit the write
	if m.bytesToWrite > 0 && len(p) > m.bytesToWrite {
		m.data = append(m.data, p[:m.bytesToWrite]...)
		return m.bytesToWrite, nil
	}

	m.data = append(m.data, p...)
	m.writeCount++
	return len(p), nil
}

// Test round-trip compatibility with binary read functions

func Test_BinaryWriteFLOAT32(t *testing.T) {
	tests := []struct {
		name          string
		input         []any
		expected      []byte
		expectError   bool
		errorMessage  string
		useFailWriter bool
	}{
		{
			name:     "zero",
			input:    []any{float32(0.0)},
			expected: []byte{0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "positive_float",
			input:    []any{float32(1.0)},
			expected: []byte{0x00, 0x00, 0x80, 0x3F}, // 1.0 in IEEE 754 little endian
		},
		{
			name:     "negative_float",
			input:    []any{float32(-1.0)},
			expected: []byte{0x00, 0x00, 0x80, 0xBF}, // -1.0 in IEEE 754 little endian
		},
		{
			name:     "pi_approximation",
			input:    []any{float32(3.1415927)},
			expected: []byte{0xDB, 0x0F, 0x49, 0x40}, // approximately 3.14159 in little endian
		},
		{
			name: "multiple_values",
			input: []any{
				float32(1.0),
				float32(2.0),
				float32(3.0),
			},
			expected: []byte{
				0x00, 0x00, 0x80, 0x3F, // 1.0
				0x00, 0x00, 0x00, 0x40, // 2.0
				0x00, 0x00, 0x40, 0x40, // 3.0
			},
		},
		{
			name:     "positive_infinity",
			input:    []any{float32(math.Inf(1))},
			expected: []byte{0x00, 0x00, 0x80, 0x7F}, // +Inf in IEEE 754 little endian
		},
		{
			name:     "negative_infinity",
			input:    []any{float32(math.Inf(-1))},
			expected: []byte{0x00, 0x00, 0x80, 0xFF}, // -Inf in IEEE 754 little endian
		},
		{
			name:     "empty_array",
			input:    []any{},
			expected: []byte{},
		},
		{
			name:         "invalid_type_string",
			input:        []any{"not_a_float"},
			expectError:  true,
			errorMessage: "is not float32",
		},
		{
			name:         "invalid_type_float64",
			input:        []any{float64(123.45)},
			expectError:  true,
			errorMessage: "is not float32",
		},
		{
			name:         "mixed_types",
			input:        []any{float32(1.0), int32(2), float32(3.0)},
			expectError:  true,
			errorMessage: "is not float32",
		},
		{
			name:          "write_error",
			input:         []any{float32(123.45)},
			expectError:   true,
			errorMessage:  "mock write error",
			useFailWriter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writer io.Writer
			var buf *bytes.Buffer
			var mockW *mockWriter

			if tt.useFailWriter {
				mockW = &mockWriter{failOnWrite: true}
				writer = mockW
			} else {
				buf = &bytes.Buffer{}
				writer = buf
			}

			err := BinaryWriteFLOAT32(writer, tt.input)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
				return
			}

			require.NoError(t, err)
			actual := buf.Bytes()
			if len(tt.expected) == 0 && len(actual) == 0 {
				// Both are empty, consider them equal regardless of nil vs empty slice
				return
			}
			require.Equal(t, tt.expected, actual)
		})
	}
}

func Test_BinaryWriteFLOAT64(t *testing.T) {
	tests := []struct {
		name          string
		input         []any
		expected      []byte
		expectError   bool
		errorMessage  string
		useFailWriter bool
	}{
		{
			name:     "zero",
			input:    []any{float64(0.0)},
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "positive_float",
			input:    []any{float64(1.0)},
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}, // 1.0 in IEEE 754 little endian
		},
		{
			name:     "negative_float",
			input:    []any{float64(-1.0)},
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF}, // -1.0 in IEEE 754 little endian
		},
		{
			name:     "max_float64",
			input:    []any{math.MaxFloat64},
			expected: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0x7F}, // math.MaxFloat64 in little endian
		},
		{
			name:     "smallest_positive_float64",
			input:    []any{math.SmallestNonzeroFloat64},
			expected: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // math.SmallestNonzeroFloat64 in little endian
		},
		{
			name: "multiple_values",
			input: []any{
				float64(1.0),
				float64(2.0),
				float64(3.0),
			},
			expected: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // 1.0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // 2.0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // 3.0
			},
		},
		{
			name:     "positive_infinity",
			input:    []any{math.Inf(1)},
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x7F}, // +Inf in IEEE 754 little endian
		},
		{
			name:     "negative_infinity",
			input:    []any{math.Inf(-1)},
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xFF}, // -Inf in IEEE 754 little endian
		},
		{
			name:     "empty_array",
			input:    []any{},
			expected: []byte{},
		},
		{
			name:         "invalid_type_string",
			input:        []any{"not_a_float"},
			expectError:  true,
			errorMessage: "is not float32", // Note: bug in original code - should be float64
		},
		{
			name:         "invalid_type_float32",
			input:        []any{float32(123.45)},
			expectError:  true,
			errorMessage: "is not float32", // Note: bug in original code - should be float64
		},
		{
			name:         "mixed_types",
			input:        []any{float64(1.0), int64(2), float64(3.0)},
			expectError:  true,
			errorMessage: "is not float32", // Note: bug in original code - should be float64
		},
		{
			name:          "write_error",
			input:         []any{float64(123.45)},
			expectError:   true,
			errorMessage:  "mock write error",
			useFailWriter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writer io.Writer
			var buf *bytes.Buffer
			var mockW *mockWriter

			if tt.useFailWriter {
				mockW = &mockWriter{failOnWrite: true}
				writer = mockW
			} else {
				buf = &bytes.Buffer{}
				writer = buf
			}

			err := BinaryWriteFLOAT64(writer, tt.input)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
				return
			}

			require.NoError(t, err)
			actual := buf.Bytes()
			if len(tt.expected) == 0 && len(actual) == 0 {
				// Both are empty, consider them equal regardless of nil vs empty slice
				return
			}
			require.Equal(t, tt.expected, actual)
		})
	}
}

func Test_BinaryWriteINT32(t *testing.T) {
	tests := []struct {
		name          string
		input         []any
		expected      []byte
		expectError   bool
		errorMessage  string
		useFailWriter bool
	}{
		{
			name:     "single_zero",
			input:    []any{int32(0)},
			expected: []byte{0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "single_positive",
			input:    []any{int32(123)},
			expected: []byte{0x7B, 0x00, 0x00, 0x00}, // 123 in little endian
		},
		{
			name:     "single_negative",
			input:    []any{int32(-1)},
			expected: []byte{0xFF, 0xFF, 0xFF, 0xFF}, // -1 in little endian
		},
		{
			name:     "max_int32",
			input:    []any{int32(2147483647)},
			expected: []byte{0xFF, 0xFF, 0xFF, 0x7F}, // 2147483647 in little endian
		},
		{
			name:     "min_int32",
			input:    []any{int32(-2147483648)},
			expected: []byte{0x00, 0x00, 0x00, 0x80}, // -2147483648 in little endian
		},
		{
			name: "multiple_values",
			input: []any{
				int32(1),
				int32(2),
				int32(-1),
			},
			expected: []byte{
				0x01, 0x00, 0x00, 0x00, // 1
				0x02, 0x00, 0x00, 0x00, // 2
				0xFF, 0xFF, 0xFF, 0xFF, // -1
			},
		},
		{
			name:     "empty_array",
			input:    []any{},
			expected: []byte{},
		},
		{
			name:         "invalid_type_string",
			input:        []any{"not_an_int"},
			expectError:  true,
			errorMessage: "is not int32",
		},
		{
			name:         "invalid_type_float",
			input:        []any{float64(123.45)},
			expectError:  true,
			errorMessage: "is not int32",
		},
		{
			name:         "mixed_types",
			input:        []any{int32(1), "invalid", int32(2)},
			expectError:  true,
			errorMessage: "is not int32",
		},
		{
			name:          "write_error",
			input:         []any{int32(123)},
			expectError:   true,
			errorMessage:  "mock write error",
			useFailWriter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writer io.Writer
			var buf *bytes.Buffer
			var mockW *mockWriter

			if tt.useFailWriter {
				mockW = &mockWriter{failOnWrite: true}
				writer = mockW
			} else {
				buf = &bytes.Buffer{}
				writer = buf
			}

			err := BinaryWriteINT32(writer, tt.input)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
				return
			}

			require.NoError(t, err)
			actual := buf.Bytes()
			if len(tt.expected) == 0 && len(actual) == 0 {
				// Both are empty, consider them equal regardless of nil vs empty slice
				return
			}
			require.Equal(t, tt.expected, actual)
		})
	}
}

func Test_BinaryWriteINT64(t *testing.T) {
	tests := []struct {
		name          string
		input         []any
		expected      []byte
		expectError   bool
		errorMessage  string
		useFailWriter bool
	}{
		{
			name:     "single_zero",
			input:    []any{int64(0)},
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "single_positive",
			input:    []any{int64(123)},
			expected: []byte{0x7B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // 123 in little endian
		},
		{
			name:     "single_negative",
			input:    []any{int64(-1)},
			expected: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, // -1 in little endian
		},
		{
			name:     "max_int64",
			input:    []any{int64(9223372036854775807)},
			expected: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, // max int64 in little endian
		},
		{
			name:     "min_int64",
			input:    []any{int64(-9223372036854775808)},
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}, // min int64 in little endian
		},
		{
			name: "multiple_values",
			input: []any{
				int64(1),
				int64(2),
				int64(-1),
			},
			expected: []byte{
				0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 1
				0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 2
				0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // -1
			},
		},
		{
			name:     "empty_array",
			input:    []any{},
			expected: []byte{},
		},
		{
			name:         "invalid_type_string",
			input:        []any{"not_an_int"},
			expectError:  true,
			errorMessage: "is not int64",
		},
		{
			name:         "invalid_type_int32",
			input:        []any{int32(123)},
			expectError:  true,
			errorMessage: "is not int64",
		},
		{
			name:         "mixed_types",
			input:        []any{int64(1), int32(2), int64(3)},
			expectError:  true,
			errorMessage: "is not int64",
		},
		{
			name:          "write_error",
			input:         []any{int64(123)},
			expectError:   true,
			errorMessage:  "mock write error",
			useFailWriter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writer io.Writer
			var buf *bytes.Buffer
			var mockW *mockWriter

			if tt.useFailWriter {
				mockW = &mockWriter{failOnWrite: true}
				writer = mockW
			} else {
				buf = &bytes.Buffer{}
				writer = buf
			}

			err := BinaryWriteINT64(writer, tt.input)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
				return
			}

			require.NoError(t, err)
			actual := buf.Bytes()
			if len(tt.expected) == 0 && len(actual) == 0 {
				// Both are empty, consider them equal regardless of nil vs empty slice
				return
			}
			require.Equal(t, tt.expected, actual)
		})
	}
}

func Test_BinaryWriteReadRoundTrip(t *testing.T) {
	t.Run("INT32_roundtrip", func(t *testing.T) {
		original := []any{int32(0), int32(123), int32(-1), int32(2147483647), int32(-2147483648)}

		buf := &bytes.Buffer{}
		err := BinaryWriteINT32(buf, original)
		require.NoError(t, err)

		result := make([]any, len(original))
		err = BinaryReadINT32(buf, result)
		require.NoError(t, err)
		require.Equal(t, original, result)
	})

	t.Run("INT64_roundtrip", func(t *testing.T) {
		original := []any{int64(0), int64(123), int64(-1), int64(9223372036854775807), int64(-9223372036854775808)}

		buf := &bytes.Buffer{}
		err := BinaryWriteINT64(buf, original)
		require.NoError(t, err)

		result := make([]any, len(original))
		err = BinaryReadINT64(buf, result)
		require.NoError(t, err)
		require.Equal(t, original, result)
	})

	t.Run("FLOAT32_roundtrip", func(t *testing.T) {
		original := []any{float32(0.0), float32(1.0), float32(-1.0), float32(3.1415927)}

		buf := &bytes.Buffer{}
		err := BinaryWriteFLOAT32(buf, original)
		require.NoError(t, err)

		result := make([]any, len(original))
		err = BinaryReadFLOAT32(buf, result)
		require.NoError(t, err)
		require.Equal(t, original, result)
	})

	t.Run("FLOAT64_roundtrip", func(t *testing.T) {
		original := []any{float64(0.0), float64(1.0), float64(-1.0), math.MaxFloat64, math.SmallestNonzeroFloat64}

		buf := &bytes.Buffer{}
		err := BinaryWriteFLOAT64(buf, original)
		require.NoError(t, err)

		result := make([]any, len(original))
		err = BinaryReadFLOAT64(buf, result)
		require.NoError(t, err)
		require.Equal(t, original, result)
	})

	t.Run("FLOAT32_infinity_roundtrip", func(t *testing.T) {
		original := []any{float32(math.Inf(1)), float32(math.Inf(-1))}

		buf := &bytes.Buffer{}
		err := BinaryWriteFLOAT32(buf, original)
		require.NoError(t, err)

		result := make([]any, len(original))
		err = BinaryReadFLOAT32(buf, result)
		require.NoError(t, err)

		// Special comparison for infinity values
		require.Len(t, result, len(original))
		require.True(t, math.IsInf(float64(result[0].(float32)), 1))
		require.True(t, math.IsInf(float64(result[1].(float32)), -1))
	})

	t.Run("FLOAT64_infinity_roundtrip", func(t *testing.T) {
		original := []any{math.Inf(1), math.Inf(-1)}

		buf := &bytes.Buffer{}
		err := BinaryWriteFLOAT64(buf, original)
		require.NoError(t, err)

		result := make([]any, len(original))
		err = BinaryReadFLOAT64(buf, result)
		require.NoError(t, err)

		// Special comparison for infinity values
		require.Len(t, result, len(original))
		require.True(t, math.IsInf(result[0].(float64), 1))
		require.True(t, math.IsInf(result[1].(float64), -1))
	})
}
