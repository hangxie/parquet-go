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

func TestBinaryWrite(t *testing.T) {
	tests := []struct {
		name          string
		typeName      string
		input         []any
		expected      []byte
		expectError   bool
		errorContains string
		useFailWriter bool
	}{
		// FLOAT32 tests
		{name: "float32/zero", typeName: "float32", input: []any{float32(0.0)}, expected: []byte{0x00, 0x00, 0x00, 0x00}},
		{name: "float32/positive", typeName: "float32", input: []any{float32(1.0)}, expected: []byte{0x00, 0x00, 0x80, 0x3F}},
		{name: "float32/negative", typeName: "float32", input: []any{float32(-1.0)}, expected: []byte{0x00, 0x00, 0x80, 0xBF}},
		{name: "float32/pi", typeName: "float32", input: []any{float32(3.1415927)}, expected: []byte{0xDB, 0x0F, 0x49, 0x40}},
		{name: "float32/multiple", typeName: "float32", input: []any{float32(1.0), float32(2.0), float32(3.0)}, expected: []byte{0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40}},
		{name: "float32/pos_inf", typeName: "float32", input: []any{float32(math.Inf(1))}, expected: []byte{0x00, 0x00, 0x80, 0x7F}},
		{name: "float32/neg_inf", typeName: "float32", input: []any{float32(math.Inf(-1))}, expected: []byte{0x00, 0x00, 0x80, 0xFF}},
		{name: "float32/empty", typeName: "float32", input: []any{}, expected: []byte{}},
		{name: "float32/invalid_string", typeName: "float32", input: []any{"not_a_float"}, expectError: true, errorContains: "is not float32"},
		{name: "float32/invalid_float64", typeName: "float32", input: []any{float64(123.45)}, expectError: true, errorContains: "is not float32"},
		{name: "float32/mixed_types", typeName: "float32", input: []any{float32(1.0), int32(2), float32(3.0)}, expectError: true, errorContains: "is not float32"},
		{name: "float32/write_error", typeName: "float32", input: []any{float32(123.45)}, expectError: true, errorContains: "mock write error", useFailWriter: true},

		// FLOAT64 tests
		{name: "float64/zero", typeName: "float64", input: []any{float64(0.0)}, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "float64/positive", typeName: "float64", input: []any{float64(1.0)}, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}},
		{name: "float64/negative", typeName: "float64", input: []any{float64(-1.0)}, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF}},
		{name: "float64/max", typeName: "float64", input: []any{math.MaxFloat64}, expected: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0x7F}},
		{name: "float64/smallest", typeName: "float64", input: []any{math.SmallestNonzeroFloat64}, expected: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "float64/multiple", typeName: "float64", input: []any{float64(1.0), float64(2.0), float64(3.0)}, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40}},
		{name: "float64/pos_inf", typeName: "float64", input: []any{math.Inf(1)}, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x7F}},
		{name: "float64/neg_inf", typeName: "float64", input: []any{math.Inf(-1)}, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xFF}},
		{name: "float64/empty", typeName: "float64", input: []any{}, expected: []byte{}},
		{name: "float64/invalid_string", typeName: "float64", input: []any{"not_a_float"}, expectError: true, errorContains: "is not float32"},
		{name: "float64/mixed_types", typeName: "float64", input: []any{float64(1.0), int64(2), float64(3.0)}, expectError: true, errorContains: "is not float32"},
		{name: "float64/write_error", typeName: "float64", input: []any{float64(123.45)}, expectError: true, errorContains: "mock write error", useFailWriter: true},

		// INT32 tests
		{name: "int32/zero", typeName: "int32", input: []any{int32(0)}, expected: []byte{0x00, 0x00, 0x00, 0x00}},
		{name: "int32/positive", typeName: "int32", input: []any{int32(123)}, expected: []byte{0x7B, 0x00, 0x00, 0x00}},
		{name: "int32/negative", typeName: "int32", input: []any{int32(-1)}, expected: []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{name: "int32/max", typeName: "int32", input: []any{int32(2147483647)}, expected: []byte{0xFF, 0xFF, 0xFF, 0x7F}},
		{name: "int32/min", typeName: "int32", input: []any{int32(-2147483648)}, expected: []byte{0x00, 0x00, 0x00, 0x80}},
		{name: "int32/multiple", typeName: "int32", input: []any{int32(1), int32(2), int32(-1)}, expected: []byte{0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF}},
		{name: "int32/empty", typeName: "int32", input: []any{}, expected: []byte{}},
		{name: "int32/invalid_string", typeName: "int32", input: []any{"not_an_int"}, expectError: true, errorContains: "is not int32"},
		{name: "int32/mixed_types", typeName: "int32", input: []any{int32(1), "invalid", int32(2)}, expectError: true, errorContains: "is not int32"},
		{name: "int32/write_error", typeName: "int32", input: []any{int32(123)}, expectError: true, errorContains: "mock write error", useFailWriter: true},

		// INT64 tests
		{name: "int64/zero", typeName: "int64", input: []any{int64(0)}, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "int64/positive", typeName: "int64", input: []any{int64(123)}, expected: []byte{0x7B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "int64/negative", typeName: "int64", input: []any{int64(-1)}, expected: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
		{name: "int64/max", typeName: "int64", input: []any{int64(9223372036854775807)}, expected: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
		{name: "int64/min", typeName: "int64", input: []any{int64(-9223372036854775808)}, expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}},
		{name: "int64/multiple", typeName: "int64", input: []any{int64(1), int64(2), int64(-1)}, expected: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
		{name: "int64/empty", typeName: "int64", input: []any{}, expected: []byte{}},
		{name: "int64/invalid_string", typeName: "int64", input: []any{"not_an_int"}, expectError: true, errorContains: "is not int64"},
		{name: "int64/mixed_types", typeName: "int64", input: []any{int64(1), int32(2), int64(3)}, expectError: true, errorContains: "is not int64"},
		{name: "int64/write_error", typeName: "int64", input: []any{int64(123)}, expectError: true, errorContains: "mock write error", useFailWriter: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writer io.Writer
			var buf *bytes.Buffer

			if tt.useFailWriter {
				writer = &mockWriter{failOnWrite: true}
			} else {
				buf = &bytes.Buffer{}
				writer = buf
			}

			var err error
			switch tt.typeName {
			case "float32":
				err = BinaryWriteFLOAT32(writer, tt.input)
			case "float64":
				err = BinaryWriteFLOAT64(writer, tt.input)
			case "int32":
				err = BinaryWriteINT32(writer, tt.input)
			case "int64":
				err = BinaryWriteINT64(writer, tt.input)
			}

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorContains)
				return
			}

			require.NoError(t, err)
			actual := buf.Bytes()
			if len(tt.expected) == 0 && len(actual) == 0 {
				return
			}
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestBinaryWriteReadRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		original []any
	}{
		{name: "int32", typeName: "int32", original: []any{int32(0), int32(123), int32(-1), int32(2147483647), int32(-2147483648)}},
		{name: "int64", typeName: "int64", original: []any{int64(0), int64(123), int64(-1), int64(9223372036854775807), int64(-9223372036854775808)}},
		{name: "float32", typeName: "float32", original: []any{float32(0.0), float32(1.0), float32(-1.0), float32(3.1415927)}},
		{name: "float64", typeName: "float64", original: []any{float64(0.0), float64(1.0), float64(-1.0), math.MaxFloat64, math.SmallestNonzeroFloat64}},
		{name: "float32_infinity", typeName: "float32", original: []any{float32(math.Inf(1)), float32(math.Inf(-1))}},
		{name: "float64_infinity", typeName: "float64", original: []any{math.Inf(1), math.Inf(-1)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			result := make([]any, len(tt.original))

			var writeErr, readErr error
			switch tt.typeName {
			case "int32":
				writeErr = BinaryWriteINT32(buf, tt.original)
				readErr = BinaryReadINT32(buf, result)
			case "int64":
				writeErr = BinaryWriteINT64(buf, tt.original)
				readErr = BinaryReadINT64(buf, result)
			case "float32":
				writeErr = BinaryWriteFLOAT32(buf, tt.original)
				readErr = BinaryReadFLOAT32(buf, result)
			case "float64":
				writeErr = BinaryWriteFLOAT64(buf, tt.original)
				readErr = BinaryReadFLOAT64(buf, result)
			}

			require.NoError(t, writeErr)
			require.NoError(t, readErr)

			// Handle infinity comparison specially
			if tt.name == "float32_infinity" || tt.name == "float64_infinity" {
				require.Len(t, result, len(tt.original))
				for i := range tt.original {
					switch v := tt.original[i].(type) {
					case float32:
						resultVal := float64(result[i].(float32))
						require.True(t, math.IsInf(resultVal, 0), "expected infinity")
						// Check sign matches
						require.Equal(t, math.Signbit(float64(v)), math.Signbit(resultVal))
					case float64:
						resultVal := result[i].(float64)
						require.True(t, math.IsInf(resultVal, 0), "expected infinity")
						// Check sign matches
						require.Equal(t, math.Signbit(v), math.Signbit(resultVal))
					}
				}
				return
			}

			require.Equal(t, tt.original, result)
		})
	}
}
