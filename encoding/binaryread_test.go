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

func TestBinaryRead(t *testing.T) {
	tests := []struct {
		name        string
		typeName    string
		input       []byte
		numValues   int
		expected    []any
		expectError bool
		errorType   error
		useMockFail bool
	}{
		// FLOAT32 tests
		{name: "float32/zero", typeName: "float32", input: []byte{0x00, 0x00, 0x00, 0x00}, numValues: 1, expected: []any{float32(0.0)}},
		{name: "float32/positive", typeName: "float32", input: []byte{0x00, 0x00, 0x80, 0x3F}, numValues: 1, expected: []any{float32(1.0)}},
		{name: "float32/negative", typeName: "float32", input: []byte{0x00, 0x00, 0x80, 0xBF}, numValues: 1, expected: []any{float32(-1.0)}},
		{name: "float32/pi", typeName: "float32", input: []byte{0xDB, 0x0F, 0x49, 0x40}, numValues: 1, expected: []any{float32(3.1415927)}},
		{name: "float32/multiple", typeName: "float32", input: []byte{0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40}, numValues: 3, expected: []any{float32(1.0), float32(2.0), float32(3.0)}},
		{name: "float32/empty", typeName: "float32", input: []byte{}, numValues: 0, expected: []any{}},
		{name: "float32/insufficient", typeName: "float32", input: []byte{0x00, 0x00, 0x00}, numValues: 1, expectError: true, errorType: io.ErrUnexpectedEOF},
		{name: "float32/partial_second", typeName: "float32", input: []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00}, numValues: 2, expectError: true, errorType: io.ErrUnexpectedEOF},
		{name: "float32/read_error", typeName: "float32", input: []byte{0x00, 0x00, 0x00, 0x00}, numValues: 1, expectError: true, useMockFail: true},

		// FLOAT64 tests
		{name: "float64/zero", typeName: "float64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, numValues: 1, expected: []any{float64(0.0)}},
		{name: "float64/positive", typeName: "float64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F}, numValues: 1, expected: []any{float64(1.0)}},
		{name: "float64/negative", typeName: "float64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF}, numValues: 1, expected: []any{float64(-1.0)}},
		{name: "float64/max", typeName: "float64", input: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0x7F}, numValues: 1, expected: []any{math.MaxFloat64}},
		{name: "float64/smallest", typeName: "float64", input: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, numValues: 1, expected: []any{math.SmallestNonzeroFloat64}},
		{name: "float64/multiple", typeName: "float64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40}, numValues: 3, expected: []any{float64(1.0), float64(2.0), float64(3.0)}},
		{name: "float64/empty", typeName: "float64", input: []byte{}, numValues: 0, expected: []any{}},
		{name: "float64/insufficient", typeName: "float64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, numValues: 1, expectError: true, errorType: io.ErrUnexpectedEOF},
		{name: "float64/read_error", typeName: "float64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, numValues: 1, expectError: true, useMockFail: true},

		// INT32 tests
		{name: "int32/zero", typeName: "int32", input: []byte{0x00, 0x00, 0x00, 0x00}, numValues: 1, expected: []any{int32(0)}},
		{name: "int32/positive", typeName: "int32", input: []byte{0x7B, 0x00, 0x00, 0x00}, numValues: 1, expected: []any{int32(123)}},
		{name: "int32/negative", typeName: "int32", input: []byte{0xFF, 0xFF, 0xFF, 0xFF}, numValues: 1, expected: []any{int32(-1)}},
		{name: "int32/max", typeName: "int32", input: []byte{0xFF, 0xFF, 0xFF, 0x7F}, numValues: 1, expected: []any{int32(2147483647)}},
		{name: "int32/min", typeName: "int32", input: []byte{0x00, 0x00, 0x00, 0x80}, numValues: 1, expected: []any{int32(-2147483648)}},
		{name: "int32/multiple", typeName: "int32", input: []byte{0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF}, numValues: 3, expected: []any{int32(1), int32(2), int32(-1)}},
		{name: "int32/empty", typeName: "int32", input: []byte{}, numValues: 0, expected: []any{}},
		{name: "int32/insufficient", typeName: "int32", input: []byte{0x00, 0x00, 0x00}, numValues: 1, expectError: true, errorType: io.ErrUnexpectedEOF},
		{name: "int32/read_error", typeName: "int32", input: []byte{0x00, 0x00, 0x00, 0x00}, numValues: 1, expectError: true, useMockFail: true},

		// INT64 tests
		{name: "int64/zero", typeName: "int64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, numValues: 1, expected: []any{int64(0)}},
		{name: "int64/positive", typeName: "int64", input: []byte{0x7B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, numValues: 1, expected: []any{int64(123)}},
		{name: "int64/negative", typeName: "int64", input: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, numValues: 1, expected: []any{int64(-1)}},
		{name: "int64/max", typeName: "int64", input: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, numValues: 1, expected: []any{int64(9223372036854775807)}},
		{name: "int64/min", typeName: "int64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}, numValues: 1, expected: []any{int64(-9223372036854775808)}},
		{name: "int64/multiple", typeName: "int64", input: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, numValues: 3, expected: []any{int64(1), int64(2), int64(-1)}},
		{name: "int64/empty", typeName: "int64", input: []byte{}, numValues: 0, expected: []any{}},
		{name: "int64/insufficient", typeName: "int64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, numValues: 1, expectError: true, errorType: io.ErrUnexpectedEOF},
		{name: "int64/read_error", typeName: "int64", input: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, numValues: 1, expectError: true, useMockFail: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reader io.Reader
			if tt.useMockFail {
				reader = &mockReader{data: tt.input, failOnRead: true}
			} else {
				reader = bytes.NewReader(tt.input)
			}

			nums := make([]any, tt.numValues)
			var err error

			switch tt.typeName {
			case "float32":
				err = BinaryReadFLOAT32(reader, nums)
			case "float64":
				err = BinaryReadFLOAT64(reader, nums)
			case "int32":
				err = BinaryReadINT32(reader, nums)
			case "int64":
				err = BinaryReadINT64(reader, nums)
			}

			if tt.expectError {
				require.Error(t, err)
				if tt.errorType != nil {
					require.ErrorIs(t, err, tt.errorType)
				}
				return
			}

			require.NoError(t, err)
			require.Len(t, nums, len(tt.expected))

			for i, expected := range tt.expected {
				require.Equal(t, expected, nums[i], "Value mismatch at index %d", i)
			}
		})
	}
}

func TestBinaryReadEdgeCases(t *testing.T) {
	edgeCases := []struct {
		name     string
		typeName string
		dataLen  int
	}{
		{name: "int32_mismatch", typeName: "int32", dataLen: 3},
		{name: "int64_mismatch", typeName: "int64", dataLen: 7},
		{name: "float32_mismatch", typeName: "float32", dataLen: 3},
		{name: "float64_mismatch", typeName: "float64", dataLen: 7},
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &mockReader{data: make([]byte, tc.dataLen)}
			nums := make([]any, 1)

			var err error
			switch tc.typeName {
			case "int32":
				err = BinaryReadINT32(reader, nums)
			case "int64":
				err = BinaryReadINT64(reader, nums)
			case "float32":
				err = BinaryReadFLOAT32(reader, nums)
			case "float64":
				err = BinaryReadFLOAT64(reader, nums)
			}

			require.Error(t, err)
		})
	}
}
