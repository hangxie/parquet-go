package encoding

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"testing"
	"unsafe"

	"github.com/hangxie/parquet-go/v2/parquet"
)

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
			if err != nil {
				t.Fatalf("WritePlainBOOLEAN failed: %v", err)
			}

			// Read data back from buffer
			result, err := ReadPlainBOOLEAN(bytes.NewReader(buf), uint64(len(testCase.input)))
			if err != nil {
				t.Fatalf("ReadPlainBOOLEAN failed: %v", err)
			}

			// Compare results
			if len(result) != len(testCase.input) {
				t.Errorf("Length mismatch: expected %d, got %d", len(testCase.input), len(result))
			}

			for i, expected := range testCase.input {
				if i < len(result) && result[i] != expected {
					t.Errorf("Value mismatch at index %d: expected %v, got %v", i, expected, result[i])
				}
			}
		})
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
			if err != nil {
				t.Fatalf("ReadPlainINT32 failed: %v", err)
			}

			if len(result) != len(testCase.expected) {
				t.Errorf("Length mismatch: expected %d, got %d", len(testCase.expected), len(result))
			}

			for i, expected := range testCase.expected {
				if i < len(result) && result[i] != expected {
					t.Errorf("Value mismatch at index %d: expected %v, got %v", i, expected, result[i])
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
			if err != nil {
				t.Fatalf("ReadPlainINT64 failed: %v", err)
			}

			if len(result) != len(testCase.expected) {
				t.Errorf("Length mismatch: expected %d, got %d", len(testCase.expected), len(result))
			}

			for i, expected := range testCase.expected {
				if i < len(result) && result[i] != expected {
					t.Errorf("Value mismatch at index %d: expected %v, got %v", i, expected, result[i])
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
			if err != nil {
				t.Fatalf("WritePlainBYTE_ARRAY failed: %v", err)
			}

			// Read data back from buffer
			result, err := ReadPlainBYTE_ARRAY(bytes.NewReader(buf), uint64(len(testCase.input)))
			if err != nil {
				t.Fatalf("ReadPlainBYTE_ARRAY failed: %v", err)
			}

			// Compare results
			if len(result) != len(testCase.input) {
				t.Errorf("Length mismatch: expected %d, got %d", len(testCase.input), len(result))
			}

			for i, expected := range testCase.input {
				if i < len(result) && result[i] != expected {
					t.Errorf("Value mismatch at index %d: expected %v, got %v", i, expected, result[i])
				}
			}
		})
	}
}

func Test_ReadPlainFIXED_LEN_BYTE_ARRAY(t *testing.T) {
	testData := [][]any{
		{("hello"), ("world")},
		{("a"), ("b"), ("c"), ("d")},
	}

	for _, data := range testData {
		buf, err := WritePlainFIXED_LEN_BYTE_ARRAY(data)
		if err != nil {
			t.Errorf("WritePlainFIXED_LEN_BYTE_ARRAY err, %v", err)
			continue
		}
		res, _ := ReadPlainFIXED_LEN_BYTE_ARRAY(bytes.NewReader(buf), uint64(len(data)), uint64(len(data[0].(string))))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainFIXED_LEN_BYTE_ARRAY err, %v", data)
		}
	}
}

func Test_ReadPlainFLOAT(t *testing.T) {
	testData := [][]any{
		{float32(0), float32(1), float32(2)},
		{float32(0), float32(0.1), float32(0.2)},
	}

	for _, data := range testData {
		buf, err := WritePlainFLOAT(data)
		if err != nil {
			t.Errorf("WritePlainFLOAT err, %v", err)
			continue
		}
		res, _ := ReadPlainFLOAT(bytes.NewReader(buf), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainFLOAT err, %v", data)
		}
	}
}

func Test_ReadPlainDOUBLE(t *testing.T) {
	testData := [][]any{
		{float64(0), float64(1), float64(2)},
		{float64(0), float64(0), float64(0)},
	}

	for _, data := range testData {
		buf, err := WritePlainDOUBLE(data)
		if err != nil {
			t.Errorf("WritePlainDOUBLE err, %v", err)
			continue
		}
		res, _ := ReadPlainDOUBLE(bytes.NewReader(buf), uint64(len(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadPlainDOUBLE err, %v", data)
		}
	}
}

func Test_ReadUnsignedVarInt(t *testing.T) {
	i32 := int32(-1570499385)

	testData := []uint64{1, 2, 3, 11, 1570499385, uint64(i32), 111, 222, 333, 0}
	for _, data := range testData {
		res, _ := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadUnsignedVarInt err, %v", data)
		}
	}
}

func Test_ReadRLEBitPackedHybrid(t *testing.T) {
	testData := [][]any{
		{int64(1), int64(2), int64(3), int64(4)},
		{int64(0), int64(0), int64(0), int64(0), int64(0)},
	}
	for _, data := range testData {
		maxVal := uint64(data[len(data)-1].(int64))
		buf, err := WriteRLEBitPackedHybrid(data, int32(bits.Len64(maxVal)), parquet.Type_INT64)
		if err != nil {
			t.Errorf("WriteRLEBitPackedHybrid err, %v", err)
			continue
		}
		res, err := ReadRLEBitPackedHybrid(bytes.NewReader(buf), uint64(bits.Len64(maxVal)), 0)
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadRLEBitpackedHybrid error, expect %v, get %v, err info:%v", data, res, err)
		}
	}
}

func Test_ReadDeltaBinaryPackedINT(t *testing.T) {
	testData := [][]any{
		{int64(1), int64(2), int64(3), int64(4)},
		{int64(math.MaxInt64), int64(math.MinInt64), int64(-15654523568543623), int64(4354365463543632), int64(0)},
	}

	for _, data := range testData {
		fmt.Println(data)
		res, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(WriteDeltaINT64(data)))
		if err != nil {
			t.Error(err)
		}

		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadDeltaBinaryPackedINT64 error, expect %v, get %v", data, res)
		}
	}
}

func Test_ReadDeltaINT32(t *testing.T) {
	bInt32 := func(n int32) string { return strconv.FormatUint(uint64(*(*uint32)(unsafe.Pointer(&n))), 2) }
	buInt64 := func(n uint64) string { return strconv.FormatUint(n, 2) }
	testData := []int32{1, -1570499385, 3, -11, 1570499385, 111, 222, 333, 0}
	for _, data := range testData {
		fmt.Println("SRC32:", bInt32(data), data)
		u64 := uint64((data >> 31) ^ (data << 1))
		fmt.Println("SRC64:", buInt64(u64))
		resZigZag, err := ReadUnsignedVarInt(bytes.NewReader(WriteUnsignedVarInt(u64)))
		if err != nil {
			t.Error(err)
		}
		res32 := int32(resZigZag)
		var res int32 = int32(uint32(res32)>>1) ^ -(res32 & 1)
		fmt.Println("RES32:", bInt32(res), res)
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadUnsignedVarInt err, %v", data)
		}
	}
}

func Test_ReadDeltaBinaryPackedINT32(t *testing.T) {
	testData := [][]any{
		{int32(1), int32(2), int32(3), int32(4)},
		{int32(-1570499385), int32(-1570499385), int32(-1570499386), int32(-1570499388), int32(-1570499385)},
	}

	for _, data := range testData {
		fmt.Println("source:", data)

		res, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(WriteDeltaINT32(data)))
		if err != nil {
			t.Error(err)
		}
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadDeltaBinaryPackedINT32 error, expect %v, get %v", data, res)
		}
	}
}

func Test_ReadDeltaByteArray(t *testing.T) {
	testData := [][]any{
		{"Hello", "world"},
	}
	for _, data := range testData {
		res, _ := ReadDeltaByteArray(bytes.NewReader(WriteDeltaByteArray(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadDeltaByteArray err, expect %v, get %v", data, res)
		}
	}
}

func Test_ReadLengthDeltaByteArray(t *testing.T) {
	testData := [][]any{
		{"Hello", "world"},
	}
	for _, data := range testData {
		res, _ := ReadDeltaLengthByteArray(bytes.NewReader(WriteDeltaLengthByteArray(data)))
		if fmt.Sprintf("%v", data) != fmt.Sprintf("%v", res) {
			t.Errorf("ReadDeltaLengthByteArray err, expect %v, get %v", data, res)
		}
	}
}

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
		if fmt.Sprintf("%v", res) != fmt.Sprintf("%v", data) {
			t.Errorf("ReadBitPacked err, expect %v, get %v", data, res)
		}
	}
}
