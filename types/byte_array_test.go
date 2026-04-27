package types

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStrIntToBinary(t *testing.T) {
	testCases := []struct {
		name        string
		expectedNum int32
		inputNumStr string
		byteOrder   string
		length      int
		isSigned    bool
	}{
		// Little Endian tests
		{
			name:        "little-endian-zero",
			expectedNum: 0,
			inputNumStr: "0",
			byteOrder:   "LittleEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "little-endian-positive",
			expectedNum: 10,
			inputNumStr: "10",
			byteOrder:   "LittleEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "little-endian-negative",
			expectedNum: -10,
			inputNumStr: "-10",
			byteOrder:   "LittleEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "little-endian-max-int32",
			expectedNum: 2147483647,
			inputNumStr: "2147483647",
			byteOrder:   "LittleEndian",
			length:      0,
			isSigned:    true,
		},
		{
			name:        "little-endian-min-int32",
			expectedNum: -2147483648,
			inputNumStr: "-2147483648",
			byteOrder:   "LittleEndian",
			length:      0,
			isSigned:    true,
		},
		{
			name:        "little-endian-unsigned-overflow",
			expectedNum: -2147483648,
			inputNumStr: "2147483648",
			byteOrder:   "LittleEndian",
			length:      0,
			isSigned:    false,
		},

		// Big Endian tests
		{
			name:        "big-endian-zero",
			expectedNum: 0,
			inputNumStr: "0",
			byteOrder:   "BigEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "big-endian-positive",
			expectedNum: 10,
			inputNumStr: "10",
			byteOrder:   "BigEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "big-endian-negative",
			expectedNum: -10,
			inputNumStr: "-10",
			byteOrder:   "BigEndian",
			length:      4,
			isSigned:    true,
		},
		{
			name:        "big-endian-max-int32",
			expectedNum: 2147483647,
			inputNumStr: "2147483647",
			byteOrder:   "BigEndian",
			length:      0,
			isSigned:    true,
		},
		{
			name:        "big-endian-min-int32",
			expectedNum: -2147483648,
			inputNumStr: "-2147483648",
			byteOrder:   "BigEndian",
			length:      0,
			isSigned:    true,
		},
		{
			name:        "big-endian-unsigned-overflow",
			expectedNum: -2147483648,
			inputNumStr: "2147483648",
			byteOrder:   "BigEndian",
			length:      0,
			isSigned:    false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Generate expected binary representation
			buf := new(bytes.Buffer)
			if testCase.byteOrder == "LittleEndian" {
				_ = binary.Write(buf, binary.LittleEndian, testCase.expectedNum)
			} else {
				_ = binary.Write(buf, binary.BigEndian, testCase.expectedNum)
			}
			expectedBinary := buf.String()

			// Call function under test
			actualBinary := StrIntToBinary(
				testCase.inputNumStr,
				testCase.byteOrder,
				testCase.length,
				testCase.isSigned,
			)

			// Compare results
			require.Equal(t, expectedBinary, actualBinary)
		})
	}
}
