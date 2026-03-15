package common

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidatePageCRC(t *testing.T) {
	data := []byte("test page data for CRC validation")
	validCRC := int32(crc32.ChecksumIEEE(data))
	invalidCRC := validCRC + 1

	testCases := map[string]struct {
		data        []byte
		hasCRC      bool
		expectedCRC int32
		mode        CRCMode
		errMsg      string
	}{
		"ignore-crc-absent":  {data, false, 0, CRCIgnore, ""},
		"ignore-crc-valid":   {data, true, validCRC, CRCIgnore, ""},
		"ignore-crc-invalid": {data, true, invalidCRC, CRCIgnore, ""},

		"auto-crc-absent":  {data, false, 0, CRCAuto, ""},
		"auto-crc-valid":   {data, true, validCRC, CRCAuto, ""},
		"auto-crc-invalid": {data, true, invalidCRC, CRCAuto, "CRC mismatch"},

		"strict-crc-absent":  {data, false, 0, CRCStrict, "CRC required but not present"},
		"strict-crc-valid":   {data, true, validCRC, CRCStrict, ""},
		"strict-crc-invalid": {data, true, invalidCRC, CRCStrict, "CRC mismatch"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := ValidatePageCRC(tc.hasCRC, tc.expectedCRC, tc.mode, tc.data)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.errMsg)
			}
		})
	}
}

func TestComputePageCRC(t *testing.T) {
	testCases := map[string]struct {
		data     [][]byte
		expected uint32
	}{
		"single-slice": {
			data:     [][]byte{[]byte("hello")},
			expected: 0x3610A686,
		},
		"multiple-slices": {
			data:     [][]byte{[]byte("hel"), []byte("lo")},
			expected: 0x3610A686,
		},
		"empty": {
			data:     [][]byte{},
			expected: 0x00000000,
		},
		"nil-slice": {
			data:     [][]byte{nil, []byte("test")},
			expected: 0xD87F7E0C,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := ComputePageCRC(tc.data...)
			require.Equal(t, tc.expected, result)
		})
	}
}
