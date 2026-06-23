package types

import "testing"

func FuzzConvertBSONLogicalValue(f *testing.F) {
	// minimal valid BSON document: 4-byte little-endian length + 0x00 terminator
	f.Add([]byte{})
	f.Add([]byte{0x05, 0x00, 0x00, 0x00, 0x00}) // empty document
	f.Add([]byte{0x00})                         // too short
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00}) // length overflow

	f.Fuzz(func(t *testing.T, b []byte) {
		ConvertBSONLogicalValue(b)
	})
}
