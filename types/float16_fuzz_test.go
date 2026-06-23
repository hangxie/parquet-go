package types

import "testing"

func FuzzConvertFloat16LogicalValue(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x00, 0x00}) // +0.0
	f.Add([]byte{0x00, 0x80}) // -0.0
	f.Add([]byte{0x00, 0x7C}) // +Inf
	f.Add([]byte{0x00, 0xFC}) // -Inf
	f.Add([]byte{0x01, 0x7C}) // NaN
	f.Add([]byte{0xFF, 0x7B}) // max normal

	f.Fuzz(func(t *testing.T, b []byte) {
		ConvertFloat16LogicalValue(b)
	})
}
