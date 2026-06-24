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

func FuzzParseFloat16String(f *testing.F) {
	f.Add("0")
	f.Add("-0")
	f.Add("1.5")
	f.Add("-65504")
	f.Add("3.14159e10")
	f.Add("inf")
	f.Add("nan")
	f.Add("")
	f.Add("notanumber")

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = ParseFloat16String(s)
	})
}
