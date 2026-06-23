package types

import "testing"

func FuzzINT96ToTime(f *testing.F) {
	f.Add("")
	f.Add("tooshort")
	f.Add("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00") // 12 zero bytes
	f.Add("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff") // 12 max bytes

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = INT96ToTime(s)
	})
}
