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

func FuzzParseINT96String(f *testing.F) {
	f.Add("")
	f.Add("2006-01-02T15:04:05Z")
	f.Add("2006-01-02T15:04:05.999999999Z")
	f.Add("1970-01-01T00:00:00+00:00")
	f.Add("not-a-timestamp")

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = ParseINT96String(s)
	})
}
