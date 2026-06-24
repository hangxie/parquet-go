package types

import "testing"

func FuzzParseTimeString(f *testing.F) {
	f.Add("00:00:00")
	f.Add("23:59:59")
	f.Add("12:34:56.123456789")
	f.Add("12:34:56.000000000")
	f.Add("")
	f.Add("25:61:61")
	f.Add("not-a-time")

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = ParseTimeString(s)
	})
}
