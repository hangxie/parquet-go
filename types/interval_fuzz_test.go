package types

import "testing"

func FuzzParseIntervalString(f *testing.F) {
	f.Add("")
	f.Add("1 mon")
	f.Add("2 day")
	f.Add("3.500 sec")
	f.Add("1 mon 2 day 3.500 sec")
	f.Add("invalid input")
	f.Add("0 day 0 day")

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = ParseIntervalString(s)
	})
}
