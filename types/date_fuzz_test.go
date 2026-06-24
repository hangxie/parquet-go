package types

import "testing"

func FuzzParseDateString(f *testing.F) {
	f.Add("1970-01-01")
	f.Add("2006-01-02")
	f.Add("0001-01-01")
	f.Add("9999-12-31")
	f.Add("")
	f.Add("not-a-date")
	f.Add("2006-13-45")

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = ParseDateString(s)
	})
}
