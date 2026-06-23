package schema

import "testing"

func FuzzParseStructFieldTag(f *testing.F) {
	f.Add("")
	f.Add("name=myfield, type=INT32, repetitiontype=REQUIRED")
	f.Add("type=BYTE_ARRAY, convertedtype=UTF8")
	f.Add("type=INT64,bloomfilter=true")
	f.Add("foo=bar")
	f.Add("name=x")

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = parseStructFieldTag(s)
	})
}
