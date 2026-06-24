package common

import "testing"

func FuzzStringToTag(f *testing.F) {
	f.Add("name=id, type=INT64")
	f.Add("name=v, type=BYTE_ARRAY, convertedtype=UTF8")
	f.Add("name=d, type=INT32, convertedtype=DECIMAL, scale=2, precision=10")
	f.Add("name=m, type=MAP, keytype=BYTE_ARRAY, valuetype=INT64")
	f.Add("type=INT64, bloomfilter=true")
	f.Add("name=x, length=16")
	f.Add("")
	f.Add("garbage")
	f.Add("key")
	f.Add("scale=notanumber")

	f.Fuzz(func(t *testing.T, tag string) {
		_, _ = StringToTag(tag)
	})
}
