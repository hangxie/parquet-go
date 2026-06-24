package schema

import (
	"strings"
	"testing"
)

func FuzzNewSchemaHandlerFromMetadata(f *testing.F) {
	f.Add("name=id, type=INT64")
	f.Add("name=a, type=INT32\nname=b, type=BYTE_ARRAY, convertedtype=UTF8")
	f.Add("")
	f.Add("type=INT32")
	f.Add("garbage")

	f.Fuzz(func(t *testing.T, joined string) {
		// each line is treated as one column's metadata tag
		mds := strings.Split(joined, "\n")
		_, _ = NewSchemaHandlerFromMetadata(mds)
	})
}
