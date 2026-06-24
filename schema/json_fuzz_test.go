package schema

import "testing"

func FuzzNewSchemaHandlerFromJSON(f *testing.F) {
	f.Add("")
	f.Add("{}")
	f.Add(`{"Tag":"name=root, repetitiontype=REQUIRED"}`)
	f.Add(`{"Tag":"name=root","Fields":[{"Tag":"name=id, type=INT64"}]}`)
	f.Add(`{"Tag":"name=root","Fields":[{"Tag":"name=l, type=LIST","Fields":[{"Tag":"name=element, type=BYTE_ARRAY"}]}]}`)
	f.Add(`{"Tag":"name=root","Fields":[{"Tag":"name=m, type=MAP"}]}`)
	f.Add(`{"Tag":"type=INT32"`)

	f.Fuzz(func(t *testing.T, str string) {
		_, _ = NewSchemaHandlerFromJSON(str)
	})
}
