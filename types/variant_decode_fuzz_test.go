package types

import "testing"

func FuzzConvertVariantValue(f *testing.F) {
	emptyMeta := []byte{0x01, 0x00, 0x00} // version=1, empty dictionary

	f.Add([]byte{}, []byte{})
	f.Add(emptyMeta, []byte{0x00})                                                 // null primitive
	f.Add(emptyMeta, []byte{0x04})                                                 // true
	f.Add(emptyMeta, []byte{0x08})                                                 // false
	f.Add(emptyMeta, []byte{0x14, 0x01, 0x00, 0x00, 0x00})                         // int32=1
	f.Add(emptyMeta, []byte{0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}) // int64=max
	f.Add(emptyMeta, []byte{0x09, 'h', 'i'})                                       // short string "hi"
	f.Add(emptyMeta, []byte{0x14})                                                 // truncated int32

	f.Fuzz(func(t *testing.T, metadata, value []byte) {
		_, _ = ConvertVariantValue(Variant{Metadata: metadata, Value: value})
	})
}
