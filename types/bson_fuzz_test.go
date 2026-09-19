package types

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// bsonFuzzSeeds are documents worth mutating: the rich one reaches types random bytes
// almost never assemble, and the rest are shapes the format handles badly.
func bsonFuzzSeeds() [][]byte {
	m := func(d bson.D) []byte {
		raw, err := bson.Marshal(d)
		if err != nil {
			panic(err)
		}
		return raw
	}
	return [][]byte{
		{},                             // no document at all
		{0x05, 0x00, 0x00, 0x00, 0x00}, // empty document
		{0x00},                         // too short
		{0xFF, 0xFF, 0xFF, 0xFF, 0x00}, // length overflow
		mustRichBSON(),                 // every type the rendering has to carry
		m(bson.D{{Key: "x", Value: bson.D{{Key: "$numberInt", Value: "1"}}}}),      // wrapper-shaped
		m(bson.D{{Key: "a", Value: int32(1)}, {Key: "a", Value: int32(2)}}),        // duplicate keys
		{0x0d, 0x00, 0x00, 0x00, 0x0b, 'r', 0x00, 'a', 0x00, 'x', 'i', 0x00, 0x00}, // regex
		{0x0c, 0x00, 0x00, 0x00, 0x10, 0x82, 0x00, '0', '0', '0', '0', 0x00},       // bad key
		{0x0c, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00},   // 0x02 binary
		// The length shapes MarshalExtJSON trusts: one panics, one drops its tail.
		make([]byte, 16),
		{0x05, 0x00, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef},
	}
}

func FuzzConvertBSONLogicalValue(f *testing.F) {
	for _, seed := range bsonFuzzSeeds() {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		// Not the bytes back, which the format cannot promise: the invariant is that
		// the lossiness settles, so a non-idempotent driver fails here.
		first, ok := bsonRenderScan(t, b)
		if !ok {
			return
		}
		second, ok := bsonRenderScan(t, first)
		if !ok {
			return
		}
		if !bytes.Equal(first, second) {
			// After the comparison, not skipped before it, so well-formed 0x02
			// documents keep their coverage. TestBSONFormatLimits pins the exception.
			require.True(t, bsonHasOldBinary(first),
				"rendering is not idempotent: %x then %x", first, second)
		}
	})
}

// bsonRenderScan renders a document and scans it back, reporting whether both worked.
func bsonRenderScan(t *testing.T, raw []byte) ([]byte, bool) {
	t.Helper()
	rendered, err := convertBSONValue(raw)
	if err != nil {
		return nil, false
	}
	require.True(t, json.Valid([]byte(rendered.(string))), "rendered %q", rendered)
	back, err := strToBSON(rendered.(string), byteArrayType)
	if err != nil {
		return nil, false
	}
	return []byte(back.(string)), true
}

// bsonHasOldBinary reports whether the document carries a deprecated 0x02 binary, the one
// value whose encoding grows on every pass through the rendering.
func bsonHasOldBinary(raw []byte) bool {
	var doc bson.D
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return false
	}
	var walk func(any) bool
	walk = func(v any) bool {
		switch t := v.(type) {
		case bson.D:
			for _, e := range t {
				if walk(e.Value) {
					return true
				}
			}
		case bson.A:
			for _, e := range t {
				if walk(e) {
					return true
				}
			}
		case bson.CodeWithScope:
			// Scope is typed any, so it is the one nesting the walk has to be told
			// about; the same reason it earns a row in TestBSONFormatLimits.
			return walk(t.Scope)
		case bson.Binary:
			return t.Subtype == 0x02
		}
		return false
	}
	return walk(doc)
}

func FuzzStrToBSON(f *testing.F) {
	f.Add(`{}`)
	f.Add(`{"i":1}`)
	f.Add(`{"i":{"$numberInt":"1"}}`)
	f.Add(`{"a":{"$oid":"6112233445566778899aabbc"}}`)
	f.Add(`{"x":{"$numberDouble":"NaN"}}`)
	f.Add(`{"a":1} trailing`)
	f.Add("{\"\xb1\":0}") // a key the parser takes and the rendering cannot carry
	f.Add(`notjson`)
	f.Add(``)

	f.Fuzz(func(t *testing.T, s string) {
		raw, err := strToBSON(s, byteArrayType)
		if err != nil {
			return
		}
		// Scanned bytes render. Not that the rendering scans back to them: the parser
		// takes a raw byte in a key that the renderer then spells U+FFFD.
		rendered, err := convertBSONValue(raw)
		require.NoError(t, err)
		require.True(t, json.Valid([]byte(rendered.(string))), "rendered %q", rendered)
	})
}
