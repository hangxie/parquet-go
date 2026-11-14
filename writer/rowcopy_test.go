package writer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type rowcopyNamedBytes []byte

type rowcopyNode struct {
	V    []byte
	Next *rowcopyNode
}

type rowcopyMixed struct {
	Exported []byte
	hidden   []byte
}

func TestCopyRow(t *testing.T) {
	type flat struct {
		A int32
		B string
		C [2]float64
	}
	type nested struct {
		Bytes  []byte
		Named  rowcopyNamedBytes
		Ptr    *int64
		List   []string
		Lists  [][]byte
		Map    map[string][]byte
		Array  [2][]byte
		Any    any
		Nested struct{ S []byte }
	}
	newNested := func() nested {
		n := int64(1)
		return nested{
			Bytes: []byte("a"), Named: rowcopyNamedBytes("b"), Ptr: &n, List: []string{"c"},
			Lists: [][]byte{[]byte("d")}, Map: map[string][]byte{"k": []byte("e")},
			Array: [2][]byte{[]byte("f"), nil}, Any: []byte("g"), Nested: struct{ S []byte }{[]byte("h")},
		}
	}
	// mutate changes every byte the caller still shares with a row, in place.
	mutate := func(n *nested) {
		n.Bytes[0], n.Named[0], *n.Ptr, n.List[0], n.Lists[0][0] = 'x', 'x', 9, "x", 'x'
		n.Map["k"][0], n.Array[0][0], n.Any.([]byte)[0], n.Nested.S[0] = 'x', 'x', 'x', 'x'
	}

	t.Run("deep", func(t *testing.T) {
		src := newNested()
		got := copyRow(src).(nested)
		mutate(&src)
		require.Equal(t, newNested(), got)
	})

	t.Run("nil-stays-nil", func(t *testing.T) {
		require.Nil(t, copyRow(nil))
		var nilBytes []byte
		got := copyRow(nested{Bytes: nilBytes}).(nested)
		require.Nil(t, got.Bytes, "a nil []byte must not become an empty one")
		require.Nil(t, got.Ptr)
		require.Nil(t, got.Map)
		require.Nil(t, got.Any)
		require.NotNil(t, copyRow(nested{Bytes: []byte{}}).(nested).Bytes)
	})

	t.Run("empty-stays-non-nil", func(t *testing.T) {
		require.NotNil(t, copyRow([]int64{}))
		require.NotNil(t, copyRow([]string{}))
		require.NotNil(t, copyRow([]any{}))
		require.NotNil(t, copyRow(rowcopyNamedBytes{}))
		nested := copyRow([][]int64{{}, nil}).([][]int64)
		require.NotNil(t, nested[0])
		require.Nil(t, nested[1])
		require.NotNil(t, copyRow(map[string][]int64{"k": {}}).(map[string][]int64)["k"])
		require.NotNil(t, copyRow(struct{ L []string }{L: []string{}}).(struct{ L []string }).L)
		var held any = []any{}
		require.NotNil(t, copyRow(struct{ V any }{V: held}).(struct{ V any }).V.([]any))
	})

	t.Run("flat-unchanged-without-allocating", func(t *testing.T) {
		var src any = flat{A: 1, B: "b", C: [2]float64{1, 2}}
		require.Equal(t, src, copyRow(src))
		require.Zero(t, testing.AllocsPerRun(100, func() { _ = copyRow(src) }))
	})

	t.Run("unexported-fields-are-shared", func(t *testing.T) {
		src := rowcopyMixed{Exported: []byte("a"), hidden: []byte("b")}
		got := copyRow(src).(rowcopyMixed)
		src.Exported[0], src.hidden[0] = 'x', 'x'
		require.Equal(t, []byte("a"), got.Exported)
		require.Equal(t, []byte("x"), got.hidden)
	})

	t.Run("recursive-type", func(t *testing.T) {
		src := &rowcopyNode{V: []byte("a"), Next: &rowcopyNode{V: []byte("b")}}
		got := copyRow(src).(*rowcopyNode)
		src.V[0], src.Next.V[0] = 'x', 'x'
		require.Equal(t, &rowcopyNode{V: []byte("a"), Next: &rowcopyNode{V: []byte("b")}}, got)
	})

	t.Run("map-keys-with-references", func(t *testing.T) {
		key := int64(1)
		src := map[*int64]string{&key: "a"}
		got := copyRow(src).(map[*int64]string)
		key = 2
		require.Len(t, got, 1)
		for k, v := range got {
			require.Equal(t, int64(1), *k)
			require.Equal(t, "a", v)
		}
	})

	t.Run("csv-row", func(t *testing.T) {
		value := []byte("a")
		src := []any{value, int32(1), nil}
		got := copyRow(src).([]any)
		value[0], src[1] = 'x', int32(2)
		require.Equal(t, []any{[]byte("a"), int32(1), nil}, got)
	})
}

func BenchmarkCopyRow(b *testing.B) {
	type references struct {
		ID      int64
		Payload []byte
		Tags    []string
		Attrs   map[string]int32
		Opt     *int32
	}
	v := int32(1)
	cases := map[string]any{
		"flat":       struct{ A, B int64 }{1, 2},
		"references": references{ID: 1, Payload: make([]byte, 256), Tags: []string{"a", "b"}, Attrs: map[string]int32{"k": 1}, Opt: &v},
	}
	for name, row := range cases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = copyRow(row)
			}
		})
	}
}
