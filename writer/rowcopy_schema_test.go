package writer

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/schema"
)

func TestSchemaCopier(t *testing.T) {
	newCopier := func(t *testing.T, obj any) *schemaCopier {
		sh, err := schema.NewSchemaHandlerFromStruct(obj)
		require.NoError(t, err)
		return newSchemaCopier(sh)
	}

	t.Run("repeated-slice", func(t *testing.T) {
		type row struct {
			Vals [][]byte `parquet:"name=vals, type=BYTE_ARRAY, repetitiontype=REPEATED"`
		}
		src := row{Vals: [][]byte{[]byte("a")}}
		got := newCopier(t, new(row)).copyRow(src).(row)
		src.Vals[0][0] = 'x'
		require.Equal(t, row{Vals: [][]byte{[]byte("a")}}, got)
	})

	t.Run("untagged-fields-are-shared", func(t *testing.T) {
		type row struct {
			ID    int64 `parquet:"name=id, type=INT64"`
			Extra []byte
		}
		src := row{ID: 1, Extra: []byte("a")}
		got := newCopier(t, new(row)).copyRow(src).(row)
		src.Extra[0] = 'x'
		require.Equal(t, "x", string(got.Extra))
	})

	t.Run("recursive-type-outside-schema", func(t *testing.T) {
		type node struct {
			ID   int64 `parquet:"name=id, type=INT64"`
			Next *node
		}
		src := node{ID: 1}
		src.Next = &src
		got := newCopier(t, new(node)).copyRow(src).(node)
		require.Equal(t, int64(1), got.ID)
		require.Same(t, &src, got.Next)
	})

	t.Run("map-row-drops-keys-outside-schema", func(t *testing.T) {
		type row struct {
			Name []byte `parquet:"name=name, type=BYTE_ARRAY"`
		}
		name := []byte("a")
		src := map[string]any{"Name": name, "Other": []byte("b")}
		got := newCopier(t, new(row)).copyRow(src).(map[string]any)
		name[0] = 'x'
		require.Equal(t, map[string]any{"Name": []byte("a")}, got)
	})

	t.Run("plain-row-unchanged", func(t *testing.T) {
		type row struct {
			ID   int64  `parquet:"name=id, type=INT64"`
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		}
		sc := newCopier(t, new(row))
		require.Nil(t, sc.plan(reflect.TypeFor[row](), sc.sh.PathMap))
		require.Nil(t, sc.copyRow(nil))
	})

	t.Run("interface-holding-a-group", func(t *testing.T) {
		type inner struct {
			V []byte `parquet:"name=v, type=BYTE_ARRAY"`
		}
		type flat struct {
			N int32 `parquet:"name=n, type=INT32"`
		}
		type row struct {
			Inner inner `parquet:"name=inner"`
			Flat  flat  `parquet:"name=flat"`
			Opt   *flat `parquet:"name=opt, repetitiontype=OPTIONAL"`
		}
		v := []byte("a")
		src := map[string]any{"Inner": inner{V: v}, "Flat": flat{N: 1}, "Opt": nil}
		got := newCopier(t, new(row)).copyRow(src).(map[string]any)
		v[0] = 'x'
		require.Equal(t, map[string]any{"Inner": inner{V: []byte("a")}, "Flat": flat{N: 1}, "Opt": nil}, got)
		require.Nil(t, newCopier(t, new(row)).copyRow(map[string]any(nil)))
	})

	t.Run("recursive-type-at-one-path", func(t *testing.T) {
		type item struct {
			V int32 `parquet:"name=v, type=INT32"`
		}
		type row struct {
			Items []item `parquet:"name=items, repetitiontype=REPEATED"`
		}
		// rec reaches itself through a pointer and a REPEATED slice, neither of which
		// moves down the schema, so planning it meets itself.
		type rec []*rec
		sc := newCopier(t, new(row))
		c := sc.plan(reflect.TypeFor[rec](), sc.sh.PathMap.Children["Items"])
		require.NotNil(t, c)
		src := rec{&rec{nil}}
		dst := reflect.New(reflect.TypeFor[rec]()).Elem()
		c(dst, reflect.ValueOf(src))
		got := dst.Interface().(rec)
		require.Equal(t, src, got)
		require.NotSame(t, src[0], got[0])
	})

	t.Run("shapes-outside-the-schema", func(t *testing.T) {
		type row struct {
			ID    int64            `parquet:"name=id, type=INT64"`
			Attrs map[string]int32 `parquet:"name=attrs, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
		}
		sc := newCopier(t, new(row))
		root := sc.sh.PathMap
		leaf := root.Children["ID"]
		noKeyValue := &schema.PathMapType{Path: root.Children["Attrs"].Path, Children: map[string]*schema.PathMapType{"X": leaf}}
		unknown := &schema.PathMapType{Path: "missing", Children: map[string]*schema.PathMapType{"X": leaf}}

		require.NotNil(t, sc.plan(reflect.TypeFor[[]byte](), root), "a []byte at a group is copied whole")
		require.Nil(t, sc.plan(reflect.TypeFor[[]int64](), root), "a slice without List.Element is not written")
		require.NotNil(t, sc.plan(reflect.TypeFor[map[string][]byte](), noKeyValue), "a MAP without Key_value is copied whole")
		require.NotNil(t, sc.plan(reflect.TypeFor[[1][]byte](), root), "an array is copied whole")
		require.NotNil(t, sc.plan(reflect.TypeFor[[]byte](), unknown), "a path outside the handler is copied whole")
	})

	t.Run("follows-replaced-schema-handler", func(t *testing.T) {
		type row struct {
			ID int64 `parquet:"name=id, type=INT64"`
		}
		pw, _, err := createTestParquetWriter(new(row), WithNP(1))
		require.NoError(t, err)
		_ = pw.copyRow(row{ID: 1})
		first := pw.rowCopier
		require.NoError(t, pw.SetSchemaHandlerFromJSON(`{"Tag": "name=parquet_go_root", "Fields": [{"Tag": "name=id, inname=ID, type=INT64"}]}`))
		_ = pw.copyRow(row{ID: 1})
		require.NotSame(t, first, pw.rowCopier)
		require.Same(t, pw.SchemaHandler, pw.rowCopier.sh)
	})
}
