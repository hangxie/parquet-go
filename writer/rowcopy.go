package writer

import (
	"bytes"
	"reflect"
	"sync"
)

// copier sets dst, a settable value of src's type, to a copy of src that shares no
// memory the caller could change.
type copier func(dst, src reflect.Value)

// copiers caches the plan for each row type; a nil plan means the type needs no copy.
var copiers sync.Map

// copyRow returns src with every part the caller can still change copied, so it can be buffered.
func copyRow(src any) any {
	if src == nil {
		return nil
	}
	// Rows are trees, as marshaling them requires; a value that refers back to itself
	// recurses without end here just as it would there.
	v := reflect.ValueOf(src)
	c := copierFor(v.Type())
	if c == nil {
		return src
	}
	out := reflect.New(v.Type()).Elem()
	c(out, v)
	return out.Interface()
}

func copierFor(t reflect.Type) copier {
	if c, ok := copiers.Load(t); ok {
		return c.(copier)
	}
	c := buildCopier(t, map[reflect.Type]*copier{})
	copiers.Store(t, c)
	return c
}

// buildCopier plans the copy of t once, so copying a row does no per-value type checks.
func buildCopier(t reflect.Type, building map[reflect.Type]*copier) copier {
	if c, ok := building[t]; ok {
		// A recursive type reaches itself through a pointer, slice or map, so its plan is never nil.
		return func(dst, src reflect.Value) { (*c)(dst, src) }
	}
	c := new(copier)
	building[t] = c
	switch t.Kind() {
	case reflect.Pointer:
		*c = pointerCopier(t, buildCopier(t.Elem(), building))
	case reflect.Interface:
		*c = interfaceCopier
	case reflect.Slice:
		*c = sliceCopier(t, buildCopier(t.Elem(), building))
	case reflect.Map:
		*c = mapCopier(t, buildCopier(t.Key(), building), buildCopier(t.Elem(), building))
	case reflect.Array:
		if elem := buildCopier(t.Elem(), building); elem != nil {
			*c = func(dst, src reflect.Value) {
				dst.Set(src)
				for i := range src.Len() {
					elem(dst.Index(i), src.Index(i))
				}
			}
		}
	case reflect.Struct:
		*c = structCopier(t, building)
	}
	delete(building, t)
	return *c
}

func pointerCopier(t reflect.Type, elem copier) copier {
	return func(dst, src reflect.Value) {
		if src.IsNil() {
			dst.SetZero()
			return
		}
		p := reflect.New(t.Elem())
		if elem == nil {
			p.Elem().Set(src.Elem())
		} else {
			elem(p.Elem(), src.Elem())
		}
		dst.Set(p)
	}
}

func interfaceCopier(dst, src reflect.Value) {
	if src.IsNil() {
		dst.SetZero()
		return
	}
	// A value held directly in an interface is already the caller's own copy.
	inner := copierFor(src.Elem().Type())
	if inner == nil {
		dst.Set(src)
		return
	}
	held := reflect.New(src.Elem().Type()).Elem()
	inner(held, src.Elem())
	dst.Set(held)
}

func sliceCopier(t reflect.Type, elem copier) copier {
	if elem == nil && t.Elem().Kind() == reflect.Uint8 {
		return func(dst, src reflect.Value) {
			if src.IsNil() {
				dst.SetZero()
				return
			}
			dst.SetBytes(bytes.Clone(src.Bytes()))
		}
	}
	return func(dst, src reflect.Value) {
		// dst may still hold the caller's slice, so it is cleared before growing.
		dst.SetZero()
		if src.IsNil() {
			return
		}
		if src.Len() == 0 {
			// Growing by nothing leaves dst nil, and an empty slice must stay distinct from nil.
			dst.Set(reflect.MakeSlice(t, 0, 0))
			return
		}
		dst.Grow(src.Len())
		dst.SetLen(src.Len())
		if elem == nil {
			reflect.Copy(dst, src)
			return
		}
		for i := range src.Len() {
			elem(dst.Index(i), src.Index(i))
		}
	}
}

func mapCopier(t reflect.Type, key, elem copier) copier {
	return func(dst, src reflect.Value) {
		if src.IsNil() {
			dst.SetZero()
			return
		}
		out := reflect.MakeMapWithSize(t, src.Len())
		// SetMapIndex copies each entry in, so one pair of holders serves the whole map.
		k, v := reflect.New(t.Key()).Elem(), reflect.New(t.Elem()).Elem()
		kc, vc := k, v
		if key != nil {
			kc = reflect.New(t.Key()).Elem()
		}
		if elem != nil {
			vc = reflect.New(t.Elem()).Elem()
		}
		var iter reflect.MapIter
		iter.Reset(src)
		for iter.Next() {
			k.SetIterKey(&iter)
			v.SetIterValue(&iter)
			if key != nil {
				key(kc, k)
			}
			if elem != nil {
				elem(vc, v)
			}
			out.SetMapIndex(kc, vc)
		}
		dst.Set(out)
	}
}

func structCopier(t reflect.Type, building map[reflect.Type]*copier) copier {
	type field struct {
		index int
		copy  copier
	}
	var fields []field
	for i := range t.NumField() {
		// Unexported fields are not written, so they keep the caller's values.
		if f := t.Field(i); f.IsExported() {
			if c := buildCopier(f.Type, building); c != nil {
				fields = append(fields, field{i, c})
			}
		}
	}
	if len(fields) == 0 {
		return nil
	}
	return func(dst, src reflect.Value) {
		dst.Set(src)
		for _, f := range fields {
			f.copy(dst.Field(f.index), src.Field(f.index))
		}
	}
}
