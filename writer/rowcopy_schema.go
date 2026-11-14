package writer

import (
	"reflect"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// schemaCopier copies only the parts of a row that marshal.Marshal reads for its schema,
// following the same paths, so values the schema never writes are neither walked nor copied.
type schemaCopier struct {
	sh       *schema.SchemaHandler
	plans    map[schemaPlanKey]copier
	building map[schemaPlanKey]*copier
}

type schemaPlanKey struct {
	t  reflect.Type
	pm *schema.PathMapType
}

func newSchemaCopier(sh *schema.SchemaHandler) *schemaCopier {
	return &schemaCopier{sh: sh, plans: map[schemaPlanKey]copier{}, building: map[schemaPlanKey]*copier{}}
}

func (sc *schemaCopier) copyRow(src any) any {
	if src == nil {
		return nil
	}
	v := reflect.ValueOf(src)
	c := sc.plan(v.Type(), sc.sh.PathMap)
	if c == nil {
		return src
	}
	out := reflect.New(v.Type()).Elem()
	c(out, v)
	return out.Interface()
}

// plan returns the copier for a value of type t written at pm; nil means it needs no copy.
func (sc *schemaCopier) plan(t reflect.Type, pm *schema.PathMapType) copier {
	key := schemaPlanKey{t, pm}
	if c, ok := sc.plans[key]; ok {
		return c
	}
	if c, ok := sc.building[key]; ok {
		// A recursive type reaches itself through a pointer, slice or map, so its plan is never nil.
		return func(dst, src reflect.Value) { (*c)(dst, src) }
	}
	c := new(copier)
	sc.building[key] = c
	*c = sc.build(t, pm)
	delete(sc.building, key)
	sc.plans[key] = *c
	return *c
}

func (sc *schemaCopier) build(t reflect.Type, pm *schema.PathMapType) copier {
	se := sc.element(pm)
	// Leaves and VARIANT columns are written whole, so everything below them is copied. The
	// root is a group even without columns, as in marshal, so it never counts as a leaf.
	leaf := len(pm.Children) == 0 && pm != sc.sh.PathMap
	if se == nil || leaf || (se.LogicalType != nil && se.LogicalType.VARIANT != nil) {
		return copierFor(t)
	}
	switch t.Kind() {
	case reflect.Pointer:
		return pointerCopier(t, sc.plan(t.Elem(), pm))
	case reflect.Interface:
		return sc.interfaceCopier(pm)
	case reflect.Struct:
		return sc.structCopier(t, pm)
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return copierFor(t)
		}
		// As in marshal.ParquetSlice: a LIST holds its values under List.Element, a
		// REPEATED field at its own path.
		elemPM := pm
		if se.GetRepetitionType() != parquet.FieldRepetitionType_REPEATED {
			list, ok := pm.Children["List"]
			if !ok || list.Children["Element"] == nil {
				return nil
			}
			elemPM = list.Children["Element"]
		}
		return sliceCopier(t, sc.plan(t.Elem(), elemPM))
	case reflect.Map:
		if !se.IsSetConvertedType() {
			return sc.mapStructCopier(t, pm)
		}
		kv, ok := pm.Children["Key_value"]
		if !ok || kv.Children["Key"] == nil || kv.Children["Value"] == nil {
			return copierFor(t)
		}
		return mapCopier(t, sc.plan(t.Key(), kv.Children["Key"]), sc.plan(t.Elem(), kv.Children["Value"]))
	}
	return copierFor(t)
}

func (sc *schemaCopier) element(pm *schema.PathMapType) *parquet.SchemaElement {
	idx, ok := sc.sh.MapIndex[pm.Path]
	if !ok || int(idx) >= len(sc.sh.SchemaElements) {
		return nil
	}
	return sc.sh.SchemaElements[idx]
}

func (sc *schemaCopier) interfaceCopier(pm *schema.PathMapType) copier {
	return func(dst, src reflect.Value) {
		if src.IsNil() {
			dst.SetZero()
			return
		}
		inner := sc.plan(src.Elem().Type(), pm)
		if inner == nil {
			dst.Set(src)
			return
		}
		held := reflect.New(src.Elem().Type()).Elem()
		inner(held, src.Elem())
		dst.Set(held)
	}
}

// structCopier copies the fields the schema has, matched by Go name as marshal.ParquetStruct
// does; the rest keep the caller's values, which are never read.
func (sc *schemaCopier) structCopier(t reflect.Type, pm *schema.PathMapType) copier {
	type field struct {
		index int
		copy  copier
	}
	var fields []field
	for i := range t.NumField() {
		f := t.Field(i)
		if child, ok := pm.Children[f.Name]; ok && f.IsExported() {
			if c := sc.plan(f.Type, child); c != nil {
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

// mapStructCopier copies a map standing in for a struct: only keys the schema has are
// written, as marshal.ParquetMapStruct does, so only those are kept.
func (sc *schemaCopier) mapStructCopier(t reflect.Type, pm *schema.PathMapType) copier {
	return func(dst, src reflect.Value) {
		if src.IsNil() {
			dst.SetZero()
			return
		}
		out := reflect.MakeMapWithSize(t, len(pm.Children))
		iter := src.MapRange()
		for iter.Next() {
			child, ok := pm.Children[iter.Key().String()]
			if !ok {
				continue
			}
			v := iter.Value()
			if c := sc.plan(t.Elem(), child); c != nil {
				held := reflect.New(t.Elem()).Elem()
				c(held, v)
				v = held
			}
			out.SetMapIndex(iter.Key(), v)
		}
		dst.Set(out)
	}
}

// copyRow copies src for buffering, along the schema when marshal.Marshal will write it.
func (pw *ParquetWriter) copyRow(src any) any {
	if !pw.copyBySchema || pw.SchemaHandler == nil || pw.SchemaHandler.PathMap == nil {
		return copyRow(src)
	}
	// The schema handler is exported and may be replaced, so plans follow the current one.
	if pw.rowCopier == nil || pw.rowCopier.sh != pw.SchemaHandler {
		pw.rowCopier = newSchemaCopier(pw.SchemaHandler)
	}
	return pw.rowCopier.copyRow(src)
}
