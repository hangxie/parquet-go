package marshal

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

// mapKeyTracker rejects distinct property names that decode to the same Parquet key,
// such as reordered struct fields, which a reader would otherwise collapse into one entry.
type mapKeyTracker struct {
	schemaHandler *schema.SchemaHandler
	keyPath       *schema.PathMapType
	opts          []types.ValueOption
	seen          map[string]string
}

func (t *mapKeyTracker) add(name string, key any) error {
	var id strings.Builder
	if err := t.writeIdentity(&id, t.keyPath, key); err != nil {
		return err
	}
	if prev, ok := t.seen[id.String()]; ok {
		return fmt.Errorf("duplicate MAP key: %q and %q", min(prev, name), max(prev, name))
	}
	t.seen[id.String()] = name
	return nil
}

// writeIdentity writes the converted Parquet values of key, so only equal keys share an identity.
func (t *mapKeyTracker) writeIdentity(id *strings.Builder, pathMap *schema.PathMapType, key any) error {
	if key == nil {
		id.WriteString("null")
		return nil
	}
	idx := t.schemaHandler.MapIndex[pathMap.Path]
	se := t.schemaHandler.SchemaElements[idx]
	object, isObject := key.(map[string]any)
	switch {
	case !isGroup(t.schemaHandler, idx):
		val, err := types.JSONTypeToParquetTypeWithLogical(reflect.ValueOf(key), se.Type, se.ConvertedType, se.LogicalType, int(se.GetTypeLength()), int(se.GetScale()), t.opts...)
		if err != nil {
			return fmt.Errorf("convert JSON value for %s: %w", pathMap.Path, err)
		}
		fmt.Fprintf(id, "%#v", val)
	case isObject && !isListColumn(se) && !isMapColumn(se):
		// Field names match the way marshalJSONStruct matches them.
		fields := make(map[string]any, len(object))
		for name, value := range object {
			fields[common.StringToVariableName(name)] = value
		}
		names := make([]string, 0, len(pathMap.Children))
		for name := range pathMap.Children {
			names = append(names, name)
		}
		slices.Sort(names)
		id.WriteByte('{')
		for _, name := range names {
			fmt.Fprintf(id, "%q:", name)
			if err := t.writeIdentity(id, pathMap.Children[name], fields[name]); err != nil {
				return err
			}
			id.WriteByte(',')
		}
		id.WriteByte('}')
	default:
		// Lists, maps, and variants inside a key compare by their sorted JSON form.
		// Decoded JSON holds only encodable values, so Marshal cannot fail here.
		data, _ := json.Marshal(key)
		id.Write(data)
	}
	return nil
}
