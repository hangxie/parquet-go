package marshal

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/schema"
)

// columnName keeps only the characters a column name contributes to a Go field name, so the
// fuzzer explores how names relate to each other -- a root that prefixes a field, a field named
// after a LIST group member -- instead of names the schema layer would reject outright.
func columnName(str string) string {
	var name strings.Builder
	for _, r := range str {
		if r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			name.WriteRune(r)
		}
	}
	if name.Len() > 32 {
		return name.String()[:32]
	}
	return name.String()
}

// FuzzConvertToJSONFriendly checks that a DOUBLE column is converted no matter what the root and
// the column are named or how the column repeats: NaN comes back quoted, and the whole result is
// JSON encodable. Conversion is driven by schema path lookups, which names can perturb.
func FuzzConvertToJSONFriendly(f *testing.F) {
	f.Add("root", "scores", uint8(0))
	f.Add("A", "Amounts", uint8(0)) // root name is a prefix of the field name
	f.Add("A", "Amounts", uint8(1))
	f.Add("A", "A", uint8(2)) // field shares the root's name
	f.Add("root", "List", uint8(0))
	f.Add("root", "Element", uint8(1))
	f.Add("Parquet_go_root", "value", uint8(2))
	f.Add("1", "_", uint8(0))

	f.Fuzz(func(t *testing.T, rootName, fieldName string, shape uint8) {
		rootName, fieldName = columnName(rootName), columnName(fieldName)
		if rootName == "" || fieldName == "" {
			return
		}

		var fieldJSON string
		var fieldType reflect.Type
		var expected any
		switch shape % 3 {
		case 0: // legacy REPEATED column: it addresses its own elements
			fieldJSON = fmt.Sprintf(`{"Tag":"name=%s, type=DOUBLE, repetitiontype=REPEATED"}`, fieldName)
			fieldType, expected = reflect.TypeOf([]float64(nil)), []any{"NaN"}
		case 1: // three-level LIST: elements live under List/Element
			fieldJSON = fmt.Sprintf(`{"Tag":"name=%s, type=LIST","Fields":[{"Tag":"name=element, type=DOUBLE"}]}`, fieldName)
			fieldType, expected = reflect.TypeOf([]float64(nil)), []any{"NaN"}
		default: // plain scalar column
			fieldJSON = fmt.Sprintf(`{"Tag":"name=%s, type=DOUBLE"}`, fieldName)
			fieldType, expected = reflect.TypeOf(float64(0)), "NaN"
		}

		schemaHandler, err := schema.NewSchemaHandlerFromJSON(
			fmt.Sprintf(`{"Tag":"name=%s","Fields":[%s]}`, rootName, fieldJSON),
		)
		require.NoError(t, err)

		// the reader builds a struct whose field names are the schema's in-names
		inName := schemaHandler.Infos[1].InName
		data := reflect.New(reflect.StructOf([]reflect.StructField{{Name: inName, Type: fieldType}})).Elem()
		if fieldType.Kind() == reflect.Slice {
			data.Field(0).Set(reflect.ValueOf([]float64{math.NaN()}))
		} else {
			data.Field(0).SetFloat(math.NaN())
		}

		result, err := ConvertToJSONFriendly(data.Interface(), schemaHandler)
		require.NoError(t, err)
		require.Equal(t, map[string]any{inName: expected}, result)

		_, err = json.Marshal(result)
		require.NoError(t, err)
	})
}
