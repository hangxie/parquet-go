package marshal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func TestMarshalJSONDuplicateMapKeys(t *testing.T) {
	const mapSchema = `{"Tag":"name=root","Fields":[{"Tag":"name=entries, type=MAP","Fields":[%s,{"Tag":"name=value, type=INT32"}]}]}`
	const pointKey = `{"Tag":"name=key","Fields":[{"Tag":"name=x, type=INT64"},{"Tag":"name=y, type=INT64, repetitiontype=OPTIONAL"}]}`
	const listFieldKey = `{"Tag":"name=key","Fields":[{"Tag":"name=x, type=INT64"},{"Tag":"name=tags, type=LIST","Fields":[{"Tag":"name=element, type=INT64"}]}]}`
	const doubleFieldKey = `{"Tag":"name=key","Fields":[{"Tag":"name=x, type=DOUBLE"}]}`
	for _, mode := range []types.ValueMode{types.ValueModeInterpreted, types.ValueModeRaw} {
		for _, tc := range []struct {
			name, key, a, b string
			duplicate       bool
		}{
			{"reordered struct fields", pointKey, `{"x":1,"y":2}`, `{"y":2,"x":1}`, true},
			{"swapped struct values", pointKey, `{"x":1,"y":2}`, `{"x":2,"y":1}`, false},
			{"omitted and null field", pointKey, `{"x":1}`, `{"x":1,"y":null}`, true},
			{"omitted and present field", pointKey, `{"x":1}`, `{"x":1,"y":0}`, false},
			{"struct number spelling", doubleFieldKey, `{"x":1}`, `{"x":1.0}`, true},
			{"reordered list field", listFieldKey, `{"x":1,"tags":[1,2]}`, `{"tags":[1,2],"x":1}`, true},
			{"distinct list field", listFieldKey, `{"x":1,"tags":[1,2]}`, `{"x":1,"tags":[2,1]}`, false},
			{"float signed zero", `{"Tag":"name=key, type=FLOAT"}`, "0", "-0", true},
			{"double signed zero", `{"Tag":"name=key, type=DOUBLE"}`, "0", "-0", true},
			{"struct float signed zero", `{"Tag":"name=key","Fields":[{"Tag":"name=x, type=FLOAT"}]}`, `{"x":0}`, `{"x":-0}`, true},
			{"struct double signed zero", doubleFieldKey, `{"x":0}`, `{"x":-0}`, true},
			{"double spelling", `{"Tag":"name=key, type=DOUBLE"}`, "1", "1.0", true},
			{"distinct doubles", `{"Tag":"name=key, type=DOUBLE"}`, "1", "1.5", false},
			{"string spelling", `{"Tag":"name=key, type=BYTE_ARRAY, convertedtype=UTF8"}`, "1", "1.0", false},
		} {
			t.Run(fmt.Sprintf("%v/%s", mode, tc.name), func(t *testing.T) {
				sch, err := schema.NewSchemaHandlerFromJSON(fmt.Sprintf(mapSchema, tc.key))
				require.NoError(t, err)
				row := fmt.Sprintf(`{"entries":{%q:1,%q:2}}`, tc.a, tc.b)
				_, err = MarshalJSON([]any{row}, sch, types.WithValueMode(mode))
				if !tc.duplicate {
					require.NoError(t, err)
					return
				}
				require.ErrorContains(t, err, fmt.Sprintf("duplicate MAP key: %q and %q", min(tc.a, tc.b), max(tc.a, tc.b)))
			})
		}
	}
}

func TestMarshalJSONInvalidMapKeyField(t *testing.T) {
	sch, err := schema.NewSchemaHandlerFromJSON(`{"Tag":"name=root","Fields":[{"Tag":"name=entries, type=MAP","Fields":[{"Tag":"name=key","Fields":[{"Tag":"name=x, type=INT64"}]},{"Tag":"name=value, type=INT32"}]}]}`)
	require.NoError(t, err)
	_, err = MarshalJSON([]any{`{"entries":{"{\"x\":1}":1,"{\"x\":true}":2}}`}, sch)
	require.ErrorContains(t, err, "convert JSON value for "+common.PathToStr([]string{"Root", "Entries", "Key_value", "Key", "X"}))
}
