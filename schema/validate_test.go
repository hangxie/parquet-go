package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestSchemaHandler_ValidateUniqueNames(t *testing.T) {
	type nested struct {
		X int32 `parquet:"name=x, type=INT32"`
		Y int32 `parquet:"name=x, type=INT32"`
	}
	type child struct {
		V int32 `parquet:"name=v, type=INT32"`
	}
	jsonSchema := func(a, b string) string {
		return `{"Tag": "name=root", "Fields": [{"Tag": "name=` + a + `, type=INT32"}, {"Tag": "name=` + b + `, type=INT32"}]}`
	}

	testCases := map[string]struct {
		build  func() (*SchemaHandler, error)
		errMsg string
	}{
		"struct-sibling-duplicate": {
			build: func() (*SchemaHandler, error) {
				return NewSchemaHandlerFromStruct(new(struct {
					A int64  `parquet:"name=value, type=INT64"`
					B string `parquet:"name=value, type=BYTE_ARRAY"`
				}))
			},
			errMsg: `duplicate column name "value" under parquet_go_root`,
		},
		"struct-nested-duplicate": {
			build: func() (*SchemaHandler, error) {
				return NewSchemaHandlerFromStruct(new(struct {
					N nested `parquet:"name=n"`
				}))
			},
			errMsg: `duplicate column name "x" under parquet_go_root.n`,
		},
		"struct-same-leaf-under-different-parents": {
			build: func() (*SchemaHandler, error) {
				return NewSchemaHandlerFromStruct(new(struct {
					A child `parquet:"name=a"`
					B child `parquet:"name=b"`
				}))
			},
		},
		"json-sibling-duplicate": {
			build:  func() (*SchemaHandler, error) { return NewSchemaHandlerFromJSON(jsonSchema("value", "value")) },
			errMsg: `duplicate column name "value" under root`,
		},
		"json-internal-name-collision": {
			build:  func() (*SchemaHandler, error) { return NewSchemaHandlerFromJSON(jsonSchema("value", "Value")) },
			errMsg: `duplicate column name "Value" under root`,
		},
		"csv-sibling-duplicate": {
			build: func() (*SchemaHandler, error) {
				return NewSchemaHandlerFromMetadata([]string{"name=value, type=INT32", "name=value, type=INT64"})
			},
			errMsg: `duplicate column name "value" under parquet_go_root`,
		},
		"arrow-sibling-duplicate": {
			build: func() (*SchemaHandler, error) {
				return NewSchemaHandlerFromArrow(arrow.NewSchema([]arrow.Field{
					{Name: "value", Type: arrow.PrimitiveTypes.Int32},
					{Name: "value", Type: arrow.PrimitiveTypes.Int64},
				}, nil))
			},
			errMsg: `duplicate column name "value" under `,
		},
		"no-infos-falls-back-to-element-names": {
			build: func() (*SchemaHandler, error) {
				sh := &SchemaHandler{SchemaElements: []*parquet.SchemaElement{
					{Name: "root", NumChildren: common.ToPtr(int32(2))},
					{Name: "value"},
					{Name: "value"},
				}}
				return sh, sh.ValidateUniqueNames()
			},
			errMsg: `duplicate column name "value" under root`,
		},
		"nil-element": {
			build: func() (*SchemaHandler, error) {
				sh := &SchemaHandler{SchemaElements: []*parquet.SchemaElement{{Name: "root", NumChildren: common.ToPtr(int32(1))}, nil}}
				return sh, sh.ValidateUniqueNames()
			},
			errMsg: "schema element 1 is nil",
		},
		"schema-list-sibling-duplicate": {
			build: func() (*SchemaHandler, error) {
				sh := NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
					{Name: "root", NumChildren: common.ToPtr(int32(2))},
					{Name: "value", Type: common.ToPtr(parquet.Type_INT32)},
					{Name: "value", Type: common.ToPtr(parquet.Type_INT64)},
				})
				return sh, sh.ValidateUniqueNames()
			},
			errMsg: `duplicate column name "value" under root`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			_, err := tc.build()
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}
