package marshal

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func TestMarshalVariant(t *testing.T) {
	type MyStruct struct {
		Var any `parquet:"name=var, type=VARIANT, repetitiontype=OPTIONAL"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(MyStruct))
	require.NoError(t, err)

	data := []any{
		MyStruct{
			Var: &types.Variant{
				Metadata: types.EncodeVariantMetadata([]string{"a"}),
				Value:    types.EncodeVariantInt8(123),
			},
		},
		MyStruct{
			Var: &types.Variant{
				Metadata: types.EncodeVariantMetadata([]string{"b"}),
				Value:    types.EncodeVariantString("hello"),
			},
		},
		MyStruct{Var: nil}, // nil variant
	}

	res, err := Marshal(data, sh)
	require.NoError(t, err)
	require.NotNil(t, res)

	// Check if all tables are present
	require.Contains(t, *res, common.ParGoRootInName+common.ParGoPathDelimiter+"Var"+common.ParGoPathDelimiter+"Metadata")
	require.Contains(t, *res, common.ParGoRootInName+common.ParGoPathDelimiter+"Var"+common.ParGoPathDelimiter+"Value")

	metadataTable := (*res)[common.ParGoRootInName+common.ParGoPathDelimiter+"Var"+common.ParGoPathDelimiter+"Metadata"]
	valueTable := (*res)[common.ParGoRootInName+common.ParGoPathDelimiter+"Var"+common.ParGoPathDelimiter+"Value"]

	require.Equal(t, 3, len(metadataTable.Values))
	require.Equal(t, 3, len(valueTable.Values))

	// HandleVariant serves this path as well as the JSON one, and it appends the encoded
	// bytes to their tables directly, so the struct path states what those bytes are
	// rather than only how many rows arrived.
	for i, want := range []types.Variant{
		{Metadata: types.EncodeVariantMetadata([]string{"a"}), Value: types.EncodeVariantInt8(123)},
		{Metadata: types.EncodeVariantMetadata([]string{"b"}), Value: types.EncodeVariantString("hello")},
	} {
		require.Equal(t, string(want.Metadata), metadataTable.Values[i], "row %d metadata", i)
		require.Equal(t, string(want.Value), valueTable.Values[i], "row %d value", i)
	}

	// Third row should be nil
	require.Nil(t, metadataTable.Values[2])
	require.Nil(t, valueTable.Values[2])
}

func TestMarshalVariant_Error(t *testing.T) {
	t.Run("unsupported_type", func(t *testing.T) {
		type MyStruct struct {
			Var any `parquet:"name=var, type=VARIANT"`
		}

		sh, err := schema.NewSchemaHandlerFromStruct(new(MyStruct))
		require.NoError(t, err)

		// complex128 is not supported by AnyToVariant
		data := []any{MyStruct{Var: complex(1, 2)}}
		_, err = Marshal(data, sh)
		require.Error(t, err)
		require.Contains(t, err.Error(), "convert to variant")
	})

	t.Run("missing_pathmap_children", func(t *testing.T) {
		type MyStruct struct {
			Var any `parquet:"name=var, type=VARIANT"`
		}

		sh, err := schema.NewSchemaHandlerFromStruct(new(MyStruct))
		require.NoError(t, err)

		// Manually break PathMap for the variant field
		pathMap := sh.PathMap.Children["Var"]
		require.NotNil(t, pathMap)

		// Save original children and delete one
		origChildren := pathMap.Children
		pathMap.Children = make(map[string]*schema.PathMapType)
		for k, v := range origChildren {
			if k != "Value" { // missing "Value"
				pathMap.Children[k] = v
			}
		}

		data := []any{MyStruct{Var: types.Variant{
			Metadata: types.EncodeVariantMetadata([]string{"a"}),
			Value:    types.EncodeVariantInt8(123),
		}}}

		// Since we want to test HandleVariant directly or through Marshal
		// Marshal uses mapStruct.Marshal internally
		_, err = Marshal(data, sh)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing required children")
	})

	t.Run("missing_child_table", func(t *testing.T) {
		type MyStruct struct {
			Var any `parquet:"name=var, type=VARIANT"`
		}

		sh, err := schema.NewSchemaHandlerFromStruct(new(MyStruct))
		require.NoError(t, err)

		pathMap := sh.PathMap.Children["Var"]
		require.NotNil(t, pathMap)
		se := sh.SchemaElements[sh.MapIndex[pathMap.Path]]

		node := &Node{
			Val:     reflect.ValueOf(map[string]any{"a": int32(1)}),
			PathMap: pathMap,
		}
		// The variant children are appended straight to their tables, so a table map
		// that does not describe them is reported rather than dereferenced.
		for _, present := range []string{"", "Metadata"} {
			res := map[string]*layout.Table{}
			if present != "" {
				res[pathMap.Children[present].Path] = layout.NewEmptyTable()
			}
			_, handled, err := HandleVariant(node, se, res, sh, NewNodeBuf(1), nil)
			require.True(t, handled)
			require.ErrorContains(t, err, "has no table")
		}
	})
}

func TestMarshalUnknown(t *testing.T) {
	type Row struct {
		NullCol *int32 `parquet:"name=null_col, type=INT32, logicaltype=UNKNOWN, repetitiontype=OPTIONAL"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(Row))
	require.NoError(t, err)

	t.Run("nil_value_accepted", func(t *testing.T) {
		rows := []any{Row{NullCol: nil}}
		_, err := Marshal(rows, sh)
		require.NoError(t, err)
	})

	t.Run("non_nil_value_rejected", func(t *testing.T) {
		val := int32(42)
		rows := []any{Row{NullCol: &val}}
		_, err := Marshal(rows, sh)
		require.Error(t, err)
		require.Contains(t, err.Error(), "UNKNOWN column")
		require.Contains(t, err.Error(), "nil value")
	})
}
