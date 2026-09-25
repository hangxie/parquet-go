package marshal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/schema"
)

// TestVariantReconstructor_reconstructElementChildren covers the code path where
// a LIST element contains non-variant struct children (not Metadata/Value/Typed_value),
// which routes through reconstructElementChildren and collectChildValues.
func TestVariantReconstructor_reconstructElementChildren(t *testing.T) {
	t.Run("list_of_struct_elements", func(t *testing.T) {
		// Schema:  Root.Var (LIST)
		//            List (REPEATED)
		//              Element
		//                A  int32
		//                B  string
		// Two values in row 0 (RepetitionLevel 0 and 1).

		aPath := []string{common.ParGoRootInName, "Var", "List", "Element", "A"}
		bPath := []string{common.ParGoRootInName, "Var", "List", "Element", "B"}
		aKey := common.PathToStr(aPath)
		bKey := common.PathToStr(bPath)

		tableMap := map[string]*layout.Table{
			aKey: {
				Path:             aPath,
				Values:           []any{int32(10), int32(20)},
				RepetitionLevels: []int32{0, 1},
				DefinitionLevels: []int32{1, 1},
			},
			bKey: {
				Path:             bPath,
				Values:           []any{"hello", "world"},
				RepetitionLevels: []int32{0, 1},
				DefinitionLevels: []int32{1, 1},
			},
		}

		sh, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var struct {
				List []struct {
					Element struct {
						A int32  `parquet:"name=A, type=INT32"`
						B string `parquet:"name=B, type=BYTE_ARRAY, convertedtype=UTF8"`
					} `parquet:"name=Element"`
				} `parquet:"name=List, repetitiontype=REPEATED"`
			} `parquet:"name=Var, type=LIST"`
		}))

		r := &variantReconstructor{
			Path:          common.ParGoRootInName + common.ParGoPathDelimiter + "Var",
			tableMap:      &tableMap,
			SchemaHandler: sh,
		}

		tableBgn := map[string]int{aKey: 0, bKey: 0}
		tableEnd := map[string]int{aKey: 2, bKey: 2}

		// Call reconstructValue at the Element path directly so the HasSuffix("/Element")
		// branch routes to reconstructElementChildren without the ConvertedType=LIST detour.
		elementPath := common.PathToStr(aPath[:4])
		res, err := r.reconstructValue(elementPath, 0, tableBgn, tableEnd, nil)
		require.NoError(t, err)

		slice, ok := res.([]any)
		require.True(t, ok, "expected []any, got %T", res)
		require.Len(t, slice, 2)

		elem0 := slice[0].(map[string]any)
		require.Equal(t, int32(10), elem0["A"])
		require.Equal(t, "hello", elem0["B"])

		elem1 := slice[1].(map[string]any)
		require.Equal(t, int32(20), elem1["A"])
		require.Equal(t, "world", elem1["B"])
	})

	t.Run("list_of_single_field_elements", func(t *testing.T) {
		// Single non-variant child — exercises the len(elementMap)==1 branch in elementFromMap.
		aPath := []string{common.ParGoRootInName, "Var", "List", "Element", "A"}
		aKey := common.PathToStr(aPath)

		tableMap := map[string]*layout.Table{
			aKey: {
				Path:             aPath,
				Values:           []any{int32(7), int32(8)},
				RepetitionLevels: []int32{0, 1},
				DefinitionLevels: []int32{1, 1},
			},
		}

		sh, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var struct {
				List []struct {
					Element struct {
						A int32 `parquet:"name=A, type=INT32"`
					} `parquet:"name=Element"`
				} `parquet:"name=List, repetitiontype=REPEATED"`
			} `parquet:"name=Var, type=LIST"`
		}))

		r := &variantReconstructor{
			Path:          common.ParGoRootInName + common.ParGoPathDelimiter + "Var",
			tableMap:      &tableMap,
			SchemaHandler: sh,
		}

		tableBgn := map[string]int{aKey: 0}
		tableEnd := map[string]int{aKey: 2}

		// Call at the Element path directly to exercise the HasSuffix("/Element") route.
		elementPath := common.PathToStr(aPath[:4])
		res, err := r.reconstructValue(elementPath, 0, tableBgn, tableEnd, nil)
		require.NoError(t, err)

		slice, ok := res.([]any)
		require.True(t, ok, "expected []any, got %T", res)
		require.Len(t, slice, 2)

		elem0 := slice[0].(map[string]any)
		require.Equal(t, int32(7), elem0["A"])
		elem1 := slice[1].(map[string]any)
		require.Equal(t, int32(8), elem1["A"])
	})
}
