package marshal

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

// Tests for shredded variant reconstruction functions

func TestSetVariantValue(t *testing.T) {
	t.Run("simple_struct_with_variant", func(t *testing.T) {
		type SimpleStruct struct {
			Name string
			Var  types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := SimpleStruct{Name: "test"}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Var", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testStruct.Var)
	})

	t.Run("struct_with_pointer_variant", func(t *testing.T) {
		type StructWithPtrVariant struct {
			Name string
			Var  *types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithPtrVariant{Name: "test"}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Var", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.NotNil(t, testStruct.Var)
		require.Equal(t, variant, *testStruct.Var)
	})

	t.Run("nested_struct_with_variant", func(t *testing.T) {
		type Inner struct {
			Var types.Variant
		}
		type Outer struct {
			Inner Inner
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := Outer{}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Inner"+common.ParGoPathDelimiter+"Var", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testStruct.Inner.Var)
	})

	t.Run("slice_of_structs_with_variant", func(t *testing.T) {
		type ItemWithVariant struct {
			Var types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testSlice := []ItemWithVariant{{}, {}}
		root := reflect.ValueOf(&testSlice).Elem()

		// Create slice record to track slice values
		sliceRecords[root] = &SliceRecord{
			Values: []reflect.Value{
				reflect.ValueOf(&testSlice[0]).Elem(),
				reflect.ValueOf(&testSlice[1]).Elem(),
			},
			Index: 0,
		}

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Var", "", sh, variant, 1, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testSlice[1].Var)
	})

	t.Run("field_not_found", func(t *testing.T) {
		type SimpleStruct struct {
			Name string
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := SimpleStruct{Name: "test"}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"NonExistent", "", sh, variant, 0, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("pointer_initialization", func(t *testing.T) {
		type StructWithPtrNested struct {
			Inner *struct {
				Var types.Variant
			}
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithPtrNested{}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Inner"+common.ParGoPathDelimiter+"Var", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.NotNil(t, testStruct.Inner)
		require.Equal(t, variant, testStruct.Inner.Var)
	})

	t.Run("slice_index_out_of_bounds", func(t *testing.T) {
		type ItemWithVariant struct {
			Var types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testSlice := []ItemWithVariant{{}}
		root := reflect.ValueOf(&testSlice).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		// rowIdx 5 is out of bounds for a slice with 1 element
		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Var", "", sh, variant, 5, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "out of bounds")
	})

	t.Run("byte_slice_error", func(t *testing.T) {
		type StructWithBytes struct {
			Data []byte
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithBytes{Data: []byte{1, 2, 3}}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Data"+common.ParGoPathDelimiter+"Var", "", sh, variant, 0, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "[]byte")
	})

	t.Run("unexpected_kind", func(t *testing.T) {
		type StructWithInt struct {
			Value int
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithInt{Value: 42}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Value"+common.ParGoPathDelimiter+"Var", "", sh, variant, 0, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected kind")
	})

	t.Run("path_end_not_variant_type", func(t *testing.T) {
		type StructWithString struct {
			Name string
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithString{Name: "test"}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Name", "", sh, variant, 0, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not set variant value at path end")
	})

	t.Run("slice_direct_access_without_sliceRecords", func(t *testing.T) {
		type ItemWithVariant struct {
			Var types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord) // Empty - no SliceRecord for this slice

		testSlice := []ItemWithVariant{{}, {}, {}}
		root := reflect.ValueOf(&testSlice).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		// Access row 1 without SliceRecord - should use direct po.Index(rowIdx)
		err := setVariantValue(root, common.ParGoRootInName+common.ParGoPathDelimiter+"Var", "", sh, variant, 1, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testSlice[1].Var)
	})

	t.Run("path_ends_at_variant_type_directly", func(t *testing.T) {
		// Test case where the path ends right at a types.Variant field
		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		// Create a Variant directly
		testVariant := types.Variant{}
		root := reflect.ValueOf(&testVariant).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		// Path is just the root (empty path after prefix)
		err := setVariantValue(root, common.ParGoRootInName, "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testVariant)
	})

	t.Run("path_ends_at_pointer_to_variant_directly", func(t *testing.T) {
		// Test case where the path ends at a *types.Variant field
		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		var testVariant *types.Variant
		root := reflect.ValueOf(&testVariant).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		// Path is just the root (empty path after prefix)
		err := setVariantValue(root, common.ParGoRootInName, "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.NotNil(t, testVariant)
		require.Equal(t, variant, *testVariant)
	})
}

func TestUnmarshal_ShreddedVariant_NoDuplicateRows(t *testing.T) {
	type VariantRow struct {
		ID      int32 `parquet:"name=id, type=INT32"`
		Variant any   `parquet:"name=variant, type=VARIANT"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(VariantRow))
	require.NoError(t, err)

	// Manually inject VariantSchemas info to simulate a shredded variant.
	// NewSchemaHandlerFromStruct creates "Parquet_go_root", "ID", "Variant", "Variant.Metadata", "Variant.Value"
	variantPath := common.ParGoRootInName + common.ParGoPathDelimiter + "Variant"

	// We verify indices
	var metadataIdx, valueIdx int32
	if idx, ok := sh.MapIndex[variantPath+common.ParGoPathDelimiter+"Metadata"]; ok {
		metadataIdx = idx
	} else {
		t.Fatal("Metadata index not found")
	}
	if idx, ok := sh.MapIndex[variantPath+common.ParGoPathDelimiter+"Value"]; ok {
		valueIdx = idx
	} else {
		t.Fatal("Value index not found")
	}

	sh.VariantSchemas = map[string]*schema.VariantSchemaInfo{
		variantPath: {
			MetadataIdx: metadataIdx,
			ValueIdx:    valueIdx,
			IsShredded:  true, // Mark as shredded to trigger the reconstruction path
		},
	}

	numRows := 10
	idValues := make([]any, numRows)
	// Metadata: version 1, empty dict -> 0x01 0x00 0x00
	metaBytes := []byte{0x01, 0x00, 0x00}
	metaValues := make([]any, numRows)
	// Value: null -> 0x00
	valBytes := []byte{0x00}
	valValues := make([]any, numRows)

	rLs := make([]int32, numRows) // all 0
	dLs := make([]int32, numRows) // all 0 (REQUIRED fields)

	for i := 0; i < numRows; i++ {
		idValues[i] = int32(i)
		metaValues[i] = metaBytes
		valValues[i] = valBytes
		rLs[i] = 0
		dLs[i] = 0
	}

	tableMap := map[string]*layout.Table{
		common.ParGoRootInName + common.ParGoPathDelimiter + "ID": {
			Path:             []string{common.ParGoRootInName, "ID"},
			Values:           idValues,
			RepetitionLevels: rLs,
			DefinitionLevels: dLs,
		},
		common.ParGoRootInName + common.ParGoPathDelimiter + "Variant" + common.ParGoPathDelimiter + "Metadata": {
			Path:             []string{common.ParGoRootInName, "Variant", "Metadata"},
			Values:           metaValues,
			RepetitionLevels: rLs,
			DefinitionLevels: dLs,
		},
		common.ParGoRootInName + common.ParGoPathDelimiter + "Variant" + common.ParGoPathDelimiter + "Value": {
			Path:             []string{common.ParGoRootInName, "Variant", "Value"},
			Values:           valValues,
			RepetitionLevels: rLs,
			DefinitionLevels: dLs,
		},
	}

	dst := make([]VariantRow, 0)
	err = Unmarshal(&tableMap, 0, numRows, &dst, sh, "")
	require.NoError(t, err)
	require.Len(t, dst, numRows, "Row count should match input rows without duplication")

	for i := 0; i < numRows; i++ {
		require.Equal(t, int32(i), dst[i].ID)
		// With Variant as any, it should be decoded.
		// A Variant with empty metadata and NULL value decodes to nil.
		require.Nil(t, dst[i].Variant)
	}
}
