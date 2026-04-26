package marshal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func TestNewShreddedVariantReconstructor(t *testing.T) {
	// Create schema with variant
	variantInfo := &schema.VariantSchemaInfo{
		MetadataIdx:    2,
		ValueIdx:       3,
		TypedValueIdxs: []int32{4},
		IsShredded:     true,
	}

	sh := &schema.SchemaHandler{
		IndexMap: map[int32]string{
			2: "Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata",
			3: "Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value",
			4: "Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Typed_value",
		},
	}

	metadataTable := &layout.Table{
		Path:             []string{"Root", "Var", "Metadata"},
		Values:           []any{[]byte{0x01, 0x00, 0x00}},
		RepetitionLevels: []int32{0},
		DefinitionLevels: []int32{1},
	}
	valueTable := &layout.Table{
		Path:             []string{"Root", "Var", "Value"},
		Values:           []any{[]byte{0x00}},
		RepetitionLevels: []int32{0},
		DefinitionLevels: []int32{2},
	}
	typedValueTable := &layout.Table{
		Path:             []string{"Root", "Var", "Typed_value"},
		Values:           []any{int32(42)},
		RepetitionLevels: []int32{0},
		DefinitionLevels: []int32{2},
	}

	tableMap := &map[string]*layout.Table{
		"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata":    metadataTable,
		"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":       valueTable,
		"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Typed_value": typedValueTable,
	}

	t.Run("all_tables_present", func(t *testing.T) {
		r := NewShreddedVariantReconstructor(
			"Root"+common.ParGoPathDelimiter+"Var",
			variantInfo,
			tableMap,
			sh,
		)

		require.NotNil(t, r)
		require.Equal(t, "Root"+common.ParGoPathDelimiter+"Var", r.Path)
		require.NotNil(t, r.MetadataTable)
		require.NotNil(t, r.ValueTable)
		require.Len(t, r.TypedValueTables, 1)
	})

	t.Run("no_value_table", func(t *testing.T) {
		infoNoValue := &schema.VariantSchemaInfo{
			MetadataIdx:    2,
			ValueIdx:       -1, // No value column
			TypedValueIdxs: []int32{4},
			IsShredded:     true,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.ParGoPathDelimiter+"Var",
			infoNoValue,
			tableMap,
			sh,
		)

		require.NotNil(t, r)
		require.NotNil(t, r.MetadataTable)
		require.Nil(t, r.ValueTable)
		require.Len(t, r.TypedValueTables, 1)
	})

	t.Run("missing_tables_in_map", func(t *testing.T) {
		emptyTableMap := &map[string]*layout.Table{}

		r := NewShreddedVariantReconstructor(
			"Root"+common.ParGoPathDelimiter+"Var",
			variantInfo,
			emptyTableMap,
			sh,
		)

		require.NotNil(t, r)
		require.Nil(t, r.MetadataTable)
		require.Nil(t, r.ValueTable)
		require.Empty(t, r.TypedValueTables)
	})
}

func TestShreddedVariantReconstructor_GetValueAtRow(t *testing.T) {
	sh := &schema.SchemaHandler{
		MapIndex: map[string]int32{
			"Root": 0,
			"Root" + common.ParGoPathDelimiter + "Var":                                          1,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": 2,
		},
		SchemaElements: []*parquet.SchemaElement{
			{Name: "Root"},
			{Name: "Var", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
			{Name: "Metadata", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
		},
	}

	r := &ShreddedVariantReconstructor{
		SchemaHandler: sh,
	}

	t.Run("nil_table", func(t *testing.T) {
		result, err := r.getValueAtRow(nil, 0, nil, nil)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("missing_table_in_maps", func(t *testing.T) {
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		tableBgn := map[string]int{}
		tableEnd := map[string]int{}

		result, err := r.getValueAtRow(table, 0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("negative_bgn", func(t *testing.T) {
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		tablePath := common.PathToStr(table.Path)
		tableBgn := map[string]int{tablePath: -1}
		tableEnd := map[string]int{tablePath: 1}

		result, err := r.getValueAtRow(table, 0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("valid_row_retrieval", func(t *testing.T) {
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}, []byte{0x02, 0x00, 0x00}},
			RepetitionLevels: []int32{0, 0},
			DefinitionLevels: []int32{1, 1},
		}
		tablePath := common.PathToStr(table.Path)
		tableBgn := map[string]int{tablePath: 0}
		tableEnd := map[string]int{tablePath: 2}

		result, err := r.getValueAtRow(table, 0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.Equal(t, []byte{0x01, 0x00, 0x00}, result)

		result, err = r.getValueAtRow(table, 1, tableBgn, tableEnd)
		require.NoError(t, err)
		require.Equal(t, []byte{0x02, 0x00, 0x00}, result)
	})

	t.Run("row_not_found", func(t *testing.T) {
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		tablePath := common.PathToStr(table.Path)
		tableBgn := map[string]int{tablePath: 0}
		tableEnd := map[string]int{tablePath: 1}

		result, err := r.getValueAtRow(table, 5, tableBgn, tableEnd)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("null_value_low_definition_level", func(t *testing.T) {
		// When definition level is less than max, value is null
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{nil},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{0}, // Lower than max
		}
		tablePath := common.PathToStr(table.Path)
		tableBgn := map[string]int{tablePath: 0}
		tableEnd := map[string]int{tablePath: 1}

		result, err := r.getValueAtRow(table, 0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.Nil(t, result)
	})
}

func TestShreddedVariantReconstructor_Reconstruct(t *testing.T) {
	sh := &schema.SchemaHandler{
		MapIndex: map[string]int32{
			"Root": 0,
			"Root" + common.ParGoPathDelimiter + "Var":                                             1,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata":    2,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":       3,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Typed_value": 4,
		},
		IndexMap: map[int32]string{
			0: "Root",
			1: "Root" + common.ParGoPathDelimiter + "Var",
			2: "Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata",
			3: "Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value",
			4: "Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Typed_value",
		},
		SchemaElements: []*parquet.SchemaElement{
			{Name: "Root"},
			{Name: "Var", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
			{Name: "Metadata", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
			{Name: "Value", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
			{Name: "Typed_value", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
		},
	}

	t.Run("metadata_as_bytes", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		valueTable := &layout.Table{
			Path:             []string{"Root", "Var", "Value"},
			Values:           []any{[]byte{0x00}}, // null variant value
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{2},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": metadataTable,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":    valueTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.ParGoPathDelimiter+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       3,
				TypedValueIdxs: nil,
				IsShredded:     false,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": 0,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":    0,
		}
		tableEnd := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": 1,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":    1,
		}

		variant, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.NotNil(t, variant.Metadata)
		require.Equal(t, []byte{0x01, 0x00, 0x00}, variant.Metadata)
	})

	t.Run("metadata_as_string", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{string([]byte{0x01, 0x00, 0x00})},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": metadataTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.ParGoPathDelimiter+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       -1,
				TypedValueIdxs: nil,
				IsShredded:     false,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": 0,
		}
		tableEnd := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": 1,
		}

		variant, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.NotNil(t, variant.Metadata)
	})

	t.Run("metadata_unexpected_type", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{int32(123)}, // Unexpected type
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": metadataTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.ParGoPathDelimiter+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       -1,
				TypedValueIdxs: nil,
				IsShredded:     false,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": 0,
		}
		tableEnd := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": 1,
		}

		_, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected metadata type")
	})

	t.Run("value_as_string", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		valueTable := &layout.Table{
			Path:             []string{"Root", "Var", "Value"},
			Values:           []any{string([]byte{0x00})},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{2},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": metadataTable,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":    valueTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.ParGoPathDelimiter+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       3,
				TypedValueIdxs: nil,
				IsShredded:     false,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": 0,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":    0,
		}
		tableEnd := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": 1,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":    1,
		}

		variant, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.NotNil(t, variant.Value)
	})

	t.Run("with_typed_value", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		typedValueTable := &layout.Table{
			Path:             []string{"Root", "Var", "Typed_value"},
			Values:           []any{int32(12345)},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{2},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata":    metadataTable,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Typed_value": typedValueTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.ParGoPathDelimiter+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       -1,
				TypedValueIdxs: []int32{4},
				IsShredded:     true,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata":    0,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Typed_value": 0,
		}
		tableEnd := map[string]int{
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata":    1,
			"Root" + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Typed_value": 1,
		}

		variant, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.NotNil(t, variant.Value)
	})
}

func TestShreddedVariantReconstructor_Recursive(t *testing.T) {
	// Root.Var (VARIANT) shredded into:
	// Root.Var.Metadata
	// Root.Var.Typed_value.a.Metadata
	// Root.Var.Typed_value.a.Value

	// metadata: dict={0=>a}
	metadata := types.EncodeVariantMetadata([]string{"A"})

	// Inner variant for "a": value=123 (INT8)
	innerVariant := types.Variant{
		Metadata: metadata,
		Value:    types.EncodeVariantInt8(123),
	}

	mKey := common.PathToStr([]string{common.ParGoRootInName, "Var", "Metadata"})
	vKey := common.PathToStr([]string{common.ParGoRootInName, "Var", "Value"})
	imKey := common.PathToStr([]string{common.ParGoRootInName, "Var", "TypedValue", "A", "Metadata"})
	ivKey := common.PathToStr([]string{common.ParGoRootInName, "Var", "TypedValue", "A", "Value"})

	tableMap := map[string]*layout.Table{
		mKey: {
			Path:             []string{common.ParGoRootInName, "Var", "Metadata"},
			Values:           []any{metadata},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		},
		vKey: {
			Path:             []string{common.ParGoRootInName, "Var", "Value"},
			Values:           []any{nil},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{0},
		},
		imKey: {
			Path:             []string{common.ParGoRootInName, "Var", "TypedValue", "A", "Metadata"},
			Values:           []any{metadata},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		},
		ivKey: {
			Path:             []string{common.ParGoRootInName, "Var", "TypedValue", "A", "Value"},
			Values:           []any{innerVariant.Value},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		},
	}

	type Inner struct {
		Metadata []byte `parquet:"name=Metadata, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
		Value    []byte `parquet:"name=Value, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
	}
	type TypedValue struct {
		A Inner `parquet:"name=A, repetitiontype=REQUIRED"`
	}
	type VarGroup struct {
		Metadata   []byte     `parquet:"name=Metadata, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
		Value      []byte     `parquet:"name=Value, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
		TypedValue TypedValue `parquet:"name=TypedValue, repetitiontype=REQUIRED"`
	}

	sh, _ := schema.NewSchemaHandlerFromStruct(new(struct {
		Var VarGroup `parquet:"name=Var, type=VARIANT, repetitiontype=REQUIRED"`
	}))

	var info *schema.VariantSchemaInfo
	var path string
	for p, i := range sh.VariantSchemas {
		path = p
		info = i
		break
	}

	r := NewShreddedVariantReconstructor(path, info, &tableMap, sh)

	tableBgn := map[string]int{mKey: 0, vKey: 0, imKey: 0, ivKey: 0}
	tableEnd := map[string]int{mKey: 1, vKey: 1, imKey: 1, ivKey: 1}

	v, err := r.Reconstruct(0, tableBgn, tableEnd)
	require.NoError(t, err)

	decoded, err := types.ConvertVariantValue(v)
	require.NoError(t, err)

	expected := map[string]any{"A": int8(123)}
	require.Equal(t, expected, decoded)
}

func TestShreddedVariantReconstructor_Reconstruct_Coverage(t *testing.T) {
	t.Run("reconstruct_value_error_propagation", func(t *testing.T) {
		metadata := types.EncodeVariantMetadata([]string{"a"})
		metadataTable := &layout.Table{
			Path:             []string{common.ParGoRootInName, "Var", "Metadata"},
			Values:           []any{metadata},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}

		// Incompatible value to trigger error in reconstructValue or getValueAtRow
		valueTable := &layout.Table{
			Path:             []string{common.ParGoRootInName, "Var", "Value"},
			Values:           []any{struct{ Bad bool }{Bad: true}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}

		tableMap := map[string]*layout.Table{
			common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": metadataTable,
			common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":    valueTable,
		}

		sh, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var any `parquet:"name=Var, type=VARIANT"`
		}))

		var info *schema.VariantSchemaInfo
		var path string
		for p, i := range sh.VariantSchemas {
			path = p
			info = i
			break
		}

		r := NewShreddedVariantReconstructor(path, info, &tableMap, sh)

		mKey := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata"
		vKey := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value"

		tableBgn := map[string]int{mKey: 0, vKey: 0}
		tableEnd := map[string]int{mKey: 1, vKey: 1}

		_, err := r.Reconstruct(0, tableBgn, tableEnd)
		// Should error because we passed a struct as BYTE_ARRAY value
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected value type")
	})

	t.Run("reconstruct_returns_existing_variant", func(t *testing.T) {
		metadata := types.EncodeVariantMetadata([]string{"a"})
		expectedVariant := types.Variant{
			Metadata: metadata,
			Value:    types.EncodeVariantInt8(123),
		}

		// tableMap containing a Variant struct directly (if that's possible in some paths)
		// Actually reconstructValue returns Variant if it finds Metadata/Value children
		metadataTable := &layout.Table{
			Path:             []string{common.ParGoRootInName, "Var", "Metadata"},
			Values:           []any{metadata},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		valueTable := &layout.Table{
			Path:             []string{common.ParGoRootInName, "Var", "Value"},
			Values:           []any{expectedVariant.Value},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}

		tableMap := map[string]*layout.Table{
			common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata": metadataTable,
			common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value":    valueTable,
		}

		sh, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var any `parquet:"name=Var, type=VARIANT"`
		}))

		var info *schema.VariantSchemaInfo
		var path string
		for p, i := range sh.VariantSchemas {
			path = p
			info = i
			break
		}

		r := NewShreddedVariantReconstructor(path, info, &tableMap, sh)

		mKey := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata"
		vKey := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value"

		tableBgn := map[string]int{mKey: 0, vKey: 0}
		tableEnd := map[string]int{mKey: 1, vKey: 1}

		v, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.Equal(t, expectedVariant, v)
	})
}

func TestShreddedVariantReconstructor_reconstructValue_Coverage(t *testing.T) {
	t.Run("nested_list_reconstruction", func(t *testing.T) {
		// Mock a LIST of Variants
		// Root.Var (LIST)
		//   List
		//     Element (VARIANT)
		//       Metadata
		//       Value

		metadata := types.EncodeVariantMetadata([]string{})
		val1 := types.EncodeVariantInt8(1)
		val2 := types.EncodeVariantInt8(2)

		mKeyInt := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "List" + common.ParGoPathDelimiter + "Element" + common.ParGoPathDelimiter + "Metadata"
		vKeyInt := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "List" + common.ParGoPathDelimiter + "Element" + common.ParGoPathDelimiter + "Value"

		metadataTable := &layout.Table{
			Path:             []string{common.ParGoRootInName, "Var", "List", "Element", "Metadata"},
			Values:           []any{metadata, metadata},
			RepetitionLevels: []int32{0, 1},
			DefinitionLevels: []int32{1, 1},
		}
		valueTable := &layout.Table{
			Path:             []string{common.ParGoRootInName, "Var", "List", "Element", "Value"},
			Values:           []any{val1, val2},
			RepetitionLevels: []int32{0, 1},
			DefinitionLevels: []int32{1, 1},
		}

		tableMap := map[string]*layout.Table{
			mKeyInt: metadataTable,
			vKeyInt: valueTable,
		}

		type ListElement struct {
			Element any `parquet:"name=element, type=VARIANT"`
		}
		type ListType struct {
			List []ListElement `parquet:"name=list, repetitiontype=REPEATED"`
		}
		sh, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var ListType `parquet:"name=Var, type=LIST"`
		}))

		// For LIST, we need to make sure MaxRepetitionLevel > 0 for children
		// But here we are calling reconstructValue on the LIST group itself.
		// reconstructValue handles LIST by going to List/Element.

		// Manually create reconstructor since it's not a top-level Variant group
		r := &ShreddedVariantReconstructor{
			Path:          common.ParGoRootInName + common.ParGoPathDelimiter + "Var",
			tableMap:      &tableMap,
			SchemaHandler: sh,
		}

		tableBgn := map[string]int{mKeyInt: 0, vKeyInt: 0}
		tableEnd := map[string]int{mKeyInt: 2, vKeyInt: 2}

		res, err := r.reconstructValue(r.Path, 0, tableBgn, tableEnd, nil)
		require.NoError(t, err)

		// For LIST, reconstructValue should return a slice
		slice, ok := res.([]any)
		require.True(t, ok, "Expected slice, got %T", res)
		require.Len(t, slice, 2)
	})

	t.Run("map_reconstruction_zip_slices", func(t *testing.T) {
		sh, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Obj []struct {
				A int32  `parquet:"name=a, type=INT32"`
				B string `parquet:"name=b, type=BYTE_ARRAY, convertedtype=UTF8"`
			} `parquet:"name=Obj, repetitiontype=REPEATED"`
		}))

		// Internal paths for repeated struct fields
		aPath := []string{common.ParGoRootInName, "Obj", "A"}
		bPath := []string{common.ParGoRootInName, "Obj", "B"}
		aKey := common.PathToStr(aPath)
		bKey := common.PathToStr(bPath)

		tableMap := map[string]*layout.Table{
			aKey: {
				Path:             aPath,
				Values:           []any{int32(1), int32(2)},
				RepetitionLevels: []int32{0, 1},
				DefinitionLevels: []int32{1, 1},
			},
			bKey: {
				Path:             bPath,
				Values:           []any{"x", "y"},
				RepetitionLevels: []int32{0, 1},
				DefinitionLevels: []int32{1, 1},
			},
		}

		r := &ShreddedVariantReconstructor{
			Path:          common.ParGoRootInName + common.ParGoPathDelimiter + "Obj",
			tableMap:      &tableMap,
			SchemaHandler: sh,
		}

		tableBgn := map[string]int{aKey: 0, bKey: 0}
		tableEnd := map[string]int{aKey: 2, bKey: 2}

		res, err := r.reconstructValue(r.Path, 0, tableBgn, tableEnd, nil)
		require.NoError(t, err)

		slice, ok := res.([]any)
		require.True(t, ok, "Expected slice, got %T", res)
		require.Len(t, slice, 2)

		m1 := slice[0].(map[string]any)
		require.Equal(t, int32(1), m1["a"])
		require.Equal(t, "x", m1["b"])

		m2 := slice[1].(map[string]any)
		require.Equal(t, int32(2), m2["a"])
		require.Equal(t, "y", m2["b"])
	})

	t.Run("metadata_as_string_and_variant_reconstruction", func(t *testing.T) {
		// 1. Test regular object reconstruction (non-variant, non-list)
		sh1, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var struct {
				A int32 `parquet:"name=A, type=INT32"`
			} `parquet:"name=Var"`
		}))

		aKey1 := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "A"
		tableMap1 := map[string]*layout.Table{
			aKey1: {
				Path:             []string{common.ParGoRootInName, "Var", "A"},
				Values:           []any{int32(42)},
				RepetitionLevels: []int32{0},
				DefinitionLevels: []int32{1},
			},
		}

		r1 := &ShreddedVariantReconstructor{
			Path:          common.ParGoRootInName + common.ParGoPathDelimiter + "Var",
			tableMap:      &tableMap1,
			SchemaHandler: sh1,
		}

		tableBgn1 := map[string]int{aKey1: 0}
		tableEnd1 := map[string]int{aKey1: 1}

		res1, err := r1.reconstructValue(r1.Path, 0, tableBgn1, tableEnd1, nil)
		require.NoError(t, err)
		m1 := res1.(map[string]any)
		require.Equal(t, int32(42), m1["A"])

		// 2. Test metadata as string in a VARIANT group
		type VarGroup struct {
			Metadata []byte `parquet:"name=Metadata, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
			Value    []byte `parquet:"name=Value, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
		}
		sh2, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var VarGroup `parquet:"name=Var, type=VARIANT, repetitiontype=REQUIRED"`
		}))
		mKey2 := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata"
		vKey2 := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value"
		tableMap2 := map[string]*layout.Table{
			mKey2: {
				Path:             []string{common.ParGoRootInName, "Var", "Metadata"},
				Values:           []any{string([]byte{0x01, 0x00, 0x00})},
				RepetitionLevels: []int32{0},
				DefinitionLevels: []int32{1},
			},
			vKey2: {
				Path:             []string{common.ParGoRootInName, "Var", "Value"},
				Values:           []any{types.EncodeVariantInt32(123)},
				RepetitionLevels: []int32{0},
				DefinitionLevels: []int32{1},
			},
		}
		r2 := &ShreddedVariantReconstructor{
			Path:          common.ParGoRootInName + common.ParGoPathDelimiter + "Var",
			tableMap:      &tableMap2,
			SchemaHandler: sh2,
		}
		tableBgn2 := map[string]int{mKey2: 0, vKey2: 0}
		tableEnd2 := map[string]int{mKey2: 1, vKey2: 1}

		res2, err := r2.reconstructValue(r2.Path, 0, tableBgn2, tableEnd2, nil)
		require.NoError(t, err)
		v2, ok := res2.(types.Variant)
		require.True(t, ok, "Expected types.Variant, got %T", res2)
		require.Equal(t, []byte{0x01, 0x00, 0x00}, v2.Metadata)

		// 3. Trigger ReconstructVariant error fallback
		type VarGroupTyped struct {
			Metadata   []byte `parquet:"name=Metadata, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
			Value      []byte `parquet:"name=Value, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
			TypedValue []byte `parquet:"name=TypedValue, type=BYTE_ARRAY, repetitiontype=REQUIRED"`
		}
		sh3, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var VarGroupTyped `parquet:"name=Var, type=VARIANT, repetitiontype=REQUIRED"`
		}))
		tKey3 := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "TypedValue"
		tableMap2[tKey3] = &layout.Table{
			Path:             []string{common.ParGoRootInName, "Var", "TypedValue"},
			Values:           []any{func() {}}, // Incompatible type for variant encoding
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		tableBgn2[tKey3], tableEnd2[tKey3] = 0, 1

		r3 := &ShreddedVariantReconstructor{
			Path:          common.ParGoRootInName + common.ParGoPathDelimiter + "Var",
			tableMap:      &tableMap2,
			SchemaHandler: sh3,
		}

		res3, err := r3.reconstructValue(r3.Path, 0, tableBgn2, tableEnd2, nil)
		require.NoError(t, err)
		require.Nil(t, res3)
	})

	t.Run("reconstruct_null_and_variant_direct", func(t *testing.T) {
		sh, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var int32 `parquet:"name=Var, type=VARIANT"`
		}))
		r := &ShreddedVariantReconstructor{
			Path:          common.ParGoRootInName + common.ParGoPathDelimiter + "Var",
			tableMap:      &map[string]*layout.Table{},
			SchemaHandler: sh,
		}
		// Null case
		v, err := r.Reconstruct(0, map[string]int{}, map[string]int{})
		require.NoError(t, err)
		valNull, _ := types.ConvertVariantValue(v)
		require.Nil(t, valNull)

		// Variant direct case
		mKey := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Metadata"
		vKey := common.ParGoRootInName + common.ParGoPathDelimiter + "Var" + common.ParGoPathDelimiter + "Value"
		tableMap := map[string]*layout.Table{
			mKey: {
				Path:             []string{common.ParGoRootInName, "Var", "Metadata"},
				Values:           []any{[]byte{0x01, 0x00, 0x00}},
				RepetitionLevels: []int32{0},
				DefinitionLevels: []int32{1},
			},
			vKey: {
				Path:             []string{common.ParGoRootInName, "Var", "Value"},
				Values:           []any{types.EncodeVariantInt32(123)},
				RepetitionLevels: []int32{0},
				DefinitionLevels: []int32{1},
			},
		}
		r.tableMap = &tableMap
		v, err = r.Reconstruct(0, map[string]int{mKey: 0, vKey: 0}, map[string]int{mKey: 1, vKey: 1})
		require.NoError(t, err)
		valDirect, _ := types.ConvertVariantValue(v)
		require.Equal(t, int32(123), valDirect)
	})

	t.Run("reconstruct_any_to_variant", func(t *testing.T) {
		sh, _ := schema.NewSchemaHandlerFromStruct(new(struct {
			Var int32 `parquet:"name=Var, type=INT32"`
		}))
		tKey := common.ParGoRootInName + common.ParGoPathDelimiter + "Var"
		tableMap := map[string]*layout.Table{
			tKey: {
				Path:             []string{common.ParGoRootInName, "Var"},
				Values:           []any{int32(123)},
				RepetitionLevels: []int32{0},
				DefinitionLevels: []int32{1},
			},
		}
		r := &ShreddedVariantReconstructor{
			Path:          common.ParGoRootInName + common.ParGoPathDelimiter + "Var",
			tableMap:      &tableMap,
			SchemaHandler: sh,
		}
		v, err := r.Reconstruct(0, map[string]int{tKey: 0}, map[string]int{tKey: 1})
		require.NoError(t, err)
		valAny, _ := types.ConvertVariantValue(v)
		require.Equal(t, int32(123), valAny)
	})
}

// TestShreddedVariantReconstructor_reconstructElementChildren covers the code path where
// a LIST element contains non-variant struct children (not Metadata/Value/Typed_value),
// which routes through reconstructElementChildren and collectChildValues.
func TestShreddedVariantReconstructor_reconstructElementChildren(t *testing.T) {
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

		r := &ShreddedVariantReconstructor{
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

		r := &ShreddedVariantReconstructor{
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
