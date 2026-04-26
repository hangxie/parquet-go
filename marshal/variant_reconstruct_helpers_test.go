package marshal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/types"
)

func TestToByteSlice(t *testing.T) {
	t.Run("nil_input_returns_nil", func(t *testing.T) {
		result, err := toByteSlice(nil, "field")
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("byte_slice_passes_through", func(t *testing.T) {
		input := []byte{0x01, 0x02, 0x03}
		result, err := toByteSlice(input, "field")
		require.NoError(t, err)
		require.Equal(t, input, result)
	})

	t.Run("string_converts_to_bytes", func(t *testing.T) {
		result, err := toByteSlice("hello", "field")
		require.NoError(t, err)
		require.Equal(t, []byte("hello"), result)
	})

	t.Run("unexpected_type_returns_error", func(t *testing.T) {
		result, err := toByteSlice(int32(42), "myfield")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected myfield type")
		require.Nil(t, result)
	})
}

func TestResolveMetadata(t *testing.T) {
	t.Run("nil_val_uses_fallback", func(t *testing.T) {
		fallback := []byte{0xAA, 0xBB}
		result, err := resolveMetadata(nil, fallback)
		require.NoError(t, err)
		require.Equal(t, fallback, result)
	})

	t.Run("non_nil_val_returned_directly", func(t *testing.T) {
		val := []byte{0x01, 0x00, 0x00}
		fallback := []byte{0xAA, 0xBB}
		result, err := resolveMetadata(val, fallback)
		require.NoError(t, err)
		require.Equal(t, val, result)
	})

	t.Run("empty_val_uses_fallback", func(t *testing.T) {
		fallback := []byte{0xCC, 0xDD}
		result, err := resolveMetadata([]byte{}, fallback)
		require.NoError(t, err)
		require.Equal(t, fallback, result)
	})

	t.Run("empty_val_and_empty_fallback_returns_default", func(t *testing.T) {
		result, err := resolveMetadata(nil, nil)
		require.NoError(t, err)
		require.Equal(t, defaultVariantMetadata, result)
	})

	t.Run("invalid_type_returns_error", func(t *testing.T) {
		result, err := resolveMetadata(int32(99), nil)
		require.Error(t, err)
		require.Nil(t, result)
	})
}

func TestEffectiveMetadataForTyped(t *testing.T) {
	fallback := []byte{0xAA, 0xBB, 0xCC}

	t.Run("metadata_set_with_byte_slice_value", func(t *testing.T) {
		meta := []byte{0x01, 0x00, 0x00}
		result := effectiveMetadataForTyped([]any{meta}, true, fallback)
		require.Equal(t, meta, result)
	})

	t.Run("metadata_set_with_string_value", func(t *testing.T) {
		meta := "\x01\x00\x00"
		result := effectiveMetadataForTyped([]any{meta}, true, fallback)
		require.Equal(t, []byte(meta), result)
	})

	t.Run("not_set_returns_fallback", func(t *testing.T) {
		result := effectiveMetadataForTyped([]any{[]byte{0x01}}, false, fallback)
		require.Equal(t, fallback, result)
	})

	t.Run("set_but_empty_values_returns_fallback", func(t *testing.T) {
		result := effectiveMetadataForTyped([]any{}, true, fallback)
		require.Equal(t, fallback, result)
	})

	t.Run("set_but_unexpected_type_returns_fallback", func(t *testing.T) {
		result := effectiveMetadataForTyped([]any{int32(99)}, true, fallback)
		require.Equal(t, fallback, result)
	})
}

func TestIsVariantResultNull(t *testing.T) {
	t.Run("empty_value_returns_true", func(t *testing.T) {
		v := types.Variant{Metadata: defaultVariantMetadata, Value: []byte{}}
		require.True(t, isVariantResultNull(v))
	})

	t.Run("single_zero_byte_returns_true", func(t *testing.T) {
		v := types.Variant{Metadata: defaultVariantMetadata, Value: []byte{0x00}}
		require.True(t, isVariantResultNull(v))
	})

	t.Run("non_empty_non_null_returns_false", func(t *testing.T) {
		v := types.Variant{Metadata: defaultVariantMetadata, Value: types.EncodeVariantInt8(42)}
		require.False(t, isVariantResultNull(v))
	})

	t.Run("single_non_zero_byte_returns_false", func(t *testing.T) {
		v := types.Variant{Metadata: defaultVariantMetadata, Value: []byte{0x01}}
		require.False(t, isVariantResultNull(v))
	})
}

func TestBuildVariantResults(t *testing.T) {
	metadata := types.EncodeVariantMetadata([]string{})
	value := types.EncodeVariantInt8(7)

	t.Run("happy_path_with_metadata_value_typed", func(t *testing.T) {
		metadataValues := []any{metadata}
		valueValues := []any{value}
		typedValueValues := []any{nil}

		results, err := buildVariantResults(1, metadataValues, valueValues, typedValueValues, defaultVariantMetadata)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, metadata, results[0].Metadata)
	})

	t.Run("metadata_from_fallback_when_not_provided", func(t *testing.T) {
		results, err := buildVariantResults(1, nil, []any{value}, nil, metadata)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, metadata, results[0].Metadata)
	})

	t.Run("invalid_metadata_type_returns_error", func(t *testing.T) {
		metadataValues := []any{int32(99)} // unexpected type
		results, err := buildVariantResults(1, metadataValues, nil, nil, nil)
		require.Error(t, err)
		require.Nil(t, results)
	})

	t.Run("invalid_value_type_returns_error", func(t *testing.T) {
		metadataValues := []any{metadata}
		valueValues := []any{int32(99)} // unexpected type for value
		results, err := buildVariantResults(1, metadataValues, valueValues, nil, defaultVariantMetadata)
		require.Error(t, err)
		require.Nil(t, results)
	})

	t.Run("multiple_elements", func(t *testing.T) {
		val2 := types.EncodeVariantInt8(8)
		metadataValues := []any{metadata, metadata}
		valueValues := []any{value, val2}

		results, err := buildVariantResults(2, metadataValues, valueValues, nil, defaultVariantMetadata)
		require.NoError(t, err)
		require.Len(t, results, 2)
	})
}

func TestVariantResultsToAny(t *testing.T) {
	metadata := types.EncodeVariantMetadata([]string{})
	nonNullVariant := types.Variant{Metadata: metadata, Value: types.EncodeVariantInt8(5)}
	nullVariant := types.Variant{Metadata: metadata, Value: types.EncodeVariantNull()}

	t.Run("is_repeated_true_returns_slice_of_any", func(t *testing.T) {
		results := []types.Variant{nonNullVariant, nullVariant}
		result, err := variantResultsToAny(results, true)
		require.NoError(t, err)
		slice, ok := result.([]any)
		require.True(t, ok)
		require.Len(t, slice, 2)
		require.Equal(t, nonNullVariant, slice[0])
		require.Nil(t, slice[1])
	})

	t.Run("is_repeated_false_returns_single_value", func(t *testing.T) {
		results := []types.Variant{nonNullVariant}
		result, err := variantResultsToAny(results, false)
		require.NoError(t, err)
		require.Equal(t, nonNullVariant, result)
	})

	t.Run("is_repeated_false_null_variant_returns_nil", func(t *testing.T) {
		results := []types.Variant{nullVariant}
		result, err := variantResultsToAny(results, false)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("is_repeated_true_all_non_null", func(t *testing.T) {
		val2 := types.Variant{Metadata: metadata, Value: types.EncodeVariantInt8(10)}
		results := []types.Variant{nonNullVariant, val2}
		result, err := variantResultsToAny(results, true)
		require.NoError(t, err)
		slice, ok := result.([]any)
		require.True(t, ok)
		require.Len(t, slice, 2)
		require.Equal(t, nonNullVariant, slice[0])
		require.Equal(t, val2, slice[1])
	})
}

func TestElementFromMap(t *testing.T) {
	metadata := types.EncodeVariantMetadata([]string{})
	value := types.EncodeVariantInt8(42)

	t.Run("metadata_bytes_and_value_routes_to_variant", func(t *testing.T) {
		result := elementFromMap(map[string]any{"Metadata": metadata, "Value": value}, nil)
		v, ok := result.(types.Variant)
		require.True(t, ok)
		require.Equal(t, metadata, v.Metadata)
	})

	t.Run("only_typed_value_routes_to_variant", func(t *testing.T) {
		result := elementFromMap(map[string]any{"Typed_value": int32(5)}, nil)
		_, ok := result.(types.Variant)
		require.True(t, ok)
	})

	t.Run("only_value_routes_to_variant", func(t *testing.T) {
		result := elementFromMap(map[string]any{"Value": value}, nil)
		_, ok := result.(types.Variant)
		require.True(t, ok)
	})

	t.Run("single_non_variant_field_returns_map", func(t *testing.T) {
		m := map[string]any{"FieldA": int32(99)}
		result := elementFromMap(m, nil)
		require.Equal(t, m, result)
	})

	t.Run("multiple_non_variant_fields_returns_map", func(t *testing.T) {
		m := map[string]any{"A": int32(1), "B": "hello"}
		result := elementFromMap(m, nil)
		require.Equal(t, m, result)
	})
}

func TestReconstructElementVariant(t *testing.T) {
	metadata := types.EncodeVariantMetadata([]string{})
	value := types.EncodeVariantInt8(7)

	t.Run("metadata_as_bytes", func(t *testing.T) {
		result := reconstructElementVariant(map[string]any{"Metadata": metadata, "Value": value}, nil)
		v, ok := result.(types.Variant)
		require.True(t, ok)
		require.Equal(t, metadata, v.Metadata)
	})

	t.Run("metadata_as_string", func(t *testing.T) {
		result := reconstructElementVariant(map[string]any{"Metadata": string(metadata)}, nil)
		v, ok := result.(types.Variant)
		require.True(t, ok)
		require.Equal(t, metadata, v.Metadata)
	})

	t.Run("uses_fallback_metadata_when_absent", func(t *testing.T) {
		fallback := types.EncodeVariantMetadata([]string{"x"})
		result := reconstructElementVariant(map[string]any{"Value": value}, fallback)
		v, ok := result.(types.Variant)
		require.True(t, ok)
		require.Equal(t, fallback, v.Metadata)
	})

	t.Run("uses_default_metadata_when_no_fallback", func(t *testing.T) {
		result := reconstructElementVariant(map[string]any{"Value": value}, nil)
		v, ok := result.(types.Variant)
		require.True(t, ok)
		require.Equal(t, defaultVariantMetadata, v.Metadata)
	})

	t.Run("metadata_unexpected_type_falls_back", func(t *testing.T) {
		// Non-bytes/string metadata is ignored; fallback or default is used instead.
		fallback := types.EncodeVariantMetadata([]string{})
		result := reconstructElementVariant(map[string]any{"Metadata": int32(99)}, fallback)
		v, ok := result.(types.Variant)
		require.True(t, ok)
		require.Equal(t, fallback, v.Metadata)
	})
}
