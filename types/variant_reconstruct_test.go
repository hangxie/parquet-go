package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeVariantWithTypedValue_BothNull(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{})

	result, err := MergeVariantWithTypedValue(nil, nil, metadata)
	if err != nil {
		t.Fatalf("merge error: %v", err)
	}

	meta, _ := decodeVariantMetadata(metadata)
	decoded, err := decodeVariantValue(result, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded != nil {
		t.Errorf("expected nil, got %v", decoded)
	}
}

func TestMergeVariantWithTypedValue_OnlyValue(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{})
	value := EncodeVariantInt32(12345)

	result, err := MergeVariantWithTypedValue(value, nil, metadata)
	if err != nil {
		t.Fatalf("merge error: %v", err)
	}

	// Should return value as-is
	if !bytesEqual(result, value) {
		t.Errorf("expected value unchanged, got different bytes")
	}
}

func TestMergeVariantWithTypedValue_OnlyTypedValue(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{})

	result, err := MergeVariantWithTypedValue(nil, int32(42), metadata)
	if err != nil {
		t.Fatalf("merge error: %v", err)
	}

	meta, _ := decodeVariantMetadata(metadata)
	decoded, err := decodeVariantValue(result, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded != int32(42) {
		t.Errorf("expected int32(42), got %v (%T)", decoded, decoded)
	}
}

func TestMergeVariantWithTypedValue_BothPresent_Objects(t *testing.T) {
	// Test merging two objects
	dictionary := []string{"age", "city", "name"}
	metadata := EncodeVariantMetadata(dictionary)
	meta, _ := decodeVariantMetadata(metadata)

	// Base object: {"name": "John"}
	baseValue := EncodeVariantObject(
		[]int{2}, // name
		[][]byte{EncodeVariantString("John")},
	)

	// Typed value: {"age": 30}
	typedValue := map[string]any{
		"age": int32(30),
	}

	result, err := MergeVariantWithTypedValue(baseValue, typedValue, metadata)
	if err != nil {
		t.Fatalf("merge error: %v", err)
	}

	decoded, err := decodeVariantValue(result, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	obj, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", decoded)
	}

	// Should have both fields
	if obj["name"] != "John" {
		t.Errorf("name: expected 'John', got %v", obj["name"])
	}
	if obj["age"] != int32(30) {
		t.Errorf("age: expected int32(30), got %v (%T)", obj["age"], obj["age"])
	}
}

func TestMergeVariantWithTypedValue_InvalidMetadata(t *testing.T) {
	// Test error path when metadata decoding fails
	invalidMetadata := []byte{0xFF, 0xFF, 0xFF} // Invalid metadata
	value := []byte{0x40, 0x00}                 // Some value
	typedValue := int32(42)                     // Some typed value

	_, err := MergeVariantWithTypedValue(value, typedValue, invalidMetadata)
	if err == nil {
		t.Error("expected error for invalid metadata")
	}
}

func TestMergeVariantWithTypedValue_TypeMismatchNotObjects(t *testing.T) {
	// Test case where both value and typedValue are present but not both objects
	// This should prefer typedValue
	metadata := []byte{0x01, 0x00, 0x00} // Valid empty metadata

	// Encode an int32 as the value (not an object)
	value := EncodeVariantInt32(123)

	// typed_value is a string (not an object)
	typedValue := "hello"

	result, err := MergeVariantWithTypedValue(value, typedValue, metadata)
	if err != nil {
		t.Fatalf("merge error: %v", err)
	}

	// Result should be the encoded typed_value (the string "hello")
	decoded, err := decodeVariantValue(result, &variantMetadata{})
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded != "hello" {
		t.Errorf("expected 'hello', got %v", decoded)
	}
}

func TestMergeVariantWithTypedValue_ValueObjectTypedNotObject(t *testing.T) {
	// Test case where value is an object but typedValue is not
	// Create a simple object encoding
	// Object with one field: fieldID=0, value=int32(1)
	metadata := []byte{0x01, 0x01, 0x00, 0x01, 'a'} // Single key "a"
	fieldValue := EncodeVariantInt32(1)
	value := EncodeVariantObject([]int{0}, [][]byte{fieldValue})

	// typed_value is a primitive (not an object)
	typedValue := int64(999)

	result, err := MergeVariantWithTypedValue(value, typedValue, metadata)
	if err != nil {
		t.Fatalf("merge error: %v", err)
	}

	// Result should be the encoded typed_value (int64)
	decoded, err := decodeVariantValue(result, &variantMetadata{})
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded != int64(999) {
		t.Errorf("expected int64(999), got %v (%T)", decoded, decoded)
	}
}

func TestMergeVariantWithTypedValue_InvalidValueBlob(t *testing.T) {
	// Test error path when value blob decoding fails
	metadata := []byte{0x01, 0x00, 0x00}          // Valid empty metadata
	value := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF} // Invalid value blob
	typedValue := map[string]any{"key": "val"}

	_, err := MergeVariantWithTypedValue(value, typedValue, metadata)
	if err == nil {
		t.Error("expected error for invalid value blob")
	}
}

func TestReconstructVariant(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{})
	value := EncodeVariantInt32(12345)

	variant, err := ReconstructVariant(metadata, value, nil)
	if err != nil {
		t.Fatalf("reconstruct error: %v", err)
	}

	if !bytesEqual(variant.Metadata, metadata) {
		t.Error("metadata mismatch")
	}
	if !bytesEqual(variant.Value, value) {
		t.Error("value mismatch")
	}
}

func TestReconstructVariant_WithTypedValue(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{})

	variant, err := ReconstructVariant(metadata, nil, int32(999))
	if err != nil {
		t.Fatalf("reconstruct error: %v", err)
	}

	if !bytesEqual(variant.Metadata, metadata) {
		t.Error("metadata mismatch")
	}

	// Verify the reconstructed value
	decoded, err := ConvertVariantValue(variant)
	if err != nil {
		t.Fatalf("convert error: %v", err)
	}

	if decoded != int32(999) {
		t.Errorf("expected int32(999), got %v (%T)", decoded, decoded)
	}
}

func TestReconstructVariant_MergeError(t *testing.T) {
	// Test error path from MergeVariantWithTypedValue
	invalidMetadata := []byte{0xFF, 0xFF, 0xFF} // Invalid metadata
	value := []byte{0x40, 0x00}
	typedValue := int32(42)

	_, err := ReconstructVariant(invalidMetadata, value, typedValue)
	if err == nil {
		t.Error("expected error from ReconstructVariant")
	}
}

func TestReconstructVariant_NullVariant(t *testing.T) {
	// Test reconstruction of null variant (both nil)
	metadata := []byte{0x01, 0x00, 0x00} // Valid empty metadata

	variant, err := ReconstructVariant(metadata, nil, nil)
	if err != nil {
		t.Fatalf("reconstruct error: %v", err)
	}

	if !bytesEqual(variant.Metadata, metadata) {
		t.Error("metadata mismatch")
	}

	// Value should be null encoding
	decoded, err := ConvertVariantValue(variant)
	if err != nil {
		t.Fatalf("convert error: %v", err)
	}

	if decoded != nil {
		t.Errorf("expected nil, got %v", decoded)
	}
}

// TestCollectKeys_UnexportedField covers the unexported-field skip in collectKeys (struct case).
// AnyToVariant with a struct triggers collectKeys for the struct kind.
func TestCollectKeys_UnexportedField(t *testing.T) {
	type withPrivate struct {
		Name    string
		private int //nolint:unused
	}
	v, err := AnyToVariant(withPrivate{Name: "hello"})
	require.NoError(t, err)
	decoded, err := ConvertVariantValue(v)
	require.NoError(t, err)
	obj := decoded.(map[string]any)
	require.Equal(t, "hello", obj["Name"])
	_, hasPrivate := obj["private"]
	require.False(t, hasPrivate)
}

// TestCollectKeys_PointerField covers the Pointer kind in collectKeys.
// collectKeys is called directly since AnyToVariant does not dereference pointer-type fields.
func TestCollectKeys_PointerField(t *testing.T) {
	type Inner struct {
		Value string
	}
	p := &Inner{Value: "world"}
	keys := make(map[string]struct{})
	collectKeys(p, keys)
	require.Contains(t, keys, "Value")
}

// TestMergeVariantWithTypedValue_NonObjectBase covers the branch where base decodes to a
// non-object value but typedValue is an object — base is replaced with an empty map.
func TestMergeVariantWithTypedValue_NonObjectBase(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{"x"})
	// value bytes that decode to int32(1), not an object
	valueBytes := EncodeVariantInt32(1)
	typedValue := map[string]any{"x": int32(2)}
	merged, err := MergeVariantWithTypedValue(valueBytes, typedValue, metadata)
	require.NoError(t, err)
	require.NotNil(t, merged)
}
