package types

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:gocognit
func TestEncodeGoValueAsVariant(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	tests := []struct {
		name     string
		input    any
		validate func(t *testing.T, result any)
	}{
		{
			name:  "nil",
			input: nil,
			validate: func(t *testing.T, result any) {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
			},
		},
		{
			name:  "bool true",
			input: true,
			validate: func(t *testing.T, result any) {
				if result != true {
					t.Errorf("expected true, got %v", result)
				}
			},
		},
		{
			name:  "bool false",
			input: false,
			validate: func(t *testing.T, result any) {
				if result != false {
					t.Errorf("expected false, got %v", result)
				}
			},
		},
		{
			name:  "int8",
			input: int8(42),
			validate: func(t *testing.T, result any) {
				if result != int8(42) {
					t.Errorf("expected int8(42), got %v (%T)", result, result)
				}
			},
		},
		{
			name:  "int16",
			input: int16(1000),
			validate: func(t *testing.T, result any) {
				if result != int16(1000) {
					t.Errorf("expected int16(1000), got %v (%T)", result, result)
				}
			},
		},
		{
			name:  "int32",
			input: int32(12345),
			validate: func(t *testing.T, result any) {
				if result != int32(12345) {
					t.Errorf("expected int32(12345), got %v (%T)", result, result)
				}
			},
		},
		{
			name:  "int64",
			input: int64(123456789),
			validate: func(t *testing.T, result any) {
				if result != int64(123456789) {
					t.Errorf("expected int64(123456789), got %v (%T)", result, result)
				}
			},
		},
		{
			name:  "int",
			input: int(999),
			validate: func(t *testing.T, result any) {
				if result != int64(999) {
					t.Errorf("expected int64(999), got %v (%T)", result, result)
				}
			},
		},
		{
			name:  "float32",
			input: float32(2.5),
			validate: func(t *testing.T, result any) {
				if result != float32(2.5) {
					t.Errorf("expected float32(2.5), got %v (%T)", result, result)
				}
			},
		},
		{
			name:  "float64",
			input: float64(3.14159),
			validate: func(t *testing.T, result any) {
				if result != float64(3.14159) {
					t.Errorf("expected float64(3.14159), got %v (%T)", result, result)
				}
			},
		},
		{
			name:  "string",
			input: "hello world",
			validate: func(t *testing.T, result any) {
				if result != "hello world" {
					t.Errorf("expected 'hello world', got %v", result)
				}
			},
		},
		{
			name:  "[]any",
			input: []any{int32(1), int32(2), int32(3)},
			validate: func(t *testing.T, result any) {
				arr, ok := result.([]any)
				if !ok {
					t.Fatalf("expected []any, got %T", result)
				}
				if len(arr) != 3 {
					t.Fatalf("expected 3 elements, got %d", len(arr))
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeGoValueAsVariant(tc.input)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			decoded, err := decodeVariantValue(encoded, meta)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			tc.validate(t, decoded)
		})
	}
}

func TestEncodeGoValueAsVariant_UnsupportedType(t *testing.T) {
	// Test with unsupported type (struct)
	type MyStruct struct {
		Field int
	}
	_, err := EncodeGoValueAsVariant(MyStruct{Field: 1})
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}

func TestEncodeGoValueAsVariant_Binary(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	input := []byte{0x01, 0x02, 0x03, 0x04}

	encoded, err := EncodeGoValueAsVariant(input)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := decodeVariantValue(encoded, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Binary returns as base64 encoded string
	if _, ok := decoded.(string); !ok {
		t.Errorf("expected string (base64), got %T", decoded)
	}
}

func TestEncodeGoValueAsVariantWithMetadata(t *testing.T) {
	// Create metadata with dictionary
	dictionary := []string{"age", "name"}
	metadata := EncodeVariantMetadata(dictionary)

	// Test encoding a map
	obj := map[string]any{
		"name": "test",
		"age":  int32(42),
	}

	encoded, err := EncodeGoValueAsVariantWithMetadata(obj, metadata)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Decode and verify
	meta, _ := decodeVariantMetadata(metadata)
	decoded, err := decodeVariantValue(encoded, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	resultObj, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", decoded)
	}
	if resultObj["name"] != "test" {
		t.Errorf("name: expected 'test', got %v", resultObj["name"])
	}
	if resultObj["age"] != int32(42) {
		t.Errorf("age: expected int32(42), got %v (%T)", resultObj["age"], resultObj["age"])
	}
}

func TestEncodeGoValueAsVariantWithMetadata_Nil(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{})

	encoded, err := EncodeGoValueAsVariantWithMetadata(nil, metadata)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	meta, _ := decodeVariantMetadata(metadata)
	decoded, err := decodeVariantValue(encoded, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded != nil {
		t.Errorf("expected nil, got %v", decoded)
	}
}

func TestEncodeGoValueAsVariantWithMetadata_MissingField(t *testing.T) {
	// Create metadata without "missing" field
	dictionary := []string{"age", "name"}
	metadata := EncodeVariantMetadata(dictionary)

	obj := map[string]any{
		"missing": "value", // Not in dictionary
	}

	_, err := EncodeGoValueAsVariantWithMetadata(obj, metadata)
	if err == nil {
		t.Error("expected error for missing field in dictionary")
	}
}

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

func TestEncodeGoValueAsVariantWithMetadata_ArrayOfObjects(t *testing.T) {
	// Dictionary for objects
	dictionary := []string{"id"}
	metadata := EncodeVariantMetadata(dictionary)

	// Array of objects
	arr := []any{
		map[string]any{"id": int32(1)},
		map[string]any{"id": int32(2)},
	}

	encoded, err := EncodeGoValueAsVariantWithMetadata(arr, metadata)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	meta, _ := decodeVariantMetadata(metadata)
	decoded, err := decodeVariantValue(encoded, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	resArr, ok := decoded.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", decoded)
	}
	if len(resArr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(resArr))
	}

	obj1 := resArr[0].(map[string]any)
	if obj1["id"] != int32(1) {
		t.Errorf("obj1: expected 1, got %v", obj1["id"])
	}
}

func TestAnyToVariant_Primitive(t *testing.T) {
	v, err := AnyToVariant(int32(123))
	if err != nil {
		t.Fatalf("AnyToVariant error: %v", err)
	}

	decoded, err := ConvertVariantValue(v)
	if err != nil {
		t.Fatalf("convert error: %v", err)
	}
	if decoded != int32(123) {
		t.Errorf("expected 123, got %v", decoded)
	}
}

func TestAnyToVariant_Object(t *testing.T) {
	input := map[string]any{
		"name": "Alice",
		"age":  int32(30),
	}
	v, err := AnyToVariant(input)
	if err != nil {
		t.Fatalf("AnyToVariant error: %v", err)
	}

	decoded, err := ConvertVariantValue(v)
	if err != nil {
		t.Fatalf("convert error: %v", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", decoded)
	}
	if obj["name"] != "Alice" {
		t.Errorf("expected Alice, got %v", obj["name"])
	}
}

func TestAnyToVariant_ArrayOfObjects(t *testing.T) {
	input := []any{
		map[string]any{"key": "val1"},
		map[string]any{"key": "val2"},
	}
	v, err := AnyToVariant(input)
	if err != nil {
		t.Fatalf("AnyToVariant error: %v", err)
	}

	decoded, err := ConvertVariantValue(v)
	if err != nil {
		t.Fatalf("convert error: %v", err)
	}
	arr, ok := decoded.([]any)
	if !ok {
		t.Fatalf("expected array, got %T", decoded)
	}
	if len(arr) != 2 {
		t.Errorf("expected 2 elements, got %d", len(arr))
	}
	// Verify metadata collected "key"
	meta, _ := decodeVariantMetadata(v.Metadata)
	if !slices.Contains(meta.dictionary, "key") {
		t.Errorf("expected 'key' in metadata dictionary")
	}
}

func TestAnyToVariant_NestedMixed(t *testing.T) {
	input := map[string]any{
		"list": []any{
			map[string]any{"inner": int32(1)},
			map[string]any{"inner": int32(2)},
		},
		"meta": "data",
	}
	v, err := AnyToVariant(input)
	if err != nil {
		t.Fatalf("AnyToVariant error: %v", err)
	}

	decoded, err := ConvertVariantValue(v)
	if err != nil {
		t.Fatalf("convert error: %v", err)
	}

	// Verify decoded value matches input
	obj, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", decoded)
	}
	if obj["meta"] != "data" {
		t.Errorf("expected meta=data, got %v", obj["meta"])
	}
	list, ok := obj["list"].([]any)
	if !ok {
		t.Fatalf("expected list array, got %T", obj["list"])
	}
	if len(list) != 2 {
		t.Errorf("expected list length 2, got %d", len(list))
	}

	// Check metadata has "list", "inner", "meta"
	meta, _ := decodeVariantMetadata(v.Metadata)
	expectedKeys := map[string]bool{"list": true, "inner": true, "meta": true}
	for _, k := range meta.dictionary {
		if !expectedKeys[k] {
			t.Errorf("unexpected key in metadata: %s", k)
		}
		delete(expectedKeys, k)
	}
	if len(expectedKeys) > 0 {
		t.Errorf("missing keys in metadata: %v", expectedKeys)
	}
}

func TestAnyToVariant_NestedStruct(t *testing.T) {
	type Inner struct {
		ID int
	}

	type Outer struct {
		Field Inner
	}

	o := Outer{Field: Inner{ID: 123}}

	v, err := AnyToVariant(o)
	if err != nil {
		t.Fatalf("AnyToVariant error: %v", err)
	}

	decoded, err := ConvertVariantValue(v)
	if err != nil {
		t.Fatalf("convert error: %v", err)
	}

	obj := decoded.(map[string]any)

	inner := obj["Field"].(map[string]any)

	// ID is int, converts to int64

	if inner["ID"].(int64) != 123 {
		t.Errorf("expected 123, got %v", inner["ID"])
	}
}

// TestEncodeGoValueAsVariant_JSONNumber covers the json.Number case (lines 49-56).
func TestEncodeGoValueAsVariant_JSONNumber(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	t.Run("json_number_int", func(t *testing.T) {
		encoded, err := EncodeGoValueAsVariant(json.Number("12345"))
		require.NoError(t, err)
		decoded, err := decodeVariantValue(encoded, meta)
		require.NoError(t, err)
		require.Equal(t, int64(12345), decoded)
	})

	t.Run("json_number_float", func(t *testing.T) {
		encoded, err := EncodeGoValueAsVariant(json.Number("3.14"))
		require.NoError(t, err)
		decoded, err := decodeVariantValue(encoded, meta)
		require.NoError(t, err)
		require.Equal(t, float64(3.14), decoded)
	})

	t.Run("json_number_invalid", func(t *testing.T) {
		_, err := EncodeGoValueAsVariant(json.Number("not_a_number"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid json.Number")
	})
}

// TestEncodeGoValueAsVariant_ArrayError covers the error path when an array element fails.
func TestEncodeGoValueAsVariant_ArrayError(t *testing.T) {
	type unsupported struct{ Field int }
	_, err := EncodeGoValueAsVariant([]any{unsupported{Field: 1}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode array element 0")
}

// TestEncodeGoValueAsVariantWithMetadata_Variant covers the Variant and *Variant paths.
func TestEncodeGoValueAsVariantWithMetadata_Variant(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{})
	meta, _ := decodeVariantMetadata(metadata)

	t.Run("variant_value", func(t *testing.T) {
		v := Variant{Metadata: metadata, Value: EncodeVariantInt32(42)}
		encoded, err := EncodeGoValueAsVariantWithMetadata(v, metadata)
		require.NoError(t, err)
		decoded, err := decodeVariantValue(encoded, meta)
		require.NoError(t, err)
		require.Equal(t, int32(42), decoded)
	})

	t.Run("variant_ptr_nil", func(t *testing.T) {
		var pv *Variant
		encoded, err := EncodeGoValueAsVariantWithMetadata(pv, metadata)
		require.NoError(t, err)
		decoded, err := decodeVariantValue(encoded, meta)
		require.NoError(t, err)
		require.Nil(t, decoded)
	})

	t.Run("variant_ptr_non_nil", func(t *testing.T) {
		pv := &Variant{Metadata: metadata, Value: EncodeVariantString("hello")}
		encoded, err := EncodeGoValueAsVariantWithMetadata(pv, metadata)
		require.NoError(t, err)
		decoded, err := decodeVariantValue(encoded, meta)
		require.NoError(t, err)
		require.Equal(t, "hello", decoded)
	})
}

// TestEncodeGoValueAsVariantWithMetadata_InvalidMetadata covers buildFieldNameToID error path.
func TestEncodeGoValueAsVariantWithMetadata_InvalidMetadata(t *testing.T) {
	type testStruct struct {
		Name string
	}
	// Pass malformed metadata bytes that will fail decodeVariantMetadata.
	invalidMeta := []byte{0xff, 0xff, 0xff}

	_, err := EncodeGoValueAsVariantWithMetadata(testStruct{Name: "hello"}, invalidMeta)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode metadata for struct encoding")
}

// TestEncodeGoValueAsVariantWithMetadata_SliceError covers the error path in encodeSliceAsVariant.
func TestEncodeGoValueAsVariantWithMetadata_SliceError(t *testing.T) {
	type unsupported struct{ Field int }
	metadata := EncodeVariantMetadata([]string{})
	_, err := EncodeGoValueAsVariantWithMetadata([]any{unsupported{Field: 1}}, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode array element 0")
}

// TestEncodeGoValueAsVariantWithMetadata_MapError covers the error path in encodeMapAsVariant.
func TestEncodeGoValueAsVariantWithMetadata_MapError(t *testing.T) {
	// Invalid metadata makes encodeMapAsVariant fail at buildFieldNameToID.
	invalidMeta := []byte{0xff, 0xff, 0xff}
	_, err := EncodeGoValueAsVariantWithMetadata(map[string]any{"key": "val"}, invalidMeta)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode metadata for object encoding")
}

// TestAnyToVariant_VariantTypes covers AnyToVariant with Variant and *Variant inputs.
func TestAnyToVariant_VariantTypes(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{})

	t.Run("variant_passthrough", func(t *testing.T) {
		v := Variant{Metadata: metadata, Value: EncodeVariantInt32(99)}
		result, err := AnyToVariant(v)
		require.NoError(t, err)
		require.Equal(t, v, result)
	})

	t.Run("variant_ptr_nil", func(t *testing.T) {
		var pv *Variant
		result, err := AnyToVariant(pv)
		require.NoError(t, err)
		require.Equal(t, EncodeVariantNull(), result.Value)
	})
}

// TestEncodeGoValueAsVariantWithMetadata_UnexportedField covers the unexported-field skip.
func TestEncodeGoValueAsVariantWithMetadata_UnexportedField(t *testing.T) {
	type withPrivate struct {
		Name    string
		private int //nolint:unused
	}
	dictionary := []string{"Name"}
	metadata := EncodeVariantMetadata(dictionary)
	meta, _ := decodeVariantMetadata(metadata)

	s := withPrivate{Name: "hello", private: 42}
	encoded, err := EncodeGoValueAsVariantWithMetadata(s, metadata)
	require.NoError(t, err)

	decoded, err := decodeVariantValue(encoded, meta)
	require.NoError(t, err)
	obj := decoded.(map[string]any)
	require.Equal(t, "hello", obj["Name"])
	_, hasPrivate := obj["private"]
	require.False(t, hasPrivate, "unexported field should not be encoded")
}

// TestEncodeGoValueAsVariantWithMetadata_StructFieldNotInDict covers struct field not in metadata.
func TestEncodeGoValueAsVariantWithMetadata_StructFieldNotInDict(t *testing.T) {
	type testStruct struct {
		Name string
		Age  int
	}
	// Metadata only contains "Name", not "Age"
	dictionary := []string{"Name"}
	metadata := EncodeVariantMetadata(dictionary)

	_, err := EncodeGoValueAsVariantWithMetadata(testStruct{Name: "hello", Age: 42}, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not in metadata dictionary")
}

// TestEncodeGoValueAsVariantWithMetadata_StructFieldEncodeError covers field encoding error.
func TestEncodeGoValueAsVariantWithMetadata_StructFieldEncodeError(t *testing.T) {
	type unsupported struct{ Field int }
	type outer struct {
		Value any
	}
	dictionary := []string{"Value"}
	metadata := EncodeVariantMetadata(dictionary)

	// Value field contains an unsupported type that will fail encoding
	_, err := EncodeGoValueAsVariantWithMetadata(outer{Value: unsupported{Field: 1}}, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode struct field")
}

// TestAnyToVariant_NilInput covers the v==nil early-return in AnyToVariant.
func TestAnyToVariant_NilInput(t *testing.T) {
	result, err := AnyToVariant(nil)
	require.NoError(t, err)
	require.Equal(t, EncodeVariantNull(), result.Value)
}

// TestAnyToVariant_NonNilVariantPtr covers the non-nil *Variant return path.
func TestAnyToVariant_NonNilVariantPtr(t *testing.T) {
	v := &Variant{
		Metadata: EncodeVariantMetadata([]string{}),
		Value:    EncodeVariantInt32(42),
	}
	result, err := AnyToVariant(v)
	require.NoError(t, err)
	require.Equal(t, v.Value, result.Value)
}

// TestAnyToVariant_EncodingError covers the error path when EncodeGoValueAsVariantWithMetadata fails.
func TestAnyToVariant_EncodingError(t *testing.T) {
	type withChan struct {
		CH chan bool
	}
	_, err := AnyToVariant(withChan{CH: make(chan bool)})
	require.Error(t, err)
}

// TestAnyToVariant_NilMapValue covers the nil-v path in collectKeys (via recursive map iteration).
func TestAnyToVariant_NilMapValue(t *testing.T) {
	input := map[string]any{"key": nil}
	v, err := AnyToVariant(input)
	require.NoError(t, err)
	decoded, err := ConvertVariantValue(v)
	require.NoError(t, err)
	obj := decoded.(map[string]any)
	require.Nil(t, obj["key"])
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

// TestEncodeMapAsVariant_ValueError covers the field encoding error in encodeMapAsVariant.
func TestEncodeMapAsVariant_ValueError(t *testing.T) {
	metadata := EncodeVariantMetadata([]string{"ch"})
	_, err := EncodeGoValueAsVariantWithMetadata(map[string]any{"ch": make(chan bool)}, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode object field")
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
