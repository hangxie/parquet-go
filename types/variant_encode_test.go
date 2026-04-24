package types

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestEncodeVariantMetadata_Empty(t *testing.T) {
	result := EncodeVariantMetadata([]string{})
	// Header byte layout: version (bits 0-3) | sorted (bit 4) | offset_size_minus_one (bits 5-6)
	// 0x01 = version=1, sorted=0, offset_size=1
	expected := []byte{0x01, 0x00, 0x00}
	if !bytesEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}

	// Verify it decodes correctly
	meta, err := decodeVariantMetadata(result)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(meta.dictionary) != 0 {
		t.Errorf("expected empty dictionary, got %d entries", len(meta.dictionary))
	}
}

func TestEncodeVariantMetadata_SingleEntry(t *testing.T) {
	result := EncodeVariantMetadata([]string{"test"})

	// Verify round-trip
	meta, err := decodeVariantMetadata(result)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(meta.dictionary) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(meta.dictionary))
	}
	if meta.dictionary[0] != "test" {
		t.Errorf("expected 'test', got %q", meta.dictionary[0])
	}
}

func TestEncodeVariantMetadata_MultipleEntries(t *testing.T) {
	dictionary := []string{"foo", "bar", "hello"}
	result := EncodeVariantMetadata(dictionary)

	// Verify round-trip
	meta, err := decodeVariantMetadata(result)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(meta.dictionary) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(meta.dictionary))
	}
	for i, exp := range dictionary {
		if meta.dictionary[i] != exp {
			t.Errorf("dictionary[%d]: expected %q, got %q", i, exp, meta.dictionary[i])
		}
	}
	// Unsorted dictionary should have sorted=false
	if meta.sorted {
		t.Error("expected sorted=false for unsorted dictionary")
	}
}

func TestEncodeVariantMetadata_Sorted(t *testing.T) {
	// Already sorted dictionary
	dictionary := []string{"alpha", "beta", "gamma"}
	result := EncodeVariantMetadata(dictionary)

	meta, err := decodeVariantMetadata(result)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !meta.sorted {
		t.Error("expected sorted=true for sorted dictionary")
	}
}

func TestEncodeVariantMetadataSorted(t *testing.T) {
	// Unsorted input
	fieldNames := []string{"zebra", "apple", "mango"}
	metadata, fieldIDs := EncodeVariantMetadataSorted(fieldNames)

	// Verify metadata decodes correctly
	meta, err := decodeVariantMetadata(metadata)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Should be sorted
	if !meta.sorted {
		t.Error("expected sorted=true")
	}

	// Dictionary should be in sorted order
	expectedOrder := []string{"apple", "mango", "zebra"}
	for i, exp := range expectedOrder {
		if meta.dictionary[i] != exp {
			t.Errorf("dictionary[%d]: expected %q, got %q", i, exp, meta.dictionary[i])
		}
	}

	// Field IDs should map correctly
	if fieldIDs["apple"] != 0 {
		t.Errorf("expected fieldIDs[apple]=0, got %d", fieldIDs["apple"])
	}
	if fieldIDs["mango"] != 1 {
		t.Errorf("expected fieldIDs[mango]=1, got %d", fieldIDs["mango"])
	}
	if fieldIDs["zebra"] != 2 {
		t.Errorf("expected fieldIDs[zebra]=2, got %d", fieldIDs["zebra"])
	}
}

func TestEncodeVariantMetadataSorted_Empty(t *testing.T) {
	metadata, fieldIDs := EncodeVariantMetadataSorted([]string{})

	if len(fieldIDs) != 0 {
		t.Errorf("expected empty fieldIDs, got %d entries", len(fieldIDs))
	}

	meta, err := decodeVariantMetadata(metadata)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(meta.dictionary) != 0 {
		t.Errorf("expected empty dictionary, got %d entries", len(meta.dictionary))
	}
}

func TestEncodeVariantNull(t *testing.T) {
	result := EncodeVariantNull()
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(result, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

func TestEncodeVariantBool(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	// Test true
	resultTrue := EncodeVariantBool(true)
	val, err := decodeVariantValue(resultTrue, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if val != true {
		t.Errorf("expected true, got %v", val)
	}

	// Test false
	resultFalse := EncodeVariantBool(false)
	val, err = decodeVariantValue(resultFalse, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if val != false {
		t.Errorf("expected false, got %v", val)
	}
}

func TestEncodeVariantInt32(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	tests := []int32{0, 1, -1, 12345, -12345, math.MaxInt32, math.MinInt32}
	for _, expected := range tests {
		result := EncodeVariantInt32(expected)
		val, err := decodeVariantValue(result, meta)
		if err != nil {
			t.Fatalf("decode error for %d: %v", expected, err)
		}
		if val != expected {
			t.Errorf("expected %d, got %v (%T)", expected, val, val)
		}
	}
}

func TestEncodeVariantInt64(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	tests := []int64{0, 1, -1, 123456789012345, -123456789012345, math.MaxInt64, math.MinInt64}
	for _, expected := range tests {
		result := EncodeVariantInt64(expected)
		val, err := decodeVariantValue(result, meta)
		if err != nil {
			t.Fatalf("decode error for %d: %v", expected, err)
		}
		if val != expected {
			t.Errorf("expected %d, got %v (%T)", expected, val, val)
		}
	}
}

func TestEncodeVariantDouble(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	tests := []float64{0, 1.0, -1.0, 3.14159, -3.14159, math.MaxFloat64, math.SmallestNonzeroFloat64}
	for _, expected := range tests {
		result := EncodeVariantDouble(expected)
		val, err := decodeVariantValue(result, meta)
		if err != nil {
			t.Fatalf("decode error for %v: %v", expected, err)
		}
		if val != expected {
			t.Errorf("expected %v, got %v (%T)", expected, val, val)
		}
	}
}

func TestEncodeVariantString_Short(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	tests := []string{"", "a", "hello", "test string"}
	for _, expected := range tests {
		result := EncodeVariantString(expected)
		val, err := decodeVariantValue(result, meta)
		if err != nil {
			t.Fatalf("decode error for %q: %v", expected, err)
		}
		if val != expected {
			t.Errorf("expected %q, got %v", expected, val)
		}
	}
}

func TestEncodeVariantString_Long(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	// Create a string longer than 63 characters
	longStr := "this is a very long string that exceeds sixty-three characters in length"
	if len(longStr) <= 63 {
		t.Fatal("test string should be longer than 63 chars")
	}

	result := EncodeVariantString(longStr)
	val, err := decodeVariantValue(result, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if val != longStr {
		t.Errorf("expected %q, got %v", longStr, val)
	}
}

func TestEncodeVariantObject_Empty(t *testing.T) {
	result := EncodeVariantObject([]int{}, [][]byte{})
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(result, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	obj, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", val)
	}
	if len(obj) != 0 {
		t.Errorf("expected empty object, got %d fields", len(obj))
	}
}

func TestEncodeVariantObject_Simple(t *testing.T) {
	// Create object {"name": "test", "age": 42}
	dictionary := []string{"age", "name"}
	metadata := EncodeVariantMetadata(dictionary)
	meta, _ := decodeVariantMetadata(metadata)

	value := EncodeVariantObject(
		[]int{1, 0}, // field IDs for "name", "age"
		[][]byte{
			EncodeVariantString("test"),
			EncodeVariantInt32(42),
		},
	)

	val, err := decodeVariantValue(value, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	obj, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", val)
	}
	if len(obj) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(obj))
	}
	if obj["name"] != "test" {
		t.Errorf("name: expected 'test', got %v", obj["name"])
	}
	if obj["age"] != int32(42) {
		t.Errorf("age: expected int32(42), got %v (%T)", obj["age"], obj["age"])
	}
}

func TestEncodeVariantArray_Empty(t *testing.T) {
	result := EncodeVariantArray([][]byte{})
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(result, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	arr, ok := val.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", val)
	}
	if len(arr) != 0 {
		t.Errorf("expected empty array, got %d elements", len(arr))
	}
}

func TestEncodeVariantArray_Simple(t *testing.T) {
	// Create array [true, 42, "hello"]
	meta := &variantMetadata{dictionary: []string{}}

	value := EncodeVariantArray([][]byte{
		EncodeVariantBool(true),
		EncodeVariantInt32(42),
		EncodeVariantString("hello"),
	})

	val, err := decodeVariantValue(value, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	arr, ok := val.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", val)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	if arr[0] != true {
		t.Errorf("arr[0]: expected true, got %v", arr[0])
	}
	if arr[1] != int32(42) {
		t.Errorf("arr[1]: expected int32(42), got %v (%T)", arr[1], arr[1])
	}
	if arr[2] != "hello" {
		t.Errorf("arr[2]: expected 'hello', got %v", arr[2])
	}
}

func TestEncodeVariantArray_Nested(t *testing.T) {
	// Create nested array [[1, 2], [3, 4]]
	meta := &variantMetadata{dictionary: []string{}}

	inner1 := EncodeVariantArray([][]byte{
		EncodeVariantInt32(1),
		EncodeVariantInt32(2),
	})
	inner2 := EncodeVariantArray([][]byte{
		EncodeVariantInt32(3),
		EncodeVariantInt32(4),
	})
	value := EncodeVariantArray([][]byte{inner1, inner2})

	val, err := decodeVariantValue(value, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	arr, ok := val.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", val)
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(arr))
	}

	inner1Val, ok := arr[0].([]any)
	if !ok {
		t.Fatalf("arr[0]: expected []any, got %T", arr[0])
	}
	if len(inner1Val) != 2 || inner1Val[0] != int32(1) || inner1Val[1] != int32(2) {
		t.Errorf("arr[0]: expected [1, 2], got %v", inner1Val)
	}
}

func TestEncodeVariantArray_Large(t *testing.T) {
	// Create array with > 255 elements to trigger isLarge path
	elements := make([][]byte, 300)
	for i := range elements {
		elements[i] = EncodeVariantInt32(int32(i))
	}

	result := EncodeVariantArray(elements)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(result, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	arr, ok := val.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", val)
	}
	if len(arr) != 300 {
		t.Errorf("expected 300 elements, got %d", len(arr))
	}
	// Verify first and last
	if arr[0] != int32(0) {
		t.Errorf("arr[0]: expected 0, got %v", arr[0])
	}
	if arr[299] != int32(299) {
		t.Errorf("arr[299]: expected 299, got %v", arr[299])
	}
}

func TestEncodeVariantObject_Large(t *testing.T) {
	// Create object with > 255 fields to trigger isLarge path
	numFields := 300
	dictionary := make([]string, numFields)
	for i := range dictionary {
		dictionary[i] = fmt.Sprintf("field%d", i)
	}
	metadata := EncodeVariantMetadata(dictionary)
	meta, _ := decodeVariantMetadata(metadata)

	fieldIDs := make([]int, numFields)
	values := make([][]byte, numFields)
	for i := range numFields {
		fieldIDs[i] = i
		values[i] = EncodeVariantInt32(int32(i))
	}

	result := EncodeVariantObject(fieldIDs, values)

	val, err := decodeVariantValue(result, meta)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	obj, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", val)
	}
	if len(obj) != numFields {
		t.Errorf("expected %d fields, got %d", numFields, len(obj))
	}
}

func TestEncodeVariantObject_MismatchedLengths(t *testing.T) {
	// fieldIDs and values have different lengths
	result := EncodeVariantObject([]int{0, 1}, [][]byte{EncodeVariantInt32(1)})
	if result != nil {
		t.Error("expected nil for mismatched lengths")
	}
}

func TestEncodeVariantMetadata_LargeStrings(t *testing.T) {
	// Create dictionary with total length > 255 to trigger 2-byte offsets
	dictionary := make([]string, 10)
	for i := range dictionary {
		dictionary[i] = strings.Repeat("x", 30) // 30 * 10 = 300 bytes
	}

	result := EncodeVariantMetadata(dictionary)

	// Verify round-trip
	meta, err := decodeVariantMetadata(result)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(meta.dictionary) != 10 {
		t.Fatalf("expected 10 entries, got %d", len(meta.dictionary))
	}
	for i, s := range meta.dictionary {
		if s != dictionary[i] {
			t.Errorf("dictionary[%d] mismatch", i)
		}
	}
}

// Object/Array decode error paths

func TestEncodeVariantFloat(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	tests := []float32{0, 1.0, -1.0, 2.5, -2.5, math.MaxFloat32, math.SmallestNonzeroFloat32}
	for _, expected := range tests {
		result := EncodeVariantFloat(expected)
		val, err := decodeVariantValue(result, meta)
		if err != nil {
			t.Fatalf("decode error for %v: %v", expected, err)
		}
		if val != expected {
			t.Errorf("expected %v, got %v (%T)", expected, val, val)
		}
	}
}

func TestEncodeVariantInt8(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	tests := []int8{0, 1, -1, 42, -42, 127, -128}
	for _, expected := range tests {
		result := EncodeVariantInt8(expected)
		val, err := decodeVariantValue(result, meta)
		if err != nil {
			t.Fatalf("decode error for %d: %v", expected, err)
		}
		if val != expected {
			t.Errorf("expected %d, got %v (%T)", expected, val, val)
		}
	}
}

func TestEncodeVariantInt16(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	tests := []int16{0, 1, -1, 1000, -1000, 32767, -32768}
	for _, expected := range tests {
		result := EncodeVariantInt16(expected)
		val, err := decodeVariantValue(result, meta)
		if err != nil {
			t.Fatalf("decode error for %d: %v", expected, err)
		}
		if val != expected {
			t.Errorf("expected %d, got %v (%T)", expected, val, val)
		}
	}
}

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
	found := false
	for _, s := range meta.dictionary {
		if s == "key" {
			found = true
			break
		}
	}
	if !found {
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
