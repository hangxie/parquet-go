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
