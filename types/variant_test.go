package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"
)

func TestDecodeVariantMetadata_Empty(t *testing.T) {
	meta, err := decodeVariantMetadata([]byte{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meta.dictionary) != 0 {
		t.Errorf("expected empty dictionary, got %d entries", len(meta.dictionary))
	}
}

func TestDecodeVariantMetadata_SingleEntry(t *testing.T) {
	// Build metadata: version=1, sorted=0, offset_size=1 (offset_size_minus_one=0)
	// Header byte layout: version (bits 0-3) | sorted (bit 4) | offset_size_minus_one (bits 5-6)
	// 0x01 = version=1, sorted=0, offset_size=1
	// dict_size: 1
	// offsets: [0, 4]
	// bytes: "test"
	data := []byte{
		0x01,               // header: version=1, sorted=0, offset_size=1
		0x01,               // dict_size=1
		0x00,               // offset[0]=0
		0x04,               // offset[1]=4
		't', 'e', 's', 't', // "test"
	}

	meta, err := decodeVariantMetadata(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meta.dictionary) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(meta.dictionary))
	}
	if meta.dictionary[0] != "test" {
		t.Errorf("expected 'test', got %q", meta.dictionary[0])
	}
}

func TestDecodeVariantMetadata_MultipleEntries(t *testing.T) {
	// Header byte layout: version (bits 0-3) | sorted (bit 4) | offset_size_minus_one (bits 5-6)
	// version=1, sorted=1, offset_size=1 (offset_size_minus_one=0)
	// 0x01 | 0x10 = 0x11
	data := []byte{
		0x11,          // header: version=1, sorted=1, offset_size=1
		0x03,          // dict_size=3
		0x00,          // offset[0]=0
		0x03,          // offset[1]=3
		0x06,          // offset[2]=6
		0x0b,          // offset[3]=11
		'f', 'o', 'o', // "foo"
		'b', 'a', 'r', // "bar"
		'h', 'e', 'l', 'l', 'o', // "hello"
	}

	meta, err := decodeVariantMetadata(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meta.dictionary) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(meta.dictionary))
	}
	expected := []string{"foo", "bar", "hello"}
	for i, exp := range expected {
		if meta.dictionary[i] != exp {
			t.Errorf("dictionary[%d]: expected %q, got %q", i, exp, meta.dictionary[i])
		}
	}
	if !meta.sorted {
		t.Errorf("expected sorted=true")
	}
}

func TestDecodeVariantValue_Null(t *testing.T) {
	// Null primitive: basic_type=0, primitive_type=0
	// value_metadata = 0 | (0 << 2) = 0x00
	data := []byte{0x00}
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

func TestDecodeVariantValue_Boolean(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}

	// True: basic_type=0, primitive_type=1
	// value_metadata = 0 | (1 << 2) = 0x04
	dataTrue := []byte{0x04}
	val, err := decodeVariantValue(dataTrue, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != true {
		t.Errorf("expected true, got %v", val)
	}

	// False: basic_type=0, primitive_type=2
	// value_metadata = 0 | (2 << 2) = 0x08
	dataFalse := []byte{0x08}
	val, err = decodeVariantValue(dataFalse, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != false {
		t.Errorf("expected false, got %v", val)
	}
}

func TestDecodeVariantValue_Int8(t *testing.T) {
	// Int8: basic_type=0, primitive_type=3
	// value_metadata = 0 | (3 << 2) = 0x0C
	data := []byte{0x0C, 0x2A} // 42
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != int8(42) {
		t.Errorf("expected int8(42), got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_Int32(t *testing.T) {
	// Int32: basic_type=0, primitive_type=5
	// value_metadata = 0 | (5 << 2) = 0x14
	data := make([]byte, 5)
	data[0] = 0x14
	binary.LittleEndian.PutUint32(data[1:], 12345)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != int32(12345) {
		t.Errorf("expected int32(12345), got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_Int64(t *testing.T) {
	// Int64: basic_type=0, primitive_type=6
	// value_metadata = 0 | (6 << 2) = 0x18
	data := make([]byte, 9)
	data[0] = 0x18
	binary.LittleEndian.PutUint64(data[1:], 123456789012345)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != int64(123456789012345) {
		t.Errorf("expected int64(123456789012345), got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_Double(t *testing.T) {
	// Double: basic_type=0, primitive_type=7
	// value_metadata = 0 | (7 << 2) = 0x1C
	data := make([]byte, 9)
	data[0] = 0x1C
	binary.LittleEndian.PutUint64(data[1:], math.Float64bits(3.14159))
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 3.14159 {
		t.Errorf("expected 3.14159, got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_Float(t *testing.T) {
	// Float: basic_type=0, primitive_type=14
	// value_metadata = 0 | (14 << 2) = 0x38
	data := make([]byte, 5)
	data[0] = 0x38
	binary.LittleEndian.PutUint32(data[1:], math.Float32bits(2.5))
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != float32(2.5) {
		t.Errorf("expected float32(2.5), got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_ShortString(t *testing.T) {
	// Short string: basic_type=1, length in value_header
	// "hello" has length 5
	// value_metadata = 1 | (5 << 2) = 0x15
	data := []byte{0x15, 'h', 'e', 'l', 'l', 'o'}
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hello" {
		t.Errorf("expected 'hello', got %v", val)
	}
}

func TestDecodeVariantValue_LongString(t *testing.T) {
	// Long string: basic_type=0, primitive_type=16
	// value_metadata = 0 | (16 << 2) = 0x40
	str := "this is a long string"
	data := make([]byte, 5+len(str))
	data[0] = 0x40
	binary.LittleEndian.PutUint32(data[1:], uint32(len(str)))
	copy(data[5:], str)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != str {
		t.Errorf("expected %q, got %v", str, val)
	}
}

func TestDecodeVariantValue_EmptyArray(t *testing.T) {
	// Array: basic_type=3, element_offset_size=1 (offset_size_minus_one=0), is_large=0
	// value_header = 0 | (0 << 2) = 0
	// value_metadata = 3 | (0 << 2) = 0x03
	// num_elements = 0
	data := []byte{0x03, 0x00}
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := val.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", val)
	}
	if len(arr) != 0 {
		t.Errorf("expected empty array, got %d elements", len(arr))
	}
}

func TestDecodeVariantValue_SimpleArray(t *testing.T) {
	// Array of [true, false]
	// Array header: element_offset_size=1, is_large=0
	// value_header = 0 | (0 << 2) = 0
	// value_metadata = 3 | (0 << 2) = 0x03
	// num_elements = 2
	// offsets = [0, 1, 2]
	// values = [0x04 (true), 0x08 (false)]
	data := []byte{
		0x03, // array, offset_size=1, is_large=0
		0x02, // num_elements=2
		0x00, // offset[0]=0
		0x01, // offset[1]=1
		0x02, // offset[2]=2
		0x04, // true
		0x08, // false
	}
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := val.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", val)
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(arr))
	}
	if arr[0] != true {
		t.Errorf("arr[0]: expected true, got %v", arr[0])
	}
	if arr[1] != false {
		t.Errorf("arr[1]: expected false, got %v", arr[1])
	}
}

func TestDecodeVariantValue_EmptyObject(t *testing.T) {
	// Object: basic_type=2, field_id_size=1, field_offset_size=1, is_large=0
	// value_header = 0 | (0 << 2) | (0 << 4) = 0
	// value_metadata = 2 | (0 << 2) = 0x02
	// num_elements = 0
	data := []byte{0x02, 0x00}
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	obj, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", val)
	}
	if len(obj) != 0 {
		t.Errorf("expected empty object, got %d fields", len(obj))
	}
}

func TestDecodeVariantValue_SimpleObject(t *testing.T) {
	// Object with {"name": "test", "age": 42}
	// Metadata dictionary: ["age", "name"] (sorted)
	// Header byte layout: version (bits 0-3) | sorted (bit 4) | offset_size_minus_one (bits 5-6)
	// 0x11 = version=1, sorted=1, offset_size=1
	metaData := []byte{
		0x11,          // header: version=1, sorted=1, offset_size=1
		0x02,          // dict_size=2
		0x00,          // offset[0]=0
		0x03,          // offset[1]=3
		0x07,          // offset[2]=7
		'a', 'g', 'e', // "age"
		'n', 'a', 'm', 'e', // "name"
	}
	meta, err := decodeVariantMetadata(metaData)
	if err != nil {
		t.Fatalf("decode metadata: %v", err)
	}

	// Object: field_id_size=1, field_offset_size=1, is_large=0
	// value_header = 0 | (0 << 2) | (0 << 4) = 0
	// value_metadata = 2 | (0 << 2) = 0x02
	// num_elements = 2
	// field_ids = [0 (age), 1 (name)]
	// field_offsets = [0, 2, 8]
	// values:
	//   age: int8(42) = 0x0C, 0x2A
	//   name: short string "test" = 0x11, 't', 'e', 's', 't'
	data := []byte{
		0x02,       // object
		0x02,       // num_elements=2
		0x00,       // field_id[0]=0 (age)
		0x01,       // field_id[1]=1 (name)
		0x00,       // offset[0]=0
		0x02,       // offset[1]=2
		0x07,       // offset[2]=7
		0x0C, 0x2A, // int8(42) for "age"
		0x11, 't', 'e', 's', 't', // short string "test" for "name"
	}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	obj, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", val)
	}
	if len(obj) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(obj))
	}
	if obj["age"] != int8(42) {
		t.Errorf("age: expected int8(42), got %v (%T)", obj["age"], obj["age"])
	}
	if obj["name"] != "test" {
		t.Errorf("name: expected 'test', got %v", obj["name"])
	}
}

func TestConvertVariantValue_Empty(t *testing.T) {
	v := Variant{
		Metadata: []byte{},
		Value:    []byte{},
	}
	val, err := ConvertVariantValue(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

func TestConvertVariantValue_SimplePrimitive(t *testing.T) {
	// Int32 value 12345
	value := make([]byte, 5)
	value[0] = 0x14 // basic_type=0, primitive_type=5 (int32)
	binary.LittleEndian.PutUint32(value[1:], 12345)

	// Empty metadata: header (version=1, offset_size=1) + dict_size=0 + one offset (0)
	v := Variant{
		Metadata: []byte{0x01, 0x00, 0x00}, // version=1, dict_size=0, offset[0]=0
		Value:    value,
	}
	val, err := ConvertVariantValue(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != int32(12345) {
		t.Errorf("expected int32(12345), got %v (%T)", val, val)
	}
}

func TestConvertVariantValue_InvalidMetadata(t *testing.T) {
	// Invalid metadata should return base64 fallback
	v := Variant{
		Metadata: []byte{0xFF, 0xFF}, // invalid
		Value:    []byte{0x04},       // true
	}
	val, err := ConvertVariantValue(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should return a map with base64 encoded data
	m, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any fallback, got %T", val)
	}
	if _, exists := m["metadata"]; !exists {
		t.Error("expected 'metadata' key in fallback")
	}
	if _, exists := m["value"]; !exists {
		t.Error("expected 'value' key in fallback")
	}
}

func TestDecodeVariantValue_Decimal4(t *testing.T) {
	// Decimal4: basic_type=0, primitive_type=8
	// value_metadata = 0 | (8 << 2) = 0x20
	// Format: 1 byte scale + 4 bytes unscaled value (little-endian)
	// Test: 12345 with scale 2 = 123.45
	data := make([]byte, 6)
	data[0] = 0x20 // primitive_type=8 (decimal4)
	data[1] = 0x02 // scale=2
	binary.LittleEndian.PutUint32(data[2:], 12345)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "123.45" {
		t.Errorf("expected '123.45', got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_Decimal4_Negative(t *testing.T) {
	// Test negative decimal: -123.45 (unscaled = -12345, scale = 2)
	data := make([]byte, 6)
	data[0] = 0x20 // primitive_type=8 (decimal4)
	data[1] = 0x02 // scale=2
	// Write -12345 as little-endian int32 (two's complement)
	negVal := int32(-12345)
	binary.LittleEndian.PutUint32(data[2:], uint32(negVal))
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "-123.45" {
		t.Errorf("expected '-123.45', got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_Decimal4_ZeroScale(t *testing.T) {
	// Test decimal with scale=0 (integer)
	data := make([]byte, 6)
	data[0] = 0x20 // primitive_type=8 (decimal4)
	data[1] = 0x00 // scale=0
	binary.LittleEndian.PutUint32(data[2:], 12345)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "12345" {
		t.Errorf("expected '12345', got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_Decimal8(t *testing.T) {
	// Decimal8: basic_type=0, primitive_type=9
	// value_metadata = 0 | (9 << 2) = 0x24
	// Test: 123456789012 with scale 4 = 12345678.9012
	data := make([]byte, 10)
	data[0] = 0x24 // primitive_type=9 (decimal8)
	data[1] = 0x04 // scale=4
	binary.LittleEndian.PutUint64(data[2:], 123456789012)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "12345678.9012" {
		t.Errorf("expected '12345678.9012', got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_Decimal16(t *testing.T) {
	// Decimal16: basic_type=0, primitive_type=10
	// value_metadata = 0 | (10 << 2) = 0x28
	// Test: 12345 with scale 3 = 12.345 (small value to verify format)
	data := make([]byte, 18)
	data[0] = 0x28 // primitive_type=10 (decimal16)
	data[1] = 0x03 // scale=3
	// Write 12345 as little-endian 128-bit integer (only low bytes used)
	binary.LittleEndian.PutUint64(data[2:10], 12345)
	binary.LittleEndian.PutUint64(data[10:18], 0) // high 64 bits = 0
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "12.345" {
		t.Errorf("expected '12.345', got %v (%T)", val, val)
	}
}

func TestFormatDecimal(t *testing.T) {
	tests := []struct {
		unscaled int64
		scale    int
		expected string
	}{
		{12345, 2, "123.45"},
		{-12345, 2, "-123.45"},
		{12345, 0, "12345"},
		{5, 3, "0.005"},
		{100, 2, "1"},       // trailing zeros removed
		{1000, 3, "1"},      // trailing zeros removed
		{12300, 2, "123"},   // trailing zeros removed
		{12340, 2, "123.4"}, // partial trailing zeros
		{-5, 3, "-0.005"},
	}

	for _, tc := range tests {
		result := formatDecimal(tc.unscaled, tc.scale)
		if result != tc.expected {
			t.Errorf("formatDecimal(%d, %d) = %q, expected %q", tc.unscaled, tc.scale, result, tc.expected)
		}
	}
}

// Tests for encoding functions

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

func TestConvertVariantValue_RoundTrip(t *testing.T) {
	// Full round-trip test: encode object, wrap in Variant, decode
	dictionary := []string{"type", "value"}
	metadata := EncodeVariantMetadata(dictionary)

	value := EncodeVariantObject(
		[]int{0, 1},
		[][]byte{
			EncodeVariantString("Example"),
			EncodeVariantInt32(123),
		},
	)

	variant := Variant{
		Metadata: metadata,
		Value:    value,
	}

	decoded, err := ConvertVariantValue(variant)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	obj, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", decoded)
	}
	if obj["type"] != "Example" {
		t.Errorf("type: expected 'Example', got %v", obj["type"])
	}
	if obj["value"] != int32(123) {
		t.Errorf("value: expected int32(123), got %v (%T)", obj["value"], obj["value"])
	}
}

// bytesEqual compares two byte slices
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Additional tests for uncovered primitive types

func TestDecodeVariantValue_Int16(t *testing.T) {
	// Int16: basic_type=0, primitive_type=4
	// value_metadata = 0 | (4 << 2) = 0x10
	data := make([]byte, 3)
	data[0] = 0x10
	binary.LittleEndian.PutUint16(data[1:], 12345)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != int16(12345) {
		t.Errorf("expected int16(12345), got %v (%T)", val, val)
	}
}

func TestDecodeVariantValue_Date(t *testing.T) {
	// Date: basic_type=0, primitive_type=11
	// value_metadata = 0 | (11 << 2) = 0x2C
	data := make([]byte, 5)
	data[0] = 0x2C
	// Days since epoch: 19000 = ~2022-01-05
	binary.LittleEndian.PutUint32(data[1:], 19000)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should return a date string
	if _, ok := val.(string); !ok {
		t.Errorf("expected string, got %T", val)
	}
}

func TestDecodeVariantValue_TimestampMicro(t *testing.T) {
	// TimestampMicro: basic_type=0, primitive_type=12
	// value_metadata = 0 | (12 << 2) = 0x30
	data := make([]byte, 9)
	data[0] = 0x30
	// Microseconds since epoch
	binary.LittleEndian.PutUint64(data[1:], 1640000000000000) // ~2021-12-20
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := val.(string); !ok {
		t.Errorf("expected string, got %T", val)
	}
}

func TestDecodeVariantValue_TimestampNTZ(t *testing.T) {
	// TimestampNTZMicro: basic_type=0, primitive_type=13
	// value_metadata = 0 | (13 << 2) = 0x34
	data := make([]byte, 9)
	data[0] = 0x34
	binary.LittleEndian.PutUint64(data[1:], 1640000000000000)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := val.(string); !ok {
		t.Errorf("expected string, got %T", val)
	}
}

func TestDecodeVariantValue_TimeNTZ(t *testing.T) {
	// TimeNTZ: basic_type=0, primitive_type=17
	// value_metadata = 0 | (17 << 2) = 0x44
	data := make([]byte, 9)
	data[0] = 0x44
	// 12:30:45.123456 = (12*3600 + 30*60 + 45)*1000000 + 123456 microseconds
	micros := int64((12*3600+30*60+45)*1000000 + 123456)
	binary.LittleEndian.PutUint64(data[1:], uint64(micros))
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	str, ok := val.(string)
	if !ok {
		t.Fatalf("expected string, got %T", val)
	}
	if str != "12:30:45.123456" {
		t.Errorf("expected '12:30:45.123456', got %q", str)
	}
}

func TestDecodeVariantValue_TimestampNano(t *testing.T) {
	// TimestampNano (UTC): basic_type=0, primitive_type=18
	// value_metadata = 0 | (18 << 2) = 0x48
	data := make([]byte, 9)
	data[0] = 0x48
	binary.LittleEndian.PutUint64(data[1:], 1640000000000000000) // nanoseconds
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := val.(string); !ok {
		t.Errorf("expected string, got %T", val)
	}
}

func TestDecodeVariantValue_TimestampNTZNano(t *testing.T) {
	// TimestampNTZNano: basic_type=0, primitive_type=19
	// value_metadata = 0 | (19 << 2) = 0x4C
	data := make([]byte, 9)
	data[0] = 0x4C
	binary.LittleEndian.PutUint64(data[1:], 1640000000000000000) // nanoseconds
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := val.(string); !ok {
		t.Errorf("expected string, got %T", val)
	}
}

func TestDecodeVariantValue_Binary(t *testing.T) {
	// Binary: basic_type=0, primitive_type=15
	// value_metadata = 0 | (15 << 2) = 0x3C
	binaryData := []byte{0x01, 0x02, 0x03, 0x04}
	data := make([]byte, 5+len(binaryData))
	data[0] = 0x3C
	binary.LittleEndian.PutUint32(data[1:], uint32(len(binaryData)))
	copy(data[5:], binaryData)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should return base64 encoded string
	if _, ok := val.(string); !ok {
		t.Errorf("expected string (base64), got %T", val)
	}
}

func TestDecodeVariantValue_UUID(t *testing.T) {
	// UUID: basic_type=0, primitive_type=20
	// value_metadata = 0 | (20 << 2) = 0x50
	data := make([]byte, 17)
	data[0] = 0x50
	// UUID bytes
	for i := 1; i <= 16; i++ {
		data[i] = byte(i)
	}
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := val.(string); !ok {
		t.Errorf("expected string (UUID), got %T", val)
	}
}

// Error path tests

func TestDecodeVariantMetadata_InvalidVersion(t *testing.T) {
	// Version 2 (unsupported) - version is in bits 0-3
	// 0x02 = version=2
	data := []byte{0x02, 0x00, 0x00}
	_, err := decodeVariantMetadata(data)
	if err == nil {
		t.Error("expected error for unsupported version")
	}
}

func TestDecodeVariantMetadata_TooShort(t *testing.T) {
	// Header only, missing dict_size
	// 0x01 = version=1, offset_size=1
	data := []byte{0x01}
	_, err := decodeVariantMetadata(data)
	if err == nil {
		t.Error("expected error for too short metadata")
	}
}

func TestDecodeVariantMetadata_OffsetOutOfBounds(t *testing.T) {
	// Valid header but offsets point beyond data
	// 0x01 = version=1, offset_size=1
	data := []byte{0x01, 0x01, 0x00, 0xFF} // dict_size=1, offsets [0, 255] but no string data
	_, err := decodeVariantMetadata(data)
	if err == nil {
		t.Error("expected error for offset out of bounds")
	}
}

func TestDecodeVariantValue_UnknownBasicType(t *testing.T) {
	// This shouldn't happen in practice, but test the default case
	// Actually all 4 basic types (0-3) are handled, so test unknown primitive
	data := []byte{0xFC} // basic_type=0, primitive_type=63 (unknown)
	meta := &variantMetadata{dictionary: []string{}}

	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for unknown primitive type")
	}
}

func TestDecodeVariantValue_ShortStringTruncated(t *testing.T) {
	// Short string claims length 10 but only has 3 bytes
	data := []byte{0x29, 'a', 'b', 'c'} // length=10 in header
	meta := &variantMetadata{dictionary: []string{}}

	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated short string")
	}
}

func TestDecodeVariantValue_EmptyData(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	val, err := decodeVariantValue([]byte{}, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil for empty data, got %v", val)
	}
}

// Test negative int128

func TestDecodeVariantValue_Decimal16_Negative(t *testing.T) {
	// Decimal16 with negative value
	data := make([]byte, 18)
	data[0] = 0x28 // primitive_type=10 (decimal16)
	data[1] = 0x02 // scale=2
	// Write -12345 as little-endian 128-bit two's complement
	// -12345 in two's complement 128-bit: all 1s except low bytes
	negVal := int64(-12345)
	binary.LittleEndian.PutUint64(data[2:10], uint64(negVal))
	// Sign extend to upper 64 bits (all 1s for negative)
	binary.LittleEndian.PutUint64(data[10:18], 0xFFFFFFFFFFFFFFFF)
	meta := &variantMetadata{dictionary: []string{}}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "-123.45" {
		t.Errorf("expected '-123.45', got %v", val)
	}
}

func TestFormatDecimal128(t *testing.T) {
	tests := []struct {
		unscaled string // big.Int string representation
		scale    int
		expected string
	}{
		{"12345", 2, "123.45"},
		{"-12345", 2, "-123.45"},
		{"12345", 0, "12345"},
		{"5", 3, "0.005"},
		{"-5", 3, "-0.005"},
		{"100", 2, "1"},
	}

	for _, tc := range tests {
		unscaled := new(big.Int)
		unscaled.SetString(tc.unscaled, 10)
		result := formatDecimal128(unscaled, tc.scale)
		if result != tc.expected {
			t.Errorf("formatDecimal128(%s, %d) = %q, expected %q", tc.unscaled, tc.scale, result, tc.expected)
		}
	}
}

// Large array/object tests

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

func TestDecodeObjectValue_FieldIDOutOfBounds(t *testing.T) {
	// Object with field_id that exceeds dictionary size
	dictionary := []string{"only_one"}
	metadata := EncodeVariantMetadata(dictionary)
	meta, _ := decodeVariantMetadata(metadata)

	// Manually craft object with field_id=5 (out of bounds)
	data := []byte{
		0x02, // object
		0x01, // num_elements=1
		0x05, // field_id=5 (out of bounds)
		0x00, // offset[0]=0
		0x01, // offset[1]=1
		0x00, // null value
	}

	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for field ID out of bounds")
	}
}

func TestDecodeArrayValue_LargeArray(t *testing.T) {
	// Test decoding a large array (is_large=1)
	// Manually construct a large array header
	meta := &variantMetadata{dictionary: []string{}}

	// Array header: element_offset_size=1 (0), is_large=1 (bit 2)
	// value_header = 0 | (1 << 2) = 0x04
	// value_metadata = 3 | (0x04 << 2) = 0x13
	data := []byte{
		0x13,                   // array with is_large=1
		0x02, 0x00, 0x00, 0x00, // num_elements=2 (4 bytes for large)
		0x00, // offset[0]=0
		0x01, // offset[1]=1
		0x02, // offset[2]=2
		0x04, // true
		0x08, // false
	}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	arr, ok := val.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", val)
	}
	if len(arr) != 2 {
		t.Errorf("expected 2 elements, got %d", len(arr))
	}
}

func TestDecodeObjectValue_LargeObject(t *testing.T) {
	// Test decoding a large object (is_large=1)
	dictionary := []string{"a", "b"}
	metadata := EncodeVariantMetadata(dictionary)
	meta, _ := decodeVariantMetadata(metadata)

	// Object header: field_id_size=1, field_offset_size=1, is_large=1
	// value_header = 0 | (0 << 2) | (1 << 4) = 0x10
	// value_metadata = 2 | (0x10 << 2) = 0x42
	data := []byte{
		0x42,                   // object with is_large=1
		0x02, 0x00, 0x00, 0x00, // num_elements=2 (4 bytes for large)
		0x00, // field_id[0]=0
		0x01, // field_id[1]=1
		0x00, // offset[0]=0
		0x01, // offset[1]=1
		0x02, // offset[2]=2
		0x04, // true
		0x08, // false
	}

	val, err := decodeVariantValue(data, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	obj, ok := val.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", val)
	}
	if len(obj) != 2 {
		t.Errorf("expected 2 fields, got %d", len(obj))
	}
}

// Additional error path tests for decodePrimitiveValue

func TestDecodePrimitiveValue_TruncatedInt8(t *testing.T) {
	// Int8 with no data after header
	data := []byte{0x0C} // primitive_type=3 (int8), but no value byte
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated int8")
	}
}

func TestDecodePrimitiveValue_TruncatedInt16(t *testing.T) {
	data := []byte{0x10, 0x01} // primitive_type=4 (int16), but only 1 byte
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated int16")
	}
}

func TestDecodePrimitiveValue_TruncatedInt32(t *testing.T) {
	data := []byte{0x14, 0x01, 0x02} // primitive_type=5 (int32), but only 2 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated int32")
	}
}

func TestDecodePrimitiveValue_TruncatedInt64(t *testing.T) {
	data := []byte{0x18, 0x01, 0x02, 0x03, 0x04} // primitive_type=6 (int64), but only 4 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated int64")
	}
}

func TestDecodePrimitiveValue_TruncatedDouble(t *testing.T) {
	data := []byte{0x1C, 0x01, 0x02, 0x03, 0x04} // primitive_type=7 (double), but only 4 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated double")
	}
}

func TestDecodePrimitiveValue_TruncatedFloat(t *testing.T) {
	data := []byte{0x38, 0x01, 0x02} // primitive_type=14 (float), but only 2 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated float")
	}
}

func TestDecodePrimitiveValue_TruncatedDate(t *testing.T) {
	data := []byte{0x2C, 0x01, 0x02} // primitive_type=11 (date), but only 2 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated date")
	}
}

func TestDecodePrimitiveValue_TruncatedTimestamp(t *testing.T) {
	data := []byte{0x30, 0x01, 0x02, 0x03, 0x04} // primitive_type=12 (timestamp), but only 4 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated timestamp")
	}
}

func TestDecodePrimitiveValue_TruncatedLongString(t *testing.T) {
	// Long string with length but truncated content
	data := []byte{0x40, 0x0A, 0x00, 0x00, 0x00, 'h', 'e', 'l'} // length=10, but only 3 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated long string")
	}
}

func TestDecodePrimitiveValue_TruncatedLongStringLength(t *testing.T) {
	// Long string with truncated length field
	data := []byte{0x40, 0x0A, 0x00} // only 2 bytes of length instead of 4
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated string length")
	}
}

func TestDecodePrimitiveValue_TruncatedBinary(t *testing.T) {
	// Binary with length but truncated content
	data := []byte{0x3C, 0x0A, 0x00, 0x00, 0x00, 0x01, 0x02} // length=10, but only 2 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated binary")
	}
}

func TestDecodePrimitiveValue_TruncatedBinaryLength(t *testing.T) {
	data := []byte{0x3C, 0x0A, 0x00} // only 2 bytes of length
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated binary length")
	}
}

func TestDecodePrimitiveValue_TruncatedUUID(t *testing.T) {
	data := []byte{0x50, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08} // only 8 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated UUID")
	}
}

func TestDecodePrimitiveValue_TruncatedDecimal4(t *testing.T) {
	data := []byte{0x20, 0x02, 0x01, 0x02} // scale + only 2 bytes of value
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated decimal4")
	}
}

func TestDecodePrimitiveValue_TruncatedDecimal8(t *testing.T) {
	data := []byte{0x24, 0x02, 0x01, 0x02, 0x03, 0x04} // scale + only 4 bytes of value
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated decimal8")
	}
}

func TestDecodePrimitiveValue_TruncatedDecimal16(t *testing.T) {
	data := []byte{0x28, 0x02, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08} // scale + only 8 bytes
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated decimal16")
	}
}

// Additional error tests for decode functions

func TestDecodeVariantValueAt_OutOfBounds(t *testing.T) {
	data := []byte{0x00}
	meta := &variantMetadata{dictionary: []string{}}
	// Try to decode at offset beyond data length
	_, _, err := decodeVariantValueAt(data, 5, meta)
	if err == nil {
		t.Error("expected error for offset out of bounds")
	}
}

func TestDecodeObjectValue_TruncatedNumElements(t *testing.T) {
	// Object header but no num_elements
	data := []byte{0x02} // object, but no num_elements
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated object")
	}
}

func TestDecodeArrayValue_TruncatedNumElements(t *testing.T) {
	// Array header but no num_elements
	data := []byte{0x03} // array, but no num_elements
	meta := &variantMetadata{dictionary: []string{}}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated array")
	}
}

func TestDecodeMetadata_TruncatedOffsets(t *testing.T) {
	// Header + dict_size but not enough offsets
	// 0x01 = version=1, offset_size=1
	data := []byte{0x01, 0x05, 0x00} // dict_size=5, but only 1 offset
	_, err := decodeVariantMetadata(data)
	if err == nil {
		t.Error("expected error for truncated offsets")
	}
}

// Tests for shredded variant encoding functions

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
