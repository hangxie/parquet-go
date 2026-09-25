package types

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
	// header = version | sorted_strings << 4 | offset_size_minus_one << 6, bit 5 reserved
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

	// object_header = is_large << 4 | field_id_size_minus_one << 2 | field_offset_size_minus_one
	// field_id_size=1, field_offset_size=1, is_large=0, so object_header = 0
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
	tests := []struct {
		name     string
		metadata []byte
	}{
		{name: "empty metadata"},
		{name: "malformed metadata", metadata: []byte{0xff}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := ConvertVariantValue(Variant{Metadata: tt.metadata})
			require.NoError(t, err)
			require.Nil(t, val)
		})
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
	v := Variant{
		Metadata: []byte{0xFF, 0xFF}, // invalid
		Value:    []byte{0x04},       // true
	}
	val, err := ConvertVariantValue(v)
	require.ErrorIs(t, err, ErrUnrenderable)
	require.ErrorContains(t, err, "decode metadata")
	require.Equal(t, map[string]any{"metadata": "//8=", "value": "BA=="}, val)
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

// Tests for encoding functions

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

// Additional error tests for decode functions

func TestDecodeVariantValueAt_OutOfBounds(t *testing.T) {
	data := []byte{0x00}
	meta := &variantMetadata{dictionary: []string{}}
	// Try to decode at offset beyond data length
	budget := 1024
	_, _, err := decodeVariantValueAt(data, 5, meta, &budget)
	if err == nil {
		t.Error("expected error for offset out of bounds")
	}
}

func TestDecodeVariantValueAt_BudgetExhausted(t *testing.T) {
	// The decode budget caps total operations to prevent the CPU exhaustion
	// fuzzing found with arrays of same-offset references. Once exhausted, any
	// further decode must error rather than recurse.
	budget := 0
	_, _, err := decodeVariantValueAt([]byte{0x00}, 0, &variantMetadata{}, &budget)
	if err == nil {
		t.Error("expected error when decode budget is exhausted")
	}
}

// TestConvertVariantValue_CorruptValue covers the value decode error path in ConvertVariantValue.
// Valid metadata + a value byte that claims a longer short string than available triggers the error.
func TestConvertVariantValue_CorruptValue(t *testing.T) {
	v := Variant{
		// Valid empty-dictionary metadata
		Metadata: EncodeVariantMetadata([]string{}),
		// basicType=1 (short string), length=1 but no string bytes follow - decode error
		Value: []byte{0x05},
	}
	val, err := ConvertVariantValue(v)
	require.ErrorIs(t, err, ErrUnrenderable)
	require.ErrorContains(t, err, "decode value")
	require.Equal(t, map[string]any{"metadata": "AQAA", "value": "BQ=="}, val)
}

func TestDecodeVariantValueConsumedLength(t *testing.T) {
	longString := EncodeVariantString(strings.Repeat("x", 64))
	tests := []struct {
		name  string
		value []byte
	}{
		{name: "null", value: EncodeVariantNull()},
		{name: "boolean", value: EncodeVariantBool(true)},
		{name: "fixed width primitive", value: EncodeVariantInt32(42)},
		{name: "short string", value: EncodeVariantString("text")},
		{name: "long string", value: longString},
		{name: "legacy empty array", value: []byte{0x03, 0x00}},
		{name: "spec empty array", value: []byte{0x03, 0x00, 0x00}},
		{name: "array", value: EncodeVariantArray([][]byte{EncodeVariantBool(true)})},
		{name: "legacy empty object", value: []byte{0x02, 0x00}},
		{name: "spec empty object", value: []byte{0x02, 0x00, 0x00}},
		{name: "object", value: EncodeVariantObject([]int{0}, [][]byte{EncodeVariantBool(true)})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			budget := 1024
			consumed, _, err := decodeVariantValueAt(tt.value, 0, &variantMetadata{dictionary: []string{"x"}}, &budget)
			require.NoError(t, err)
			require.Equal(t, len(tt.value), consumed)
		})
	}
}

func TestDecodeVariantValueRejectsTrailingData(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
	}{
		{name: "null", value: EncodeVariantNull()},
		{name: "boolean", value: EncodeVariantBool(true)},
		{name: "fixed width primitive", value: EncodeVariantInt32(42)},
		{name: "short string", value: EncodeVariantString("text")},
		{name: "long string", value: EncodeVariantString(strings.Repeat("x", 64))},
		{name: "legacy empty array", value: []byte{0x03, 0x00}},
		{name: "array", value: EncodeVariantArray([][]byte{EncodeVariantBool(true)})},
		{name: "legacy empty object", value: []byte{0x02, 0x00}},
		{name: "object", value: EncodeVariantObject([]int{0}, [][]byte{EncodeVariantBool(true)})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := append(append([]byte{}, tt.value...), 0xff)
			_, err := decodeVariantValue(value, &variantMetadata{dictionary: []string{"x"}})
			require.Error(t, err)
		})
	}
}

func TestDecodeVariantValueRejectsMalformedContainerOffsets(t *testing.T) {
	tests := []struct {
		name  string
		meta  *variantMetadata
		value []byte
	}{
		{
			name:  "trailing bytes in array element",
			meta:  &variantMetadata{},
			value: []byte{0x03, 0x01, 0x00, 0x02, 0x04, 0xff},
		},
		{
			name:  "array first offset is not zero",
			meta:  &variantMetadata{},
			value: []byte{0x03, 0x01, 0x01, 0x02, 0xff, 0x04},
		},
		{
			name:  "array offsets overlap",
			meta:  &variantMetadata{},
			value: []byte{0x03, 0x02, 0x00, 0x00, 0x01, 0x04},
		},
		{
			name:  "trailing bytes in object field",
			meta:  &variantMetadata{dictionary: []string{"x"}},
			value: []byte{0x02, 0x01, 0x00, 0x00, 0x02, 0x04, 0xff},
		},
		{
			name:  "object field offsets overlap",
			meta:  &variantMetadata{dictionary: []string{"x", "y"}},
			value: []byte{0x02, 0x02, 0x00, 0x01, 0x00, 0x00, 0x01, 0x04},
		},
		{
			name:  "empty object has nonzero final offset",
			meta:  &variantMetadata{},
			value: []byte{0x02, 0x00, 0x01, 0xff},
		},
		{
			name:  "object first sorted offset is not zero",
			meta:  &variantMetadata{dictionary: []string{"x"}},
			value: []byte{0x02, 0x01, 0x00, 0x01, 0x02, 0xff, 0x04},
		},
		{
			name:  "object first physical offset is not zero",
			meta:  &variantMetadata{dictionary: []string{"x", "y"}},
			value: []byte{0x02, 0x02, 0x00, 0x01, 0x02, 0x01, 0x03, 0xff, 0x04, 0x08},
		},
		{
			name:  "out-of-order object offsets overlap",
			meta:  &variantMetadata{dictionary: []string{"x", "y"}},
			value: []byte{0x02, 0x02, 0x00, 0x01, 0x01, 0x00, 0x01, 0x04},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeVariantValue(tt.value, tt.meta)
			require.Error(t, err)
		})
	}
}

func TestDecodeVariantValueRejectsNonLegacyEmptyContainersWithoutOffsets(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
	}{
		{name: "array with four-byte offsets", value: []byte{0x0f, 0x00}},
		{name: "large array", value: []byte{0x13, 0x00, 0x00, 0x00, 0x00}},
		{name: "object with two-byte field IDs", value: []byte{0x06, 0x00}},
		{name: "object with two-byte offsets", value: []byte{0x12, 0x00}},
		{name: "large object", value: []byte{0x42, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeVariantValue(tt.value, &variantMetadata{})
			require.Error(t, err)
		})
	}
}

func TestDecodeVariantValueObjectOffsetsMayBeOutOfOrder(t *testing.T) {
	value := []byte{
		0x02, 0x02, // object, two fields
		0x00, 0x01, // field IDs for a and b
		0x01, 0x00, 0x02, // a starts second, b starts first, total length is two
		0x08, 0x04, // false, true
	}

	got, err := decodeVariantValue(value, &variantMetadata{dictionary: []string{"a", "b"}})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": true, "b": false}, got)
}

func TestConvertVariantValueTrailingData(t *testing.T) {
	v := Variant{Metadata: EncodeVariantMetadata(nil), Value: []byte{0x04, 0xff}}
	got, err := ConvertVariantValue(v)
	require.ErrorIs(t, err, ErrUnrenderable)
	require.ErrorContains(t, err, "decode value")
	require.Equal(t, map[string]any{"metadata": "AQAA", "value": "BP8="}, got)
}
