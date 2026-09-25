package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Large array/object tests

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

	// object_header = is_large << 4 | field_id_size_minus_one << 2 | field_offset_size_minus_one
	// field_id_size=1, field_offset_size=1, is_large=1, so object_header = 0x10
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

// TestDecodeObjectValue_LargeObjTruncatedNumElements covers the isLarge num_elements bounds check.
// valueHeader bit4=1 -> isLarge; valueMetadata = (0x10<<2)|2 = 0x42.
func TestDecodeObjectValue_LargeObjTruncatedNumElements(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	// Header claims large object but lacks 4 bytes for num_elements.
	data := []byte{0x42, 0x00, 0x00}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated large object num_elements")
	}
}

// TestDecodeObjectValue_TruncatedFieldIDs covers the field-ID bounds check.
// Non-large object (valueHeader=0), num_elements=1 but only 2 bytes total.
func TestDecodeObjectValue_TruncatedFieldIDs(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{"x"}}
	// header + num_elements=1, but fieldID bytes are missing.
	data := []byte{0x02, 0x01}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated object field IDs")
	}
}

// TestDecodeObjectValue_TruncatedFieldOffsets covers the field-offset bounds check.
// header + num_elements=1 + fieldID=0 but field-offset bytes are missing.
func TestDecodeObjectValue_TruncatedFieldOffsets(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{"x"}}
	data := []byte{0x02, 0x01, 0x00}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated object field offsets")
	}
}

// TestDecodeObjectValue_FieldIDOutOfRange covers the field-ID-exceeds-dictionary check.
// Empty dictionary but fieldID=0 in the data.
func TestDecodeObjectValue_FieldIDOutOfRange(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	// header + num_elements=1 + fieldID=0 + two fieldOffset bytes (0, 0)
	data := []byte{0x02, 0x01, 0x00, 0x00, 0x00}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for field ID exceeding dictionary size")
	}
}

// TestDecodeArrayValue_LargeArrayTruncatedNumElements covers the large-array num_elements bounds check.
// valueHeader=4 -> elementOffsetSize=1, isLarge=1; valueMetadata=(4<<2)|3=0x13.
func TestDecodeArrayValue_LargeArrayTruncatedNumElements(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	// Header claims large array but only 2 bytes total (need 5 for large num_elements).
	data := []byte{0x13, 0x00}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated large array num_elements")
	}
}

// TestDecodeArrayValue_TruncatedElementOffsets covers the element-offset bounds check.
// Non-large array (valueHeader=0), num_elements=2 but insufficient bytes for offsets.
func TestDecodeArrayValue_TruncatedElementOffsets(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	// header(0x03) + num_elements=2, needs 3 offset bytes but none follow.
	data := []byte{0x03, 0x02}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for truncated array element offsets")
	}
}

// TestDecodeObjectValue_FieldDecodeError covers the "decode object field" error path.
// The object header is valid but the embedded value bytes are corrupt (truncated short string).
func TestDecodeObjectValue_FieldDecodeError(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{"x"}}
	// object: 1 field, fieldID=0, offset[0]=0, offset[1]=1; value is 0x05 (short string len=1, no data)
	data := []byte{0x02, 0x01, 0x00, 0x00, 0x01, 0x05}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for corrupt object field value")
	}
}

// TestDecodeArrayValue_ElementDecodeError covers the "decode array element" error path.
// The array header is valid but the embedded value bytes are corrupt (truncated short string).
func TestDecodeArrayValue_ElementDecodeError(t *testing.T) {
	meta := &variantMetadata{dictionary: []string{}}
	// array: 1 element, offset[0]=0, offset[1]=1; value is 0x05 (short string len=1, no data)
	data := []byte{0x03, 0x01, 0x00, 0x01, 0x05}
	_, err := decodeVariantValue(data, meta)
	if err == nil {
		t.Error("expected error for corrupt array element value")
	}
}

func TestDecodeObjectValue_SizeBits(t *testing.T) {
	// object_header = is_large << 4 | field_id_size_minus_one << 2 | field_offset_size_minus_one.
	// Both vectors hold {"a": int8(7), "b": int8(8)} with unequal widths.
	wide := []byte{ // one-byte field IDs, two-byte offsets
		0x06,       // object, object_header=0x01
		0x02,       // num_elements=2
		0x00, 0x01, // field IDs
		0x00, 0x00, 0x02, 0x00, 0x04, 0x00, // offsets 0, 2, 4
		0x0C, 0x07, 0x0C, 0x08, // int8(7), int8(8)
	}
	narrow := []byte{ // two-byte field IDs, one-byte offsets
		0x12,                   // object, object_header=0x04
		0x02,                   // num_elements=2
		0x00, 0x00, 0x01, 0x00, // field IDs
		0x00, 0x02, 0x04, // offsets 0, 2, 4
		0x0C, 0x07, 0x0C, 0x08, // int8(7), int8(8)
	}
	// Releases up to v3.8.3 wrote the two widths in the opposite bit positions,
	// so the same bodies carry the other header byte.
	legacyWide := append([]byte{0x12}, wide[1:]...)
	legacyNarrow := append([]byte{0x06}, narrow[1:]...)

	meta := &variantMetadata{dictionary: []string{"a", "b"}}
	testCases := []struct {
		name string
		data []byte
	}{
		{"two byte offsets", wide},
		{"two byte field IDs", narrow},
		{"legacy two byte offsets", legacyWide},
		{"legacy two byte field IDs", legacyNarrow},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := decodeVariantValue(tc.data, meta)
			require.NoError(t, err)
			require.Equal(t, map[string]any{"a": int8(7), "b": int8(8)}, val)
		})
	}
}

func TestDecodeObjectValue_LegacyShortParse(t *testing.T) {
	// The spec widths read this object as a one-byte empty string and stop after seven of
	// its 264 bytes, so the older layout cannot be reached by waiting for an error.
	value := EncodeVariantString(strings.Repeat("x", 252))
	legacy := EncodeVariantObject([]int{0}, [][]byte{value})
	legacy[0] = 0x12 // the widths as releases up to v3.8.3 wrote them

	val, err := decodeVariantValue(legacy, &variantMetadata{dictionary: []string{"a"}})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": strings.Repeat("x", 252)}, val)
}
