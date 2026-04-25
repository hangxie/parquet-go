package types

import (
	"encoding/binary"
	"math"
	"slices"
)

// EncodeVariantMetadata creates variant metadata with the given dictionary strings.
// Returns the encoded metadata bytes.
// Note: For optimal lookup performance, provide dictionary strings in sorted order.
// The function will detect if strings are sorted and set the sorted_strings flag accordingly.
func EncodeVariantMetadata(dictionary []string) []byte {
	if len(dictionary) == 0 {
		// Empty dictionary: header (version=1, offset_size=1) + dict_size(0) + one offset(0)
		return []byte{0x01, 0x00, 0x00}
	}

	// Check if dictionary is already sorted and unique
	isSorted := slices.IsSorted(dictionary)

	// Calculate total string length and determine offset size
	totalLen := 0
	for _, s := range dictionary {
		totalLen += len(s)
	}

	// Determine offset size (1, 2, 3, or 4 bytes)
	offsetSize := 1
	if totalLen > 255 {
		offsetSize = 2
	}
	if totalLen > 65535 {
		offsetSize = 4
	}

	// Header byte layout (per Parquet Variant spec):
	//   bits 0-3: version (must be 1)
	//   bit 4: sorted_strings
	//   bits 5-6: offset_size_minus_one
	//   header := byte(0x01 | ((offsetSize - 1) << 5)) // version=1 in low nibble
	header := byte(0x01 | ((offsetSize - 1) << 5)) // version=1 in low nibble
	if isSorted {
		header |= 0x10 // Set sorted_strings bit (bit 4)
	}

	// Build metadata
	result := []byte{header}

	// Dictionary size
	result = appendLittleEndianUint(result, uint64(len(dictionary)), offsetSize)

	// Offsets
	offset := uint64(0)
	for _, s := range dictionary {
		result = appendLittleEndianUint(result, offset, offsetSize)
		offset += uint64(len(s))
	}
	result = appendLittleEndianUint(result, offset, offsetSize) // Final offset

	// Dictionary strings
	for _, s := range dictionary {
		result = append(result, []byte(s)...)
	}

	return result
}

// appendLittleEndianUint appends an unsigned integer in little-endian order
func appendLittleEndianUint(buf []byte, val uint64, size int) []byte {
	for i := range size {
		buf = append(buf, byte(val>>(8*i)))
	}
	return buf
}

// EncodeVariantMetadataSorted creates variant metadata with sorted dictionary strings.
// It takes a list of field names, sorts them, and returns:
// - The encoded metadata bytes
// - A map from field name to field ID (index in sorted order)
// This is the recommended way to create metadata for optimal lookup performance.
func EncodeVariantMetadataSorted(fieldNames []string) ([]byte, map[string]int) {
	if len(fieldNames) == 0 {
		return []byte{0x01, 0x00, 0x00}, map[string]int{}
	}

	// Create sorted copy
	sorted := make([]string, len(fieldNames))
	copy(sorted, fieldNames)
	slices.Sort(sorted)

	// Build field ID map
	fieldIDs := make(map[string]int, len(sorted))
	for i, name := range sorted {
		fieldIDs[name] = i
	}

	// Encode with sorted dictionary
	metadata := EncodeVariantMetadata(sorted)

	return metadata, fieldIDs
}

// EncodeVariantNull returns a variant-encoded null value
func EncodeVariantNull() []byte {
	return []byte{0x00} // basic_type=0 (primitive), primitive_type=0 (null)
}

// EncodeVariantBool returns a variant-encoded boolean value
func EncodeVariantBool(b bool) []byte {
	if b {
		return []byte{0x04} // basic_type=0, primitive_type=1 (true)
	}
	return []byte{0x08} // basic_type=0, primitive_type=2 (false)
}

// EncodeVariantInt32 returns a variant-encoded int32 value
func EncodeVariantInt32(v int32) []byte {
	buf := make([]byte, 5)
	buf[0] = 0x14 // basic_type=0, primitive_type=5 (int32)
	binary.LittleEndian.PutUint32(buf[1:], uint32(v))
	return buf
}

// EncodeVariantInt64 returns a variant-encoded int64 value
func EncodeVariantInt64(v int64) []byte {
	buf := make([]byte, 9)
	buf[0] = 0x18 // basic_type=0, primitive_type=6 (int64)
	binary.LittleEndian.PutUint64(buf[1:], uint64(v))
	return buf
}

// EncodeVariantDouble returns a variant-encoded double value
func EncodeVariantDouble(v float64) []byte {
	buf := make([]byte, 9)
	buf[0] = 0x1C // basic_type=0, primitive_type=7 (double)
	binary.LittleEndian.PutUint64(buf[1:], math.Float64bits(v))
	return buf
}

// EncodeVariantString returns a variant-encoded string value
func EncodeVariantString(s string) []byte {
	if len(s) < 64 {
		// Short string: basic_type=1, length in value_header
		buf := make([]byte, 1+len(s))
		buf[0] = byte(0x01 | (len(s) << 2)) // basic_type=1, length in upper 6 bits
		copy(buf[1:], s)
		return buf
	}
	// Long string: basic_type=0, primitive_type=16
	buf := make([]byte, 5+len(s))
	buf[0] = 0x40 // basic_type=0, primitive_type=16 (string)
	binary.LittleEndian.PutUint32(buf[1:], uint32(len(s)))
	copy(buf[5:], s)
	return buf
}

// EncodeVariantObject returns a variant-encoded object value.
// The fieldIDs must correspond to indices in the metadata dictionary.
// Each value should already be variant-encoded.
func EncodeVariantObject(fieldIDs []int, values [][]byte) []byte {
	if len(fieldIDs) != len(values) {
		return nil
	}

	numElements := len(fieldIDs)
	if numElements == 0 {
		return []byte{0x02, 0x00} // Empty object
	}

	// Calculate total value size for offset sizing
	totalValueSize := 0
	for _, v := range values {
		totalValueSize += len(v)
	}

	// Determine field ID size and offset size
	maxFieldID := 0
	for _, id := range fieldIDs {
		if id > maxFieldID {
			maxFieldID = id
		}
	}

	fieldIDSize := 1
	if maxFieldID > 255 {
		fieldIDSize = 2
	}
	if maxFieldID > 65535 {
		fieldIDSize = 4
	}

	offsetSize := 1
	if totalValueSize > 255 {
		offsetSize = 2
	}
	if totalValueSize > 65535 {
		offsetSize = 4
	}

	isLarge := numElements > 255

	// Object header: field_id_size_minus_one (2 bits) | field_offset_size_minus_one (2 bits) | is_large (1 bit)
	valueHeader := byte((fieldIDSize - 1) | ((offsetSize - 1) << 2))
	if isLarge {
		valueHeader |= 0x10
	}

	// value_metadata: basic_type=2 (object) | value_header
	buf := []byte{byte(0x02 | (valueHeader << 2))}

	// num_elements
	if isLarge {
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, uint32(numElements))
		buf = append(buf, tmp...)
	} else {
		buf = append(buf, byte(numElements))
	}

	// Field IDs
	for _, id := range fieldIDs {
		buf = appendLittleEndianUint(buf, uint64(id), fieldIDSize)
	}

	// Field offsets
	offset := 0
	for _, v := range values {
		buf = appendLittleEndianUint(buf, uint64(offset), offsetSize)
		offset += len(v)
	}
	buf = appendLittleEndianUint(buf, uint64(offset), offsetSize)

	// Values
	for _, v := range values {
		buf = append(buf, v...)
	}

	return buf
}

// EncodeVariantArray returns a variant-encoded array value.
// Each element should already be variant-encoded.
func EncodeVariantArray(elements [][]byte) []byte {
	numElements := len(elements)
	if numElements == 0 {
		return []byte{0x03, 0x00} // Empty array
	}

	// Calculate total element size
	totalSize := 0
	for _, e := range elements {
		totalSize += len(e)
	}

	offsetSize := 1
	if totalSize > 255 {
		offsetSize = 2
	}
	if totalSize > 65535 {
		offsetSize = 4
	}

	isLarge := numElements > 255

	// Array header: element_offset_size_minus_one (2 bits) | is_large (1 bit)
	valueHeader := byte(offsetSize - 1)
	if isLarge {
		valueHeader |= 0x04
	}

	// value_metadata: basic_type=3 (array) | value_header
	buf := []byte{byte(0x03 | (valueHeader << 2))}

	// num_elements
	if isLarge {
		tmp := make([]byte, 4)
		binary.LittleEndian.PutUint32(tmp, uint32(numElements))
		buf = append(buf, tmp...)
	} else {
		buf = append(buf, byte(numElements))
	}

	// Element offsets
	offset := 0
	for _, e := range elements {
		buf = appendLittleEndianUint(buf, uint64(offset), offsetSize)
		offset += len(e)
	}
	buf = appendLittleEndianUint(buf, uint64(offset), offsetSize)

	// Elements
	for _, e := range elements {
		buf = append(buf, e...)
	}

	return buf
}

// EncodeVariantFloat returns a variant-encoded float32 value
func EncodeVariantFloat(v float32) []byte {
	buf := make([]byte, 5)
	buf[0] = 0x38 // basic_type=0, primitive_type=14 (float)
	binary.LittleEndian.PutUint32(buf[1:], math.Float32bits(v))
	return buf
}

// EncodeVariantInt8 returns a variant-encoded int8 value
func EncodeVariantInt8(v int8) []byte {
	return []byte{0x0C, byte(v)} // basic_type=0, primitive_type=3 (int8)
}

// EncodeVariantInt16 returns a variant-encoded int16 value
func EncodeVariantInt16(v int16) []byte {
	buf := make([]byte, 3)
	buf[0] = 0x10 // basic_type=0, primitive_type=4 (int16)
	binary.LittleEndian.PutUint16(buf[1:], uint16(v))
	return buf
}
