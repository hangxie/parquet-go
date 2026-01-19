package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strings"
)

// Variant represents a Parquet VARIANT type.
// According to the Parquet specification, VARIANT is a semi-structured data type
// that must be stored as a GROUP with two required binary fields:
//   - Metadata: contains the variant metadata (field dictionary, version info)
//   - Value: contains the encoded variant value
//
// The binary encoding format is defined in the Parquet Variant specification.
//
// # Shredding
//
// The Parquet VARIANT spec supports "shredding" - decomposing semi-structured data
// into typed columns for better compression and query performance. For example,
// a variant containing {"name": "John", "age": 30} could be shredded into separate
// typed columns for frequently-occurring fields.
//
// Current shredding support:
//   - Writing: Always produces UNSHREDDED output (metadata + value only)
//   - Reading: Shredded variant reading is implemented. The implementation
//     reconstructs the full Variant value from metadata, value, and
//     typed_value columns.
//
// # Canonicalization
//
// The Parquet Variant spec recommends canonical encoding for optimal interoperability:
//   - Dictionary strings should be sorted (enables binary search)
//   - Object fields should appear in field_id order
//
// This implementation:
//   - On DECODE: Accepts both sorted and unsorted dictionaries (spec-compliant).
//     Unsorted dictionaries may result in non-canonical Variant blobs where
//     semantically equal values have different byte representations.
//   - On ENCODE: Use EncodeVariantMetadataSorted for canonical output.
//     Regular EncodeVariantMetadata preserves input order and auto-detects sorting.
//
// Note: Decode followed by re-encode does NOT guarantee canonical form preservation.
// Dictionary order and object field order may differ from the original encoding.
type Variant struct {
	Metadata []byte
	Value    []byte
}

// Variant basic types (2-bit values in value_metadata)
// Defined in: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
const (
	variantBasicTypePrimitive   = 0
	variantBasicTypeShortString = 1
	variantBasicTypeObject      = 2
	variantBasicTypeArray       = 3
)

// Variant primitive type IDs (6-bit values in value_header for basic_type=0)
// Defined in: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
const (
	variantPrimitiveNull              = 0
	variantPrimitiveTrue              = 1
	variantPrimitiveFalse             = 2
	variantPrimitiveInt8              = 3
	variantPrimitiveInt16             = 4
	variantPrimitiveInt32             = 5
	variantPrimitiveInt64             = 6
	variantPrimitiveDouble            = 7
	variantPrimitiveDecimal4          = 8
	variantPrimitiveDecimal8          = 9
	variantPrimitiveDecimal16         = 10
	variantPrimitiveDate              = 11
	variantPrimitiveTimestampMicro    = 12 // UTC timestamp in microseconds
	variantPrimitiveTimestampNTZMicro = 13 // Local timestamp in microseconds (no timezone)
	variantPrimitiveFloat             = 14
	variantPrimitiveBinary            = 15
	variantPrimitiveString            = 16
	variantPrimitiveTimeNTZ           = 17 // Local time in microseconds (no timezone)
	variantPrimitiveTimestampNano     = 18 // UTC timestamp in nanoseconds
	variantPrimitiveTimestampNTZNano  = 19 // Local timestamp in nanoseconds (no timezone)
	variantPrimitiveUUID              = 20
)

// variantMetadata holds the decoded dictionary from variant metadata
type variantMetadata struct {
	dictionary []string
	sorted     bool
}

// decodeVariantMetadata decodes the variant metadata binary format.
// It accepts both sorted and unsorted dictionaries as per the spec.
// The sorted flag is recorded but not enforced - unsorted dictionaries
// are valid and will be processed correctly (linear lookup vs binary search).
func decodeVariantMetadata(data []byte) (*variantMetadata, error) {
	if len(data) == 0 {
		return &variantMetadata{dictionary: []string{}}, nil
	}

	// Header byte layout (per Parquet Variant spec):
	//   bits 0-3: version (must be 1)
	//   bit 4: sorted_strings
	//   bits 5-6: offset_size_minus_one
	//   bit 7: unused
	header := data[0]
	version := header & 0x0F
	if version != 1 {
		return nil, fmt.Errorf("unsupported variant metadata version: %d", version)
	}

	sortedStrings := (header>>4)&1 == 1
	offsetSize := int(((header >> 5) & 0x03) + 1)

	pos := 1

	// Read dictionary_size
	if pos+offsetSize > len(data) {
		return nil, fmt.Errorf("variant metadata too short for dictionary size")
	}
	dictSize := readLittleEndianUint(data[pos:pos+offsetSize], offsetSize)
	pos += offsetSize

	// Read offsets (dictSize + 1 offsets)
	numOffsets := int(dictSize) + 1
	if pos+numOffsets*offsetSize > len(data) {
		return nil, fmt.Errorf("variant metadata too short for offsets")
	}

	offsets := make([]uint64, numOffsets)
	for i := range numOffsets {
		offsets[i] = readLittleEndianUint(data[pos:pos+offsetSize], offsetSize)
		pos += offsetSize
	}

	// Read dictionary strings
	dictionary := make([]string, dictSize)
	for i := range int(dictSize) {
		start := int(offsets[i])
		end := int(offsets[i+1])
		if pos+end > len(data) {
			return nil, fmt.Errorf("variant metadata string offset out of bounds")
		}
		dictionary[i] = string(data[pos+start : pos+end])
	}

	return &variantMetadata{
		dictionary: dictionary,
		sorted:     sortedStrings,
	}, nil
}

// readLittleEndianUint reads an unsigned integer of the specified size in little-endian order
func readLittleEndianUint(data []byte, size int) uint64 {
	var result uint64
	for i := range size {
		result |= uint64(data[i]) << (8 * i)
	}
	return result
}

// readLittleEndianInt128 reads a 128-bit signed integer in little-endian order
func readLittleEndianInt128(data []byte) *big.Int {
	// Read as little-endian: reverse bytes to get big-endian for big.Int
	reversed := make([]byte, 16)
	for i := 0; i < 16; i++ {
		reversed[15-i] = data[i]
	}
	// big.Int.SetBytes interprets as unsigned, so handle sign separately
	result := new(big.Int).SetBytes(reversed)
	// Check if negative (high bit set)
	if reversed[0]&0x80 != 0 {
		// Two's complement: subtract 2^128
		twoTo128 := new(big.Int).Lsh(big.NewInt(1), 128)
		result.Sub(result, twoTo128)
	}
	return result
}

// formatDecimal formats an int64 unscaled value with the given scale as a decimal string
func formatDecimal(unscaled int64, scale int) string {
	if scale == 0 {
		return fmt.Sprintf("%d", unscaled)
	}

	r := new(big.Rat).SetInt64(unscaled)
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	r.Quo(r, new(big.Rat).SetInt(divisor))

	result := r.FloatString(scale)

	// Remove trailing zeros after decimal point
	result = strings.TrimRight(result, "0")
	result = strings.TrimRight(result, ".")

	return result
}

// formatDecimal128 formats a big.Int unscaled value with the given scale as a decimal string
func formatDecimal128(unscaled *big.Int, scale int) string {
	if scale == 0 {
		return unscaled.String()
	}

	r := new(big.Rat).SetInt(unscaled)
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	r.Quo(r, new(big.Rat).SetInt(divisor))

	result := r.FloatString(scale)

	// Remove trailing zeros after decimal point
	result = strings.TrimRight(result, "0")
	result = strings.TrimRight(result, ".")

	return result
}

// decodeVariantValue decodes a variant value using the provided metadata
func decodeVariantValue(data []byte, meta *variantMetadata) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	_, val, err := decodeVariantValueAt(data, 0, meta)
	return val, err
}

// decodeVariantValueAt decodes a variant value starting at the given offset
// Returns the number of bytes consumed and the decoded value
func decodeVariantValueAt(data []byte, offset int, meta *variantMetadata) (int, any, error) {
	if offset >= len(data) {
		return 0, nil, fmt.Errorf("variant value offset out of bounds")
	}

	// value_metadata byte: basic_type (2 bits) | value_header (6 bits)
	valueMetadata := data[offset]
	basicType := valueMetadata & 0x03
	valueHeader := valueMetadata >> 2

	switch basicType {
	case variantBasicTypePrimitive:
		return decodePrimitiveValue(data, offset+1, valueHeader)

	case variantBasicTypeShortString:
		// Short string: value_header contains the length (0-63)
		length := int(valueHeader)
		if offset+1+length > len(data) {
			return 0, nil, fmt.Errorf("short string length exceeds data")
		}
		return 1 + length, string(data[offset+1 : offset+1+length]), nil

	case variantBasicTypeObject:
		return decodeObjectValue(data, offset, valueHeader, meta)

	case variantBasicTypeArray:
		return decodeArrayValue(data, offset, valueHeader, meta)

	default:
		return 0, nil, fmt.Errorf("unknown variant basic type: %d", basicType)
	}
}

// decodePrimitiveValue decodes a primitive variant value
func decodePrimitiveValue(data []byte, offset int, primitiveType uint8) (int, any, error) {
	switch primitiveType {
	case variantPrimitiveNull:
		return 0, nil, nil

	case variantPrimitiveTrue:
		return 0, true, nil

	case variantPrimitiveFalse:
		return 0, false, nil

	case variantPrimitiveInt8:
		if offset >= len(data) {
			return 0, nil, fmt.Errorf("not enough data for int8")
		}
		return 1, int8(data[offset]), nil

	case variantPrimitiveInt16:
		if offset+2 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for int16")
		}
		return 2, int16(binary.LittleEndian.Uint16(data[offset:])), nil

	case variantPrimitiveInt32:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for int32")
		}
		return 4, int32(binary.LittleEndian.Uint32(data[offset:])), nil

	case variantPrimitiveInt64:
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for int64")
		}
		return 8, int64(binary.LittleEndian.Uint64(data[offset:])), nil

	case variantPrimitiveDouble:
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for double")
		}
		bits := binary.LittleEndian.Uint64(data[offset:])
		return 8, math.Float64frombits(bits), nil

	case variantPrimitiveFloat:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for float")
		}
		bits := binary.LittleEndian.Uint32(data[offset:])
		return 4, math.Float32frombits(bits), nil

	case variantPrimitiveDate:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for date")
		}
		days := int32(binary.LittleEndian.Uint32(data[offset:]))
		return 4, ConvertDateLogicalValue(days), nil

	case variantPrimitiveTimestampMicro, variantPrimitiveTimestampNTZMicro:
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for timestamp")
		}
		micros := int64(binary.LittleEndian.Uint64(data[offset:]))
		// Convert to ISO8601 format (with Z suffix for UTC, without for NTZ)
		return 8, TIMESTAMP_MICROSToISO8601(micros, primitiveType == variantPrimitiveTimestampMicro), nil

	case variantPrimitiveTimeNTZ:
		// Time without timezone: 8-byte little-endian microseconds since midnight
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for time")
		}
		micros := int64(binary.LittleEndian.Uint64(data[offset:]))
		// Format as HH:MM:SS.ffffff
		hours := micros / 3600000000
		micros %= 3600000000
		minutes := micros / 60000000
		micros %= 60000000
		seconds := micros / 1000000
		microsFrac := micros % 1000000
		return 8, fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, microsFrac), nil

	case variantPrimitiveTimestampNano, variantPrimitiveTimestampNTZNano:
		// Timestamp in nanoseconds
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for timestamp")
		}
		nanos := int64(binary.LittleEndian.Uint64(data[offset:]))
		// Convert to ISO8601 format with nanosecond precision
		return 8, TIMESTAMP_NANOSToISO8601(nanos, primitiveType == variantPrimitiveTimestampNano), nil

	case variantPrimitiveString:
		// Long string: 4-byte length followed by UTF-8 bytes
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for string length")
		}
		length := int(binary.LittleEndian.Uint32(data[offset:]))
		if offset+4+length > len(data) {
			return 0, nil, fmt.Errorf("string length exceeds data")
		}
		return 4 + length, string(data[offset+4 : offset+4+length]), nil

	case variantPrimitiveBinary:
		// Binary: 4-byte length followed by bytes
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for binary length")
		}
		length := int(binary.LittleEndian.Uint32(data[offset:]))
		if offset+4+length > len(data) {
			return 0, nil, fmt.Errorf("binary length exceeds data")
		}
		// Return as base64 for JSON compatibility
		return 4 + length, base64.StdEncoding.EncodeToString(data[offset+4 : offset+4+length]), nil

	case variantPrimitiveUUID:
		if offset+16 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for UUID")
		}
		return 16, ConvertUUIDValue(data[offset : offset+16]), nil

	case variantPrimitiveDecimal4:
		// Decimal4: 1 byte scale + 4 bytes unscaled value (little-endian int32)
		if offset+5 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for decimal4")
		}
		scale := int(data[offset])
		unscaled := int64(int32(binary.LittleEndian.Uint32(data[offset+1:])))
		return 5, formatDecimal(unscaled, scale), nil

	case variantPrimitiveDecimal8:
		// Decimal8: 1 byte scale + 8 bytes unscaled value (little-endian int64)
		if offset+9 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for decimal8")
		}
		scale := int(data[offset])
		unscaled := int64(binary.LittleEndian.Uint64(data[offset+1:]))
		return 9, formatDecimal(unscaled, scale), nil

	case variantPrimitiveDecimal16:
		// Decimal16: 1 byte scale + 16 bytes unscaled value (little-endian int128)
		if offset+17 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for decimal16")
		}
		scale := int(data[offset])
		unscaled := readLittleEndianInt128(data[offset+1 : offset+17])
		return 17, formatDecimal128(unscaled, scale), nil

	default:
		return 0, nil, fmt.Errorf("unknown variant primitive type: %d", primitiveType)
	}
}

// decodeObjectValue decodes a variant object value
func decodeObjectValue(data []byte, offset int, valueHeader uint8, meta *variantMetadata) (int, any, error) {
	// Object header: field_id_size_minus_one (2 bits) | field_offset_size_minus_one (2 bits) | is_large (1 bit) | unused (1 bit)
	fieldIDSize := int((valueHeader & 0x03) + 1)
	fieldOffsetSize := int(((valueHeader >> 2) & 0x03) + 1)
	isLarge := (valueHeader>>4)&1 == 1

	pos := offset + 1

	// Read num_elements
	var numElements int
	if isLarge {
		if pos+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for large object num_elements")
		}
		numElements = int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
	} else {
		if pos >= len(data) {
			return 0, nil, fmt.Errorf("not enough data for object num_elements")
		}
		numElements = int(data[pos])
		pos++
	}

	if numElements == 0 {
		return pos - offset, map[string]any{}, nil
	}

	// Read field IDs
	if pos+numElements*fieldIDSize > len(data) {
		return 0, nil, fmt.Errorf("not enough data for object field IDs")
	}
	fieldIDs := make([]int, numElements)
	for i := range numElements {
		fieldIDs[i] = int(readLittleEndianUint(data[pos:pos+fieldIDSize], fieldIDSize))
		pos += fieldIDSize
	}

	// Read field offsets (numElements + 1)
	if pos+(numElements+1)*fieldOffsetSize > len(data) {
		return 0, nil, fmt.Errorf("not enough data for object field offsets")
	}
	fieldOffsets := make([]int, numElements+1)
	for i := range numElements + 1 {
		fieldOffsets[i] = int(readLittleEndianUint(data[pos:pos+fieldOffsetSize], fieldOffsetSize))
		pos += fieldOffsetSize
	}

	// Decode values
	valuesStart := pos
	result := make(map[string]any)
	for i := range numElements {
		fieldID := fieldIDs[i]
		if fieldID >= len(meta.dictionary) {
			return 0, nil, fmt.Errorf("field ID %d exceeds dictionary size %d", fieldID, len(meta.dictionary))
		}
		fieldName := meta.dictionary[fieldID]

		valueOffset := valuesStart + fieldOffsets[i]
		_, val, err := decodeVariantValueAt(data, valueOffset, meta)
		if err != nil {
			return 0, nil, fmt.Errorf("decode object field %q: %w", fieldName, err)
		}
		result[fieldName] = val
	}

	// Total consumed bytes
	totalConsumed := valuesStart + fieldOffsets[numElements] - offset
	return totalConsumed, result, nil
}

// decodeArrayValue decodes a variant array value
func decodeArrayValue(data []byte, offset int, valueHeader uint8, meta *variantMetadata) (int, any, error) {
	// Array header: element_offset_size_minus_one (2 bits) | is_large (1 bit) | unused (3 bits)
	elementOffsetSize := int((valueHeader & 0x03) + 1)
	isLarge := (valueHeader>>2)&1 == 1

	pos := offset + 1

	// Read num_elements
	var numElements int
	if isLarge {
		if pos+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for large array num_elements")
		}
		numElements = int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
	} else {
		if pos >= len(data) {
			return 0, nil, fmt.Errorf("not enough data for array num_elements")
		}
		numElements = int(data[pos])
		pos++
	}

	if numElements == 0 {
		return pos - offset, []any{}, nil
	}

	// Read element offsets (numElements + 1)
	if pos+(numElements+1)*elementOffsetSize > len(data) {
		return 0, nil, fmt.Errorf("not enough data for array element offsets")
	}
	elementOffsets := make([]int, numElements+1)
	for i := range numElements + 1 {
		elementOffsets[i] = int(readLittleEndianUint(data[pos:pos+elementOffsetSize], elementOffsetSize))
		pos += elementOffsetSize
	}

	// Decode values
	valuesStart := pos
	result := make([]any, numElements)
	for i := range numElements {
		valueOffset := valuesStart + elementOffsets[i]
		_, val, err := decodeVariantValueAt(data, valueOffset, meta)
		if err != nil {
			return 0, nil, fmt.Errorf("decode array element %d: %w", i, err)
		}
		result[i] = val
	}

	// Total consumed bytes
	totalConsumed := valuesStart + elementOffsets[numElements] - offset
	return totalConsumed, result, nil
}

// ConvertVariantValue decodes a Variant struct to its JSON-friendly representation
func ConvertVariantValue(v Variant) (any, error) {
	// Handle empty variant
	if len(v.Value) == 0 {
		return nil, nil
	}

	// Decode metadata
	meta, err := decodeVariantMetadata(v.Metadata)
	if err != nil {
		// If metadata decode fails, return base64 encoded raw data
		return map[string]any{
			"metadata": base64.StdEncoding.EncodeToString(v.Metadata),
			"value":    base64.StdEncoding.EncodeToString(v.Value),
		}, nil
	}

	// Decode value
	val, err := decodeVariantValue(v.Value, meta)
	if err != nil {
		// If value decode fails, return base64 encoded raw data
		return map[string]any{
			"metadata": base64.StdEncoding.EncodeToString(v.Metadata),
			"value":    base64.StdEncoding.EncodeToString(v.Value),
		}, nil
	}

	return val, nil
}

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

// EncodeGoValueAsVariant encodes a Go value as variant value bytes.
// This is used for converting typed_value columns back to variant format
// during shredded variant reconstruction.
//
// Supported types:
//   - nil: variant null
//   - bool: variant true/false
//   - int8, int16, int32, int64: variant integers
//   - float32: variant float
//   - float64: variant double
//   - string: variant string (short or long based on length)
//   - []byte: variant binary
//   - []any: variant array (recursively encodes elements)
//   - map[string]any: variant object (requires metadata for field IDs)
func EncodeGoValueAsVariant(v any) ([]byte, error) {
	if v == nil {
		return EncodeVariantNull(), nil
	}

	switch val := v.(type) {
	case bool:
		return EncodeVariantBool(val), nil
	case int8:
		return EncodeVariantInt8(val), nil
	case int16:
		return EncodeVariantInt16(val), nil
	case int32:
		return EncodeVariantInt32(val), nil
	case int64:
		return EncodeVariantInt64(val), nil
	case int:
		// Convert to int64 for encoding
		return EncodeVariantInt64(int64(val)), nil
	case float32:
		return EncodeVariantFloat(val), nil
	case float64:
		return EncodeVariantDouble(val), nil
	case string:
		return EncodeVariantString(val), nil
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return EncodeVariantInt64(i), nil
		}
		if f, err := val.Float64(); err == nil {
			return EncodeVariantDouble(f), nil
		}
		return nil, fmt.Errorf("invalid json.Number: %s", val)
	case []byte:
		// Encode as binary
		buf := make([]byte, 5+len(val))
		buf[0] = 0x3C // basic_type=0, primitive_type=15 (binary)
		binary.LittleEndian.PutUint32(buf[1:], uint32(len(val)))
		copy(buf[5:], val)
		return buf, nil
	case []any:
		// Encode each element and wrap as array
		encodedElements := make([][]byte, len(val))
		for i, elem := range val {
			encoded, err := EncodeGoValueAsVariant(elem)
			if err != nil {
				return nil, fmt.Errorf("encode array element %d: %w", i, err)
			}
			encodedElements[i] = encoded
		}
		return EncodeVariantArray(encodedElements), nil
	default:
		return nil, fmt.Errorf("unsupported type for variant encoding: %T", v)
	}
}

// EncodeGoValueAsVariantWithMetadata encodes a Go value as variant value bytes,
// using the provided metadata for field ID lookup in objects.
// This handles map[string]any by looking up field names in the metadata dictionary.
func EncodeGoValueAsVariantWithMetadata(v any, metadata []byte) ([]byte, error) {
	if v == nil {
		return EncodeVariantNull(), nil
	}

	// If already a Variant, return its encoded value directly
	if variant, ok := v.(Variant); ok {
		return variant.Value, nil
	}
	if ptr, ok := v.(*Variant); ok {
		if ptr == nil {
			return EncodeVariantNull(), nil
		}
		return ptr.Value, nil
	}

	// Use reflection to handle arbitrary structs and slice/maps generically
	val := reflect.ValueOf(v)

	// Handle structs as objects
	if val.Kind() == reflect.Struct {
		meta, err := decodeVariantMetadata(metadata)
		if err != nil {
			return nil, fmt.Errorf("decode metadata for struct encoding: %w", err)
		}

		fieldNameToID := make(map[string]int)
		for i, name := range meta.dictionary {
			fieldNameToID[name] = i
		}

		fieldIDs := make([]int, 0, val.NumField())
		values := make([][]byte, 0, val.NumField())

		t := val.Type()
		for i := 0; i < val.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" { // unexported
				continue
			}
			name := field.Name
			id, exists := fieldNameToID[name]
			if !exists {
				// If struct field not in metadata, we can either error or skip.
				// Since collectKeys should have found it, erroring is appropriate if we expect strict schema.
				// However, if metadata is partial, skipping might be preferred.
				// For AnyToVariant consistency, we error if metadata is missing a field present in the value.
				return nil, fmt.Errorf("struct field %q not in metadata dictionary", name)
			}

			encoded, err := EncodeGoValueAsVariantWithMetadata(val.Field(i).Interface(), metadata)
			if err != nil {
				return nil, fmt.Errorf("encode struct field %q: %w", name, err)
			}
			fieldIDs = append(fieldIDs, id)
			values = append(values, encoded)
		}
		return EncodeVariantObject(fieldIDs, values), nil
	}

	// Handle map[string]any with metadata
	if obj, ok := v.(map[string]any); ok {
		meta, err := decodeVariantMetadata(metadata)
		if err != nil {
			return nil, fmt.Errorf("decode metadata for object encoding: %w", err)
		}

		// Build field name to ID mapping
		fieldNameToID := make(map[string]int)
		for i, name := range meta.dictionary {
			fieldNameToID[name] = i
		}

		fieldIDs := make([]int, 0, len(obj))
		values := make([][]byte, 0, len(obj))

		for name, val := range obj {
			id, exists := fieldNameToID[name]
			if !exists {
				return nil, fmt.Errorf("field %q not in metadata dictionary", name)
			}
			encoded, err := EncodeGoValueAsVariantWithMetadata(val, metadata)
			if err != nil {
				return nil, fmt.Errorf("encode object field %q: %w", name, err)
			}
			fieldIDs = append(fieldIDs, id)
			values = append(values, encoded)
		}

		return EncodeVariantObject(fieldIDs, values), nil
	}

	// Handle []any with metadata (to support nested objects)
	if arr, ok := v.([]any); ok {
		encodedElements := make([][]byte, len(arr))
		for i, elem := range arr {
			encoded, err := EncodeGoValueAsVariantWithMetadata(elem, metadata)
			if err != nil {
				return nil, fmt.Errorf("encode array element %d: %w", i, err)
			}
			encodedElements[i] = encoded
		}
		return EncodeVariantArray(encodedElements), nil
	}

	// For non-objects, use the basic encoder
	return EncodeGoValueAsVariant(v)
}

// AnyToVariant converts a Go value to a Variant struct.
// It supports the same types as EncodeGoValueAsVariant (primitives, []any, map[string]any).
// For maps, it automatically builds the metadata dictionary from all keys found in the value.
func AnyToVariant(v any) (Variant, error) {
	if v == nil {
		return Variant{Metadata: []byte{0x01, 0x00, 0x00}, Value: EncodeVariantNull()}, nil
	}

	// If already a Variant, return as-is
	if variant, ok := v.(Variant); ok {
		return variant, nil
	}

	// Handle pointer to Variant
	if ptr, ok := v.(*Variant); ok {
		if ptr == nil {
			return Variant{Metadata: []byte{0x01, 0x00, 0x00}, Value: EncodeVariantNull()}, nil
		}
		return *ptr, nil
	}

	// 1. Collect all keys for metadata
	keys := make(map[string]struct{})
	collectKeys(v, keys)

	// 2. Encode metadata (sorted for performance/canonicalization)
	fieldNames := make([]string, 0, len(keys))
	for k := range keys {
		fieldNames = append(fieldNames, k)
	}
	metadata, _ := EncodeVariantMetadataSorted(fieldNames)

	// 3. Encode value with metadata
	val, err := EncodeGoValueAsVariantWithMetadata(v, metadata)
	if err != nil {
		return Variant{}, err
	}

	return Variant{
		Metadata: metadata,
		Value:    val,
	}, nil
}

func collectKeys(v any, keys map[string]struct{}) {
	if v == nil {
		return
	}
	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Map:
		// iterate map keys
		iter := val.MapRange()
		for iter.Next() {
			k := iter.Key()
			if k.Kind() == reflect.String {
				keys[k.String()] = struct{}{}
				collectKeys(iter.Value().Interface(), keys)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			collectKeys(val.Index(i).Interface(), keys)
		}
	case reflect.Struct:
		t := val.Type()
		for i := 0; i < val.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" { // unexported
				continue
			}
			// Use field name
			name := field.Name
			keys[name] = struct{}{}
			collectKeys(val.Field(i).Interface(), keys)
		}
	case reflect.Pointer, reflect.Interface:
		if !val.IsNil() {
			collectKeys(val.Elem().Interface(), keys)
		}
	}
}

// MergeVariantWithTypedValue merges a partially decoded variant value with a typed value.
// This is used during shredded variant reconstruction when both value and typed_value are present.
//
// Merge rules per the Parquet VARIANT spec:
//   - If value is null and typedValue is null: return null variant
//   - If value is non-null and typedValue is null: return value as-is (decode from blob)
//   - If value is null and typedValue is non-null: encode typedValue as variant
//   - If value is non-null and typedValue is non-null: merge (partially shredded object)
//
// The metadata is used for field ID lookup when encoding typed values to objects.
func MergeVariantWithTypedValue(value []byte, typedValue any, metadata []byte) ([]byte, error) {
	valueIsNull := len(value) == 0
	typedValueIsNull := typedValue == nil

	// Case 1: Both null -> null variant
	if valueIsNull && typedValueIsNull {
		return EncodeVariantNull(), nil
	}

	// Case 2: Only value is present -> return value as-is
	if !valueIsNull && typedValueIsNull {
		return value, nil
	}

	// Case 3: Only typed_value is present -> encode typed_value
	if valueIsNull && !typedValueIsNull {
		return EncodeGoValueAsVariantWithMetadata(typedValue, metadata)
	}

	// Case 4: Both present -> partially shredded object (merge)
	// The value blob contains the base object, and typed_value contains shredded fields.
	// For initial implementation, we decode the value, merge with typed_value, and re-encode.
	meta, err := decodeVariantMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("decode metadata for merge: %w", err)
	}

	// Decode the value blob
	baseValue, err := decodeVariantValue(value, meta)
	if err != nil {
		return nil, fmt.Errorf("decode value for merge: %w", err)
	}

	// If base is an object and typed is an object, merge them
	baseObj, baseIsObj := baseValue.(map[string]any)
	typedObj, typedIsObj := typedValue.(map[string]any)

	if typedIsObj {
		if !baseIsObj {
			baseObj = make(map[string]any)
		}
		// Merge: typed_value fields override base fields
		for k, v := range typedObj {
			baseObj[k] = v
		}
		return EncodeGoValueAsVariantWithMetadata(baseObj, metadata)
	}

	// If types don't match for merge, prefer typed_value as it's the "extracted" value
	return EncodeGoValueAsVariantWithMetadata(typedValue, metadata)
}

// ReconstructVariant reconstructs a Variant from its potentially shredded components.
// This is the main entry point for shredded variant reading.
//
// Parameters:
//   - metadata: the variant metadata bytes (always required)
//   - value: the variant value bytes (may be nil if fully shredded)
//   - typedValue: the typed_value column value (may be nil if not shredded)
//
// Returns a Variant struct with properly merged data.
func ReconstructVariant(metadata, value []byte, typedValue any) (Variant, error) {
	mergedValue, err := MergeVariantWithTypedValue(value, typedValue, metadata)
	if err != nil {
		return Variant{}, fmt.Errorf("reconstruct variant: %w", err)
	}

	return Variant{
		Metadata: metadata,
		Value:    mergedValue,
	}, nil
}
