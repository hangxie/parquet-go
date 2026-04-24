package types

import (
	"fmt"
	"math/big"
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
