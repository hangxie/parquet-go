package types

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strings"
)

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
func decodePrimitiveTemporal(data []byte, offset int, primitiveType uint8) (int, any, error) {
	switch primitiveType {
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
		return 8, TIMESTAMP_MICROSToISO8601(micros, primitiveType == variantPrimitiveTimestampMicro), nil

	case variantPrimitiveTimeNTZ:
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for time")
		}
		micros := int64(binary.LittleEndian.Uint64(data[offset:]))
		hours := micros / 3600000000
		micros %= 3600000000
		minutes := micros / 60000000
		micros %= 60000000
		seconds := micros / 1000000
		microsFrac := micros % 1000000
		return 8, fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, microsFrac), nil

	default: // variantPrimitiveTimestampNano, variantPrimitiveTimestampNTZNano
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for timestamp")
		}
		nanos := int64(binary.LittleEndian.Uint64(data[offset:]))
		return 8, TIMESTAMP_NANOSToISO8601(nanos, primitiveType == variantPrimitiveTimestampNano), nil
	}
}

func decodePrimitiveVarLen(data []byte, offset int, primitiveType uint8) (int, any, error) {
	switch primitiveType {
	case variantPrimitiveString:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for string length")
		}
		length := int(binary.LittleEndian.Uint32(data[offset:]))
		if offset+4+length > len(data) {
			return 0, nil, fmt.Errorf("string length exceeds data")
		}
		return 4 + length, string(data[offset+4 : offset+4+length]), nil

	case variantPrimitiveBinary:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for binary length")
		}
		length := int(binary.LittleEndian.Uint32(data[offset:]))
		if offset+4+length > len(data) {
			return 0, nil, fmt.Errorf("binary length exceeds data")
		}
		return 4 + length, base64.StdEncoding.EncodeToString(data[offset+4 : offset+4+length]), nil

	case variantPrimitiveDecimal4:
		if offset+5 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for decimal4")
		}
		scale := int(data[offset])
		unscaled := int64(int32(binary.LittleEndian.Uint32(data[offset+1:])))
		return 5, formatDecimal(unscaled, scale), nil

	case variantPrimitiveDecimal8:
		if offset+9 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for decimal8")
		}
		scale := int(data[offset])
		unscaled := int64(binary.LittleEndian.Uint64(data[offset+1:]))
		return 9, formatDecimal(unscaled, scale), nil

	case variantPrimitiveDecimal16:
		if offset+17 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for decimal16")
		}
		scale := int(data[offset])
		unscaled := readLittleEndianInt128(data[offset+1 : offset+17])
		return 17, formatDecimal128(unscaled, scale), nil

	default: // variantPrimitiveUUID
		if offset+16 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for UUID")
		}
		return 16, ConvertUUIDValue(data[offset : offset+16]), nil
	}
}

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

	case variantPrimitiveDate, variantPrimitiveTimestampMicro, variantPrimitiveTimestampNTZMicro,
		variantPrimitiveTimeNTZ, variantPrimitiveTimestampNano, variantPrimitiveTimestampNTZNano:
		return decodePrimitiveTemporal(data, offset, primitiveType)

	case variantPrimitiveString, variantPrimitiveBinary, variantPrimitiveUUID,
		variantPrimitiveDecimal4, variantPrimitiveDecimal8, variantPrimitiveDecimal16:
		return decodePrimitiveVarLen(data, offset, primitiveType)

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
