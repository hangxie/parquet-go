package types

import (
	"encoding/base64"
	"fmt"
)

// decodeVariantValue decodes a variant value using the provided metadata
func decodeVariantValue(data []byte, meta *variantMetadata) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Budget limits total decode operations to prevent exponential blowup when
	// multiple elements share the same offset (e.g. 227-element arrays of same-offset refs).
	budget := len(data) * 16
	if budget < 1024 {
		budget = 1024
	}
	if budget > 1_000_000 {
		budget = 1_000_000
	}
	consumed, val, err := decodeVariantValueAt(data, 0, meta, &budget)
	if err != nil {
		return val, fmt.Errorf("decode variant value: %w", err)
	}
	if consumed != len(data) {
		return val, fmt.Errorf("decode variant value: consumed %d of %d bytes", consumed, len(data))
	}
	return val, nil
}

// decodeVariantValueAt decodes a variant value starting at the given offset
// Returns the full encoded length, including the value metadata byte, and the decoded value.
func decodeVariantValueAt(data []byte, offset int, meta *variantMetadata, budget *int) (int, any, error) {
	if *budget <= 0 {
		return 0, nil, fmt.Errorf("variant decode budget exceeded: too many nested values")
	}
	*budget--

	if offset >= len(data) {
		return 0, nil, fmt.Errorf("variant value offset out of bounds")
	}

	// value_metadata byte: basic_type (2 bits) | value_header (6 bits)
	valueMetadata := data[offset]
	basicType := valueMetadata & 0x03
	valueHeader := valueMetadata >> 2

	switch basicType {
	case variantBasicTypePrimitive:
		consumed, val, err := decodePrimitiveValue(data, offset+1, valueHeader)
		if err != nil {
			return 0, val, err
		}
		return 1 + consumed, val, nil

	case variantBasicTypeShortString:
		// Short string: value_header contains the length (0-63)
		length := int(valueHeader)
		if offset+1+length > len(data) {
			return 0, nil, fmt.Errorf("short string length exceeds data")
		}
		return 1 + length, string(data[offset+1 : offset+1+length]), nil

	case variantBasicTypeObject:
		return decodeObjectValue(data, offset, valueHeader, meta, budget)

	case variantBasicTypeArray:
		return decodeArrayValue(data, offset, valueHeader, meta, budget)

	default:
		return 0, nil, fmt.Errorf("unknown variant basic type: %d", basicType)
	}
}

// ConvertVariantValue decodes a Variant, returning a base64 fallback with ErrUnrenderable for invalid data.
func ConvertVariantValue(v Variant) (any, error) {
	// Preserve historical behavior for an empty value: return nil without validating metadata.
	if len(v.Value) == 0 {
		return nil, nil
	}

	// Decode metadata
	meta, err := decodeVariantMetadata(v.Metadata)
	if err != nil {
		return variantFallback(v), errUnrenderableCause("VARIANT", fmt.Errorf("decode metadata: %w", err))
	}

	// Decode value
	val, err := decodeVariantValue(v.Value, meta)
	if err != nil {
		return variantFallback(v), errUnrenderableCause("VARIANT", fmt.Errorf("decode value: %w", err))
	}

	return val, nil
}

func variantFallback(v Variant) map[string]any {
	return map[string]any{
		"metadata": base64.StdEncoding.EncodeToString(v.Metadata),
		"value":    base64.StdEncoding.EncodeToString(v.Value),
	}
}
