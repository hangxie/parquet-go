package types

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// readContainerElementCount reads the element count following an object or array header.
func readContainerElementCount(data []byte, pos int, isLarge bool, kind string) (int, int, error) {
	if isLarge {
		if pos+4 > len(data) {
			return 0, 0, fmt.Errorf("not enough data for large %s num_elements", kind)
		}
		return int(binary.LittleEndian.Uint32(data[pos:])), pos + 4, nil
	}
	if pos >= len(data) {
		return 0, 0, fmt.Errorf("not enough data for %s num_elements", kind)
	}
	return int(data[pos]), pos + 1, nil
}

// decodeObjectValue decodes a variant object value
func decodeObjectValue(data []byte, offset int, valueHeader uint8, meta *variantMetadata, budget *int) (int, any, error) {
	// object_header = is_large << 4 | field_id_size_minus_one << 2 | field_offset_size_minus_one
	fieldIDSize := int((valueHeader>>2)&0x03) + 1
	fieldOffsetSize := int(valueHeader&0x03) + 1

	consumed, val, err := decodeObjectSized(data, offset, valueHeader, fieldIDSize, fieldOffsetSize, meta, budget)
	if err != nil && fieldIDSize != fieldOffsetSize {
		// Releases up to v3.8.3 wrote the two widths in the opposite bit positions. Nothing
		// in the header tells the layouts apart, so the older reading is only tried once the
		// conforming one has failed, which the span check above makes it do promptly.
		if legacyConsumed, legacyVal, legacyErr := decodeObjectSized(data, offset, valueHeader, fieldOffsetSize, fieldIDSize, meta, budget); legacyErr == nil {
			return legacyConsumed, legacyVal, nil
		}
	}
	return consumed, val, err
}

// decodeObjectSized decodes a variant object value with the given header field widths.
func decodeObjectSized(data []byte, offset int, valueHeader uint8, fieldIDSize, fieldOffsetSize int, meta *variantMetadata, budget *int) (int, any, error) {
	isLarge := (valueHeader>>4)&1 == 1
	numElements, pos, err := readContainerElementCount(data, offset+1, isLarge, "object")
	if err != nil {
		return 0, nil, err
	}

	// Historical parquet-go encodings omitted the required zero offset from empty containers.
	if numElements == 0 && pos == len(data) && valueHeader == 0 {
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
	valuesLength := fieldOffsets[numElements]
	// Every caller trims data to the end of this value, so an object that does not end where
	// the data does has been read with the wrong widths, however well its parts parse.
	if valuesStart+valuesLength != len(data) {
		return 0, nil, fmt.Errorf("object is %d bytes, value carries %d", valuesStart+valuesLength-offset, len(data)-offset)
	}
	if numElements == 0 {
		if valuesLength != 0 {
			return 0, nil, fmt.Errorf("empty object has nonzero final offset")
		}
		return valuesStart - offset, map[string]any{}, nil
	}

	ends, err := objectValueEnds(fieldOffsets)
	if err != nil {
		return 0, nil, err
	}

	result := make(map[string]any)
	for i := range numElements {
		fieldID := fieldIDs[i]
		if fieldID >= len(meta.dictionary) {
			return 0, nil, fmt.Errorf("field ID %d exceeds dictionary size %d", fieldID, len(meta.dictionary))
		}
		fieldName := meta.dictionary[fieldID]

		start := fieldOffsets[i]
		valueOffset := valuesStart + start
		end := fieldOffsets[i+1]
		if ends != nil {
			end = ends[start]
		}
		valueEnd := valuesStart + end
		consumed, val, err := decodeVariantValueAt(data[:valueEnd], valueOffset, meta, budget)
		if err != nil {
			return 0, nil, fmt.Errorf("decode object field %q: %w", fieldName, err)
		}
		if consumed != valueEnd-valueOffset {
			return 0, nil, fmt.Errorf("decode object field %q: consumed %d of %d bytes", fieldName, consumed, valueEnd-valueOffset)
		}
		result[fieldName] = val
	}
	return len(data) - offset, result, nil
}

func objectValueEnds(offsets []int) (map[int]int, error) {
	starts := offsets[:len(offsets)-1]
	final := offsets[len(offsets)-1]
	if slices.IsSorted(starts) {
		if starts[0] != 0 {
			return nil, fmt.Errorf("object first value offset is %d, must be 0", starts[0])
		}
		for i, start := range starts {
			if start >= offsets[i+1] {
				return nil, fmt.Errorf("object value offsets overlap at %d", start)
			}
		}
		return nil, nil
	}

	physical := append([]int(nil), starts...)
	slices.Sort(physical)
	if physical[0] != 0 {
		return nil, fmt.Errorf("object first value offset is %d, must be 0", physical[0])
	}
	ends := make(map[int]int, len(starts))
	for i, start := range physical {
		end := final
		if i+1 < len(physical) {
			end = physical[i+1]
		}
		if start >= end {
			return nil, fmt.Errorf("object value offsets overlap at %d", start)
		}
		ends[start] = end
	}
	return ends, nil
}

// decodeArrayValue decodes a variant array value
func decodeArrayValue(data []byte, offset int, valueHeader uint8, meta *variantMetadata, budget *int) (int, any, error) {
	// array_header = is_large << 2 | element_offset_size_minus_one
	elementOffsetSize := int((valueHeader & 0x03) + 1)
	isLarge := (valueHeader>>2)&1 == 1

	numElements, pos, err := readContainerElementCount(data, offset+1, isLarge, "array")
	if err != nil {
		return 0, nil, err
	}

	// Historical parquet-go encodings omitted the required zero offset from empty containers.
	if numElements == 0 && pos == len(data) && valueHeader == 0 {
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
	valuesLength := elementOffsets[numElements]
	if valuesLength > len(data)-valuesStart {
		return 0, nil, fmt.Errorf("array values exceed data")
	}
	if elementOffsets[0] != 0 {
		return 0, nil, fmt.Errorf("array first element offset is %d, must be 0", elementOffsets[0])
	}
	if numElements == 0 {
		return valuesStart - offset, []any{}, nil
	}
	for i := range numElements {
		if elementOffsets[i] >= elementOffsets[i+1] {
			return 0, nil, fmt.Errorf("array element offsets overlap at %d", elementOffsets[i])
		}
	}

	result := make([]any, numElements)
	for i := range numElements {
		valueOffset := valuesStart + elementOffsets[i]
		valueEnd := valuesStart + elementOffsets[i+1]
		consumed, val, err := decodeVariantValueAt(data[:valueEnd], valueOffset, meta, budget)
		if err != nil {
			return 0, nil, fmt.Errorf("decode array element %d: %w", i, err)
		}
		if consumed != valueEnd-valueOffset {
			return 0, nil, fmt.Errorf("decode array element %d: consumed %d of %d bytes", i, consumed, valueEnd-valueOffset)
		}
		result[i] = val
	}

	// Total consumed bytes
	totalConsumed := valuesStart + valuesLength - offset
	return totalConsumed, result, nil
}
