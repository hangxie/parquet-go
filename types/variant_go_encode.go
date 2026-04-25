package types

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
)

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

func buildFieldNameToID(metadata []byte, context string) (map[string]int, error) {
	meta, err := decodeVariantMetadata(metadata)
	if err != nil {
		return nil, fmt.Errorf("decode metadata for %s encoding: %w", context, err)
	}
	fieldNameToID := make(map[string]int)
	for i, name := range meta.dictionary {
		fieldNameToID[name] = i
	}
	return fieldNameToID, nil
}

func encodeStructAsVariant(val reflect.Value, metadata []byte) ([]byte, error) {
	fieldNameToID, err := buildFieldNameToID(metadata, "struct")
	if err != nil {
		return nil, err
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

func encodeMapAsVariant(obj map[string]any, metadata []byte) ([]byte, error) {
	fieldNameToID, err := buildFieldNameToID(metadata, "object")
	if err != nil {
		return nil, err
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

func encodeSliceAsVariant(arr []any, metadata []byte) ([]byte, error) {
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

// EncodeGoValueAsVariantWithMetadata encodes a Go value as variant value bytes,
// using the provided metadata for field ID lookup in objects.
// This handles map[string]any by looking up field names in the metadata dictionary.
func EncodeGoValueAsVariantWithMetadata(v any, metadata []byte) ([]byte, error) {
	if v == nil {
		return EncodeVariantNull(), nil
	}

	if variant, ok := v.(Variant); ok {
		return variant.Value, nil
	}
	if ptr, ok := v.(*Variant); ok {
		if ptr == nil {
			return EncodeVariantNull(), nil
		}
		return ptr.Value, nil
	}

	if obj, ok := v.(map[string]any); ok {
		return encodeMapAsVariant(obj, metadata)
	}
	if arr, ok := v.([]any); ok {
		return encodeSliceAsVariant(arr, metadata)
	}

	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Struct {
		return encodeStructAsVariant(val, metadata)
	}

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
