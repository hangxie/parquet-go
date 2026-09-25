package types

import (
	"fmt"
	"reflect"
)

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
