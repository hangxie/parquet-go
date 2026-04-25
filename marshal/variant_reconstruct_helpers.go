package marshal

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/types"
)

func toByteSlice(val any, label string) ([]byte, error) {
	if val == nil {
		return nil, nil
	}
	switch v := val.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("unexpected %s type: %T", label, v)
	}
}

var defaultVariantMetadata = []byte{0x01, 0x00, 0x00}

func resolveMetadata(val any, fallback []byte) ([]byte, error) {
	b, err := toByteSlice(val, "metadata")
	if err != nil {
		return nil, err
	}
	if len(b) > 0 {
		return b, nil
	}
	if len(fallback) > 0 {
		return fallback, nil
	}
	return defaultVariantMetadata, nil
}

func effectiveMetadataForTyped(metadataValues []any, metadataSet bool, fallback []byte) []byte {
	if metadataSet && len(metadataValues) > 0 {
		if m, ok := metadataValues[0].([]byte); ok {
			return m
		}
		if s, ok := metadataValues[0].(string); ok {
			return []byte(s)
		}
	}
	return fallback
}

func isVariantResultNull(v types.Variant) bool {
	return len(v.Value) == 0 || (len(v.Value) == 1 && v.Value[0] == 0)
}

func buildVariantResults(maxLen int, metadataValues, valueValues, typedValueValues []any, metadata []byte) ([]types.Variant, error) {
	results := make([]types.Variant, maxLen)
	for i := range maxLen {
		var metaVal any
		if i < len(metadataValues) {
			metaVal = metadataValues[i]
		}
		elMetadata, err := resolveMetadata(metaVal, metadata)
		if err != nil {
			return nil, err
		}

		var elValue []byte
		if i < len(valueValues) && valueValues[i] != nil {
			elValue, err = toByteSlice(valueValues[i], "value")
			if err != nil {
				return nil, err
			}
		}

		var elTypedValue any
		if i < len(typedValueValues) {
			elTypedValue = typedValueValues[i]
		}

		v, vErr := types.ReconstructVariant(elMetadata, elValue, elTypedValue)
		if vErr != nil {
			results[i] = types.Variant{Metadata: elMetadata, Value: types.EncodeVariantNull()}
		} else {
			results[i] = v
		}
	}
	return results, nil
}

func variantResultsToAny(results []types.Variant, isRepeated bool) (any, error) {
	if isRepeated {
		anyResults := make([]any, len(results))
		for i, v := range results {
			if isVariantResultNull(v) {
				anyResults[i] = nil
			} else {
				anyResults[i] = v
			}
		}
		return anyResults, nil
	}
	v := results[0]
	if isVariantResultNull(v) {
		return nil, nil
	}
	return v, nil
}

func reconstructElementVariant(elementMap map[string]any, metadata []byte) any {
	var elMetadata []byte
	metadataSet := false
	if raw, ok := elementMap["Metadata"]; ok {
		switch v := raw.(type) {
		case []byte:
			elMetadata = v
			metadataSet = true
		case string:
			elMetadata = []byte(v)
			metadataSet = true
		}
	}
	if !metadataSet {
		if len(metadata) > 0 {
			elMetadata = metadata
		} else {
			elMetadata = defaultVariantMetadata
		}
	}
	elValue, _ := elementMap["Value"].([]byte)
	elTypedValue := elementMap["Typed_value"]
	v, _ := types.ReconstructVariant(elMetadata, elValue, elTypedValue)
	return v
}

func elementFromMap(elementMap map[string]any, metadata []byte) any {
	_, hasMetadata := elementMap["Metadata"]
	_, hasValue := elementMap["Value"]
	_, hasTypedValue := elementMap["Typed_value"]
	if hasMetadata || hasValue || hasTypedValue {
		return reconstructElementVariant(elementMap, metadata)
	}
	if len(elementMap) == 1 {
		for name, v := range elementMap {
			if name == "Value" || name == "Typed_value" {
				return v
			}
			return elementMap
		}
	}
	return elementMap
}
