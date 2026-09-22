package marshal

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

// jsonConverter holds configuration and caches for JSON conversion operations
type jsonConverter struct {
	enforceUTF8      bool
	schemaCache      sync.Map                // map[string]*parquet.SchemaElement
	fieldCache       sync.Map                // map[reflect.Type]map[string]fieldInfo
	geospatialConfig *types.GeospatialConfig // nil means use default
}

// JSONConvertOption configures ConvertToJSONFriendly behavior.
type JSONConvertOption func(*jsonConverter)

// WithGeospatialConfig sets a custom GeospatialConfig for geospatial type rendering.
// If not provided or nil, the default config (Hex for GEOMETRY, GeoJSON for GEOGRAPHY) is used.
func WithGeospatialConfig(cfg *types.GeospatialConfig) JSONConvertOption {
	return func(converter *jsonConverter) { converter.geospatialConfig = cfg }
}

// WithEnforceUTF8 enables UTF-8 validation for STRING, UTF8, JSON and ENUM values.
func WithEnforceUTF8(enabled bool) JSONConvertOption {
	return func(converter *jsonConverter) { converter.enforceUTF8 = enabled }
}

type fieldInfo struct {
	name  string
	index []int
}

// ConvertToJSONFriendly converts parquet data to JSON-friendly format by applying logical type conversions.
// Optional JSONConvertOption values can be passed to customize behavior (e.g., WithGeospatialConfig).
func ConvertToJSONFriendly(data any, schemaHandler *schema.SchemaHandler, opts ...JSONConvertOption) (any, error) {
	converter := &jsonConverter{}
	for _, opt := range opts {
		opt(converter)
	}
	return convertValueToJSONFriendlyWithContext(reflect.ValueOf(data), schemaHandler, "", converter)
}

// getFieldNameFromTag extracts the name from JSON tag since the struct from parquet reading uses JSON tags
func getFieldNameFromTag(field reflect.StructField) string {
	jsonTag := field.Tag.Get("json")
	if jsonTag != "" {
		// Parse JSON tag to get the field name (format: "name,option1,option2")
		parts := strings.Split(jsonTag, ",")
		if len(parts) > 0 && parts[0] != "" && parts[0] != "-" {
			return parts[0]
		}
	}

	// Fallback to Go struct field name
	return field.Name
}

// convertValueToJSONFriendlyWithContext recursively converts a value to JSON-friendly format with caching context
func convertValueToJSONFriendlyWithContext(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, converter *jsonConverter) (any, error) {
	if !val.IsValid() {
		return nil, nil
	}

	switch val.Kind() {
	case reflect.Interface:
		if val.IsNil() {
			return nil, nil
		}
		return convertValueToJSONFriendlyWithContext(val.Elem(), schemaHandler, pathPrefix, converter)

	case reflect.Pointer:
		if val.IsNil() {
			return nil, nil
		}
		return convertValueToJSONFriendlyWithContext(val.Elem(), schemaHandler, pathPrefix, converter)

	case reflect.Slice:
		primitive, err := isTextByteSlice(val, schemaHandler, pathPrefix, converter)
		if err != nil {
			return nil, err
		}
		if primitive {
			return convertPrimitiveToJSONFriendly(val, schemaHandler, pathPrefix, converter)
		}
		return convertSliceToJSONFriendly(val, schemaHandler, pathPrefix, converter)

	case reflect.Map:
		return convertMapToJSONFriendly(val, schemaHandler, pathPrefix, converter)

	case reflect.Struct:
		return convertStructToJSONFriendly(val, schemaHandler, pathPrefix, converter)

	default:
		return convertPrimitiveToJSONFriendly(val, schemaHandler, pathPrefix, converter)
	}
}

// isTextByteSlice reports whether UTF-8 enforcement needs a byte slice treated as one text value.
func isTextByteSlice(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, converter *jsonConverter) (bool, error) {
	if !converter.enforceUTF8 || pathPrefix == "" || val.Type().Elem().Kind() != reflect.Uint8 {
		return false, nil
	}
	element, err := lookupSchemaElement(schemaHandler, pathPrefix, converter)
	if err != nil || element == nil || element.Type == nil || element.GetNumChildren() != 0 ||
		element.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		return false, err
	}
	if *element.Type != parquet.Type_BYTE_ARRAY && *element.Type != parquet.Type_FIXED_LEN_BYTE_ARRAY {
		return false, nil
	}
	if logical := element.LogicalType; logical != nil &&
		(logical.IsSetSTRING() || logical.IsSetJSON() || logical.IsSetENUM()) {
		return true, nil
	}
	if converted := element.ConvertedType; converted != nil {
		switch *converted {
		case parquet.ConvertedType_UTF8, parquet.ConvertedType_JSON, parquet.ConvertedType_ENUM:
			return true, nil
		}
	}
	return false, nil
}

// convertSliceToJSONFriendly optimized slice conversion
func convertSliceToJSONFriendly(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, converter *jsonConverter) (any, error) {
	result := make([]any, val.Len())
	elementPath, err := listElementPath(schemaHandler, pathPrefix, converter)
	if err != nil {
		return nil, err
	}

	for i := range val.Len() {
		converted, err := convertValueToJSONFriendlyWithContext(val.Index(i), schemaHandler, elementPath, converter)
		if err != nil {
			return nil, fmt.Errorf("convert list element %d: %w", i, err)
		}
		result[i] = converted
	}
	return result, nil
}

// convertMapToJSONFriendly optimized map conversion
func convertMapToJSONFriendly(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, converter *jsonConverter) (any, error) {
	result := make(map[string]any)
	var keyPath, valuePath string

	if pathPrefix != "" {
		var builder strings.Builder
		builder.WriteString(pathPrefix)
		builder.WriteString(common.ParGoPathDelimiter)
		builder.WriteString("Key_value")
		builder.WriteString(common.ParGoPathDelimiter)
		builder.WriteString("Key")
		keyPath = builder.String()

		builder.Reset()
		builder.WriteString(pathPrefix)
		builder.WriteString(common.ParGoPathDelimiter)
		builder.WriteString("Key_value")
		builder.WriteString(common.ParGoPathDelimiter)
		builder.WriteString("Value")
		valuePath = builder.String()
	}

	for _, key := range val.MapKeys() {
		converted, err := convertValueToJSONFriendlyWithContext(key, schemaHandler, keyPath, converter)
		if err != nil {
			return nil, fmt.Errorf("convert map key: %w", err)
		}
		keyStr := fmt.Sprint(converted)

		converted, err = convertValueToJSONFriendlyWithContext(val.MapIndex(key), schemaHandler, valuePath, converter)
		if err != nil {
			return nil, fmt.Errorf("convert map value for key %q: %w", keyStr, err)
		}
		result[keyStr] = converted
	}
	return result, nil
}

// convertStructToJSONFriendly optimized struct conversion with field caching
func convertStructToJSONFriendly(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, converter *jsonConverter) (any, error) {
	valType := val.Type()

	// Special handling for types.Variant: decode the variant binary data
	if valType == reflect.TypeOf(types.Variant{}) {
		variant := val.Interface().(types.Variant)
		decoded, err := types.ConvertVariantValue(variant)
		if err != nil {
			// On error, still return the decoded value (which will be base64 fallback)
			return decoded, nil
		}
		return decoded, nil
	}

	// Special handling for old list format: if struct has single "array" field with slice data,
	// return the slice directly instead of wrapping in map
	if val.NumField() == 1 &&
		strings.EqualFold(valType.Field(0).Name, "array") &&
		valType.Field(0).Type.Kind() == reflect.Slice {
		fieldPath := valType.Field(0).Name
		if pathPrefix != "" {
			fieldPath = pathPrefix + common.ParGoPathDelimiter + fieldPath
		}
		return convertValueToJSONFriendlyWithContext(val.Field(0), schemaHandler, fieldPath, converter)
	}

	result := make(map[string]any)
	var fieldMap map[string]fieldInfo

	fieldMapInterface, exists := converter.fieldCache.Load(valType)
	if !exists {
		fieldMap = make(map[string]fieldInfo)
		for i := range val.NumField() {
			if field := valType.Field(i); field.IsExported() {
				fieldMap[field.Name] = fieldInfo{
					name:  getFieldNameFromTag(field),
					index: field.Index,
				}
			}
		}
		converter.fieldCache.Store(valType, fieldMap)
	} else {
		fieldMap = fieldMapInterface.(map[string]fieldInfo)
	}

	for i := range val.NumField() {
		field := valType.Field(i)
		fieldVal := val.Field(i)

		if !fieldVal.CanInterface() {
			continue
		}

		fInfo, exists := fieldMap[field.Name]
		if !exists {
			continue
		}

		fieldPath := field.Name
		if pathPrefix != "" {
			fieldPath = pathPrefix + common.ParGoPathDelimiter + fieldPath
		}

		converted, err := convertValueToJSONFriendlyWithContext(fieldVal, schemaHandler, fieldPath, converter)
		if err != nil {
			return nil, fmt.Errorf("convert field %s: %w", field.Name, err)
		}
		result[fInfo.name] = converted
	}
	return result, nil
}

// listElementPath returns the schema path of a slice's elements, or "" for a rootless slice.
// A legacy REPEATED column repeats in place, so its own path already addresses the elements;
// a three-level LIST nests them under List/Element.
func listElementPath(schemaHandler *schema.SchemaHandler, pathPrefix string, converter *jsonConverter) (string, error) {
	if pathPrefix == "" {
		return "", nil
	}
	// checked before List/Element, so a repeated group whose own fields happen to be named
	// List and Element is not mistaken for a three-level LIST
	element, err := lookupSchemaElement(schemaHandler, pathPrefix, converter)
	if err != nil {
		return "", err
	}
	if element != nil && element.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED {
		return pathPrefix, nil
	}
	return pathPrefix + common.ParGoPathDelimiter + "List" + common.ParGoPathDelimiter + "Element", nil
}

// lookupSchemaElement resolves a conversion path against the schema. A nil element means the
// path carries no schema-driven conversion, which is normal: values reached through a partial
// read or a schema handler that does not describe them are passed through untouched. An error
// means the schema handler itself is inconsistent. Results are cached on the converter.
func lookupSchemaElement(schemaHandler *schema.SchemaHandler, path string, converter *jsonConverter) (*parquet.SchemaElement, error) {
	// paths are built from Go field names alone, so the root is always missing and never
	// ambiguous: a top-level field may share the root's name without colliding with it
	path = schemaHandler.GetRootInName() + common.ParGoPathDelimiter + path

	if cached, ok := converter.schemaCache.Load(path); ok {
		element, _ := cached.(*parquet.SchemaElement)
		return element, nil
	}

	var element *parquet.SchemaElement
	if schemaIndex, exists := schemaHandler.MapIndex[path]; exists {
		// every index is a position in SchemaElements by construction, so a stale one means
		// the two have been mutated apart and no lookup on this handler can be trusted
		if int(schemaIndex) < 0 || int(schemaIndex) >= len(schemaHandler.SchemaElements) {
			return nil, fmt.Errorf("schema handler is inconsistent: path %q maps to element %d of %d",
				strings.ReplaceAll(path, common.ParGoPathDelimiter, "."), schemaIndex, len(schemaHandler.SchemaElements))
		}
		element = schemaHandler.SchemaElements[schemaIndex]
	}
	converter.schemaCache.Store(path, element)
	return element, nil
}

// convertPrimitiveToJSONFriendly optimized primitive conversion with schema caching
func convertPrimitiveToJSONFriendly(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, converter *jsonConverter) (any, error) {
	if pathPrefix == "" {
		return val.Interface(), nil
	}

	schemaElement, err := lookupSchemaElement(schemaHandler, pathPrefix, converter)
	if err != nil {
		return nil, err
	}
	if schemaElement == nil {
		return val.Interface(), nil
	}

	var typeOpts []types.ValueOption
	if converter.enforceUTF8 {
		typeOpts = append(typeOpts, types.WithEnforceUTF8(true))
	}
	if converter.geospatialConfig != nil {
		typeOpts = append(typeOpts, types.WithGeospatialConfig(converter.geospatialConfig))
	}
	// ConvertValue returns the substitute alongside the error; the conversion abandons it,
	// so one value it cannot render costs the batch rather than hiding in it.
	converted, err := types.ConvertValue(val.Interface(), schemaElement, typeOpts...)
	if err != nil {
		return nil, err
	}
	return converted, nil
}
