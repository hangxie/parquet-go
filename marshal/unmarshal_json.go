package marshal

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

// jsonConverter holds configuration and caches for JSON conversion operations
type jsonConverter struct {
	enforceUTF8      bool
	schemaCache      sync.Map                // map[string]*parquet.SchemaElement
	fieldCache       sync.Map                // map[reflect.Type]map[string]fieldInfo
	geospatialConfig *types.GeospatialConfig // nil means use default
	prefixPath       string                  // subtree the data is rooted at, "" for the file root
	rootPath         string                  // prefixPath in in-path form, resolved once per call
}

// JSONConvertOption configures ConvertToJSONFriendly behavior.
type JSONConvertOption func(*jsonConverter)

// WithGeospatialConfig sets a custom GeospatialConfig for geospatial type rendering.
// If not provided or nil, the default config (Hex for GEOMETRY, GeoJSON for GEOGRAPHY) is used.
func WithGeospatialConfig(cfg *types.GeospatialConfig) JSONConvertOption {
	return func(converter *jsonConverter) { converter.geospatialConfig = cfg }
}

// WithPrefixPath names the subtree the data is rooted at, as passed to ReadPartial.
func WithPrefixPath(prefixPath string) JSONConvertOption {
	// Without it those objects are looked up under the file root, where they do not exist,
	// and a path resolving to nothing means "pass the value through" rather than an error.
	return func(converter *jsonConverter) { converter.prefixPath = prefixPath }
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
	if err := converter.resolveRootPath(schemaHandler); err != nil {
		return nil, err
	}
	val := reflect.ValueOf(data)
	batch, err := converter.isRowBatch(val, schemaHandler)
	if err != nil {
		return nil, err
	}
	if batch {
		rows := make([]any, val.Len())
		for i := range val.Len() {
			converted, err := convertValueToJSONFriendlyWithContext(val.Index(i), schemaHandler, converter.rootPath, converter)
			if err != nil {
				return nil, fmt.Errorf("convert row %d: %w", i, err)
			}
			rows[i] = converted
		}
		return rows, nil
	}
	return convertValueToJSONFriendlyWithContext(val, schemaHandler, converter.rootPath, converter)
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
		primitive, err := isPrimitiveByteSlice(val, schemaHandler, pathPrefix, converter)
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
			return nil, err
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

// convertPrimitiveToJSONFriendly optimized primitive conversion with schema caching
func convertPrimitiveToJSONFriendly(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, converter *jsonConverter) (any, error) {
	if pathPrefix == "" {
		return val.Interface(), nil
	}

	schemaElement, err := lookupSchemaElement(schemaHandler, pathPrefix, converter)
	if err != nil {
		return nil, err
	}
	// A group has no physical type, and a value sits at one only when rooted there.
	if schemaElement == nil || schemaElement.Type == nil {
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
