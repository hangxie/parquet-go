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

// isRowBatch reports whether the value is the rows a read returns rather than one of them.
func (converter *jsonConverter) isRowBatch(val reflect.Value, schemaHandler *schema.SchemaHandler) (bool, error) {
	// Rows are a container, not a schema element, so each is one value at the root path.
	// For a partial read that is the prefix itself: a group, a leaf, a LIST or a MAP.
	if !val.IsValid() || (val.Kind() != reflect.Slice && val.Kind() != reflect.Array) {
		return false, nil
	}
	if converter.rootPath == "" {
		return true, nil
	}
	element, err := lookupSchemaElement(schemaHandler, converter.rootPath, converter)
	if err != nil {
		return false, err
	}
	if !isListAnnotated(element) {
		return true, nil
	}
	// A LIST root is the one place both readings are slices. A batch holds a list each, so
	// its entries are slices of their own; a single list holds that list's elements.
	switch val.Type().Elem().Kind() {
	case reflect.Slice, reflect.Array, reflect.Interface:
		return true, nil
	}
	return false, nil
}

// isListAnnotated reports whether the element holds a list of values rather than one.
func isListAnnotated(element *parquet.SchemaElement) bool {
	switch {
	case element == nil:
		return false
	case element.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED:
		return true
	case element.LogicalType != nil && element.LogicalType.IsSetLIST():
		return true
	default:
		return element.ConvertedType != nil && *element.ConvertedType == parquet.ConvertedType_LIST
	}
}

// resolveRootPath fixes the path every lookup hangs off, once per call.
func (converter *jsonConverter) resolveRootPath(schemaHandler *schema.SchemaHandler) error {
	if schemaHandler == nil {
		return nil
	}
	if converter.prefixPath == "" {
		converter.rootPath = schemaHandler.GetRootInName()
		return nil
	}
	// Reported rather than left to miss: a prefix naming nothing would convert no value
	// and read exactly like the bug this option exists to fix.
	inPath, err := schemaHandler.ConvertToInPathStr(converter.prefixPath)
	if err != nil {
		return fmt.Errorf("convert prefix path: %w", err)
	}
	converter.rootPath = inPath
	return nil
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

// lookupSchemaElement resolves a conversion path, named from the root the data sits at.
func lookupSchemaElement(schemaHandler *schema.SchemaHandler, path string, converter *jsonConverter) (*parquet.SchemaElement, error) {
	// A nil element is normal: a handler that does not describe the value passes it
	// through untouched, where an error means the handler itself is inconsistent.
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
