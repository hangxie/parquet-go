package marshal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

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

// isPrimitiveByteSlice reports whether a byte slice is one column value rather than a list.
func isPrimitiveByteSlice(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, converter *jsonConverter) (bool, error) {
	if pathPrefix == "" || val.Type().Elem().Kind() != reflect.Uint8 {
		return false, nil
	}
	element, err := lookupSchemaElement(schemaHandler, pathPrefix, converter)
	if err != nil || element == nil || element.Type == nil || element.GetNumChildren() != 0 {
		return false, err
	}
	// The column decides, not the Go type: a LIST carried in a []byte is still a list, and
	// a repeated primitive names one path for the column ([][]byte) and for its values.
	return *element.Type == parquet.Type_BYTE_ARRAY || *element.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY, nil
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
