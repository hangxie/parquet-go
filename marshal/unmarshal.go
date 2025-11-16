package marshal

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/types"
)

// Record Map KeyValue pair
type KeyValue struct {
	Key   reflect.Value
	Value reflect.Value
}

type MapRecord struct {
	KeyValues []KeyValue
	Index     int
}

type SliceRecord struct {
	Values []reflect.Value
	Index  int
}

// conversionContext holds cached data for performance optimization
type conversionContext struct {
	schemaCache sync.Map // map[string]*parquet.SchemaElement
	fieldCache  sync.Map // map[reflect.Type]map[string]fieldInfo
}

type fieldInfo struct {
	name  string
	index []int
}

// Convert the table map to objects slice. dstInterface is a slice of pointers of objects
func Unmarshal(tableMap *map[string]*layout.Table, bgn, end int, dstInterface any, schemaHandler *schema.SchemaHandler, prefixPath string) error {
	rootValue := reflect.ValueOf(dstInterface)
	if !rootValue.IsValid() || rootValue.Kind() != reflect.Ptr || rootValue.IsNil() {
		return fmt.Errorf("dstInterface must be a non-nil pointer")
	}

	tableNeeds := make(map[string]*layout.Table)
	tableBgn, tableEnd := make(map[string]int), make(map[string]int)
	for name, table := range *tableMap {
		if !strings.HasPrefix(name, prefixPath) {
			continue
		}

		tableNeeds[name] = table

		ln := len(table.Values)
		num := -1
		tableBgn[name], tableEnd[name] = -1, -1
		for i := range ln {
			if table.RepetitionLevels[i] == 0 {
				num++
				if num == bgn {
					tableBgn[name] = i
				}
				if num == end {
					tableEnd[name] = i
					break
				}
			}
		}

		if tableEnd[name] < 0 {
			tableEnd[name] = ln
		}
		if tableBgn[name] < 0 {
			return nil
		}
	}

	mapRecords := make(map[reflect.Value]*MapRecord)
	mapRecordsStack := make([]reflect.Value, 0)
	sliceRecords := make(map[reflect.Value]*SliceRecord)
	sliceRecordsStack := make([]reflect.Value, 0)
	root := rootValue.Elem()
	prefixIndex := common.PathStrIndex(prefixPath) - 1

	for name, table := range tableNeeds {
		path := table.Path
		bgn := tableBgn[name]
		end := tableEnd[name]
		schemaIndexs := make([]int, len(path))
		for i := range path {
			curPathStr := common.PathToStr(path[:i+1])
			schemaIndexs[i] = int(schemaHandler.MapIndex[curPathStr])
		}

		repetitionLevels, definitionLevels := make([]int32, len(path)), make([]int32, len(path))
		for i := range path {
			repetitionLevels[i], _ = schemaHandler.MaxRepetitionLevel(path[:i+1])
			definitionLevels[i], _ = schemaHandler.MaxDefinitionLevel(path[:i+1])
		}

		for _, rc := range sliceRecords {
			rc.Index = -1
		}
		for _, rc := range mapRecords {
			rc.Index = -1
		}

		var prevType reflect.Type
		var prevFieldName string
		var prevFieldIndex []int

		var prevSlicePo reflect.Value
		var prevSliceRecord *SliceRecord

		for i := bgn; i < end; i++ {
			rl, dl, val := table.RepetitionLevels[i], table.DefinitionLevels[i], table.Values[i]
			po, index := root, prefixIndex
		OuterLoop:
			for index < len(path) {
				schemaIndex := schemaIndexs[index]
				_, cT := schemaHandler.SchemaElements[schemaIndex].Type, schemaHandler.SchemaElements[schemaIndex].ConvertedType

				if !po.IsValid() {
					return fmt.Errorf("invalid reflect value encountered during unmarshal")
				}

				poType := po.Type()
				switch poType.Kind() {
				case reflect.Slice:
					cTIsList := cT != nil && *cT == parquet.ConvertedType_LIST

					if po.IsNil() {
						po.Set(reflect.MakeSlice(poType, 0, 0))
					}

					sliceRec := prevSliceRecord
					if prevSlicePo != po {
						prevSlicePo = po
						var ok bool
						sliceRec, ok = sliceRecords[po]
						if !ok {
							sliceRec = &SliceRecord{
								Values: []reflect.Value{},
								Index:  -1,
							}
							sliceRecords[po] = sliceRec
							sliceRecordsStack = append(sliceRecordsStack, po)
						}
						prevSliceRecord = sliceRec
					}

					if cTIsList {
						index++
						if definitionLevels[index] > dl {
							break OuterLoop
						}
					}

					// Apply old list format logic for the inner array level in old list format files
					// Old list format pattern: [..., array, array] where we're at the second array
					// This pattern can appear at any depth in nested schemas, not just at depth 3
					// Examples: [Root, Field, array, array] or [Root, A, B, C, ListField, array, array]
					// Minimum path length of 2 is required to safely check path[index-1]
					isOldListFormatArray := len(path) >= 2 &&
						index < len(path) && strings.EqualFold(path[index], "array") &&
						index > 0 && strings.EqualFold(path[index-1], "array") &&
						index < len(schemaHandler.SchemaElements) &&
						schemaHandler.SchemaElements[schemaIndexs[index]].Type != nil

					var shouldIncrement bool
					if isOldListFormatArray {
						// This is the inner array level - use old list format logic
						if sliceRec.Index < 0 {
							// First element
							shouldIncrement = true
						} else if rl <= 1 {
							// rl=0 (new record) or rl=1 (new inner array) -> increment
							shouldIncrement = true
						} else {
							// rl=2 (continue inner array) -> don't increment
							shouldIncrement = false
						}
					} else {
						// Use normal logic for other levels
						shouldIncrement = rl == repetitionLevels[index] || sliceRec.Index < 0
					}

					if shouldIncrement {
						sliceRec.Index++
					}

					if sliceRec.Index >= len(sliceRec.Values) {
						sliceRec.Values = append(sliceRec.Values, reflect.New(poType.Elem()).Elem())
					}

					po = sliceRec.Values[sliceRec.Index]

					// For old list format at the leaf level, assign the value to the array field
					if isOldListFormatArray && po.Kind() == reflect.Struct &&
						po.NumField() == 1 && strings.EqualFold(po.Type().Field(0).Name, "array") &&
						po.Type().Field(0).Type.Kind() == reflect.Slice {

						arrayField := po.Field(0)
						if arrayField.Kind() == reflect.Slice {
							elemValue := reflect.ValueOf(val)
							// Convert if needed
							if elemValue.Type() != arrayField.Type().Elem() {
								if elemValue.Type().ConvertibleTo(arrayField.Type().Elem()) {
									elemValue = elemValue.Convert(arrayField.Type().Elem())
								}
							}
							arrayField.Set(reflect.Append(arrayField, elemValue))
						}
					}

					if cTIsList {
						index++
						if index < len(definitionLevels) && definitionLevels[index] > dl {
							break OuterLoop
						}
					}
				case reflect.Map:
					if po.IsNil() {
						po.Set(reflect.MakeMap(poType))
					}

					mapRec, ok := mapRecords[po]
					if !ok {
						mapRec = &MapRecord{
							KeyValues: []KeyValue{},
							Index:     -1,
						}
						mapRecords[po] = mapRec
						mapRecordsStack = append(mapRecordsStack, po)
					}

					index++
					if definitionLevels[index] > dl {
						break OuterLoop
					}

					if rl == repetitionLevels[index] || mapRec.Index < 0 {
						mapRec.Index++
					}

					if mapRec.Index >= len(mapRec.KeyValues) {
						mapRec.KeyValues = append(mapRec.KeyValues,
							KeyValue{
								Key:   reflect.New(poType.Key()).Elem(),
								Value: reflect.New(poType.Elem()).Elem(),
							})
					}

					if strings.ToLower(path[index+1]) == "key" {
						po = mapRec.KeyValues[mapRec.Index].Key
					} else {
						po = mapRec.KeyValues[mapRec.Index].Value
					}

					index++
					if definitionLevels[index] > dl {
						break OuterLoop
					}

				case reflect.Ptr:
					if po.IsNil() {
						po.Set(reflect.New(poType.Elem()))
					}

					po = po.Elem()
					if !po.IsValid() {
						return fmt.Errorf("pointer dereference resulted in invalid value")
					}

				case reflect.Struct:
					index++
					if index < len(definitionLevels) && definitionLevels[index] > dl {
						break OuterLoop
					}
					if index >= len(path) {
						break OuterLoop
					}
					name := path[index]

					if prevType != poType || name != prevFieldName {
						prevType = poType
						prevFieldName = name
						f, ok := prevType.FieldByName(name)
						if !ok {
							return fmt.Errorf("field %q not found in struct type %v", name, prevType)
						}
						prevFieldIndex = f.Index
					}
					po = po.FieldByIndex(prevFieldIndex)
					if !po.IsValid() {
						return fmt.Errorf("field access resulted in invalid value for field %q", name)
					}

				default:
					if !po.IsValid() {
						return fmt.Errorf("invalid reflect value encountered before setting value")
					}

					// Handle nil values gracefully
					if val == nil {
						// Skip nil values - this might be expected for optional fields
						break OuterLoop
					}

					value := reflect.ValueOf(val)
					if !value.IsValid() {
						// Skip invalid values - this might be expected for optional fields
						break OuterLoop
					}

					// Re-check validity and get type safely
					if !po.IsValid() {
						return fmt.Errorf("reflect value became invalid before type comparison")
					}

					// Use defer/recover to safely get the type
					var poTypeForConvert reflect.Type
					func() {
						defer func() {
							if r := recover(); r != nil {
								poTypeForConvert = nil
							}
						}()
						poTypeForConvert = po.Type()
					}()

					if poTypeForConvert == nil {
						return fmt.Errorf("get type from reflect value, possibly corrupted")
					}

					// Safely get value type
					var valueType reflect.Type
					func() {
						defer func() {
							if r := recover(); r != nil {
								valueType = nil
							}
						}()
						valueType = value.Type()
					}()

					if valueType == nil {
						// This should not happen since we checked value.IsValid() above
						return fmt.Errorf("get type from valid value, this is unexpected")
					}

					if poTypeForConvert != valueType {
						var convertErr error
						func() {
							defer func() {
								if r := recover(); r != nil {
									convertErr = fmt.Errorf("convert type: %v", r)
								}
							}()
							value = value.Convert(poTypeForConvert)
						}()
						if convertErr != nil {
							return convertErr
						}
					}
					var setErr error
					func() {
						defer func() {
							if r := recover(); r != nil {
								setErr = fmt.Errorf("set value: %v", r)
							}
						}()
						po.Set(value)
					}()
					if setErr != nil {
						return setErr
					}
					break OuterLoop
				}
			}
		}
	}

	for i := len(sliceRecordsStack) - 1; i >= 0; i-- {
		po := sliceRecordsStack[i]
		vs := sliceRecords[po]
		potmp := reflect.Append(po, vs.Values...)
		po.Set(potmp)
	}

	for i := len(mapRecordsStack) - 1; i >= 0; i-- {
		po := mapRecordsStack[i]
		for _, kv := range mapRecords[po].KeyValues {
			po.SetMapIndex(kv.Key, kv.Value)
		}
	}

	return nil
}

// ConvertToJSONFriendly converts parquet data to JSON-friendly format by applying logical type conversions
func ConvertToJSONFriendly(data any, schemaHandler *schema.SchemaHandler) (any, error) {
	ctx := &conversionContext{}
	return convertValueToJSONFriendlyWithContext(reflect.ValueOf(data), schemaHandler, "", ctx)
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
func convertValueToJSONFriendlyWithContext(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, ctx *conversionContext) (any, error) {
	if !val.IsValid() {
		return nil, nil
	}

	switch val.Kind() {
	case reflect.Interface:
		if val.IsNil() {
			return nil, nil
		}
		return convertValueToJSONFriendlyWithContext(val.Elem(), schemaHandler, pathPrefix, ctx)

	case reflect.Pointer:
		if val.IsNil() {
			return nil, nil
		}
		return convertValueToJSONFriendlyWithContext(val.Elem(), schemaHandler, pathPrefix, ctx)

	case reflect.Slice:
		return convertSliceToJSONFriendly(val, schemaHandler, pathPrefix, ctx)

	case reflect.Map:
		return convertMapToJSONFriendly(val, schemaHandler, pathPrefix, ctx)

	case reflect.Struct:
		return convertStructToJSONFriendly(val, schemaHandler, pathPrefix, ctx)

	default:
		return convertPrimitiveToJSONFriendly(val, schemaHandler, pathPrefix, ctx)
	}
}

// convertSliceToJSONFriendly optimized slice conversion
func convertSliceToJSONFriendly(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, ctx *conversionContext) (any, error) {
	result := make([]any, val.Len())
	var elementPath string
	if pathPrefix != "" {
		var builder strings.Builder
		builder.WriteString(pathPrefix)
		builder.WriteString(common.PAR_GO_PATH_DELIMITER)
		builder.WriteString("List")
		builder.WriteString(common.PAR_GO_PATH_DELIMITER)
		builder.WriteString("Element")
		elementPath = builder.String()
	}

	for i := range val.Len() {
		converted, err := convertValueToJSONFriendlyWithContext(val.Index(i), schemaHandler, elementPath, ctx)
		if err != nil {
			return nil, err
		}
		result[i] = converted
	}
	return result, nil
}

// convertMapToJSONFriendly optimized map conversion
func convertMapToJSONFriendly(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, ctx *conversionContext) (any, error) {
	result := make(map[string]any)
	var keyPath, valuePath string

	if pathPrefix != "" {
		var builder strings.Builder
		builder.WriteString(pathPrefix)
		builder.WriteString(common.PAR_GO_PATH_DELIMITER)
		builder.WriteString("Key_value")
		builder.WriteString(common.PAR_GO_PATH_DELIMITER)
		builder.WriteString("Key")
		keyPath = builder.String()

		builder.Reset()
		builder.WriteString(pathPrefix)
		builder.WriteString(common.PAR_GO_PATH_DELIMITER)
		builder.WriteString("Key_value")
		builder.WriteString(common.PAR_GO_PATH_DELIMITER)
		builder.WriteString("Value")
		valuePath = builder.String()
	}

	for _, key := range val.MapKeys() {
		converted, err := convertValueToJSONFriendlyWithContext(key, schemaHandler, keyPath, ctx)
		if err != nil {
			return nil, err
		}
		keyStr := fmt.Sprint(converted)

		converted, err = convertValueToJSONFriendlyWithContext(val.MapIndex(key), schemaHandler, valuePath, ctx)
		if err != nil {
			return nil, err
		}
		result[keyStr] = converted
	}
	return result, nil
}

// convertStructToJSONFriendly optimized struct conversion with field caching
func convertStructToJSONFriendly(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, ctx *conversionContext) (any, error) {
	valType := val.Type()

	// Special handling for old list format: if struct has single "array" field with slice data,
	// return the slice directly instead of wrapping in map
	if val.NumField() == 1 &&
		strings.EqualFold(valType.Field(0).Name, "array") &&
		valType.Field(0).Type.Kind() == reflect.Slice {
		fieldPath := valType.Field(0).Name
		if pathPrefix != "" {
			fieldPath = pathPrefix + common.PAR_GO_PATH_DELIMITER + fieldPath
		}
		return convertValueToJSONFriendlyWithContext(val.Field(0), schemaHandler, fieldPath, ctx)
	}

	result := make(map[string]any)
	var fieldMap map[string]fieldInfo

	fieldMapInterface, exists := ctx.fieldCache.Load(valType)
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
		ctx.fieldCache.Store(valType, fieldMap)
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
			fieldPath = pathPrefix + common.PAR_GO_PATH_DELIMITER + fieldPath
		}

		converted, err := convertValueToJSONFriendlyWithContext(fieldVal, schemaHandler, fieldPath, ctx)
		if err != nil {
			return nil, err
		}
		result[fInfo.name] = converted
	}
	return result, nil
}

// convertPrimitiveToJSONFriendly optimized primitive conversion with schema caching
func convertPrimitiveToJSONFriendly(val reflect.Value, schemaHandler *schema.SchemaHandler, pathPrefix string, ctx *conversionContext) (any, error) {
	if pathPrefix == "" {
		return val.Interface(), nil
	}

	rootName := schemaHandler.GetRootInName()
	expectedSchemaPath := pathPrefix
	if !strings.HasPrefix(pathPrefix, rootName) {
		expectedSchemaPath = rootName + common.PAR_GO_PATH_DELIMITER + expectedSchemaPath
	}

	var schemaElement *parquet.SchemaElement

	schemaElementInterface, cached := ctx.schemaCache.Load(expectedSchemaPath)
	if !cached {
		schemaIndex, exists := schemaHandler.MapIndex[expectedSchemaPath]
		if !exists || int(schemaIndex) >= len(schemaHandler.SchemaElements) {
			return val.Interface(), nil
		}
		schemaElement = schemaHandler.SchemaElements[schemaIndex]
		ctx.schemaCache.Store(expectedSchemaPath, schemaElement)
	} else {
		schemaElement = schemaElementInterface.(*parquet.SchemaElement)
	}

	if schemaElement == nil {
		return val.Interface(), nil
	}

	pT, cT, lT := schemaElement.Type, schemaElement.ConvertedType, schemaElement.LogicalType
	precision, scale := int(schemaElement.GetPrecision()), int(schemaElement.GetScale())
	converted := types.ParquetTypeToJSONTypeWithLogical(val.Interface(), pT, cT, lT, precision, scale)

	if converted != val.Interface() {
		return converted, nil
	}
	return val.Interface(), nil
}
