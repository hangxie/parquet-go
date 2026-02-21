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

// ShreddedVariantReconstructor handles the reconstruction of shredded VARIANT columns.
// It collects related tables (metadata, value, typed_value) and reconstructs full
// Variant values row by row.
type ShreddedVariantReconstructor struct {
	Path             string                    // Path of the variant group
	Info             *schema.VariantSchemaInfo // Schema info for this variant
	MetadataTable    *layout.Table             // metadata column (always present)
	ValueTable       *layout.Table             // value column (may be nil if fully shredded)
	TypedValueTables []*layout.Table           // typed_value columns (legacy, kept for tests)
	tableMap         *map[string]*layout.Table // map of all tables for recursive lookup
	SchemaHandler    *schema.SchemaHandler     // Schema handler for path resolution
}

// NewShreddedVariantReconstructor creates a reconstructor for a shredded variant column.
func NewShreddedVariantReconstructor(
	path string,
	info *schema.VariantSchemaInfo,
	tableMap *map[string]*layout.Table,
	sh *schema.SchemaHandler,
) *ShreddedVariantReconstructor {
	r := &ShreddedVariantReconstructor{
		Path:          path,
		Info:          info,
		SchemaHandler: sh,
		tableMap:      tableMap,
	}

	// Find metadata table
	metadataPath := sh.IndexMap[info.MetadataIdx]
	if table, ok := (*tableMap)[metadataPath]; ok {
		r.MetadataTable = table
	}

	// Find value table (may not exist in fully shredded variant)
	var valuePath string
	if info.ValueIdx >= 0 {
		valuePath = sh.IndexMap[info.ValueIdx]
		if table, ok := (*tableMap)[valuePath]; ok {
			r.ValueTable = table
		}
	}

	// Find all typed_value leaf tables (kept for test compatibility)
	for tableName, table := range *tableMap {
		if !strings.HasPrefix(tableName, path+common.PAR_GO_PATH_DELIMITER) {
			continue
		}
		if tableName == metadataPath || (valuePath != "" && tableName == valuePath) {
			continue
		}

		// Also skip if it looks like metadata or value by name (safety/robustness)
		relPath := strings.TrimPrefix(tableName, path+common.PAR_GO_PATH_DELIMITER)
		if strings.EqualFold(relPath, "Metadata") || strings.EqualFold(relPath, "Value") {
			continue
		}

		r.TypedValueTables = append(r.TypedValueTables, table)
	}

	return r
}

// getValueAtRow returns the value from a table at the given row index.
// It handles definition levels to return nil for missing values.
// getValueAtRow returns the value from a table at the given row index.
// It handles repeated fields by returning a slice of values.
func (r *ShreddedVariantReconstructor) getValueAtRow(table *layout.Table, rowIdx int, tableBgn, tableEnd map[string]int) (any, error) {
	if table == nil {
		return nil, nil
	}

	tableName := common.PathToStr(table.Path)
	bgn, ok1 := tableBgn[tableName]
	end, ok2 := tableEnd[tableName]
	if !ok1 || !ok2 || bgn < 0 {
		return nil, nil
	}

	// Find the values for this row by scanning repetition levels
	currentRow := -1
	var values []any
	for i := bgn; i < end; i++ {
		if table.RepetitionLevels[i] == 0 {
			currentRow++
		}
		if currentRow == rowIdx {
			// Check definition level to see if value is present
			maxDL, err := r.SchemaHandler.MaxDefinitionLevel(table.Path)
			if err != nil {
				return nil, err
			}
			if table.DefinitionLevels[i] >= maxDL {
				values = append(values, table.Values[i])
			} else {
				values = append(values, nil)
			}
		} else if currentRow > rowIdx {
			break
		}
	}

	if len(values) == 0 {
		return nil, nil
	}

	// Check if the field is repeated in the schema
	isRepeated := false
	maxRL, err := r.SchemaHandler.MaxRepetitionLevel(table.Path)
	if err != nil {
		return nil, err
	}
	if maxRL > 0 {
		isRepeated = true
	}

	if isRepeated {
		return values, nil
	}
	return values[0], nil
}

// reconstructValue recursively builds a Go value from shredded columns.
func (r *ShreddedVariantReconstructor) reconstructValue(pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte) (any, error) {
	// 1. Check if this is a leaf column
	if table, ok := (*r.tableMap)[pathPrefix]; ok {
		val, err := r.getValueAtRow(table, rowIdx, tableBgn, tableEnd)
		if err != nil {
			fmt.Printf("Error at path %q: %v\n", pathPrefix, err)
			return nil, err
		}
		return val, nil
	}

	// 2. Not a leaf, check for children
	childTables := make(map[string][]*layout.Table)
	for tableName, table := range *r.tableMap {
		if strings.HasPrefix(tableName, pathPrefix+common.PAR_GO_PATH_DELIMITER) {
			relPath := strings.TrimPrefix(tableName, pathPrefix+common.PAR_GO_PATH_DELIMITER)
			parts := strings.Split(relPath, common.PAR_GO_PATH_DELIMITER)
			childName := parts[0]
			childTables[childName] = append(childTables[childName], table)
		}
	}

	if len(childTables) == 0 {
		return nil, nil
	}

	// Special case: if we have Metadata/Value/Typed_value, it's a variant
	var metaName, valueName, typedName string
	for name := range childTables {
		if strings.EqualFold(name, "Metadata") {
			metaName = name
		} else if strings.EqualFold(name, "Value") {
			valueName = name
		} else if strings.EqualFold(name, "Typed_value") || strings.EqualFold(name, "TypedValue") {
			typedName = name
		}
	}

	if metaName != "" || valueName != "" || typedName != "" {
		metadataValues := make([]any, 0)
		metadataSet := false
		if metaName != "" {
			mVal, err := r.reconstructValue(pathPrefix+common.PAR_GO_PATH_DELIMITER+metaName, rowIdx, tableBgn, tableEnd, nil)
			if err != nil {
				return nil, err
			}
			if slice, ok := mVal.([]any); ok {
				metadataValues = slice
				metadataSet = true
			} else if mVal != nil {
				metadataValues = []any{mVal}
				metadataSet = true
			}
		}

		valueValues := make([]any, 0)
		valueSet := false
		if valueName != "" {
			vVal, err := r.reconstructValue(pathPrefix+common.PAR_GO_PATH_DELIMITER+valueName, rowIdx, tableBgn, tableEnd, nil)
			if err != nil {
				return nil, err
			}
			if slice, ok := vVal.([]any); ok {
				valueValues = slice
				valueSet = true
			} else if vVal != nil {
				valueValues = []any{vVal}
				valueSet = true
			}
		}

		typedValueValues := make([]any, 0)
		typedValueSet := false
		if typedName != "" {
			var err error
			// Pass metadata down if set, otherwise use current metadata
			effectiveMetadata := metadata
			if metadataSet && len(metadataValues) > 0 {
				if m, ok := metadataValues[0].([]byte); ok {
					effectiveMetadata = m
				} else if s, ok := metadataValues[0].(string); ok {
					effectiveMetadata = []byte(s)
				}
			}
			tVal, err := r.reconstructValue(pathPrefix+common.PAR_GO_PATH_DELIMITER+typedName, rowIdx, tableBgn, tableEnd, effectiveMetadata)
			if err != nil {
				return nil, err
			}
			if slice, ok := tVal.([]any); ok {
				typedValueValues = slice
				typedValueSet = true
			} else if tVal != nil {
				typedValueValues = []any{tVal}
				typedValueSet = true
			}
		}

		if !metadataSet && !valueSet && !typedValueSet {
			return nil, nil
		}

		// Calculate max length for zipping
		maxLen := 0
		if len(metadataValues) > maxLen {
			maxLen = len(metadataValues)
		}
		if len(valueValues) > maxLen {
			maxLen = len(valueValues)
		}
		if len(typedValueValues) > maxLen {
			maxLen = len(typedValueValues)
		}

		if maxLen == 0 {
			return nil, nil
		}

		// Determine if we should return a slice or a single value
		isRepeated := false
		if (metadataSet && len(metadataValues) > 1) || (valueSet && len(valueValues) > 1) || (typedValueSet && len(typedValueValues) > 1) {
			isRepeated = true
		} else {
			// Check if any of the underlying leaf columns are repeated
			for _, tables := range childTables {
				for _, table := range tables {
					maxRL, err := r.SchemaHandler.MaxRepetitionLevel(table.Path)
					if err != nil {
						return nil, err
					}
					if maxRL > 0 {
						isRepeated = true
						break
					}
				}
				if isRepeated {
					break
				}
			}
		}

		results := make([]types.Variant, maxLen)
		for i := range maxLen {
			var elMetadata []byte
			if i < len(metadataValues) && metadataValues[i] != nil {
				switch v := metadataValues[i].(type) {
				case []byte:
					elMetadata = v
				case string:
					elMetadata = []byte(v)
				default:
					return nil, fmt.Errorf("unexpected metadata type: %T", v)
				}
			}
			if len(elMetadata) == 0 {
				if len(metadata) > 0 {
					elMetadata = metadata
				} else {
					elMetadata = []byte{0x01, 0x00, 0x00}
				}
			}

			var elValue []byte
			if i < len(valueValues) && valueValues[i] != nil {
				switch v := valueValues[i].(type) {
				case []byte:
					elValue = v
				case string:
					elValue = []byte(v)
				default:
					return nil, fmt.Errorf("unexpected value type: %T", v)
				}
			}

			var elTypedValue any
			if i < len(typedValueValues) {
				elTypedValue = typedValueValues[i]
			}

			v, err := types.ReconstructVariant(elMetadata, elValue, elTypedValue)
			if err != nil {
				// Fallback
				results[i] = types.Variant{Metadata: elMetadata, Value: types.EncodeVariantNull()}
			} else {
				results[i] = v
			}
		}

		if isRepeated {
			// Convert []types.Variant to []any
			anyResults := make([]any, len(results))
			for i, v := range results {
				if len(v.Value) == 0 || (len(v.Value) == 1 && v.Value[0] == 0) {
					anyResults[i] = nil
				} else {
					anyResults[i] = v
				}
			}
			return anyResults, nil
		}

		v := results[0]
		if len(v.Value) == 0 || (len(v.Value) == 1 && v.Value[0] == 0) {
			return nil, nil
		}
		return v, nil
	}

	// Not a variant itself. Could be an object, a list, or just a wrapper for a leaf value (e.g. A.Value).
	if len(childTables) == 1 {
		// If it's a single child named "Value" or "Typed_value", return its reconstruction directly
		// This handles the A.Value or B.Value case where A is a group with one child.
		for name := range childTables {
			if name == "Value" || name == "Typed_value" {
				return r.reconstructValue(pathPrefix+common.PAR_GO_PATH_DELIMITER+name, rowIdx, tableBgn, tableEnd, metadata)
			}
		}
	}

	// Check if the current path is a LIST in the schema
	idx, ok := r.SchemaHandler.MapIndex[pathPrefix]
	isList := ok && r.SchemaHandler.SchemaElements[idx].ConvertedType != nil &&
		*r.SchemaHandler.SchemaElements[idx].ConvertedType == parquet.ConvertedType_LIST

	if isList {
		return r.reconstructValue(pathPrefix+common.PAR_GO_PATH_DELIMITER+"List", rowIdx, tableBgn, tableEnd, metadata)
	}

	// Handle 3-level list "List" group
	if strings.HasSuffix(pathPrefix, common.PAR_GO_PATH_DELIMITER+"List") {
		return r.reconstructValue(pathPrefix+common.PAR_GO_PATH_DELIMITER+"Element", rowIdx, tableBgn, tableEnd, metadata)
	}

	// Handle "Element" group (repeated)
	if strings.HasSuffix(pathPrefix, common.PAR_GO_PATH_DELIMITER+"Element") {
		// ... existing repeated element logic ...
		tableValues := make(map[string][]any)
		maxLen := 0
		for childName := range childTables {
			val, err := r.reconstructValue(pathPrefix+common.PAR_GO_PATH_DELIMITER+childName, rowIdx, tableBgn, tableEnd, metadata)
			if err != nil {
				return nil, err
			}
			if slice, ok := val.([]any); ok {
				tableValues[childName] = slice
				if len(slice) > maxLen {
					maxLen = len(slice)
				}
			} else if val != nil {
				tableValues[childName] = []any{val}
				if maxLen == 0 {
					maxLen = 1
				}
			}
		}

		if maxLen == 0 {
			return nil, nil
		}

		elements := make([]any, maxLen)
		for i := range maxLen {
			elementMap := make(map[string]any)
			for childName, slice := range tableValues {
				if i < len(slice) {
					elementMap[childName] = slice[i]
				}
			}

			_, hasMetadata := elementMap["Metadata"]
			_, hasValue := elementMap["Value"]
			_, hasTypedValue := elementMap["Typed_value"]
			if hasMetadata || hasValue || hasTypedValue {
				var elMetadata []byte
				metadataSet := false
				if hasMetadata {
					switch v := elementMap["Metadata"].(type) {
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
						elMetadata = []byte{0x01, 0x00, 0x00}
					}
				}
				elValue, _ := elementMap["Value"].([]byte)
				elTypedValue := elementMap["Typed_value"]
				v, _ := types.ReconstructVariant(elMetadata, elValue, elTypedValue)
				elements[i] = v
			} else {
				if len(elementMap) == 1 {
					for name, v := range elementMap {
						if name == "Value" || name == "Typed_value" {
							elements[i] = v
						} else {
							elements[i] = elementMap
						}
					}
				} else {
					elements[i] = elementMap
				}
			}
		}
		return elements, nil
	}

	// General case: map (OBJECT variant fields)
	tableValues := make(map[string][]any)
	maxLen := -1
	isRepeated := false

	for childName := range childTables {
		childPath := pathPrefix + common.PAR_GO_PATH_DELIMITER + childName
		val, err := r.reconstructValue(childPath, rowIdx, tableBgn, tableEnd, metadata)
		if err != nil {
			return nil, err
		}
		if val != nil {
			if slice, ok := val.([]any); ok {
				isRepeated = true
				tableValues[childName] = slice
				if maxLen == -1 || len(slice) > maxLen {
					maxLen = len(slice)
				}
			} else {
				tableValues[childName] = []any{val}
				if maxLen == -1 {
					maxLen = 1
				}
			}
		}
	}

	if maxLen == -1 {
		return nil, nil
	}

	if isRepeated {
		// Zip slices into a slice of maps
		results := make([]any, maxLen)
		for i := range maxLen {
			obj := make(map[string]any)
			for childName, slice := range tableValues {
				if i < len(slice) {
					val := slice[i]
					if val != nil {
						// Use ExName for the field name
						actualFieldName := childName
						childPath := pathPrefix + common.PAR_GO_PATH_DELIMITER + childName
						if idx, ok := r.SchemaHandler.MapIndex[childPath]; ok {
							actualFieldName = r.SchemaHandler.Infos[idx].ExName
						}
						obj[actualFieldName] = val
					}
				}
			}
			results[i] = obj
		}
		return results, nil
	}

	// Not repeated, return a single map
	obj := make(map[string]any)
	for childName, slice := range tableValues {
		val := slice[0]
		// Use ExName for the field name
		actualFieldName := childName
		childPath := pathPrefix + common.PAR_GO_PATH_DELIMITER + childName
		if idx, ok := r.SchemaHandler.MapIndex[childPath]; ok {
			actualFieldName = r.SchemaHandler.Infos[idx].ExName
		}
		obj[actualFieldName] = val
	}
	return obj, nil
}

// Reconstruct reconstructs a Variant value for the given row index.
func (r *ShreddedVariantReconstructor) Reconstruct(rowIdx int, tableBgn, tableEnd map[string]int) (types.Variant, error) {
	val, err := r.reconstructValue(r.Path, rowIdx, tableBgn, tableEnd, nil)
	if err != nil {
		return types.Variant{}, err
	}
	if val == nil {
		// Return a NULL variant
		return types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    types.EncodeVariantNull(),
		}, nil
	}
	if v, ok := val.(types.Variant); ok {
		return v, nil
	}
	v, err := types.AnyToVariant(val)
	if err != nil {
		return types.Variant{}, fmt.Errorf("failed to encode reconstructed value at %s: %w", r.Path, err)
	}
	return v, nil
}

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

	// Identify shredded variant groups and their reconstructors
	variantReconstructors := make(map[string]*ShreddedVariantReconstructor)
	variantChildPaths := make(map[string]string) // maps child path to variant path

	if schemaHandler.VariantSchemas != nil {
		for variantPath, info := range schemaHandler.VariantSchemas {
			if !strings.HasPrefix(variantPath, prefixPath) {
				continue
			}

			// Create reconstructor for this variant
			reconstructor := NewShreddedVariantReconstructor(variantPath, info, tableMap, schemaHandler)
			variantReconstructors[variantPath] = reconstructor

			// Mark all child paths as belonging to this variant
			for childPath := range tableNeeds {
				if strings.HasPrefix(childPath, variantPath+common.PAR_GO_PATH_DELIMITER) {
					variantChildPaths[childPath] = variantPath
				}
			}
		}
	}

	for name, table := range tableNeeds {
		// Skip tables that are children of shredded variants - they're handled separately
		if _, isVariantChild := variantChildPaths[name]; isVariantChild {
			continue
		}
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
			var err error
			if repetitionLevels[i], err = schemaHandler.MaxRepetitionLevel(path[:i+1]); err != nil {
				return err
			}
			if definitionLevels[i], err = schemaHandler.MaxDefinitionLevel(path[:i+1]); err != nil {
				return err
			}
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
					// []byte should be treated as primitive BYTE_ARRAY, not as a slice/list
					if poType.Elem().Kind() == reflect.Uint8 {
						// Handle nil values gracefully
						if val == nil {
							break OuterLoop
						}
						value := reflect.ValueOf(val)
						if !value.IsValid() {
							break OuterLoop
						}
						// Convert string to []byte if needed
						if value.Kind() == reflect.String {
							po.Set(reflect.ValueOf([]byte(value.String())))
						} else if value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
							po.Set(value)
						} else {
							return fmt.Errorf("cannot assign %v to []byte field", value.Type())
						}
						break OuterLoop
					}

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

					if index+1 >= len(path) {
						return fmt.Errorf("invalid path: missing key/value component after map")
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

					poTypeForConvert := po.Type()
					valueType := value.Type()

					if poTypeForConvert != valueType {
						// Special handling for string -> []byte conversion (BYTE_ARRAY to []byte)
						if valueType.Kind() == reflect.String && poTypeForConvert.Kind() == reflect.Slice && poTypeForConvert.Elem().Kind() == reflect.Uint8 {
							strVal := value.String()
							value = reflect.ValueOf([]byte(strVal))
						} else {
							if !valueType.ConvertibleTo(poTypeForConvert) {
								return fmt.Errorf("cannot convert value of type %v to type %v", valueType, poTypeForConvert)
							}
							value = value.Convert(poTypeForConvert)
						}
					}

					if !po.CanSet() {
						return fmt.Errorf("cannot set value for field (unaddressable or unexported)")
					}
					// Ensure value is assignable to po
					if !value.Type().AssignableTo(po.Type()) {
						return fmt.Errorf("cannot assign value of type %v to field of type %v", value.Type(), po.Type())
					}

					po.Set(value)
					break OuterLoop
				}
			}
		}
	}

	// Process shredded variants
	for variantPath, reconstructor := range variantReconstructors {
		// Calculate actual number of rows in this batch from the metadata table
		// We can't use end - bgn because that's the requested batch size, which may
		// exceed the actual rows in the file.
		numRows := 0
		if reconstructor.MetadataTable != nil {
			metaPath := common.PathToStr(reconstructor.MetadataTable.Path)
			metaBgn, metaEnd := tableBgn[metaPath], tableEnd[metaPath]
			if metaBgn >= 0 && metaEnd > metaBgn {
				for i := metaBgn; i < metaEnd; i++ {
					if reconstructor.MetadataTable.RepetitionLevels[i] == 0 {
						numRows++
					}
				}
			}
		}

		// Navigate to the variant field location and set each row's variant value
		// Ensure root slice is large enough if we're unmarshaling into a slice
		shouldExpand := true
		if sliceRec, ok := sliceRecords[root]; ok && len(sliceRec.Values) >= numRows {
			shouldExpand = false
		}

		if shouldExpand && root.Kind() == reflect.Slice && root.Len() < numRows {
			newSlice := reflect.MakeSlice(root.Type(), numRows, numRows)
			reflect.Copy(newSlice, root)
			root.Set(newSlice)
		}

		for rowIdx := range numRows {
			variant, err := reconstructor.Reconstruct(rowIdx, tableBgn, tableEnd)
			if err != nil {
				return fmt.Errorf("reconstruct variant at %s row %d: %w", variantPath, rowIdx, err)
			}

			// Set the variant value at the appropriate location in the struct
			if err := setVariantValue(root, variantPath, prefixPath, schemaHandler, variant, rowIdx, sliceRecords); err != nil {
				return fmt.Errorf("set variant at %s row %d: %w", variantPath, rowIdx, err)
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

// setVariantValue navigates the struct path and sets a variant value at the specified location.
func setVariantValue(root reflect.Value, variantPath, prefixPath string, _ *schema.SchemaHandler, variant types.Variant, rowIdx int, sliceRecords map[reflect.Value]*SliceRecord) error {
	path := common.StrToPath(variantPath)
	prefixIndex := common.PathStrIndex(prefixPath)

	po := root

	for i := prefixIndex; i < len(path); i++ {
		if !po.IsValid() {
			return fmt.Errorf("invalid reflect value at path index %d", i)
		}

		switch po.Kind() {
		case reflect.Ptr:
			if po.IsNil() {
				po.Set(reflect.New(po.Type().Elem()))
			}
			po = po.Elem()
			i-- // Stay at the same path element
			continue

		case reflect.Slice:
			// Handle slice navigation
			if po.Type().Elem().Kind() == reflect.Uint8 {
				// []byte - not navigable
				return fmt.Errorf("unexpected []byte at path index %d", i)
			}

			// Ensure slice has enough elements
			sliceRec, ok := sliceRecords[po]
			if ok && rowIdx < len(sliceRec.Values) {
				po = sliceRec.Values[rowIdx]
			} else if rowIdx < po.Len() {
				po = po.Index(rowIdx)
			} else {
				return fmt.Errorf("row index %d out of bounds for slice at path index %d", rowIdx, i)
			}
			i-- // Stay at the same path element
			continue

		case reflect.Struct:
			// Navigate to the next field
			fieldName := path[i]
			field := po.FieldByName(fieldName)
			if !field.IsValid() {
				return fmt.Errorf("field %q not found at path index %d", fieldName, i)
			}
			po = field

		case reflect.Interface:
			if po.IsNil() {
				// If we're at the end of the path, we can set the variant directly
				if i == len(path)-1 {
					po.Set(reflect.ValueOf(variant))
					return nil
				}
				return fmt.Errorf("cannot navigate through nil interface at path index %d", i)
			}
			po = po.Elem()
			i-- // Stay at the same path element
			continue

		default:
			return fmt.Errorf("unexpected kind %v at path index %d", po.Kind(), i)
		}
	}

	// Final assignment if we reached the end of path
	isNull := len(variant.Value) == 0 || (len(variant.Value) == 1 && variant.Value[0] == 0 && len(variant.Metadata) == 0)

	if po.Type() == reflect.TypeOf(types.Variant{}) || po.Kind() == reflect.Interface {
		if isNull && po.Kind() == reflect.Interface {
			po.Set(reflect.Zero(po.Type()))
			return nil
		}
		if po.Kind() == reflect.Interface && po.Type() != reflect.TypeOf(types.Variant{}) {
			decoded, err := types.ConvertVariantValue(variant)
			if err != nil {
				// Fallback to variant struct if decode fails
				po.Set(reflect.ValueOf(variant))
			} else if decoded != nil {
				po.Set(reflect.ValueOf(decoded))
			} else {
				// Decoded is nil (should be handled by isNull above, but just in case)
				po.Set(reflect.Zero(po.Type()))
			}
			return nil
		}
		po.Set(reflect.ValueOf(variant))
		return nil
	}
	if po.Kind() == reflect.Ptr && (po.Type().Elem() == reflect.TypeOf(types.Variant{}) || po.Type().Elem().Kind() == reflect.Interface) {
		if isNull {
			po.Set(reflect.Zero(po.Type()))
			return nil
		}
		if po.IsNil() {
			po.Set(reflect.New(po.Type().Elem()))
		}
		if po.Type().Elem().Kind() == reflect.Interface && po.Type().Elem() != reflect.TypeOf(types.Variant{}) {
			decoded, err := types.ConvertVariantValue(variant)
			if err != nil {
				po.Elem().Set(reflect.ValueOf(variant))
			} else if decoded != nil {
				po.Elem().Set(reflect.ValueOf(decoded))
			} else {
				po.Elem().Set(reflect.Zero(po.Type().Elem()))
			}
			return nil
		}
		po.Elem().Set(reflect.ValueOf(variant))
		return nil
	}

	return fmt.Errorf("could not set variant value at path end")
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
