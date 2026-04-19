package marshal

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
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
		if !strings.HasPrefix(tableName, path+common.ParGoPathDelimiter) {
			continue
		}
		if tableName == metadataPath || (valuePath != "" && tableName == valuePath) {
			continue
		}

		// Also skip if it looks like metadata or value by name (safety/robustness)
		relPath := strings.TrimPrefix(tableName, path+common.ParGoPathDelimiter)
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

func (r *ShreddedVariantReconstructor) findChildTables(pathPrefix string) map[string][]*layout.Table {
	childTables := make(map[string][]*layout.Table)
	for tableName, table := range *r.tableMap {
		if strings.HasPrefix(tableName, pathPrefix+common.ParGoPathDelimiter) {
			relPath := strings.TrimPrefix(tableName, pathPrefix+common.ParGoPathDelimiter)
			parts := strings.Split(relPath, common.ParGoPathDelimiter)
			childTables[parts[0]] = append(childTables[parts[0]], table)
		}
	}
	return childTables
}

type variantChildNames struct {
	meta, value, typed string
}

func findVariantChildNames(childTables map[string][]*layout.Table) (variantChildNames, bool) {
	var names variantChildNames
	for name := range childTables {
		switch {
		case strings.EqualFold(name, "Metadata"):
			names.meta = name
		case strings.EqualFold(name, "Value"):
			names.value = name
		case strings.EqualFold(name, "Typed_value") || strings.EqualFold(name, "TypedValue"):
			names.typed = name
		}
	}
	return names, names.meta != "" || names.value != "" || names.typed != ""
}

func (r *ShreddedVariantReconstructor) reconstructChildValues(pathPrefix, childName string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte) ([]any, bool, error) {
	if childName == "" {
		return nil, false, nil
	}
	val, err := r.reconstructValue(pathPrefix+common.ParGoPathDelimiter+childName, rowIdx, tableBgn, tableEnd, metadata)
	if err != nil {
		return nil, false, err
	}
	if slice, ok := val.([]any); ok {
		return slice, true, nil
	}
	if val != nil {
		return []any{val}, true, nil
	}
	return nil, false, nil
}

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

func (r *ShreddedVariantReconstructor) isChildTablesRepeated(childTables map[string][]*layout.Table) (bool, error) {
	for _, tables := range childTables {
		for _, table := range tables {
			maxRL, err := r.SchemaHandler.MaxRepetitionLevel(table.Path)
			if err != nil {
				return false, err
			}
			if maxRL > 0 {
				return true, nil
			}
		}
	}
	return false, nil
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

func (r *ShreddedVariantReconstructor) reconstructVariantGroup(
	pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte,
	childTables map[string][]*layout.Table, names variantChildNames,
) (any, error) {
	metadataValues, metadataSet, err := r.reconstructChildValues(pathPrefix, names.meta, rowIdx, tableBgn, tableEnd, nil)
	if err != nil {
		return nil, err
	}
	valueValues, valueSet, err := r.reconstructChildValues(pathPrefix, names.value, rowIdx, tableBgn, tableEnd, nil)
	if err != nil {
		return nil, err
	}

	typedMeta := effectiveMetadataForTyped(metadataValues, metadataSet, metadata)
	typedValueValues, typedValueSet, err := r.reconstructChildValues(pathPrefix, names.typed, rowIdx, tableBgn, tableEnd, typedMeta)
	if err != nil {
		return nil, err
	}

	if !metadataSet && !valueSet && !typedValueSet {
		return nil, nil
	}

	maxLen := max(len(metadataValues), len(valueValues), len(typedValueValues))
	if maxLen == 0 {
		return nil, nil
	}

	isRepeated := (metadataSet && len(metadataValues) > 1) || (valueSet && len(valueValues) > 1) || (typedValueSet && len(typedValueValues) > 1)
	if !isRepeated {
		isRepeated, err = r.isChildTablesRepeated(childTables)
		if err != nil {
			return nil, err
		}
	}

	results, err := buildVariantResults(maxLen, metadataValues, valueValues, typedValueValues, metadata)
	if err != nil {
		return nil, err
	}
	return variantResultsToAny(results, isRepeated)
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

func (r *ShreddedVariantReconstructor) reconstructElementChildren(
	pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte,
	childTables map[string][]*layout.Table,
) (any, error) {
	tableValues, maxLen, err := r.collectChildValues(pathPrefix, rowIdx, tableBgn, tableEnd, metadata, childTables)
	if err != nil {
		return nil, err
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
		elements[i] = elementFromMap(elementMap, metadata)
	}
	return elements, nil
}

func (r *ShreddedVariantReconstructor) collectChildValues(
	pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte,
	childTables map[string][]*layout.Table,
) (map[string][]any, int, error) {
	tableValues := make(map[string][]any)
	maxLen := 0
	for childName := range childTables {
		val, err := r.reconstructValue(pathPrefix+common.ParGoPathDelimiter+childName, rowIdx, tableBgn, tableEnd, metadata)
		if err != nil {
			return nil, 0, err
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
	return tableValues, maxLen, nil
}

func (r *ShreddedVariantReconstructor) resolveExName(childName, pathPrefix string) string {
	childPath := pathPrefix + common.ParGoPathDelimiter + childName
	if idx, ok := r.SchemaHandler.MapIndex[childPath]; ok {
		return r.SchemaHandler.Infos[idx].ExName
	}
	return childName
}

func (r *ShreddedVariantReconstructor) reconstructMapChildren(
	pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte,
	childTables map[string][]*layout.Table,
) (any, error) {
	tableValues := make(map[string][]any)
	maxLen := -1
	isRepeated := false

	for childName := range childTables {
		childPath := pathPrefix + common.ParGoPathDelimiter + childName
		val, err := r.reconstructValue(childPath, rowIdx, tableBgn, tableEnd, metadata)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
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

	if maxLen == -1 {
		return nil, nil
	}

	if isRepeated {
		results := make([]any, maxLen)
		for i := range maxLen {
			obj := make(map[string]any)
			for childName, slice := range tableValues {
				if i < len(slice) && slice[i] != nil {
					obj[r.resolveExName(childName, pathPrefix)] = slice[i]
				}
			}
			results[i] = obj
		}
		return results, nil
	}

	obj := make(map[string]any)
	for childName, slice := range tableValues {
		obj[r.resolveExName(childName, pathPrefix)] = slice[0]
	}
	return obj, nil
}

// reconstructValue recursively builds a Go value from shredded columns.
func (r *ShreddedVariantReconstructor) reconstructValue(pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte) (any, error) {
	if table, ok := (*r.tableMap)[pathPrefix]; ok {
		return r.getValueAtRow(table, rowIdx, tableBgn, tableEnd)
	}

	childTables := r.findChildTables(pathPrefix)
	if len(childTables) == 0 {
		return nil, nil
	}

	if names, isVariant := findVariantChildNames(childTables); isVariant {
		return r.reconstructVariantGroup(pathPrefix, rowIdx, tableBgn, tableEnd, metadata, childTables, names)
	}

	if len(childTables) == 1 {
		for name := range childTables {
			if name == "Value" || name == "Typed_value" {
				return r.reconstructValue(pathPrefix+common.ParGoPathDelimiter+name, rowIdx, tableBgn, tableEnd, metadata)
			}
		}
	}

	idx, ok := r.SchemaHandler.MapIndex[pathPrefix]
	if ok && r.SchemaHandler.SchemaElements[idx].ConvertedType != nil &&
		*r.SchemaHandler.SchemaElements[idx].ConvertedType == parquet.ConvertedType_LIST {
		return r.reconstructValue(pathPrefix+common.ParGoPathDelimiter+"List", rowIdx, tableBgn, tableEnd, metadata)
	}

	if strings.HasSuffix(pathPrefix, common.ParGoPathDelimiter+"List") {
		return r.reconstructValue(pathPrefix+common.ParGoPathDelimiter+"Element", rowIdx, tableBgn, tableEnd, metadata)
	}

	if strings.HasSuffix(pathPrefix, common.ParGoPathDelimiter+"Element") {
		return r.reconstructElementChildren(pathPrefix, rowIdx, tableBgn, tableEnd, metadata, childTables)
	}

	return r.reconstructMapChildren(pathPrefix, rowIdx, tableBgn, tableEnd, metadata, childTables)
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

// jsonConverter holds configuration and caches for JSON conversion operations
type jsonConverter struct {
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

type fieldInfo struct {
	name  string
	index []int
}

type unmarshalState struct {
	schemaHandler     *schema.SchemaHandler
	mapRecords        map[reflect.Value]*MapRecord
	mapRecordsStack   []reflect.Value
	sliceRecords      map[reflect.Value]*SliceRecord
	sliceRecordsStack []reflect.Value
	prevType          reflect.Type
	prevFieldName     string
	prevFieldIndex    []int
	prevSlicePo       reflect.Value
	prevSliceRecord   *SliceRecord
}

type tableContext struct {
	path             []string
	schemaIndexs     []int
	repetitionLevels []int32
	definitionLevels []int32
}

func computeTableBounds(tableMap *map[string]*layout.Table, prefixPath string, bgn, end int) (map[string]*layout.Table, map[string]int, map[string]int, bool) {
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
			return nil, nil, nil, false
		}
	}
	return tableNeeds, tableBgn, tableEnd, true
}

func identifyVariants(schemaHandler *schema.SchemaHandler, prefixPath string, tableNeeds map[string]*layout.Table, tableMap *map[string]*layout.Table) (map[string]*ShreddedVariantReconstructor, map[string]string) {
	variantReconstructors := make(map[string]*ShreddedVariantReconstructor)
	variantChildPaths := make(map[string]string)
	if schemaHandler.VariantSchemas == nil {
		return variantReconstructors, variantChildPaths
	}
	for variantPath, info := range schemaHandler.VariantSchemas {
		if !strings.HasPrefix(variantPath, prefixPath) {
			continue
		}
		variantReconstructors[variantPath] = NewShreddedVariantReconstructor(variantPath, info, tableMap, schemaHandler)
		for childPath := range tableNeeds {
			if strings.HasPrefix(childPath, variantPath+common.ParGoPathDelimiter) {
				variantChildPaths[childPath] = variantPath
			}
		}
	}
	return variantReconstructors, variantChildPaths
}

func buildTableContext(path []string, sh *schema.SchemaHandler) (tableContext, error) {
	tc := tableContext{
		path:             path,
		schemaIndexs:     make([]int, len(path)),
		repetitionLevels: make([]int32, len(path)),
		definitionLevels: make([]int32, len(path)),
	}
	for i := range path {
		curPathStr := common.PathToStr(path[:i+1])
		tc.schemaIndexs[i] = int(sh.MapIndex[curPathStr])
		var err error
		if tc.repetitionLevels[i], err = sh.MaxRepetitionLevel(path[:i+1]); err != nil {
			return tc, err
		}
		if tc.definitionLevels[i], err = sh.MaxDefinitionLevel(path[:i+1]); err != nil {
			return tc, err
		}
	}
	return tc, nil
}

func setByteSliceValue(po reflect.Value, val any) (bool, error) {
	if val == nil {
		return true, nil
	}
	value := reflect.ValueOf(val)
	if !value.IsValid() {
		return true, nil
	}
	switch {
	case value.Kind() == reflect.String:
		po.Set(reflect.ValueOf([]byte(value.String())))
	case value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8:
		po.Set(value)
	default:
		return false, fmt.Errorf("cannot assign %v to []byte field", value.Type())
	}
	return true, nil
}

func isOldListFormat(path []string, index int, schemaIndexs []int, sh *schema.SchemaHandler) bool {
	return len(path) >= 2 &&
		index < len(path) && strings.EqualFold(path[index], "array") &&
		index > 0 && strings.EqualFold(path[index-1], "array") &&
		index < len(sh.SchemaElements) &&
		sh.SchemaElements[schemaIndexs[index]].Type != nil
}

func shouldIncrementSlice(isOldList bool, sliceRec *SliceRecord, rl, repetitionLevel int32) bool {
	if isOldList {
		return sliceRec.Index < 0 || rl <= 1
	}
	return rl == repetitionLevel || sliceRec.Index < 0
}

func handleOldListLeaf(po reflect.Value, val any) {
	if !po.IsValid() || po.Kind() != reflect.Struct || po.NumField() != 1 {
		return
	}
	if !strings.EqualFold(po.Type().Field(0).Name, "array") || po.Type().Field(0).Type.Kind() != reflect.Slice {
		return
	}
	arrayField := po.Field(0)
	if arrayField.Kind() != reflect.Slice {
		return
	}
	elemValue := reflect.ValueOf(val)
	if elemValue.Type() != arrayField.Type().Elem() && elemValue.Type().ConvertibleTo(arrayField.Type().Elem()) {
		elemValue = elemValue.Convert(arrayField.Type().Elem())
	}
	arrayField.Set(reflect.Append(arrayField, elemValue))
}

func (s *unmarshalState) getSliceRecord(po reflect.Value) *SliceRecord {
	if s.prevSlicePo == po {
		return s.prevSliceRecord
	}
	s.prevSlicePo = po
	sliceRec, ok := s.sliceRecords[po]
	if !ok {
		sliceRec = &SliceRecord{Values: []reflect.Value{}, Index: -1}
		s.sliceRecords[po] = sliceRec
		s.sliceRecordsStack = append(s.sliceRecordsStack, po)
	}
	s.prevSliceRecord = sliceRec
	return sliceRec
}

func (s *unmarshalState) handleSlice(po reflect.Value, tc *tableContext, index int, rl, dl int32, val any) (reflect.Value, int, bool, error) {
	poType := po.Type()
	if poType.Elem().Kind() == reflect.Uint8 {
		done, err := setByteSliceValue(po, val)
		return po, index, done, err
	}

	cT := s.schemaHandler.SchemaElements[tc.schemaIndexs[index]].ConvertedType
	cTIsList := cT != nil && *cT == parquet.ConvertedType_LIST

	if po.IsNil() {
		po.Set(reflect.MakeSlice(poType, 0, 0))
	}

	sliceRec := s.getSliceRecord(po)

	if cTIsList {
		index++
		if tc.definitionLevels[index] > dl {
			return po, index, true, nil
		}
	}

	isOldList := isOldListFormat(tc.path, index, tc.schemaIndexs, s.schemaHandler)
	if shouldIncrementSlice(isOldList, sliceRec, rl, tc.repetitionLevels[index]) {
		sliceRec.Index++
	}

	if sliceRec.Index >= len(sliceRec.Values) {
		sliceRec.Values = append(sliceRec.Values, reflect.New(poType.Elem()).Elem())
	}
	po = sliceRec.Values[sliceRec.Index]

	if isOldList {
		handleOldListLeaf(po, val)
	}

	if cTIsList {
		index++
		if index < len(tc.definitionLevels) && tc.definitionLevels[index] > dl {
			return po, index, true, nil
		}
	}
	return po, index, false, nil
}

func (s *unmarshalState) handleMap(po reflect.Value, tc *tableContext, index int, rl, dl int32) (reflect.Value, int, bool, error) {
	poType := po.Type()
	if po.IsNil() {
		po.Set(reflect.MakeMap(poType))
	}

	mapRec, ok := s.mapRecords[po]
	if !ok {
		mapRec = &MapRecord{KeyValues: []KeyValue{}, Index: -1}
		s.mapRecords[po] = mapRec
		s.mapRecordsStack = append(s.mapRecordsStack, po)
	}

	index++
	if tc.definitionLevels[index] > dl {
		return po, index, true, nil
	}

	if rl == tc.repetitionLevels[index] || mapRec.Index < 0 {
		mapRec.Index++
	}

	if mapRec.Index >= len(mapRec.KeyValues) {
		mapRec.KeyValues = append(mapRec.KeyValues, KeyValue{
			Key: reflect.New(poType.Key()).Elem(), Value: reflect.New(poType.Elem()).Elem(),
		})
	}

	if index+1 >= len(tc.path) {
		return po, index, false, fmt.Errorf("invalid path: missing key/value component after map")
	}
	if strings.ToLower(tc.path[index+1]) == "key" {
		po = mapRec.KeyValues[mapRec.Index].Key
	} else {
		po = mapRec.KeyValues[mapRec.Index].Value
	}

	index++
	if tc.definitionLevels[index] > dl {
		return po, index, true, nil
	}
	return po, index, false, nil
}

func (s *unmarshalState) handleStruct(po reflect.Value, tc *tableContext, index int, dl int32) (reflect.Value, int, bool, error) {
	poType := po.Type()
	index++
	if index < len(tc.definitionLevels) && tc.definitionLevels[index] > dl {
		return po, index, true, nil
	}
	if index >= len(tc.path) {
		return po, index, true, nil
	}
	name := tc.path[index]
	if s.prevType != poType || name != s.prevFieldName {
		s.prevType = poType
		s.prevFieldName = name
		f, ok := poType.FieldByName(name)
		if !ok {
			return po, index, false, fmt.Errorf("field %q not found in struct type %v", name, poType)
		}
		s.prevFieldIndex = f.Index
	}
	po = po.FieldByIndex(s.prevFieldIndex)
	if !po.IsValid() {
		return po, index, false, fmt.Errorf("field access resulted in invalid value for field %q", name)
	}
	return po, index, false, nil
}

func setPrimitiveValue(po reflect.Value, val any) error {
	if !po.IsValid() {
		return fmt.Errorf("invalid reflect value encountered before setting value")
	}
	if val == nil {
		return nil
	}
	value := reflect.ValueOf(val)
	if !value.IsValid() {
		return nil
	}

	poType := po.Type()
	valueType := value.Type()
	if poType != valueType {
		if valueType.Kind() == reflect.String && poType.Kind() == reflect.Slice && poType.Elem().Kind() == reflect.Uint8 {
			value = reflect.ValueOf([]byte(value.String()))
		} else if !valueType.ConvertibleTo(poType) {
			return fmt.Errorf("cannot convert value of type %v to type %v", valueType, poType)
		} else {
			value = value.Convert(poType)
		}
	}
	if !po.CanSet() {
		return fmt.Errorf("cannot set value for field (unaddressable or unexported)")
	}
	if !value.Type().AssignableTo(po.Type()) {
		return fmt.Errorf("cannot assign value of type %v to field of type %v", value.Type(), po.Type())
	}
	po.Set(value)
	return nil
}

func (s *unmarshalState) processValue(root reflect.Value, prefixIndex int, tc *tableContext, rl, dl int32, val any) error {
	po, index := root, prefixIndex
	for index < len(tc.path) {
		if !po.IsValid() {
			return fmt.Errorf("invalid reflect value encountered during unmarshal")
		}

		var done bool
		var err error
		switch po.Type().Kind() {
		case reflect.Slice:
			po, index, done, err = s.handleSlice(po, tc, index, rl, dl, val)
		case reflect.Map:
			po, index, done, err = s.handleMap(po, tc, index, rl, dl)
		case reflect.Ptr:
			if po.IsNil() {
				po.Set(reflect.New(po.Type().Elem()))
			}
			po = po.Elem()
			if !po.IsValid() {
				return fmt.Errorf("pointer dereference resulted in invalid value")
			}
			continue
		case reflect.Struct:
			po, index, done, err = s.handleStruct(po, tc, index, dl)
		default:
			return setPrimitiveValue(po, val)
		}
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return nil
}

func (s *unmarshalState) processTable(root reflect.Value, prefixIndex int, table *layout.Table, bgn, end int) error {
	tc, err := buildTableContext(table.Path, s.schemaHandler)
	if err != nil {
		return err
	}

	for _, rc := range s.sliceRecords {
		rc.Index = -1
	}
	for _, rc := range s.mapRecords {
		rc.Index = -1
	}

	s.prevType = nil
	s.prevFieldName = ""
	s.prevFieldIndex = nil
	s.prevSlicePo = reflect.Value{}
	s.prevSliceRecord = nil

	for i := bgn; i < end; i++ {
		if err := s.processValue(root, prefixIndex, &tc, table.RepetitionLevels[i], table.DefinitionLevels[i], table.Values[i]); err != nil {
			return err
		}
	}
	return nil
}

func processVariantReconstruction(variantReconstructors map[string]*ShreddedVariantReconstructor, root reflect.Value, prefixPath string, schemaHandler *schema.SchemaHandler, tableBgn, tableEnd map[string]int, sliceRecords map[reflect.Value]*SliceRecord) error {
	for variantPath, reconstructor := range variantReconstructors {
		numRows := countVariantRows(reconstructor, tableBgn, tableEnd)
		expandRootForVariants(root, numRows, sliceRecords)

		for rowIdx := range numRows {
			variant, err := reconstructor.Reconstruct(rowIdx, tableBgn, tableEnd)
			if err != nil {
				return fmt.Errorf("reconstruct variant at %s row %d: %w", variantPath, rowIdx, err)
			}
			if err := setVariantValue(root, variantPath, prefixPath, schemaHandler, variant, rowIdx, sliceRecords); err != nil {
				return fmt.Errorf("set variant at %s row %d: %w", variantPath, rowIdx, err)
			}
		}
	}
	return nil
}

func countVariantRows(reconstructor *ShreddedVariantReconstructor, tableBgn, tableEnd map[string]int) int {
	if reconstructor.MetadataTable == nil {
		return 0
	}
	metaPath := common.PathToStr(reconstructor.MetadataTable.Path)
	metaBgn, metaEnd := tableBgn[metaPath], tableEnd[metaPath]
	if metaBgn < 0 || metaEnd <= metaBgn {
		return 0
	}
	numRows := 0
	for i := metaBgn; i < metaEnd; i++ {
		if reconstructor.MetadataTable.RepetitionLevels[i] == 0 {
			numRows++
		}
	}
	return numRows
}

func expandRootForVariants(root reflect.Value, numRows int, sliceRecords map[reflect.Value]*SliceRecord) {
	if sliceRec, ok := sliceRecords[root]; ok && len(sliceRec.Values) >= numRows {
		return
	}
	if root.Kind() == reflect.Slice && root.Len() < numRows {
		newSlice := reflect.MakeSlice(root.Type(), numRows, numRows)
		reflect.Copy(newSlice, root)
		root.Set(newSlice)
	}
}

// Convert the table map to objects slice. dstInterface is a slice of pointers of objects
func Unmarshal(tableMap *map[string]*layout.Table, bgn, end int, dstInterface any, schemaHandler *schema.SchemaHandler, prefixPath string) error {
	rootValue := reflect.ValueOf(dstInterface)
	if !rootValue.IsValid() || rootValue.Kind() != reflect.Ptr || rootValue.IsNil() {
		return fmt.Errorf("dstInterface must be a non-nil pointer")
	}

	tableNeeds, tableBgn, tableEnd, ok := computeTableBounds(tableMap, prefixPath, bgn, end)
	if !ok {
		return nil
	}

	root := rootValue.Elem()
	prefixIndex := common.PathStrIndex(prefixPath) - 1
	variantReconstructors, variantChildPaths := identifyVariants(schemaHandler, prefixPath, tableNeeds, tableMap)

	state := &unmarshalState{
		schemaHandler: schemaHandler,
		mapRecords:    make(map[reflect.Value]*MapRecord),
		sliceRecords:  make(map[reflect.Value]*SliceRecord),
	}

	for name, table := range tableNeeds {
		if _, isVariantChild := variantChildPaths[name]; isVariantChild {
			continue
		}
		if err := state.processTable(root, prefixIndex, table, tableBgn[name], tableEnd[name]); err != nil {
			return err
		}
	}

	if err := processVariantReconstruction(variantReconstructors, root, prefixPath, schemaHandler, tableBgn, tableEnd, state.sliceRecords); err != nil {
		return err
	}

	for i := len(state.sliceRecordsStack) - 1; i >= 0; i-- {
		po := state.sliceRecordsStack[i]
		vs := state.sliceRecords[po]
		po.Set(reflect.Append(po, vs.Values...))
	}
	for i := len(state.mapRecordsStack) - 1; i >= 0; i-- {
		po := state.mapRecordsStack[i]
		for _, kv := range state.mapRecords[po].KeyValues {
			po.SetMapIndex(kv.Key, kv.Value)
		}
	}

	return nil
}

// setVariantValue navigates the struct path and sets a variant value at the specified location.
func isVariantNull(variant types.Variant) bool {
	return len(variant.Value) == 0 || (len(variant.Value) == 1 && variant.Value[0] == 0 && len(variant.Metadata) == 0)
}

func setDecodedOrFallback(target reflect.Value, variant types.Variant) {
	decoded, err := types.ConvertVariantValue(variant)
	if err != nil {
		target.Set(reflect.ValueOf(variant))
	} else if decoded != nil {
		target.Set(reflect.ValueOf(decoded))
	} else {
		target.Set(reflect.Zero(target.Type()))
	}
}

var variantType = reflect.TypeOf(types.Variant{})

func assignVariantDirect(po reflect.Value, variant types.Variant) error {
	if isVariantNull(variant) && po.Kind() == reflect.Interface {
		po.Set(reflect.Zero(po.Type()))
		return nil
	}
	if po.Kind() == reflect.Interface && po.Type() != variantType {
		setDecodedOrFallback(po, variant)
		return nil
	}
	po.Set(reflect.ValueOf(variant))
	return nil
}

func assignVariantPtr(po reflect.Value, variant types.Variant) error {
	if isVariantNull(variant) {
		po.Set(reflect.Zero(po.Type()))
		return nil
	}
	if po.IsNil() {
		po.Set(reflect.New(po.Type().Elem()))
	}
	if po.Type().Elem().Kind() == reflect.Interface && po.Type().Elem() != variantType {
		setDecodedOrFallback(po.Elem(), variant)
		return nil
	}
	po.Elem().Set(reflect.ValueOf(variant))
	return nil
}

func assignVariantToTarget(po reflect.Value, variant types.Variant) error {
	if po.Type() == variantType || po.Kind() == reflect.Interface {
		return assignVariantDirect(po, variant)
	}
	if po.Kind() == reflect.Ptr && (po.Type().Elem() == variantType || po.Type().Elem().Kind() == reflect.Interface) {
		return assignVariantPtr(po, variant)
	}
	return fmt.Errorf("could not set variant value at path end")
}

func navigateToVariantTarget(root reflect.Value, path []string, prefixIndex int, variant types.Variant, rowIdx int, sliceRecords map[reflect.Value]*SliceRecord) (reflect.Value, error) {
	po := root
	for i := prefixIndex; i < len(path); i++ {
		if !po.IsValid() {
			return po, fmt.Errorf("invalid reflect value at path index %d", i)
		}

		switch po.Kind() {
		case reflect.Ptr:
			if po.IsNil() {
				po.Set(reflect.New(po.Type().Elem()))
			}
			po = po.Elem()
			i--
			continue

		case reflect.Slice:
			if po.Type().Elem().Kind() == reflect.Uint8 {
				return po, fmt.Errorf("unexpected []byte at path index %d", i)
			}
			sliceRec, ok := sliceRecords[po]
			if ok && rowIdx < len(sliceRec.Values) {
				po = sliceRec.Values[rowIdx]
			} else if rowIdx < po.Len() {
				po = po.Index(rowIdx)
			} else {
				return po, fmt.Errorf("row index %d out of bounds for slice at path index %d", rowIdx, i)
			}
			i--
			continue

		case reflect.Struct:
			field := po.FieldByName(path[i])
			if !field.IsValid() {
				return po, fmt.Errorf("field %q not found at path index %d", path[i], i)
			}
			po = field

		case reflect.Interface:
			if po.IsNil() {
				if i == len(path)-1 {
					po.Set(reflect.ValueOf(variant))
					return po, nil
				}
				return po, fmt.Errorf("cannot navigate through nil interface at path index %d", i)
			}
			po = po.Elem()
			i--
			continue

		default:
			return po, fmt.Errorf("unexpected kind %v at path index %d", po.Kind(), i)
		}
	}
	return po, nil
}

func setVariantValue(root reflect.Value, variantPath, prefixPath string, _ *schema.SchemaHandler, variant types.Variant, rowIdx int, sliceRecords map[reflect.Value]*SliceRecord) error {
	path := common.StrToPath(variantPath)
	prefixIndex := common.PathStrIndex(prefixPath)

	po, err := navigateToVariantTarget(root, path, prefixIndex, variant, rowIdx, sliceRecords)
	if err != nil {
		return err
	}

	return assignVariantToTarget(po, variant)
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
	var elementPath string
	if pathPrefix != "" {
		var builder strings.Builder
		builder.WriteString(pathPrefix)
		builder.WriteString(common.ParGoPathDelimiter)
		builder.WriteString("List")
		builder.WriteString(common.ParGoPathDelimiter)
		builder.WriteString("Element")
		elementPath = builder.String()
	}

	for i := range val.Len() {
		converted, err := convertValueToJSONFriendlyWithContext(val.Index(i), schemaHandler, elementPath, converter)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		keyStr := fmt.Sprint(converted)

		converted, err = convertValueToJSONFriendlyWithContext(val.MapIndex(key), schemaHandler, valuePath, converter)
		if err != nil {
			return nil, err
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
			return nil, err
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

	rootName := schemaHandler.GetRootInName()
	expectedSchemaPath := pathPrefix
	if !strings.HasPrefix(pathPrefix, rootName) {
		expectedSchemaPath = rootName + common.ParGoPathDelimiter + expectedSchemaPath
	}

	var schemaElement *parquet.SchemaElement

	schemaElementInterface, cached := converter.schemaCache.Load(expectedSchemaPath)
	if !cached {
		schemaIndex, exists := schemaHandler.MapIndex[expectedSchemaPath]
		if !exists || int(schemaIndex) >= len(schemaHandler.SchemaElements) {
			return val.Interface(), nil
		}
		schemaElement = schemaHandler.SchemaElements[schemaIndex]
		converter.schemaCache.Store(expectedSchemaPath, schemaElement)
	} else {
		schemaElement = schemaElementInterface.(*parquet.SchemaElement)
	}

	if schemaElement == nil {
		return val.Interface(), nil
	}

	var typeOpts []types.JSONTypeOption
	if converter.geospatialConfig != nil {
		typeOpts = append(typeOpts, types.WithGeospatialConfig(converter.geospatialConfig))
	}
	converted := types.ConvertToJSONType(val.Interface(), schemaElement, typeOpts...)
	if converted != val.Interface() {
		return converted, nil
	}
	return val.Interface(), nil
}
