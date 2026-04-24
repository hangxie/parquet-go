package marshal

import (
	"fmt"
	"strings"

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
