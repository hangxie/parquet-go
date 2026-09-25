package marshal

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

// variantReconstructor handles the reconstruction of shredded VARIANT columns.
// It collects related tables (metadata, value, typed_value) and reconstructs full
// Variant values row by row.
type variantReconstructor struct {
	Path             string                    // Path of the variant group
	Info             *schema.VariantSchemaInfo // Schema info for this variant
	MetadataTable    *layout.Table             // metadata column (always present)
	ValueTable       *layout.Table             // value column (may be nil if fully shredded)
	TypedValueTables []*layout.Table           // typed_value columns (legacy, kept for tests)
	tableMap         *map[string]*layout.Table // map of all tables for recursive lookup
	SchemaHandler    *schema.SchemaHandler     // Schema handler for path resolution
}

// newVariantReconstructor creates a reconstructor for a shredded variant column.
func newVariantReconstructor(
	path string,
	info *schema.VariantSchemaInfo,
	tableMap *map[string]*layout.Table,
	sh *schema.SchemaHandler,
) *variantReconstructor {
	r := &variantReconstructor{
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
		if !strings.HasPrefix(tableName, path+common.ParGoPathDelimiter) || tableName == metadataPath || (valuePath != "" && tableName == valuePath) {
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
func (r *variantReconstructor) getValueAtRow(table *layout.Table, rowIdx int, tableBgn, tableEnd map[string]int) (any, error) {
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
		if currentRow < rowIdx {
			continue
		}
		if currentRow > rowIdx {
			break
		}
		// Check definition level to see if value is present
		maxDL, err := r.SchemaHandler.MaxDefinitionLevel(table.Path)
		if err != nil {
			return nil, fmt.Errorf("max definition level for %v: %w", table.Path, err)
		}
		if table.DefinitionLevels[i] >= maxDL {
			values = append(values, table.Values[i])
		} else {
			values = append(values, nil)
		}
	}

	if len(values) == 0 {
		return nil, nil
	}

	// Check if the field is repeated in the schema
	isRepeated := false
	maxRL, err := r.SchemaHandler.MaxRepetitionLevel(table.Path)
	if err != nil {
		return nil, fmt.Errorf("max repetition level for %v: %w", table.Path, err)
	}
	if maxRL > 0 {
		isRepeated = true
	}

	if isRepeated {
		return values, nil
	}
	return values[0], nil
}

func (r *variantReconstructor) reconstructVariantGroup(
	pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte,
	childTables map[string][]*layout.Table, names variantChildNames,
) (any, error) {
	metadataValues, metadataSet, err := r.reconstructChildValues(pathPrefix, names.meta, rowIdx, tableBgn, tableEnd, nil)
	if err != nil {
		return nil, fmt.Errorf("variant %s metadata: %w", pathPrefix, err)
	}
	valueValues, valueSet, err := r.reconstructChildValues(pathPrefix, names.value, rowIdx, tableBgn, tableEnd, nil)
	if err != nil {
		return nil, fmt.Errorf("variant %s value: %w", pathPrefix, err)
	}

	typedMeta := effectiveMetadataForTyped(metadataValues, metadataSet, metadata)
	typedValueValues, typedValueSet, err := r.reconstructChildValues(pathPrefix, names.typed, rowIdx, tableBgn, tableEnd, typedMeta)
	if err != nil {
		return nil, fmt.Errorf("variant %s typed value: %w", pathPrefix, err)
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
			return nil, fmt.Errorf("check child repeatedness: %w", err)
		}
	}

	results, err := buildVariantResults(maxLen, metadataValues, valueValues, typedValueValues, metadata)
	if err != nil {
		return nil, fmt.Errorf("build variant results: %w", err)
	}
	return variantResultsToAny(results, isRepeated)
}

// reconstructValue recursively builds a Go value from shredded columns.
func (r *variantReconstructor) reconstructValue(pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte) (any, error) {
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
func (r *variantReconstructor) Reconstruct(rowIdx int, tableBgn, tableEnd map[string]int) (types.Variant, error) {
	val, err := r.reconstructValue(r.Path, rowIdx, tableBgn, tableEnd, nil)
	if err != nil {
		return types.Variant{}, fmt.Errorf("reconstruct variant at %s row %d: %w", r.Path, rowIdx, err)
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
