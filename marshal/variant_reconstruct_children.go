package marshal

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
)

func (r *variantReconstructor) findChildTables(pathPrefix string) map[string][]*layout.Table {
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

func (r *variantReconstructor) reconstructChildValues(pathPrefix, childName string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte) ([]any, bool, error) {
	if childName == "" {
		return nil, false, nil
	}
	val, err := r.reconstructValue(pathPrefix+common.ParGoPathDelimiter+childName, rowIdx, tableBgn, tableEnd, metadata)
	if err != nil {
		return nil, false, fmt.Errorf("reconstruct child %s/%s: %w", pathPrefix, childName, err)
	}
	if slice, ok := val.([]any); ok {
		return slice, true, nil
	}
	if val != nil {
		return []any{val}, true, nil
	}
	return nil, false, nil
}

func (r *variantReconstructor) isChildTablesRepeated(childTables map[string][]*layout.Table) (bool, error) {
	for _, tables := range childTables {
		for _, table := range tables {
			maxRL, err := r.SchemaHandler.MaxRepetitionLevel(table.Path)
			if err != nil {
				return false, fmt.Errorf("max repetition level for %v: %w", table.Path, err)
			}
			if maxRL > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

func (r *variantReconstructor) reconstructElementChildren(
	pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte,
	childTables map[string][]*layout.Table,
) (any, error) {
	tableValues, maxLen, err := r.collectChildValues(pathPrefix, rowIdx, tableBgn, tableEnd, metadata, childTables)
	if err != nil {
		return nil, fmt.Errorf("collect child values for %s: %w", pathPrefix, err)
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

func (r *variantReconstructor) collectChildValues(
	pathPrefix string, rowIdx int, tableBgn, tableEnd map[string]int, metadata []byte,
	childTables map[string][]*layout.Table,
) (map[string][]any, int, error) {
	tableValues := make(map[string][]any)
	maxLen := 0
	for childName := range childTables {
		val, err := r.reconstructValue(pathPrefix+common.ParGoPathDelimiter+childName, rowIdx, tableBgn, tableEnd, metadata)
		if err != nil {
			return nil, 0, fmt.Errorf("reconstruct child %s/%s: %w", pathPrefix, childName, err)
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

func (r *variantReconstructor) resolveExName(childName, pathPrefix string) string {
	childPath := pathPrefix + common.ParGoPathDelimiter + childName
	if idx, ok := r.SchemaHandler.MapIndex[childPath]; ok {
		return r.SchemaHandler.Infos[idx].ExName
	}
	return childName
}

func (r *variantReconstructor) reconstructMapChildren(
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
			return nil, fmt.Errorf("reconstruct map child %s: %w", childPath, err)
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
