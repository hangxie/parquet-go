package marshal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/schema"
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

type unmarshalState struct {
	schemaHandler     *schema.SchemaHandler
	mapRecords        map[any]*MapRecord
	mapRecordsStack   []reflect.Value
	sliceRecords      map[any]*SliceRecord
	sliceRecordsStack []reflect.Value
	prevType          reflect.Type
	prevFieldName     string
	prevFieldIndex    []int
	prevSlicePo       any
	prevSliceRecord   *SliceRecord
}

// valueIdentity keys a reflect.Value by its underlying storage, used by the
// record maps in place of the reflect.Value itself, whose equality the Go spec
// does not define for identity use. The typed *T pointer (not a bare uintptr)
// keeps the storage GC-visible and distinguishes values by type as well as
// address. Every value tracked here is addressable.
func valueIdentity(v reflect.Value) any {
	return v.Addr().Interface()
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
		if prefixPath != "" && !common.IsChildPath(prefixPath, name) {
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

func identifyVariants(schemaHandler *schema.SchemaHandler, prefixPath string, tableNeeds map[string]*layout.Table, tableMap *map[string]*layout.Table) (map[string]*variantReconstructor, map[string]string) {
	variantReconstructors := make(map[string]*variantReconstructor)
	variantChildPaths := make(map[string]string)
	if schemaHandler.VariantSchemas == nil {
		return variantReconstructors, variantChildPaths
	}
	for variantPath, info := range schemaHandler.VariantSchemas {
		if prefixPath != "" && !common.IsChildPath(prefixPath, variantPath) {
			continue
		}
		variantReconstructors[variantPath] = newVariantReconstructor(variantPath, info, tableMap, schemaHandler)
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
			return tc, fmt.Errorf("max repetition level for %s: %w", curPathStr, err)
		}
		if tc.definitionLevels[i], err = sh.MaxDefinitionLevel(path[:i+1]); err != nil {
			return tc, fmt.Errorf("max definition level for %s: %w", curPathStr, err)
		}
	}
	return tc, nil
}

func (s *unmarshalState) processTable(root reflect.Value, prefixIndex int, table *layout.Table, bgn, end int) error {
	tc, err := buildTableContext(table.Path, s.schemaHandler)
	if err != nil {
		return fmt.Errorf("build table context for %s: %w", table.Path, err)
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
	s.prevSlicePo = nil
	s.prevSliceRecord = nil

	for i := bgn; i < end; i++ {
		if err := s.processValue(root, prefixIndex, &tc, table.RepetitionLevels[i], table.DefinitionLevels[i], table.Values[i]); err != nil {
			return fmt.Errorf("process row %d of %v: %w", i, table.Path, err)
		}
	}
	return nil
}

// Convert the table map to objects slice. dstInterface is a slice of pointers of objects
func Unmarshal(tableMap *map[string]*layout.Table, bgn, end int, dstInterface any, schemaHandler *schema.SchemaHandler, prefixPath string) error {
	rootValue := reflect.ValueOf(dstInterface)
	if !rootValue.IsValid() || rootValue.Kind() != reflect.Pointer || rootValue.IsNil() {
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
		mapRecords:    make(map[any]*MapRecord),
		sliceRecords:  make(map[any]*SliceRecord),
	}

	for name, table := range tableNeeds {
		if _, isVariantChild := variantChildPaths[name]; isVariantChild {
			continue
		}
		if err := state.processTable(root, prefixIndex, table, tableBgn[name], tableEnd[name]); err != nil {
			return fmt.Errorf("process table %s: %w", name, err)
		}
	}

	if err := processVariantReconstruction(variantReconstructors, root, prefixPath, tableBgn, tableEnd, state.sliceRecords); err != nil {
		return fmt.Errorf("reconstruct variants: %w", err)
	}

	for i := len(state.sliceRecordsStack) - 1; i >= 0; i-- {
		po := state.sliceRecordsStack[i]
		vs := state.sliceRecords[valueIdentity(po)]
		po.Set(reflect.Append(po, vs.Values...))
	}
	for i := len(state.mapRecordsStack) - 1; i >= 0; i-- {
		po := state.mapRecordsStack[i]
		for _, kv := range state.mapRecords[valueIdentity(po)].KeyValues {
			po.SetMapIndex(kv.Key, kv.Value)
		}
	}

	return nil
}
