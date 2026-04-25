package marshal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
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
