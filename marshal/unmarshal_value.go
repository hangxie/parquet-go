package marshal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

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
	key := valueIdentity(po)
	if s.prevSlicePo != nil && s.prevSlicePo == key {
		return s.prevSliceRecord
	}
	s.prevSlicePo = key
	sliceRec, ok := s.sliceRecords[key]
	if !ok {
		sliceRec = &SliceRecord{Values: []reflect.Value{}, Index: -1}
		s.sliceRecords[key] = sliceRec
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

	key := valueIdentity(po)
	mapRec, ok := s.mapRecords[key]
	if !ok {
		mapRec = &MapRecord{KeyValues: []KeyValue{}, Index: -1}
		s.mapRecords[key] = mapRec
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
		case reflect.Pointer:
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
			return fmt.Errorf("process value at index %d for %v: %w", index, tc.path, err)
		}
		if done {
			return nil
		}
	}
	return nil
}
