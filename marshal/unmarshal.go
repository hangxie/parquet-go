package marshal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
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

// Convert the table map to objects slice. dstInterface is a slice of pointers of objects
func Unmarshal(tableMap *map[string]*layout.Table, bgn, end int, dstInterface any, schemaHandler *schema.SchemaHandler, prefixPath string) (err error) {
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
			return
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

					if rl == repetitionLevels[index] || sliceRec.Index < 0 {
						sliceRec.Index++
					}

					if sliceRec.Index >= len(sliceRec.Values) {
						sliceRec.Values = append(sliceRec.Values, reflect.New(poType.Elem()).Elem())
					}

					po = sliceRec.Values[sliceRec.Index]

					if cTIsList {
						index++
						if definitionLevels[index] > dl {
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

				case reflect.Struct:
					index++
					if definitionLevels[index] > dl {
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
					value := reflect.ValueOf(val)
					if value.IsValid() {
						if po.Type() != value.Type() {
							value = value.Convert(poType)
						}
						po.Set(value)
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
