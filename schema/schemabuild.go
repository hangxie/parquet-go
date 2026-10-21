package schema

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

type Item struct {
	GoType reflect.Type
	Info   *common.Tag
}

func NewItem() *Item {
	item := new(Item)
	item.Info = &common.Tag{}
	return item
}

func NewSchemaHandlerFromStruct(obj any) (sh *SchemaHandler, err error) {
	ot := reflect.TypeOf(obj).Elem()
	item := NewItem()
	item.GoType = ot
	item.Info.InName = common.ParGoRootInName
	item.Info.ExName = common.ParGoRootExName
	item.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED

	stack := make([]*Item, 1)
	stack[0] = item
	schemaElements := make([]*parquet.SchemaElement, 0)
	infos := make([]*common.Tag, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item = stack[ln-1]
		stack = stack[:ln-1]

		if item.Info.Type == "VARIANT" {
			if err = createVariantSchema(item, &stack, &schemaElements, &infos); err != nil {
				return nil, fmt.Errorf("create variant schema for %s: %w", item.Info.InName, err)
			}
		} else if item.GoType.Kind() == reflect.Struct {
			if err = createStructSchema(item, &stack, &schemaElements, &infos); err != nil {
				return nil, fmt.Errorf("create struct schema for %s: %w", item.Info.InName, err)
			}
		} else if item.GoType.Kind() == reflect.Slice &&
			item.Info.Type != "BYTE_ARRAY" && item.Info.Type != "FIXED_LEN_BYTE_ARRAY" &&
			item.Info.RepetitionType != parquet.FieldRepetitionType_REPEATED {
			createListSchema(item, &stack, &schemaElements, &infos)
		} else if item.GoType.Kind() == reflect.Slice &&
			item.Info.RepetitionType == parquet.FieldRepetitionType_REPEATED {
			newItem := NewItem()
			newItem.Info = item.Info
			newItem.GoType = item.GoType.Elem()
			stack = append(stack, newItem)

		} else if item.GoType.Kind() == reflect.Map {
			createMapSchema(item, &stack, &schemaElements, &infos)
		} else {
			schema, err := common.NewSchemaElementFromTagMap(item.Info)
			if err != nil {
				return nil, fmt.Errorf("create schema from tag map: %w", err)
			}
			schemaElements = append(schemaElements, schema)
			newInfo := &common.Tag{}
			common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)
		}
	}

	res := &SchemaHandler{
		SchemaElements: schemaElements,
		Infos:          infos,
		MapIndex:       make(map[string]int32),
		IndexMap:       make(map[int32]string),
		InPathToExPath: make(map[string]string),
		ExPathToInPath: make(map[string]string),
	}

	// use DFS get path of schema
	ln := int32(len(schemaElements))
	var pos int32 = 0
	stack2 := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack2) > 0 {
		if len(stack2) == 0 || stack2[len(stack2)-1][1] > 0 {
			if len(stack2) > 0 {
				stack2[len(stack2)-1][1]--
			}
			item := [2]int32{pos, schemaElements[pos].GetNumChildren()}
			stack2 = append(stack2, item)
			pos++
		} else {
			path := make([]string, 0)
			for i := range len(stack2) {
				inname := res.Infos[stack2[i][0]].InName
				path = append(path, inname)
			}
			topPos := stack2[len(stack2)-1][0]
			res.MapIndex[common.PathToStr(path)] = topPos
			res.IndexMap[topPos] = common.PathToStr(path)
			stack2 = stack2[:len(stack2)-1]
		}
	}
	res.setPathMap()
	res.setValueColumns()
	res.setVariantSchemas()

	res.CreateInExMap()
	if err := res.ValidateUniqueNames(); err != nil {
		return nil, fmt.Errorf("validate column names: %w", err)
	}
	return res, nil
}

func NewSchemaHandlerFromSchemaHandler(sh *SchemaHandler) *SchemaHandler {
	schemaHandler := new(SchemaHandler)
	schemaHandler.MapIndex = make(map[string]int32)
	schemaHandler.IndexMap = make(map[int32]string)
	schemaHandler.InPathToExPath = make(map[string]string)
	schemaHandler.ExPathToInPath = make(map[string]string)
	schemaHandler.SchemaElements = sh.SchemaElements

	schemaHandler.Infos = make([]*common.Tag, len(sh.SchemaElements))
	for i := range len(sh.SchemaElements) {
		InName, ExName := sh.GetInName(i), sh.GetExName(i)
		schemaHandler.Infos[i] = &common.Tag{
			InName: InName,
			ExName: ExName,
		}
	}
	schemaHandler.CreateInExMap()

	// use DFS get path of schema
	ln := int32(len(sh.SchemaElements))
	var pos int32 = 0
	stack := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}
			item := [2]int32{pos, sh.SchemaElements[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else {
			path := make([]string, 0)
			for i := range len(stack) {
				inname := schemaHandler.Infos[stack[i][0]].InName
				path = append(path, inname)
			}
			topPos := stack[len(stack)-1][0]
			schemaHandler.MapIndex[common.PathToStr(path)] = topPos
			schemaHandler.IndexMap[topPos] = common.PathToStr(path)
			stack = stack[:len(stack)-1]
		}
	}
	schemaHandler.setPathMap()
	schemaHandler.setValueColumns()
	schemaHandler.setVariantSchemas()

	return schemaHandler
}

// NewSchemaHandlerFromSchemaList creates schema handler from schema list
func NewSchemaHandlerFromSchemaList(schemas []*parquet.SchemaElement) *SchemaHandler {
	schemaHandler := new(SchemaHandler)
	schemaHandler.MapIndex = make(map[string]int32)
	schemaHandler.IndexMap = make(map[int32]string)
	schemaHandler.InPathToExPath = make(map[string]string)
	schemaHandler.ExPathToInPath = make(map[string]string)
	schemaHandler.SchemaElements = schemas

	schemaHandler.Infos = make([]*common.Tag, len(schemas))
	for i := range schemas {
		name := schemas[i].GetName()
		InName, ExName := common.StringToVariableName(name), name
		schemaHandler.Infos[i] = &common.Tag{
			InName: InName,
			ExName: ExName,
		}
	}
	schemaHandler.normalizeListWrapperNames()
	schemaHandler.CreateInExMap()

	// use DFS get path of schema
	ln := int32(len(schemas))
	var pos int32 = 0
	stack := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		// pos < ln guards against malformed schemas whose declared child counts
		// exceed the number of elements, which would otherwise index out of range.
		if pos < ln && (len(stack) == 0 || stack[len(stack)-1][1] > 0) {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}
			item := [2]int32{pos, schemas[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else {
			path := make([]string, 0)
			for i := range len(stack) {
				inname := schemaHandler.Infos[stack[i][0]].InName
				path = append(path, inname)
			}
			topPos := stack[len(stack)-1][0]
			schemaHandler.MapIndex[common.PathToStr(path)] = topPos
			schemaHandler.IndexMap[topPos] = common.PathToStr(path)
			stack = stack[:len(stack)-1]
		}
	}
	schemaHandler.setPathMap()
	schemaHandler.setValueColumns()
	schemaHandler.setVariantSchemas()

	return schemaHandler
}

// normalizeListWrapperNames gives three-level LIST wrappers the internal names List and Element.
func (sh *SchemaHandler) normalizeListWrapperNames() {
	// Writers may name the wrapper and element anything (Athena uses bag and
	// array_element); keeping their external names lets footer paths still resolve.
	if sh.childrenMap == nil {
		sh.childrenMap = sh.buildChildrenMap()
	}
	for idx := range sh.SchemaElements {
		if wrapperIdx, ok := sh.listWrapperIndex(int32(idx), sh.childrenMap); ok {
			sh.Infos[wrapperIdx].InName = "List"
			sh.Infos[sh.childrenMap[wrapperIdx][0]].InName = "Element"
		}
	}
}

// IsListWrapper reports whether the element at idx is the repeated group of a three-level LIST.
func (sh *SchemaHandler) IsListWrapper(idx int32) bool {
	if idx <= 0 || int(idx) >= len(sh.SchemaElements) {
		return false
	}
	path := common.StrToPath(sh.IndexMap[idx])
	parentIdx, ok := sh.MapIndex[common.PathToStr(path[:len(path)-1])]
	if !ok {
		return false
	}
	// Building locally keeps this safe for concurrent readers when no map is cached.
	children := sh.childrenMap
	if children == nil {
		children = sh.buildChildrenMap()
	}
	wrapperIdx, ok := sh.listWrapperIndex(parentIdx, children)
	return ok && wrapperIdx == idx
}

// listWrapperIndex returns the repeated wrapper of the LIST group at idx, if the group has one.
func (sh *SchemaHandler) listWrapperIndex(idx int32, children [][]int32) (int32, bool) {
	elem := sh.SchemaElements[idx]
	if !isListAnnotated(elem) || len(children[idx]) != 1 {
		return 0, false
	}
	wrapperIdx := children[idx][0]
	wrapper := sh.SchemaElements[wrapperIdx]
	if wrapper.GetRepetitionType() != parquet.FieldRepetitionType_REPEATED || len(children[wrapperIdx]) != 1 {
		return 0, false
	}
	// The spec's backward-compatibility rules make the repeated group the element
	// itself when its only field is repeated or when it is named array or <list>_tuple.
	if sh.SchemaElements[children[wrapperIdx][0]].GetRepetitionType() == parquet.FieldRepetitionType_REPEATED ||
		wrapper.GetName() == "array" || wrapper.GetName() == elem.GetName()+"_tuple" {
		return 0, false
	}
	return wrapperIdx, true
}
