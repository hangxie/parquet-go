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
