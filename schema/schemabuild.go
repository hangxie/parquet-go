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

func parseStructFieldTag(tag string) (*common.Tag, error) {
	info, err := common.StringToTag(tag)
	if err != nil {
		return nil, fmt.Errorf("parse tag: %w", err)
	}
	if err := common.ValidateTagAnnotations(info); err != nil {
		return nil, fmt.Errorf("validate tag annotations: %w", err)
	}
	return info, nil
}

// Create schema handler from a object
func createVariantSchema(item *Item, stack *[]*Item, schemaElements *[]*parquet.SchemaElement, infos *[]*common.Tag) error {
	// VARIANT is a GROUP with two required BYTE_ARRAY children: Metadata and Value
	schema := parquet.NewSchemaElement()
	schema.Name = item.Info.InName
	rt := item.Info.RepetitionType
	schema.RepetitionType = &rt

	// Get the LogicalType from the Tag (handles specification_version)
	// Always ensure VARIANT LogicalType is set
	logicalType, err := common.GetLogicalTypeFromTag(item.Info)
	if err != nil {
		return fmt.Errorf("get logicaltype from tag: %w", err)
	}
	if logicalType == nil || logicalType.VARIANT == nil {
		logicalType = parquet.NewLogicalType()
		logicalType.VARIANT = parquet.NewVariantType()
	}
	schema.LogicalType = logicalType
	*schemaElements = append(*schemaElements, schema)
	newInfo := &common.Tag{}
	common.DeepCopy(item.Info, newInfo)
	*infos = append(*infos, newInfo)

	useStruct := false
	if item.GoType.Kind() == reflect.Struct {
		for i := 0; i < item.GoType.NumField(); i++ {
			if item.GoType.Field(i).Tag.Get("parquet") != "" {
				useStruct = true
				break
			}
		}
	}

	if useStruct {
		numField := int32(item.GoType.NumField())
		schema.NumChildren = &numField
		for i := int(numField - 1); i >= 0; i-- {
			f := item.GoType.Field(i)
			tagStr := f.Tag.Get("parquet")

			// ignore item without parquet tag
			if len(tagStr) <= 0 {
				(*schema.NumChildren)--
				continue
			}

			newItem := NewItem()
			var err error
			newItem.Info, err = parseStructFieldTag(tagStr)
			if err != nil {
				return err
			}
			newItem.Info.InName = f.Name
			newItem.GoType = f.Type
			if f.Type.Kind() == reflect.Ptr {
				newItem.GoType = f.Type.Elem()
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
			}
			*stack = append(*stack, newItem)
		}
	} else {
		var numChildren int32 = 2
		schema.NumChildren = &numChildren

		// Add Value child (required binary)
		valueItem := NewItem()
		valueItem.Info = &common.Tag{InName: "Value", ExName: "value"}
		valueItem.Info.Type = "BYTE_ARRAY"
		valueItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
		valueItem.Info.Encoding = item.Info.Encoding
		valueItem.Info.CompressionCodec = item.Info.CompressionCodec
		valueItem.GoType = reflect.TypeOf("")
		*stack = append(*stack, valueItem)

		// Add Metadata child (required binary)
		metadataItem := NewItem()
		metadataItem.Info = &common.Tag{InName: "Metadata", ExName: "metadata"}
		metadataItem.Info.Type = "BYTE_ARRAY"
		metadataItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
		metadataItem.Info.Encoding = item.Info.Encoding
		metadataItem.Info.CompressionCodec = item.Info.CompressionCodec
		metadataItem.GoType = reflect.TypeOf("")
		*stack = append(*stack, metadataItem)
	}
	return nil
}

func createStructSchema(item *Item, stack *[]*Item, schemaElements *[]*parquet.SchemaElement, infos *[]*common.Tag) error {
	schema := parquet.NewSchemaElement()
	schema.Name = item.Info.InName
	schema.RepetitionType = &item.Info.RepetitionType
	numField := int32(item.GoType.NumField())
	schema.NumChildren = &numField
	*schemaElements = append(*schemaElements, schema)

	newInfo := &common.Tag{}
	common.DeepCopy(item.Info, newInfo)
	*infos = append(*infos, newInfo)

	for i := int(numField - 1); i >= 0; i-- {
		f := item.GoType.Field(i)
		tagStr := f.Tag.Get("parquet")

		// ignore item without parquet tag
		if len(tagStr) <= 0 {
			numField--
			continue
		}

		newItem := NewItem()
		var err error
		newItem.Info, err = parseStructFieldTag(tagStr)
		if err != nil {
			return err
		}
		newItem.Info.InName = f.Name
		newItem.GoType = f.Type
		if f.Type.Kind() == reflect.Ptr {
			newItem.GoType = f.Type.Elem()
			newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
		}
		*stack = append(*stack, newItem)
	}
	return nil
}

func createListSchema(item *Item, stack *[]*Item, schemaElements *[]*parquet.SchemaElement, infos *[]*common.Tag) {
	schema := parquet.NewSchemaElement()
	schema.Name = item.Info.InName
	rt1 := item.Info.RepetitionType
	schema.RepetitionType = &rt1
	var numField int32 = 1
	schema.NumChildren = &numField
	ct1 := parquet.ConvertedType_LIST
	schema.ConvertedType = &ct1
	*schemaElements = append(*schemaElements, schema)
	newInfo := &common.Tag{}
	common.DeepCopy(item.Info, newInfo)
	*infos = append(*infos, newInfo)

	schema = parquet.NewSchemaElement()
	schema.Name = "List"
	rt2 := parquet.FieldRepetitionType_REPEATED
	schema.RepetitionType = &rt2
	schema.NumChildren = &numField
	*schemaElements = append(*schemaElements, schema)
	newInfo = &common.Tag{}
	common.DeepCopy(item.Info, newInfo)
	newInfo.InName, newInfo.ExName = "List", "list"
	*infos = append(*infos, newInfo)

	newItem := NewItem()
	newItem.Info = common.GetValueTagMap(item.Info)
	newItem.Info.InName = "Element"
	newItem.Info.ExName = "element"
	newItem.GoType = item.GoType.Elem()
	if newItem.GoType.Kind() == reflect.Ptr {
		newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
		newItem.GoType = item.GoType.Elem().Elem()
	} else {
		newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	}
	*stack = append(*stack, newItem)
}

func createMapSchema(item *Item, stack *[]*Item, schemaElements *[]*parquet.SchemaElement, infos *[]*common.Tag) {
	schema := parquet.NewSchemaElement()
	schema.Name = item.Info.InName
	rt1 := item.Info.RepetitionType
	schema.RepetitionType = &rt1
	var numField1 int32 = 1
	schema.NumChildren = &numField1
	ct1 := parquet.ConvertedType_MAP
	schema.ConvertedType = &ct1
	*schemaElements = append(*schemaElements, schema)
	newInfo := &common.Tag{}
	common.DeepCopy(item.Info, newInfo)
	*infos = append(*infos, newInfo)

	schema = parquet.NewSchemaElement()
	schema.Name = "Key_value"
	rt2 := parquet.FieldRepetitionType_REPEATED
	schema.RepetitionType = &rt2
	var numField2 int32 = 2
	schema.NumChildren = &numField2
	ct2 := parquet.ConvertedType_MAP_KEY_VALUE
	schema.ConvertedType = &ct2
	*schemaElements = append(*schemaElements, schema)
	newInfo = &common.Tag{}
	common.DeepCopy(item.Info, newInfo)
	newInfo.InName, newInfo.ExName = "Key_value", "key_value"
	*infos = append(*infos, newInfo)

	newItem := NewItem()
	newItem.Info = common.GetValueTagMap(item.Info)
	newItem.GoType = item.GoType.Elem()
	if newItem.GoType.Kind() == reflect.Ptr {
		newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
		newItem.GoType = item.GoType.Elem().Elem()
	} else {
		newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	}
	*stack = append(*stack, newItem)

	newItem = NewItem()
	newItem.Info = common.GetKeyTagMap(item.Info)
	newItem.GoType = item.GoType.Key()
	newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	*stack = append(*stack, newItem)
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
				return nil, err
			}
		} else if item.GoType.Kind() == reflect.Struct {
			if err = createStructSchema(item, &stack, &schemaElements, &infos); err != nil {
				return nil, err
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
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
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
