package schema

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

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
				return fmt.Errorf("parse tag for field %s: %w", f.Name, err)
			}
			newItem.Info.InName = f.Name
			newItem.GoType = f.Type
			if f.Type.Kind() == reflect.Pointer {
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
		valueItem.Info.CompressionLevel = item.Info.CompressionLevel
		valueItem.GoType = reflect.TypeOf("")
		*stack = append(*stack, valueItem)

		// Add Metadata child (required binary)
		metadataItem := NewItem()
		metadataItem.Info = &common.Tag{InName: "Metadata", ExName: "metadata"}
		metadataItem.Info.Type = "BYTE_ARRAY"
		metadataItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
		metadataItem.Info.Encoding = item.Info.Encoding
		metadataItem.Info.CompressionCodec = item.Info.CompressionCodec
		metadataItem.Info.CompressionLevel = item.Info.CompressionLevel
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
			return fmt.Errorf("parse tag for field %s: %w", f.Name, err)
		}
		newItem.Info.InName = f.Name
		newItem.GoType = f.Type
		if f.Type.Kind() == reflect.Pointer {
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
	if newItem.GoType.Kind() == reflect.Pointer {
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
	if newItem.GoType.Kind() == reflect.Pointer {
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
