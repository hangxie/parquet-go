package schema

import (
	"encoding/json"
	"fmt"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

type JSONSchemaItemType struct {
	Tag    string                `json:"Tag"`
	Fields []*JSONSchemaItemType `json:"Fields,omitempty"`
}

func NewJSONSchemaItem() *JSONSchemaItemType {
	return new(JSONSchemaItemType)
}

func NewSchemaHandlerFromJSON(str string) (sh *SchemaHandler, err error) {
	schema := NewJSONSchemaItem()
	if err := json.Unmarshal([]byte(str), schema); err != nil {
		return nil, fmt.Errorf("unmarshal json schema string: %w", err)
	}

	stack := make([]*JSONSchemaItemType, 0)
	stack = append(stack, schema)
	schemaElements := make([]*parquet.SchemaElement, 0)
	infos := make([]*common.Tag, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item := stack[ln-1]
		stack = stack[:ln-1]
		info, err := common.StringToTag(item.Tag)
		if err != nil {
			return nil, fmt.Errorf("parse tag: %w", err)
		}
		var newInfo *common.Tag
		switch info.Type {
		case "": // struct
			schema := parquet.NewSchemaElement()
			schema.Name = info.InName
			rt := info.RepetitionType
			schema.RepetitionType = &rt
			numField := int32(len(item.Fields))
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)

			newInfo = &common.Tag{}
			common.DeepCopy(info, newInfo)
			infos = append(infos, newInfo)

			for i := int(numField - 1); i >= 0; i-- {
				newItem := item.Fields[i]
				stack = append(stack, newItem)
			}
		case "LIST": // list
			schema := parquet.NewSchemaElement()
			schema.Name = info.InName
			rt1 := info.RepetitionType
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			ct1 := parquet.ConvertedType_LIST
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)

			newInfo = &common.Tag{}
			common.DeepCopy(info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "List"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			var numField2 int32 = 1
			schema.NumChildren = &numField2
			schemaElements = append(schemaElements, schema)

			newInfo = &common.Tag{}
			common.DeepCopy(info, newInfo)
			newInfo.InName, newInfo.ExName = "List", "list"
			infos = append(infos, newInfo)

			if len(item.Fields) != 1 {
				return nil, fmt.Errorf("LIST needs exactly 1 field to define element type, got %d fields", len(item.Fields))
			}
			stack = append(stack, item.Fields[0])
		case "MAP": // map
			schema := parquet.NewSchemaElement()
			schema.Name = info.InName
			rt1 := info.RepetitionType
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			ct1 := parquet.ConvertedType_MAP
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)

			newInfo = &common.Tag{}
			common.DeepCopy(info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "Key_value"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			ct2 := parquet.ConvertedType_MAP_KEY_VALUE
			schema.ConvertedType = &ct2
			var numField2 int32 = 2
			schema.NumChildren = &numField2
			schemaElements = append(schemaElements, schema)

			newInfo = &common.Tag{}
			common.DeepCopy(info, newInfo)
			newInfo.InName, newInfo.ExName = "Key_value", "key_value"
			infos = append(infos, newInfo)

			if len(item.Fields) != 2 {
				return nil, fmt.Errorf("MAP needs exactly 2 fields to define key and value type, got %d fields", len(item.Fields))
			}
			stack = append(stack, item.Fields[1]) // put value
			stack = append(stack, item.Fields[0]) // put key
		case "VARIANT": // variant type - GROUP with metadata and value children
			schema := parquet.NewSchemaElement()
			schema.Name = info.InName
			rt := info.RepetitionType
			schema.RepetitionType = &rt
			var numChildren int32 = 2
			schema.NumChildren = &numChildren
			// Set LogicalType for VARIANT
			logicalType := common.GetLogicalTypeFromTag(info)
			if logicalType == nil {
				logicalType = parquet.NewLogicalType()
				logicalType.VARIANT = parquet.NewVariantType()
			}
			schema.LogicalType = logicalType
			schemaElements = append(schemaElements, schema)

			newInfo = &common.Tag{}
			common.DeepCopy(info, newInfo)
			infos = append(infos, newInfo)

			// Add metadata child (required binary)
			metadataSchema := parquet.NewSchemaElement()
			metadataSchema.Name = "Metadata"
			metadataType := parquet.Type_BYTE_ARRAY
			metadataSchema.Type = &metadataType
			metadataRt := parquet.FieldRepetitionType_REQUIRED
			metadataSchema.RepetitionType = &metadataRt
			schemaElements = append(schemaElements, metadataSchema)
			metadataInfo := &common.Tag{InName: "Metadata", ExName: "metadata"}
			metadataInfo.Type = "BYTE_ARRAY"
			metadataInfo.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			infos = append(infos, metadataInfo)

			// Add value child (required binary)
			valueSchema := parquet.NewSchemaElement()
			valueSchema.Name = "Value"
			valueType := parquet.Type_BYTE_ARRAY
			valueSchema.Type = &valueType
			valueRt := parquet.FieldRepetitionType_REQUIRED
			valueSchema.RepetitionType = &valueRt
			schemaElements = append(schemaElements, valueSchema)
			valueInfo := &common.Tag{InName: "Value", ExName: "value"}
			valueInfo.Type = "BYTE_ARRAY"
			valueInfo.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			infos = append(infos, valueInfo)
		default: // normal variable
			schema, err := common.NewSchemaElementFromTagMap(info)
			if err != nil {
				return nil, fmt.Errorf("create schema from tag map: %w", err)
			}
			schemaElements = append(schemaElements, schema)

			newInfo = &common.Tag{}
			common.DeepCopy(info, newInfo)
			infos = append(infos, newInfo)
		}
	}
	res := NewSchemaHandlerFromSchemaList(schemaElements)
	res.Infos = infos
	res.CreateInExMap()
	return res, nil
}
