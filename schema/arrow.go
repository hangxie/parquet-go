package schema

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// Schema metadata used to parse the native and converted types and
// creating the schema definitions
const (
	rootNodeName = "Parquet45go45root"
)

// ConvertArrowToParquetSchema converts arrow schema to representation
// understandable by parquet-go library.
// We need this coversion and can't directly use arrow format because the
// go parquet type contains metadata which the base writer is using to
// determine the size of the objects.
func ConvertArrowToParquetSchema(schema *arrow.Schema) ([]string, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	metaData := make([]string, len(schema.Fields()))
	for k, v := range schema.Fields() {
		repetitionType := parquet.FieldRepetitionType_REQUIRED
		if v.Nullable {
			repetitionType = parquet.FieldRepetitionType_OPTIONAL
		}
		switch fieldType := v.Type.(type) {
		case *arrow.Int8Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT32, parquet.ConvertedType_INT_8, repetitionType)
		case *arrow.Int16Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT32, parquet.ConvertedType_INT_16, repetitionType)
		case *arrow.Int32Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT32, repetitionType)
		case *arrow.Int64Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT64, repetitionType)
		case *arrow.Uint8Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT32, parquet.ConvertedType_UINT_8, repetitionType)
		case *arrow.Uint16Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT32, parquet.ConvertedType_UINT_16, repetitionType)
		case *arrow.Uint32Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT32, parquet.ConvertedType_UINT_32, repetitionType)
		case *arrow.Uint64Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT64, parquet.ConvertedType_UINT_64, repetitionType)
		case *arrow.Float32Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, repetitiontype=%s",
				v.Name, parquet.Type_FLOAT, repetitionType)
		case *arrow.Float64Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, repetitiontype=%s",
				v.Name, parquet.Type_DOUBLE, repetitionType)
		case *arrow.Float16Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, length=2, logicaltype=FLOAT16, repetitiontype=%s",
				v.Name, parquet.Type_FIXED_LEN_BYTE_ARRAY, repetitionType)
		case *arrow.Date32Type, *arrow.Date64Type:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT32, parquet.ConvertedType_DATE, repetitionType)
		case *arrow.BinaryType:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, repetitiontype=%s",
				v.Name, parquet.Type_BYTE_ARRAY, repetitionType)
		case *arrow.StringType:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_BYTE_ARRAY, parquet.ConvertedType_UTF8, repetitionType)
		case *arrow.BooleanType:
			metaData[k] = fmt.Sprintf("name=%s, type=%s, repetitiontype=%s",
				v.Name, parquet.Type_BOOLEAN, repetitionType)
		case *arrow.Time32Type:
			if fieldType.Unit != arrow.Millisecond {
				return nil, fmt.Errorf("unsupported arrow format: %s", fieldType.String())
			}
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT32, parquet.ConvertedType_TIME_MILLIS, repetitionType)
		case *arrow.TimestampType:
			if fieldType.Unit != arrow.Millisecond {
				return nil, fmt.Errorf("unsupported arrow format: %s", fieldType.String())
			}
			metaData[k] = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=%s",
				v.Name, parquet.Type_INT64, parquet.ConvertedType_TIMESTAMP_MILLIS, repetitionType)
		default:
			return nil,
				fmt.Errorf("unsupported arrow format: %s", v.Type.Name())
		}
	}
	return metaData, nil
}

// NewSchemaHandlerFromArrow creates a schema handler from arrow format.
// This handler is needed since the base ParquetWriter does not understand
// arrow schema and we need to translate it to the native format which the
// parquet-go library understands.
func NewSchemaHandlerFromArrow(arrowSchema *arrow.Schema) (
	*SchemaHandler, error,
) {
	schemaList := make([]*parquet.SchemaElement, 0)
	infos := make([]*common.Tag, 0)

	fields, err := ConvertArrowToParquetSchema(arrowSchema)
	if err != nil {
		return nil, fmt.Errorf("convert arrow to parquet schema: %w", err)
	}

	rootSchema := parquet.NewSchemaElement()
	rootSchema.Name = rootNodeName
	rootNumChildren := int32(len(fields))
	rootSchema.NumChildren = &rootNumChildren
	rt := parquet.FieldRepetitionType_REQUIRED
	rootSchema.RepetitionType = &rt
	schemaList = append(schemaList, rootSchema)

	rootInfo := &common.Tag{}
	rootInfo.InName = rootNodeName
	rootInfo.ExName = rootNodeName
	rootInfo.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	infos = append(infos, rootInfo)

	for _, field := range fields {
		info, err := common.StringToTag(field)
		if err != nil {
			return nil, fmt.Errorf("parse tag %q: %w", field, err)
		}
		infos = append(infos, info)
		schema, err := common.NewSchemaElementFromTagMap(info)
		if err != nil {
			return nil, fmt.Errorf("build schema element for %s: %w", info.InName, err)
		}
		schemaList = append(schemaList, schema)
	}
	res := NewSchemaHandlerFromSchemaList(schemaList)
	res.Infos = infos
	res.CreateInExMap()

	return res, nil
}
