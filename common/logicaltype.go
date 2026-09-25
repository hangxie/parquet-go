package common

import (
	"github.com/hangxie/parquet-go/v3/parquet"
)

// GetLogicalTypeFromTag returns the LogicalType from a Tag.
// This is used when creating GROUP elements that need LogicalType annotation (e.g., VARIANT).
func GetLogicalTypeFromTag(info *Tag) (*parquet.LogicalType, error) {
	if len(info.logicalTypeFields) > 0 {
		return newLogicalTypeFromFieldsMap(info.logicalTypeFields)
	}
	return nil, nil
}

var logicalIntToConvertedTypeMap = map[struct {
	bitWidth int8
	isSigned bool
}]parquet.ConvertedType{
	{8, true}:   parquet.ConvertedType_INT_8,
	{8, false}:  parquet.ConvertedType_UINT_8,
	{16, true}:  parquet.ConvertedType_INT_16,
	{16, false}: parquet.ConvertedType_UINT_16,
	{32, true}:  parquet.ConvertedType_INT_32,
	{32, false}: parquet.ConvertedType_UINT_32,
	{64, true}:  parquet.ConvertedType_INT_64,
	{64, false}: parquet.ConvertedType_UINT_64,
}

// convertedTypeFromLogicalType returns the corresponding ConvertedType for a LogicalType.
// This is used for backward compatibility with older Parquet readers.
// Note: newer logical types like VARIANT, GEOMETRY, UUID, FLOAT16 do not have corresponding ConvertedTypes.
func convertedTypeFromLogicalType(lt *parquet.LogicalType) *parquet.ConvertedType {
	if lt == nil {
		return nil
	}

	var ct parquet.ConvertedType
	switch {
	case lt.STRING != nil:
		ct = parquet.ConvertedType_UTF8
	case lt.MAP != nil:
		ct = parquet.ConvertedType_MAP
	case lt.LIST != nil:
		ct = parquet.ConvertedType_LIST
	case lt.ENUM != nil:
		ct = parquet.ConvertedType_ENUM
	case lt.DECIMAL != nil:
		ct = parquet.ConvertedType_DECIMAL
	case lt.DATE != nil:
		ct = parquet.ConvertedType_DATE
	case lt.TIME != nil:
		if lt.TIME.Unit == nil {
			return nil
		}
		if lt.TIME.Unit.MILLIS != nil {
			ct = parquet.ConvertedType_TIME_MILLIS
		} else if lt.TIME.Unit.MICROS != nil {
			ct = parquet.ConvertedType_TIME_MICROS
		} else {
			// NANOS has no corresponding ConvertedType
			return nil
		}
	case lt.TIMESTAMP != nil:
		if lt.TIMESTAMP.Unit == nil {
			return nil
		}
		if lt.TIMESTAMP.Unit.MILLIS != nil {
			ct = parquet.ConvertedType_TIMESTAMP_MILLIS
		} else if lt.TIMESTAMP.Unit.MICROS != nil {
			ct = parquet.ConvertedType_TIMESTAMP_MICROS
		} else {
			// NANOS has no corresponding ConvertedType
			return nil
		}
	case lt.INTEGER != nil:
		key := struct {
			bitWidth int8
			isSigned bool
		}{lt.INTEGER.BitWidth, lt.INTEGER.IsSigned}
		if val, ok := logicalIntToConvertedTypeMap[key]; ok {
			ct = val
		} else {
			return nil
		}
	case lt.JSON != nil:
		ct = parquet.ConvertedType_JSON
	case lt.BSON != nil:
		ct = parquet.ConvertedType_BSON
	default:
		return nil
	}

	return &ct
}

func newLogicalTypeFromConvertedType(schemaElement *parquet.SchemaElement, info *Tag) *parquet.LogicalType {
	if schemaElement.ConvertedType == nil {
		return nil
	}

	logicalType := parquet.NewLogicalType()
	if attr, ok := intAttrMap[*schemaElement.ConvertedType]; ok {
		logicalType.INTEGER = parquet.NewIntType()
		logicalType.INTEGER.BitWidth = attr.bitWidth
		logicalType.INTEGER.IsSigned = attr.isSigned
		return logicalType
	}

	switch *schemaElement.ConvertedType {
	case parquet.ConvertedType_DECIMAL:
		logicalType.DECIMAL = parquet.NewDecimalType()
		logicalType.DECIMAL.Precision = info.Precision
		logicalType.DECIMAL.Scale = info.Scale
	case parquet.ConvertedType_DATE:
		logicalType.DATE = parquet.NewDateType()
	case parquet.ConvertedType_TIME_MICROS, parquet.ConvertedType_TIME_MILLIS:
		logicalType.TIME = parquet.NewTimeType()
		logicalType.TIME.IsAdjustedToUTC = info.isAdjustedToUTC
		logicalType.TIME.Unit, _ = newTimeUnitFromString(schemaElement.ConvertedType.String()[5:])
	case parquet.ConvertedType_TIMESTAMP_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS:
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		logicalType.TIMESTAMP.IsAdjustedToUTC = info.isAdjustedToUTC
		logicalType.TIMESTAMP.Unit, _ = newTimeUnitFromString(schemaElement.ConvertedType.String()[10:])
	case parquet.ConvertedType_BSON:
		logicalType.BSON = parquet.NewBsonType()
	case parquet.ConvertedType_ENUM:
		logicalType.ENUM = parquet.NewEnumType()
	case parquet.ConvertedType_JSON:
		logicalType.JSON = parquet.NewJsonType()
	case parquet.ConvertedType_LIST:
		logicalType.LIST = parquet.NewListType()
	case parquet.ConvertedType_MAP:
		logicalType.MAP = parquet.NewMapType()
	case parquet.ConvertedType_UTF8:
		logicalType.STRING = parquet.NewStringType()
	default:
		return nil
	}

	return logicalType
}
