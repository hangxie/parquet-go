package common

import (
	"fmt"
	"strconv"
	"strings"

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

func newTimeUnitFromString(unitStr string) (*parquet.TimeUnit, error) {
	unit := parquet.NewTimeUnit()
	switch unitStr {
	case "MILLIS":
		unit.MILLIS = parquet.NewMilliSeconds()
	case "MICROS":
		unit.MICROS = parquet.NewMicroSeconds()
	case "NANOS":
		unit.NANOS = parquet.NewNanoSeconds()
	default:
		return nil, fmt.Errorf("logicaltype time error, unknown unit: %s", unitStr)
	}
	return unit, nil
}

func newEdgeInterpolationAlgorithmFromString(algoStr string) (*parquet.EdgeInterpolationAlgorithm, error) {
	if algoStr == "" {
		return nil, nil
	}
	switch strings.ToUpper(algoStr) {
	case "SPHERICAL":
		v := parquet.EdgeInterpolationAlgorithm_SPHERICAL
		return parquet.EdgeInterpolationAlgorithmPtr(v), nil
	case "VINCENTY":
		v := parquet.EdgeInterpolationAlgorithm_VINCENTY
		return parquet.EdgeInterpolationAlgorithmPtr(v), nil
	case "THOMAS":
		v := parquet.EdgeInterpolationAlgorithm_THOMAS
		return parquet.EdgeInterpolationAlgorithmPtr(v), nil
	case "ANDOYER":
		v := parquet.EdgeInterpolationAlgorithm_ANDOYER
		return parquet.EdgeInterpolationAlgorithmPtr(v), nil
	case "KARNEY":
		v := parquet.EdgeInterpolationAlgorithm_KARNEY
		return parquet.EdgeInterpolationAlgorithmPtr(v), nil
	default:
		return nil, fmt.Errorf("logicaltype geography error, unknown algorithm: %s", algoStr)
	}
}

func newDecimalLogicalType(mp map[string]string) (*parquet.DecimalType, error) {
	decimal := parquet.NewDecimalType()
	precisionVal := mp["logicaltype.precision"]
	valInt, err := strconv.ParseInt(precisionVal, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("parse logicaltype.precision value '%s' as int32: %w", precisionVal, err)
	}
	decimal.Precision = int32(valInt)

	scaleVal := mp["logicaltype.scale"]
	valInt, err = strconv.ParseInt(scaleVal, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("parse logicaltype.scale value '%s' as int32: %w", scaleVal, err)
	}
	decimal.Scale = int32(valInt)
	return decimal, nil
}

func newTimeLogicalType(mp map[string]string) (*parquet.TimeType, error) {
	timeType := parquet.NewTimeType()
	var err error
	if timeType.IsAdjustedToUTC, err = strconv.ParseBool(mp["logicaltype.isadjustedtoutc"]); err != nil {
		return nil, fmt.Errorf("parse logicaltype.isadjustedtoutc as boolean: %w", err)
	}
	if timeType.Unit, err = newTimeUnitFromString(mp["logicaltype.unit"]); err != nil {
		return nil, err
	}
	return timeType, nil
}

func newTimestampLogicalType(mp map[string]string) (*parquet.TimestampType, error) {
	timestampType := parquet.NewTimestampType()
	var err error
	if timestampType.IsAdjustedToUTC, err = strconv.ParseBool(mp["logicaltype.isadjustedtoutc"]); err != nil {
		return nil, fmt.Errorf("parse logicaltype.isadjustedtoutc as boolean: %w", err)
	}
	if timestampType.Unit, err = newTimeUnitFromString(mp["logicaltype.unit"]); err != nil {
		return nil, err
	}
	return timestampType, nil
}

func newIntegerLogicalType(mp map[string]string) (*parquet.IntType, error) {
	intType := parquet.NewIntType()
	valInt, err := strconv.ParseInt(mp["logicaltype.bitwidth"], 10, 8)
	if err != nil {
		return nil, fmt.Errorf("parse logicaltype.bitwidth as int32: %w", err)
	}
	intType.BitWidth = int8(valInt)
	if intType.IsSigned, err = strconv.ParseBool(mp["logicaltype.issigned"]); err != nil {
		return nil, fmt.Errorf("parse logicaltype.issigned as boolean: %w", err)
	}
	return intType, nil
}

func newVariantLogicalType(mp map[string]string) (*parquet.VariantType, error) {
	variantType := parquet.NewVariantType()
	if vStr, ok := mp["logicaltype.specification_version"]; ok && vStr != "" {
		valInt, err := strconv.ParseInt(vStr, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("parse logicaltype.specification_version as int8: %w", err)
		}
		v := int8(valInt)
		variantType.SpecificationVersion = &v
	}
	return variantType, nil
}

func newGeometryLogicalType(mp map[string]string) (*parquet.GeometryType, error) {
	geometryType := parquet.NewGeometryType()
	if crs, ok := mp["logicaltype.crs"]; ok && crs != "" {
		geometryType.CRS = &crs
	}
	return geometryType, nil
}

func newGeographyLogicalType(mp map[string]string) (*parquet.GeographyType, error) {
	geographyType := parquet.NewGeographyType()
	if crs, ok := mp["logicaltype.crs"]; ok && crs != "" {
		geographyType.CRS = &crs
	}
	if algoStr, ok := mp["logicaltype.algorithm"]; ok && algoStr != "" {
		algo, err := newEdgeInterpolationAlgorithmFromString(algoStr)
		if err != nil {
			return nil, err
		}
		geographyType.Algorithm = algo
	}
	return geographyType, nil
}

func newLogicalTypeFromFieldsMap(mp map[string]string) (*parquet.LogicalType, error) {
	val, ok := mp["logicaltype"]
	if !ok {
		return nil, fmt.Errorf("missing logicaltype")
	}

	var err error
	logicalType := parquet.NewLogicalType()
	switch val {
	case "STRING":
		logicalType.STRING = parquet.NewStringType()
	case "MAP":
		logicalType.MAP = parquet.NewMapType()
	case "LIST":
		logicalType.LIST = parquet.NewListType()
	case "ENUM":
		logicalType.ENUM = parquet.NewEnumType()
	case "DECIMAL":
		if logicalType.DECIMAL, err = newDecimalLogicalType(mp); err != nil {
			return nil, err
		}
	case "DATE":
		logicalType.DATE = parquet.NewDateType()
	case "TIME":
		if logicalType.TIME, err = newTimeLogicalType(mp); err != nil {
			return nil, err
		}
	case "TIMESTAMP":
		if logicalType.TIMESTAMP, err = newTimestampLogicalType(mp); err != nil {
			return nil, err
		}
	case "INTEGER":
		if logicalType.INTEGER, err = newIntegerLogicalType(mp); err != nil {
			return nil, err
		}
	case "JSON":
		logicalType.JSON = parquet.NewJsonType()
	case "BSON":
		logicalType.BSON = parquet.NewBsonType()
	case "UUID":
		logicalType.UUID = parquet.NewUUIDType()
	case "FLOAT16":
		logicalType.FLOAT16 = parquet.NewFloat16Type()
	case "VARIANT":
		if logicalType.VARIANT, err = newVariantLogicalType(mp); err != nil {
			return nil, err
		}
	case "GEOMETRY":
		if logicalType.GEOMETRY, err = newGeometryLogicalType(mp); err != nil {
			return nil, err
		}
	case "GEOGRAPHY":
		if logicalType.GEOGRAPHY, err = newGeographyLogicalType(mp); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown logicaltype: %s", val)
	}

	return logicalType, nil
}

var intAttrMap = map[parquet.ConvertedType]struct {
	bitWidth int8
	isSigned bool
}{
	parquet.ConvertedType_INT_8:   {8, true},
	parquet.ConvertedType_INT_16:  {16, true},
	parquet.ConvertedType_INT_32:  {32, true},
	parquet.ConvertedType_INT_64:  {64, true},
	parquet.ConvertedType_UINT_8:  {8, false},
	parquet.ConvertedType_UINT_16: {16, false},
	parquet.ConvertedType_UINT_32: {32, false},
	parquet.ConvertedType_UINT_64: {64, false},
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
