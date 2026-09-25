package common

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

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
		return nil, fmt.Errorf("parse TIME unit: %w", err)
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
		return nil, fmt.Errorf("parse TIMESTAMP unit: %w", err)
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
			return nil, fmt.Errorf("parse GEOGRAPHY algorithm: %w", err)
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
			return nil, fmt.Errorf("build DECIMAL logical type: %w", err)
		}
	case "DATE":
		logicalType.DATE = parquet.NewDateType()
	case "TIME":
		if logicalType.TIME, err = newTimeLogicalType(mp); err != nil {
			return nil, fmt.Errorf("build TIME logical type: %w", err)
		}
	case "TIMESTAMP":
		if logicalType.TIMESTAMP, err = newTimestampLogicalType(mp); err != nil {
			return nil, fmt.Errorf("build TIMESTAMP logical type: %w", err)
		}
	case "INTEGER":
		if logicalType.INTEGER, err = newIntegerLogicalType(mp); err != nil {
			return nil, fmt.Errorf("build INTEGER logical type: %w", err)
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
			return nil, fmt.Errorf("build VARIANT logical type: %w", err)
		}
	case "GEOMETRY":
		if logicalType.GEOMETRY, err = newGeometryLogicalType(mp); err != nil {
			return nil, fmt.Errorf("build GEOMETRY logical type: %w", err)
		}
	case "GEOGRAPHY":
		if logicalType.GEOGRAPHY, err = newGeographyLogicalType(mp); err != nil {
			return nil, fmt.Errorf("build GEOGRAPHY logical type: %w", err)
		}
	case "UNKNOWN":
		logicalType.UNKNOWN = parquet.NewNullType()
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
