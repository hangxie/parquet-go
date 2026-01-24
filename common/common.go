package common

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v2/parquet"
)

const (
	PAR_GO_PATH_DELIMITER = "\x01"
	DefaultPageSize       = 8 * 1024
	DefaultRowGroupSize   = 128 * 1024 * 1024
)

type fieldAttr struct {
	Type            string
	Length          int32
	Scale           int32
	Precision       int32
	Encoding        parquet.Encoding
	OmitStats       bool
	RepetitionType  parquet.FieldRepetitionType
	CompressionType *parquet.CompressionCodec // nil means use file-level compression

	convertedType     string
	isAdjustedToUTC   bool
	fieldID           int32
	logicalTypeFields map[string]string
}

func (mp *fieldAttr) update(key, val string) error {
	var err error
	switch key {
	case "type":
		mp.Type = val
	case "convertedtype":
		mp.convertedType = val
	case "length":
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse length value '%s': %w", val, err)
		}
		mp.Length = int32(valInt)
	case "scale":
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse scale value '%s': %w", val, err)
		}
		mp.Scale = int32(valInt)
	case "precision":
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse precision value '%s': %w", val, err)
		}
		mp.Precision = int32(valInt)
	case "fieldid":
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse fieldid value '%s': %w", val, err)
		}
		mp.fieldID = int32(valInt)
	case "isadjustedtoutc":
		if mp.isAdjustedToUTC, err = strconv.ParseBool(val); err != nil {
			return fmt.Errorf("parse isadjustedtoutc value '%s': %w", val, err)
		}
	case "omitstats":
		if mp.OmitStats, err = strconv.ParseBool(val); err != nil {
			return fmt.Errorf("parse omitstats value '%s': %w", val, err)
		}
	case "repetitiontype":
		mp.RepetitionType, err = parquet.FieldRepetitionTypeFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("parse repetitiontype: %w", err)
		}
	case "encoding":
		mp.Encoding, err = parquet.EncodingFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("parse encoding: %w", err)
		}
	case "compression":
		codec, err := parquet.CompressionCodecFromString(strings.ToUpper(val))
		if err != nil {
			return fmt.Errorf("parse compression: %w", err)
		}
		mp.CompressionType = &codec
	default:
		if strings.HasPrefix(key, "logicaltype") {
			if mp.logicalTypeFields == nil {
				mp.logicalTypeFields = make(map[string]string)
			}
			mp.logicalTypeFields[key] = val
		} else {
			return fmt.Errorf("unrecognized tag '%v'", key)
		}
	}
	return nil
}

type Tag struct {
	InName string
	ExName string
	fieldAttr
	Key   fieldAttr
	Value fieldAttr
}

func StringToTag(tag string) (*Tag, error) {
	mp := &Tag{}
	tagStr := strings.Replace(tag, "\t", "", -1)

	for tag := range strings.SplitSeq(tagStr, ",") {
		kv := strings.SplitN(tag, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("expect 'key=value' but got '%s'", tag)
		}
		key, val := kv[0], kv[1]
		key = strings.ToLower(key)
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		if key == "name" {
			if mp.InName == "" {
				mp.InName = StringToVariableName(val)
			}
			mp.ExName = val
			continue
		}

		if key == "inname" {
			mp.InName = val
			continue
		}

		var err error
		if strings.HasPrefix(key, "key") {
			err = mp.Key.update(strings.TrimPrefix(key, "key"), val)
		} else if strings.HasPrefix(key, "value") {
			err = mp.Value.update(strings.TrimPrefix(key, "value"), val)
		} else {
			err = mp.fieldAttr.update(key, val)
		}
		if err != nil {
			return nil, fmt.Errorf("parse tag '%s': %w", tag, err)
		}
	}
	return mp, nil
}

func NewSchemaElementFromTagMap(info *Tag) (*parquet.SchemaElement, error) {
	schema := parquet.NewSchemaElement()
	schema.Name = info.InName
	schema.TypeLength = &info.Length
	schema.Scale = &info.Scale
	schema.Precision = &info.Precision
	schema.FieldID = &info.fieldID
	schema.RepetitionType = &info.RepetitionType
	schema.NumChildren = nil

	if t, err := parquet.TypeFromString(info.Type); err == nil {
		schema.Type = &t
	} else {
		return nil, fmt.Errorf("field [%s] with type [%s]: %w", info.InName, info.Type, err)
	}

	// Validate encoding compatibility with type per Parquet spec
	switch info.Encoding {
	case parquet.Encoding_RLE:
		// RLE: BOOLEAN only for data pages (per Parquet spec)
		// For INT32/INT64, RLE is only valid for repetition/definition levels (handled internally)
		// Using RLE for INT data pages is non-compliant and crashes readers like DuckDB
		if *schema.Type != parquet.Type_BOOLEAN {
			return nil, fmt.Errorf("field [%s]: RLE encoding is only supported for BOOLEAN, not %v", info.InName, *schema.Type)
		}
	case parquet.Encoding_BIT_PACKED:
		// BIT_PACKED: deprecated, BOOLEAN only (INT32/INT64 use RLE instead)
		if *schema.Type != parquet.Type_BOOLEAN {
			return nil, fmt.Errorf("field [%s]: BIT_PACKED encoding is deprecated and only supported for BOOLEAN, not %v", info.InName, *schema.Type)
		}
	case parquet.Encoding_DELTA_BINARY_PACKED:
		// DELTA_BINARY_PACKED: INT32, INT64 only
		switch *schema.Type {
		case parquet.Type_INT32, parquet.Type_INT64:
			// valid
		default:
			return nil, fmt.Errorf("field [%s]: DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64, not %v", info.InName, *schema.Type)
		}
	case parquet.Encoding_DELTA_BYTE_ARRAY:
		// DELTA_BYTE_ARRAY: BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY
		switch *schema.Type {
		case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			// valid
		default:
			return nil, fmt.Errorf("field [%s]: DELTA_BYTE_ARRAY encoding is only supported for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY, not %v", info.InName, *schema.Type)
		}
	case parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		// DELTA_LENGTH_BYTE_ARRAY: BYTE_ARRAY only (per Parquet spec)
		if *schema.Type != parquet.Type_BYTE_ARRAY {
			return nil, fmt.Errorf("field [%s]: DELTA_LENGTH_BYTE_ARRAY encoding is only supported for BYTE_ARRAY, not %v", info.InName, *schema.Type)
		}
	case parquet.Encoding_BYTE_STREAM_SPLIT:
		// BYTE_STREAM_SPLIT: FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY
		switch *schema.Type {
		case parquet.Type_FLOAT, parquet.Type_DOUBLE, parquet.Type_INT32, parquet.Type_INT64, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			// valid
		default:
			return nil, fmt.Errorf("field [%s]: BYTE_STREAM_SPLIT encoding is only supported for FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY, not %v", info.InName, *schema.Type)
		}
	}

	if ct, err := parquet.ConvertedTypeFromString(info.convertedType); err == nil {
		schema.ConvertedType = &ct
	}

	var logicalType *parquet.LogicalType
	var err error
	if len(info.logicalTypeFields) > 0 {
		logicalType, err = newLogicalTypeFromFieldsMap(info.logicalTypeFields)
		if err != nil {
			return nil, fmt.Errorf("create logicaltype from field map: %w", err)
		}
	} else {
		logicalType = newLogicalTypeFromConvertedType(schema, info)
	}

	schema.LogicalType = logicalType

	// Set ConvertedType for backward compatibility with older Parquet readers.
	// Per Parquet spec, logical types should have corresponding converted types when applicable.
	// Only set if not already provided by the user.
	if logicalType != nil {
		if schema.ConvertedType == nil {
			schema.ConvertedType = convertedTypeFromLogicalType(logicalType)
		}
		// For DECIMAL, also sync schema's Scale and Precision with the logical type's values.
		if logicalType.DECIMAL != nil {
			schema.Scale = &logicalType.DECIMAL.Scale
			schema.Precision = &logicalType.DECIMAL.Precision
		}
	}

	if err := ValidateSchemaElement(schema); err != nil {
		return nil, err
	}

	return schema, nil
}

// ValidateSchemaElement checks if the ConvertedType and LogicalType are compatible with the physical Type.
func ValidateSchemaElement(schema *parquet.SchemaElement) error {
	if schema.Type == nil {
		return nil
	}

	// LogicalType validation
	if schema.LogicalType != nil {
		if schema.LogicalType.STRING != nil || schema.LogicalType.JSON != nil || schema.LogicalType.BSON != nil || schema.LogicalType.ENUM != nil {
			if *schema.Type != parquet.Type_BYTE_ARRAY {
				return fmt.Errorf("LogicalType STRING/JSON/BSON/ENUM can only be used with BYTE_ARRAY")
			}
		}
		if schema.LogicalType.UUID != nil {
			if *schema.Type != parquet.Type_FIXED_LEN_BYTE_ARRAY {
				return fmt.Errorf("LogicalType UUID can only be used with FIXED_LEN_BYTE_ARRAY")
			}
		}
		if schema.LogicalType.DATE != nil {
			if *schema.Type != parquet.Type_INT32 {
				return fmt.Errorf("LogicalType DATE can only be used with INT32")
			}
		}
		if schema.LogicalType.TIME != nil {
			if schema.LogicalType.TIME.Unit.MILLIS != nil {
				if *schema.Type != parquet.Type_INT32 {
					return fmt.Errorf("LogicalType TIME(MILLIS) can only be used with INT32")
				}
			} else {
				if *schema.Type != parquet.Type_INT64 {
					return fmt.Errorf("LogicalType TIME(MICROS/NANOS) can only be used with INT64")
				}
			}
		}
		if schema.LogicalType.TIMESTAMP != nil {
			if *schema.Type != parquet.Type_INT64 {
				return fmt.Errorf("LogicalType TIMESTAMP can only be used with INT64")
			}
		}
		if schema.LogicalType.INTEGER != nil {
			if schema.LogicalType.INTEGER.BitWidth <= 32 {
				if *schema.Type != parquet.Type_INT32 {
					return fmt.Errorf("LogicalType INTEGER(bitwidth<=32) can only be used with INT32")
				}
			} else {
				if *schema.Type != parquet.Type_INT64 {
					return fmt.Errorf("LogicalType INTEGER(bitwidth=64) can only be used with INT64")
				}
			}
		}
	}

	// ConvertedType validation
	if schema.ConvertedType != nil {
		switch *schema.ConvertedType {
		case parquet.ConvertedType_UTF8, parquet.ConvertedType_JSON, parquet.ConvertedType_BSON, parquet.ConvertedType_ENUM:
			if *schema.Type != parquet.Type_BYTE_ARRAY {
				return fmt.Errorf("ConvertedType %s can only be used with BYTE_ARRAY", schema.ConvertedType)
			}
		case parquet.ConvertedType_DATE, parquet.ConvertedType_TIME_MILLIS,
			parquet.ConvertedType_INT_8, parquet.ConvertedType_INT_16, parquet.ConvertedType_INT_32,
			parquet.ConvertedType_UINT_8, parquet.ConvertedType_UINT_16, parquet.ConvertedType_UINT_32:
			if *schema.Type != parquet.Type_INT32 {
				return fmt.Errorf("ConvertedType %s can only be used with INT32", schema.ConvertedType)
			}
		case parquet.ConvertedType_INT_64, parquet.ConvertedType_UINT_64,
			parquet.ConvertedType_TIME_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS, parquet.ConvertedType_TIMESTAMP_MICROS:
			if *schema.Type != parquet.Type_INT64 {
				return fmt.Errorf("ConvertedType %s can only be used with INT64", schema.ConvertedType)
			}
		case parquet.ConvertedType_INTERVAL:
			if *schema.Type != parquet.Type_FIXED_LEN_BYTE_ARRAY {
				return fmt.Errorf("ConvertedType %s can only be used with FIXED_LEN_BYTE_ARRAY", schema.ConvertedType)
			}
			if schema.TypeLength == nil || *schema.TypeLength != 12 {
				return fmt.Errorf("ConvertedType %s requires FIXED_LEN_BYTE_ARRAY with length 12", schema.ConvertedType)
			}
		}
	}

	return nil
}

// ValidateEncodingForDataPageVersion checks if an encoding is valid for a given data page version.
// Returns an error if the encoding is not compatible with the version.
func ValidateEncodingForDataPageVersion(fieldName string, encoding parquet.Encoding, version int32) error {
	switch encoding {
	case parquet.Encoding_PLAIN_DICTIONARY:
		// PLAIN_DICTIONARY is deprecated in Parquet 2.0+, only valid for v1 data pages
		if version != 1 {
			return fmt.Errorf("field [%s]: PLAIN_DICTIONARY encoding is deprecated and only valid for data page v1, use RLE_DICTIONARY for v2", fieldName)
		}
	case parquet.Encoding_DELTA_BINARY_PACKED, parquet.Encoding_DELTA_BYTE_ARRAY, parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY:
		// Delta encodings are for v2 data pages only
		if version == 1 {
			return fmt.Errorf("field [%s]: %v encoding is only supported for data page v2, not v1", fieldName, encoding)
		}
	}
	return nil
}

// GetLogicalTypeFromTag returns the LogicalType from a Tag.
// This is used when creating GROUP elements that need LogicalType annotation (e.g., VARIANT).
func GetLogicalTypeFromTag(info *Tag) *parquet.LogicalType {
	if len(info.logicalTypeFields) > 0 {
		logicalType, _ := newLogicalTypeFromFieldsMap(info.logicalTypeFields)
		return logicalType
	}
	return nil
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
		if lt.TIME.Unit != nil {
			if lt.TIME.Unit.MILLIS != nil {
				ct = parquet.ConvertedType_TIME_MILLIS
			} else if lt.TIME.Unit.MICROS != nil {
				ct = parquet.ConvertedType_TIME_MICROS
			} else {
				// NANOS has no corresponding ConvertedType
				return nil
			}
		} else {
			return nil
		}
	case lt.TIMESTAMP != nil:
		if lt.TIMESTAMP.Unit != nil {
			if lt.TIMESTAMP.Unit.MILLIS != nil {
				ct = parquet.ConvertedType_TIMESTAMP_MILLIS
			} else if lt.TIMESTAMP.Unit.MICROS != nil {
				ct = parquet.ConvertedType_TIMESTAMP_MICROS
			} else {
				// NANOS has no corresponding ConvertedType
				return nil
			}
		} else {
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
		logicalType.DECIMAL = parquet.NewDecimalType()
		precisionVal := mp["logicaltype.precision"]
		valInt, err := strconv.ParseInt(precisionVal, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse logicaltype.precision value '%s' as int32: %w", precisionVal, err)
		}
		logicalType.DECIMAL.Precision = int32(valInt)

		scaleVal := mp["logicaltype.scale"]
		valInt, err = strconv.ParseInt(scaleVal, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse logicaltype.scale value '%s' as int32: %w", scaleVal, err)
		}
		logicalType.DECIMAL.Scale = int32(valInt)
	case "DATE":
		logicalType.DATE = parquet.NewDateType()
	case "TIME":
		logicalType.TIME = parquet.NewTimeType()
		if logicalType.TIME.IsAdjustedToUTC, err = strconv.ParseBool(mp["logicaltype.isadjustedtoutc"]); err != nil {
			return nil, fmt.Errorf("parse logicaltype.isadjustedtoutc as boolean: %w", err)
		}
		if logicalType.TIME.Unit, err = newTimeUnitFromString(mp["logicaltype.unit"]); err != nil {
			return nil, err
		}
	case "TIMESTAMP":
		logicalType.TIMESTAMP = parquet.NewTimestampType()
		if logicalType.TIMESTAMP.IsAdjustedToUTC, err = strconv.ParseBool(mp["logicaltype.isadjustedtoutc"]); err != nil {
			return nil, fmt.Errorf("parse logicaltype.isadjustedtoutc as boolean: %w", err)
		}
		if logicalType.TIMESTAMP.Unit, err = newTimeUnitFromString(mp["logicaltype.unit"]); err != nil {
			return nil, err
		}
	case "INTEGER":
		logicalType.INTEGER = parquet.NewIntType()
		valInt, err := strconv.ParseInt(mp["logicaltype.bitwidth"], 10, 8)
		if err != nil {
			return nil, fmt.Errorf("parse logicaltype.bitwidth as int32: %w", err)
		}
		logicalType.INTEGER.BitWidth = int8(valInt)
		if logicalType.INTEGER.IsSigned, err = strconv.ParseBool(mp["logicaltype.issigned"]); err != nil {
			return nil, fmt.Errorf("parse logicaltype.issigned as boolean: %w", err)
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
		logicalType.VARIANT = parquet.NewVariantType()
		if vStr, ok := mp["logicaltype.specification_version"]; ok && vStr != "" {
			valInt, err := strconv.ParseInt(vStr, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("parse logicaltype.specification_version as int32: %w", err)
			}
			v := int8(valInt)
			logicalType.VARIANT.SpecificationVersion = &v
		}
	case "GEOMETRY":
		logicalType.GEOMETRY = parquet.NewGeometryType()
		if crs, ok := mp["logicaltype.crs"]; ok && crs != "" {
			logicalType.GEOMETRY.CRS = &crs
		}
	case "GEOGRAPHY":
		logicalType.GEOGRAPHY = parquet.NewGeographyType()
		if crs, ok := mp["logicaltype.crs"]; ok && crs != "" {
			logicalType.GEOGRAPHY.CRS = &crs
		}
		if algoStr, ok := mp["logicaltype.algorithm"]; ok && algoStr != "" {
			algo, err := newEdgeInterpolationAlgorithmFromString(algoStr)
			if err != nil {
				return nil, err
			}
			logicalType.GEOGRAPHY.Algorithm = algo
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

func DeepCopy(src, dst *Tag) {
	*dst = *src
	if src.logicalTypeFields != nil {
		dst.logicalTypeFields = make(map[string]string)
		for k, v := range src.logicalTypeFields {
			dst.logicalTypeFields[k] = v
		}
	}
	if src.Key.logicalTypeFields != nil {
		dst.Key.logicalTypeFields = make(map[string]string)
		for k, v := range src.Key.logicalTypeFields {
			dst.Key.logicalTypeFields[k] = v
		}
	}
	if src.Value.logicalTypeFields != nil {
		dst.Value.logicalTypeFields = make(map[string]string)
		for k, v := range src.Value.logicalTypeFields {
			dst.Value.logicalTypeFields[k] = v
		}
	}
}

// Get key tag map for map
func GetKeyTagMap(src *Tag) *Tag {
	res := &Tag{}
	res.InName = "Key"
	res.ExName = "key"
	res.fieldAttr = src.Key
	if src.Key.logicalTypeFields != nil {
		res.logicalTypeFields = make(map[string]string)
		for k, v := range src.Key.logicalTypeFields {
			res.logicalTypeFields[k] = v
		}
	}
	return res
}

// Get value tag map for map
func GetValueTagMap(src *Tag) *Tag {
	res := &Tag{}
	res.InName = "Value"
	res.ExName = "value"
	res.fieldAttr = src.Value
	if src.Value.logicalTypeFields != nil {
		res.logicalTypeFields = make(map[string]string)
		for k, v := range src.Value.logicalTypeFields {
			res.logicalTypeFields[k] = v
		}
	}
	return res
}

// Convert string to a golang variable name
func StringToVariableName(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	name := ""
	for i := range ln {
		c := str[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			name += string(c)
		} else {
			name += strconv.Itoa(int(c))
		}
	}

	name = headToUpper(name)
	return name
}

// Convert the first letter of a string to uppercase
func headToUpper(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	c := str[0]
	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
		return strings.ToUpper(str[0:1]) + str[1:]
	}
	// handle non-alpha prefix such as "_"
	return "PARGO_PREFIX_" + str
}

// . -> \x01
func ReformPathStr(pathStr string) string {
	return strings.ReplaceAll(pathStr, ".", PAR_GO_PATH_DELIMITER)
}

// Convert path slice to string
func PathToStr(path []string) string {
	return strings.Join(path, PAR_GO_PATH_DELIMITER)
}

// Convert string to path slice
func StrToPath(str string) []string {
	return strings.Split(str, PAR_GO_PATH_DELIMITER)
}

// Get the pathStr index in a path
func PathStrIndex(str string) int {
	return len(strings.Split(str, PAR_GO_PATH_DELIMITER))
}

func IsChildPath(parent, child string) bool {
	ln := len(parent)
	return strings.HasPrefix(child, parent) && (len(child) == ln || child[ln] == PAR_GO_PATH_DELIMITER[0])
}

func ToPtr[T any](value T) *T {
	return &value
}
