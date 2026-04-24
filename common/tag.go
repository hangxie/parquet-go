package common

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
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
	BloomFilter     bool                      // enable bloom filter for this column
	BloomFilterSize int32                     // bloom filter size in bytes (0 = default)

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
	case "bloomfilter":
		if mp.BloomFilter, err = strconv.ParseBool(val); err != nil {
			return fmt.Errorf("parse bloomfilter value '%s': %w", val, err)
		}
	case "bloomfiltersize":
		valInt, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return fmt.Errorf("parse bloomfiltersize value '%s': %w", val, err)
		}
		mp.BloomFilterSize = int32(valInt)
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
	tagStr := strings.ReplaceAll(tag, "\t", "")

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
			err = mp.update(key, val)
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
func validateLogicalTime(lt *parquet.LogicalType, pT *parquet.Type) error {
	if lt.TIME == nil {
		return nil
	}
	if lt.TIME.Unit.MILLIS != nil {
		if *pT != parquet.Type_INT32 {
			return fmt.Errorf("LogicalType TIME(MILLIS) can only be used with INT32")
		}
	} else {
		if *pT != parquet.Type_INT64 {
			return fmt.Errorf("LogicalType TIME(MICROS/NANOS) can only be used with INT64")
		}
	}
	return nil
}

func validateLogicalInteger(lt *parquet.LogicalType, pT *parquet.Type) error {
	if lt.INTEGER == nil {
		return nil
	}
	if lt.INTEGER.BitWidth <= 32 {
		if *pT != parquet.Type_INT32 {
			return fmt.Errorf("LogicalType INTEGER(bitwidth<=32) can only be used with INT32")
		}
	} else {
		if *pT != parquet.Type_INT64 {
			return fmt.Errorf("LogicalType INTEGER(bitwidth=64) can only be used with INT64")
		}
	}
	return nil
}

func validateLogicalType(schema *parquet.SchemaElement) error {
	if schema.LogicalType == nil {
		return nil
	}
	lt := schema.LogicalType
	if lt.STRING != nil || lt.JSON != nil || lt.BSON != nil || lt.ENUM != nil {
		if *schema.Type != parquet.Type_BYTE_ARRAY {
			return fmt.Errorf("LogicalType STRING/JSON/BSON/ENUM can only be used with BYTE_ARRAY")
		}
	}
	if lt.UUID != nil {
		if *schema.Type != parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return fmt.Errorf("LogicalType UUID can only be used with FIXED_LEN_BYTE_ARRAY")
		}
	}
	if lt.DATE != nil {
		if *schema.Type != parquet.Type_INT32 {
			return fmt.Errorf("LogicalType DATE can only be used with INT32")
		}
	}
	if err := validateLogicalTime(lt, schema.Type); err != nil {
		return err
	}
	if lt.TIMESTAMP != nil {
		if *schema.Type != parquet.Type_INT64 {
			return fmt.Errorf("LogicalType TIMESTAMP can only be used with INT64")
		}
	}
	return validateLogicalInteger(lt, schema.Type)
}

func validateConvertedType(schema *parquet.SchemaElement) error {
	if schema.ConvertedType == nil {
		return nil
	}
	ct := *schema.ConvertedType
	switch ct {
	case parquet.ConvertedType_UTF8, parquet.ConvertedType_JSON, parquet.ConvertedType_BSON, parquet.ConvertedType_ENUM:
		if *schema.Type != parquet.Type_BYTE_ARRAY {
			return fmt.Errorf("ConvertedType %s can only be used with BYTE_ARRAY", ct)
		}
	case parquet.ConvertedType_DATE, parquet.ConvertedType_TIME_MILLIS,
		parquet.ConvertedType_INT_8, parquet.ConvertedType_INT_16, parquet.ConvertedType_INT_32,
		parquet.ConvertedType_UINT_8, parquet.ConvertedType_UINT_16, parquet.ConvertedType_UINT_32:
		if *schema.Type != parquet.Type_INT32 {
			return fmt.Errorf("ConvertedType %s can only be used with INT32", ct)
		}
	case parquet.ConvertedType_INT_64, parquet.ConvertedType_UINT_64,
		parquet.ConvertedType_TIME_MICROS, parquet.ConvertedType_TIMESTAMP_MILLIS, parquet.ConvertedType_TIMESTAMP_MICROS:
		if *schema.Type != parquet.Type_INT64 {
			return fmt.Errorf("ConvertedType %s can only be used with INT64", ct)
		}
	case parquet.ConvertedType_INTERVAL:
		if *schema.Type != parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return fmt.Errorf("ConvertedType %s can only be used with FIXED_LEN_BYTE_ARRAY", ct)
		}
		if schema.TypeLength == nil || *schema.TypeLength != 12 {
			return fmt.Errorf("ConvertedType %s requires FIXED_LEN_BYTE_ARRAY with length 12", ct)
		}
	}
	return nil
}

func ValidateSchemaElement(schema *parquet.SchemaElement) error {
	if schema.Type == nil {
		return nil
	}

	if err := validateLogicalType(schema); err != nil {
		return err
	}

	if err := validateConvertedType(schema); err != nil {
		return err
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

// StringToVariableName converts a string to a valid Go variable name.
func StringToVariableName(str string) string {
	ln := len(str)
	if ln <= 0 {
		return str
	}

	var b strings.Builder
	b.Grow(ln)
	for i := range ln {
		c := str[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			b.WriteByte(c)
		} else {
			b.WriteString(strconv.Itoa(int(c)))
		}
	}

	return headToUpper(b.String())
}

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
