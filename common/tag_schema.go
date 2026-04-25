package common

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
)

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
