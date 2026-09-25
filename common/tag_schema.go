package common

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// Fixed widths required by the Parquet spec for FIXED_LEN_BYTE_ARRAY annotations.
const (
	UUIDByteLen     = 16
	Float16ByteLen  = 2
	IntervalByteLen = 12
)

// applyFixedTypeLength fills in the width the spec fixes for UUID, FLOAT16, and INTERVAL
// when the tag left length out, rather than rejecting a column whose size the caller
// cannot choose. An explicit length, including an explicit 0, is left for validation.
func applyFixedTypeLength(schema *parquet.SchemaElement, info *Tag) {
	if info.lengthSet || info.Length != 0 {
		return
	}
	if schema.Type == nil || *schema.Type != parquet.Type_FIXED_LEN_BYTE_ARRAY {
		return
	}

	var width int32
	lt := schema.LogicalType
	switch {
	case lt != nil && lt.UUID != nil:
		width = UUIDByteLen
	case lt != nil && lt.FLOAT16 != nil:
		width = Float16ByteLen
	case schema.ConvertedType != nil && *schema.ConvertedType == parquet.ConvertedType_INTERVAL:
		width = IntervalByteLen
	default:
		return
	}
	// A fresh pointer, so the caller's tag keeps whatever it declared.
	schema.TypeLength = &width
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

	if info.convertedType != "" {
		if ct, err := parquet.ConvertedTypeFromString(info.convertedType); err == nil {
			schema.ConvertedType = &ct
		} else {
			return nil, fmt.Errorf("field [%s] with convertedtype [%s]: %w", info.InName, info.convertedType, err)
		}
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

	applyFixedTypeLength(schema, info)

	if err := ValidateSchemaElement(schema); err != nil {
		return nil, fmt.Errorf("validate schema element: %w", err)
	}

	return schema, nil
}
