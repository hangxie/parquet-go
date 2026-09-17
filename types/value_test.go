package types

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// TestStrToParquetTypeWithLogical_TextLogicalTypes covers a text column carrying only a
// logical type, which fell through to the BYTE_ARRAY scan and was read as base64.
func TestStrToParquetTypeWithLogical_TextLogicalTypes(t *testing.T) {
	byteArray := parquet.Type_BYTE_ARRAY
	logicalTypes := map[string]*parquet.LogicalType{
		"STRING": {STRING: parquet.NewStringType()},
		"ENUM":   {ENUM: parquet.NewEnumType()},
		"JSON":   {JSON: parquet.NewJsonType()},
	}

	for name, lT := range logicalTypes {
		t.Run(name, func(t *testing.T) {
			for _, value := range []string{"hello", "TEST", "null", ""} {
				for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw} {
					got, err := StrToParquetTypeWithLogical(value, &byteArray, nil, lT, 0, 0, WithValueMode(mode))
					require.NoError(t, err, "%s in %s mode", value, mode)
					require.Equal(t, value, got, "%s in %s mode", value, mode)
				}
			}
		})
	}
}

// TestStrToParquetTypeNilPhysicalType pins that a schema with no physical type is reported
// rather than dereferenced, whatever the annotation.
func TestStrToParquetTypeNilPhysicalType(t *testing.T) {
	decimalLT := parquet.NewLogicalType()
	decimalLT.DECIMAL = &parquet.DecimalType{Precision: 9, Scale: 2}
	timeLT := parquet.NewLogicalType()
	timeLT.TIME = &parquet.TimeType{Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}

	logicalTypes := map[string]*parquet.LogicalType{
		"none":    nil,
		"STRING":  {STRING: parquet.NewStringType()},
		"DATE":    {DATE: parquet.NewDateType()},
		"UUID":    {UUID: parquet.NewUUIDType()},
		"FLOAT16": {FLOAT16: parquet.NewFloat16Type()},
		"INTEGER": createIntegerLogicalType(32, true),
		"DECIMAL": decimalLT,
		"TIME":    timeLT,
	}
	convertedTypes := map[string]*parquet.ConvertedType{
		"none":    nil,
		"UTF8":    parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		"DATE":    parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
		"INT_32":  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32),
		"UINT_8":  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8),
		"DECIMAL": parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
	}

	for name, cT := range convertedTypes {
		t.Run("StrToParquetType/"+name, func(t *testing.T) {
			_, err := StrToParquetType("42", nil, cT, 0, 0)
			require.ErrorContains(t, err, "without a physical type")
		})
	}

	// The wrapper has to check ahead of the logical types, which reach for the physical
	// type at their own pace: DECIMAL dereferenced it, STRING and DATE returned a value
	// no column could hold, and UUID and FLOAT16 complained about the length instead.
	for name, lT := range logicalTypes {
		t.Run("StrToParquetTypeWithLogical/"+name, func(t *testing.T) {
			for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw} {
				_, err := StrToParquetTypeWithLogical("42", nil, nil, lT, 16, 2, WithValueMode(mode))
				require.ErrorContains(t, err, "without a physical type", "%s mode", mode)
			}
		})
		t.Run("JSONTypeToParquetTypeWithLogical/"+name, func(t *testing.T) {
			for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw} {
				_, err := JSONTypeToParquetTypeWithLogical(
					reflect.ValueOf("42"), nil, nil, lT, 16, 2, WithValueMode(mode),
				)
				require.ErrorContains(t, err, "without a physical type", "%s mode", mode)
			}
		})
	}
}
