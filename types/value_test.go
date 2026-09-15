package types

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// TestStrToParquetTypeNilPhysicalType pins that a schema with no physical type is reported
// rather than dereferenced, whatever the annotation. The check has to sit ahead of the
// logical types, which reach for the physical type at their own pace: DECIMAL dereferenced
// it, STRING and DATE returned a value no column could hold, and UUID and FLOAT16
// complained about the length instead, so one missing type had four outcomes.
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

	for name, lT := range logicalTypes {
		t.Run("StrToParquetTypeWithLogical/"+name, func(t *testing.T) {
			_, err := StrToParquetTypeWithLogical("42", nil, nil, lT, 16, 2)
			require.ErrorContains(t, err, "without a physical type")
		})
		t.Run("JSONTypeToParquetTypeWithLogical/"+name, func(t *testing.T) {
			_, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf("42"), nil, nil, lT, 16, 2)
			require.ErrorContains(t, err, "without a physical type")
		})
	}
}
