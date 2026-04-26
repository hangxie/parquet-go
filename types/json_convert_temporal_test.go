package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestConvertTimestampValue_Nil(t *testing.T) {
	result := ConvertTimestampValue(nil, parquet.ConvertedType_TIMESTAMP_MILLIS)
	require.Nil(t, result)
}

func TestConvertTimestampValue_MillisNonInt64(t *testing.T) {
	// non-int64 value should be returned as-is
	result := ConvertTimestampValue("not-an-int", parquet.ConvertedType_TIMESTAMP_MILLIS)
	require.Equal(t, "not-an-int", result)
}

func TestConvertTimestampValue_Millis(t *testing.T) {
	// Epoch in millis = 0 → 1970-01-01T00:00:00Z
	result := ConvertTimestampValue(int64(0), parquet.ConvertedType_TIMESTAMP_MILLIS)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "1970-01-01")
}

func TestConvertTimestampValue_Micros(t *testing.T) {
	result := ConvertTimestampValue(int64(0), parquet.ConvertedType_TIMESTAMP_MICROS)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "1970-01-01")
}

func TestConvertTimestampValue_UnknownConvertedType(t *testing.T) {
	// For an unsupported ConvertedType, return value unchanged
	result := ConvertTimestampValue(int64(12345), parquet.ConvertedType_INT_8)
	require.Equal(t, int64(12345), result)
}

func TestConvertTimeLogicalValue_Nil(t *testing.T) {
	result := ConvertTimeLogicalValue(nil, nil)
	require.Nil(t, result)
}

func TestConvertTimeLogicalValue_NilTimeType(t *testing.T) {
	result := ConvertTimeLogicalValue(int32(1000), nil)
	require.Equal(t, int32(1000), result)
}

func TestConvertTimeLogicalValue_MILLIS(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.MILLIS = parquet.NewMilliSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	// 3661000 ms = 1 hour + 1 minute + 1 second
	result := ConvertTimeLogicalValue(int32(3661000), timeType)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "01:01:01")
}

func TestConvertTimeLogicalValue_MILLIS_WrongValueType(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.MILLIS = parquet.NewMilliSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	// int64 instead of int32 → returned as-is
	result := ConvertTimeLogicalValue(int64(1000), timeType)
	require.Equal(t, int64(1000), result)
}

func TestConvertTimeLogicalValue_MICROS(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.MICROS = parquet.NewMicroSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	// 3661000000 µs = 1 hour + 1 minute + 1 second
	result := ConvertTimeLogicalValue(int64(3661000000), timeType)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "01:01:01")
}

func TestConvertTimeLogicalValue_MICROS_WrongValueType(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.MICROS = parquet.NewMicroSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	result := ConvertTimeLogicalValue(int32(1000), timeType)
	require.Equal(t, int32(1000), result)
}

func TestConvertTimeLogicalValue_NANOS(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.NANOS = parquet.NewNanoSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	// 3661000000000 ns = 1 hour + 1 minute + 1 second
	result := ConvertTimeLogicalValue(int64(3661000000000), timeType)
	require.IsType(t, "", result)
	require.Contains(t, result.(string), "01:01:01")
}

func TestConvertTimeLogicalValue_NANOS_WrongValueType(t *testing.T) {
	unit := parquet.NewTimeUnit()
	unit.NANOS = parquet.NewNanoSeconds()
	timeType := &parquet.TimeType{Unit: unit}
	result := ConvertTimeLogicalValue(int32(100), timeType)
	require.Equal(t, int32(100), result)
}

func TestConvertTimeLogicalValue_NilUnit(t *testing.T) {
	timeType := &parquet.TimeType{Unit: nil}
	result := ConvertTimeLogicalValue(int32(1000), timeType)
	require.Equal(t, int32(1000), result)
}

func TestConvertDateLogicalValue_Nil(t *testing.T) {
	result := ConvertDateLogicalValue(nil)
	require.Nil(t, result)
}

func TestConvertDateLogicalValue_Int32(t *testing.T) {
	// Day 0 = 1970-01-01
	result := ConvertDateLogicalValue(int32(0))
	require.Equal(t, "1970-01-01", result)
}

func TestConvertDateLogicalValue_Int32Positive(t *testing.T) {
	// Day 1 = 1970-01-02
	result := ConvertDateLogicalValue(int32(1))
	require.Equal(t, "1970-01-02", result)
}

func TestConvertDateLogicalValue_Int32Negative(t *testing.T) {
	// Day -1 = 1969-12-31
	result := ConvertDateLogicalValue(int32(-1))
	require.Equal(t, "1969-12-31", result)
}

func TestConvertDateLogicalValue_NonInt32(t *testing.T) {
	result := ConvertDateLogicalValue(int64(365))
	require.Equal(t, int64(365), result)
}
