package types

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
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

// annotatedIntegerCase is one integer column spelled both ways: an INTEGER logical type
// and the legacy converted type a schema may carry instead.
type annotatedIntegerCase struct {
	name  string
	width int
	pT    parquet.Type
	cT    parquet.ConvertedType
	lT    *parquet.LogicalType
}

func annotatedIntegerCases() []annotatedIntegerCase {
	return []annotatedIntegerCase{
		{"INT_8", 8, parquet.Type_INT32, parquet.ConvertedType_INT_8, createIntegerLogicalType(8, true)},
		{"INT_16", 16, parquet.Type_INT32, parquet.ConvertedType_INT_16, createIntegerLogicalType(16, true)},
		{"INT_32", 32, parquet.Type_INT32, parquet.ConvertedType_INT_32, createIntegerLogicalType(32, true)},
		{"INT_64", 64, parquet.Type_INT64, parquet.ConvertedType_INT_64, createIntegerLogicalType(64, true)},
		{"UINT_8", 8, parquet.Type_INT32, parquet.ConvertedType_UINT_8, createIntegerLogicalType(8, false)},
		{"UINT_16", 16, parquet.Type_INT32, parquet.ConvertedType_UINT_16, createIntegerLogicalType(16, false)},
		{"UINT_32", 32, parquet.Type_INT32, parquet.ConvertedType_UINT_32, createIntegerLogicalType(32, false)},
		{"UINT_64", 64, parquet.Type_INT64, parquet.ConvertedType_UINT_64, createIntegerLogicalType(64, false)},
	}
}

// TestAnnotatedIntegerStrictness covers the four ways a value reaches an annotated integer
// column: as text or a Go number, under the logical or the converted spelling. All four
// must agree, so one document reads alike whether or not the decoder used UseNumber.
func TestAnnotatedIntegerStrictness(t *testing.T) {
	rejected := []struct {
		name string
		text string
		num  any
		// maxWidth limits the case to columns narrow enough to reject the value.
		maxWidth int
	}{
		{name: "fractional", text: "1.5", num: float64(1.5)},
		{name: "trailing characters", text: "123abc"},
		{name: "past the declared width", text: "100000", num: int64(100000), maxWidth: 16},
		{name: "non-finite", text: "NaN", num: math.NaN()},
	}

	for _, col := range annotatedIntegerCases() {
		t.Run(col.name, func(t *testing.T) {
			for _, spelling := range []string{"converted", "logical"} {
				var cT *parquet.ConvertedType
				var lT *parquet.LogicalType
				if spelling == "converted" {
					cT = parquet.ConvertedTypePtr(col.cT)
				} else {
					lT = col.lT
				}

				for _, tc := range rejected {
					if tc.maxWidth != 0 && col.width > tc.maxWidth {
						continue
					}
					_, err := StrToParquetTypeWithLogical(tc.text, &col.pT, cT, lT, 0, 0)
					require.Error(t, err, "%s text %q as %s", col.name, tc.text, spelling)

					if tc.num != nil {
						_, err = JSONTypeToParquetTypeWithLogical(
							reflect.ValueOf(tc.num), &col.pT, cT, lT, 0, 0,
						)
						require.Error(t, err, "%s number %v as %s", col.name, tc.num, spelling)
					}
				}

				// A value the column does hold reads the same from either form.
				fromText, err := StrToParquetTypeWithLogical("7", &col.pT, cT, lT, 0, 0)
				require.NoError(t, err)
				fromNumber, err := JSONTypeToParquetTypeWithLogical(
					reflect.ValueOf(int64(7)), &col.pT, cT, lT, 0, 0,
				)
				require.NoError(t, err)
				require.Equal(t, fromText, fromNumber, "%s as %s", col.name, spelling)
			}
		})
	}
}

// TestJSONNumberText covers how a Go number is rendered for the string scanners: %v uses
// exponent notation above seven digits, which no parser here accepts.
func TestJSONNumberText(t *testing.T) {
	i32 := parquet.Type_INT32
	int32CT := parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)

	t.Run("a whole float keeps its digits", func(t *testing.T) {
		for _, value := range []any{float64(math.MaxInt32), float32(1 << 24)} {
			got, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(value), &i32, int32CT, nil, 0, 0)
			require.NoError(t, err, "%v", value)
			require.Equal(t, int32(reflect.ValueOf(value).Float()), got)
		}
	})

	t.Run("a long value is elided in the error", func(t *testing.T) {
		_, err := JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf(math.MaxFloat64), parquet.TypePtr(parquet.Type_INT64),
			parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64), nil, 0, 0,
		)
		require.ErrorContains(t, err, "...(309 bytes)")
		require.ErrorIs(t, err, strconv.ErrRange)
		require.Less(t, len(err.Error()), 150)
	})

	t.Run("a non-number renders as itself", func(t *testing.T) {
		_, err := JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf(map[string]any{"k": "v"}), &i32, int32CT, nil, 0, 0,
		)
		require.ErrorContains(t, err, `"map[k:v]"`)
	})
}

// TestAnnotatedIntegerSignedness pins the boundary each annotation puts on its column:
// an unsigned column refuses a negative value and takes the whole unsigned range, and a
// signed one is the mirror image.
func TestAnnotatedIntegerSignedness(t *testing.T) {
	i32 := parquet.Type_INT32
	uint8CT := parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8)
	int8CT := parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8)

	for _, tc := range []struct {
		name   string
		num    any
		cT     *parquet.ConvertedType
		want   any
		errMsg string
	}{
		{name: "UINT_8 takes 255", num: int64(255), cT: uint8CT, want: int32(255)},
		{name: "UINT_8 refuses 256", num: int64(256), cT: uint8CT, errMsg: "out of range"},
		{name: "UINT_8 refuses -1", num: int64(-1), cT: uint8CT, errMsg: "UINT_8"},
		{name: "INT_8 takes -128", num: int64(-128), cT: int8CT, want: int32(-128)},
		{name: "INT_8 refuses 128", num: int64(128), cT: int8CT, errMsg: "out of range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(tc.num), &i32, tc.cT, nil, 0, 0)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestJSONNumberStrictness covers a value reaching the conversion as a Go number rather
// than a json.Number, where the direct path truncated: 1.5 became 1, 1<<40 became 0.
func TestJSONNumberStrictness(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		pT     parquet.Type
		want   any
		errMsg string
	}{
		{name: "whole float", value: float64(42), pT: parquet.Type_INT32, want: int32(42)},
		{name: "fractional float", value: float64(1.5), pT: parquet.Type_INT32, errMsg: "not a whole number"},
		{name: "NaN", value: math.NaN(), pT: parquet.Type_INT64, errMsg: "not a whole number"},
		{name: "infinity", value: math.Inf(1), pT: parquet.Type_INT64, errMsg: "not a whole number"},
		{name: "int too wide for INT32", value: int64(1) << 40, pT: parquet.Type_INT32, errMsg: "out of range"},
		{name: "float too wide for INT32", value: float64(1) * (1 << 40), pT: parquet.Type_INT32, errMsg: "out of range"},
		{name: "uint too wide for INT64", value: uint64(math.MaxUint64), pT: parquet.Type_INT64, errMsg: "out of range"},
		{name: "uint at the INT64 edge", value: uint64(math.MaxInt64), pT: parquet.Type_INT64, want: int64(math.MaxInt64)},
		{name: "double takes any float", value: float64(1.5), pT: parquet.Type_DOUBLE, want: float64(1.5)},
		{name: "float too wide for FLOAT", value: math.MaxFloat64, pT: parquet.Type_FLOAT, errMsg: "out of range"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONTypeToParquetTypeWithLogical(
				reflect.ValueOf(tt.value), &tt.pT, nil, nil, 0, 0,
			)
			if tt.errMsg != "" {
				require.ErrorContains(t, err, tt.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// BenchmarkAnnotatedIntegerValue tracks the detour an annotated integer takes: rendered and
// handed to strToIntegerLogical, so text and number cannot disagree. Unannotated is the base.
func BenchmarkAnnotatedIntegerValue(b *testing.B) {
	pT := parquet.Type_INT64
	cT := parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
	number := reflect.ValueOf(int64(123456789))
	text := reflect.ValueOf(json.Number("123456789"))

	b.Run("annotated number", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = JSONTypeToParquetTypeWithLogical(number, &pT, cT, nil, 0, 0)
		}
	})
	b.Run("annotated json.Number", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = JSONTypeToParquetTypeWithLogical(text, &pT, cT, nil, 0, 0)
		}
	})
	b.Run("unannotated number", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = JSONTypeToParquetTypeWithLogical(number, &pT, nil, nil, 0, 0)
		}
	})
}

// TestDayAndTickCountsAreStrict covers the bare day and tick counts DATE and TIMESTAMP
// accept alongside their text form, which fmt.Sscanf read as far as "19723abc" -> 19723.
func TestDayAndTickCountsAreStrict(t *testing.T) {
	int32T, int64T := parquet.Type_INT32, parquet.Type_INT64
	dateCT := parquet.ConvertedTypePtr(parquet.ConvertedType_DATE)
	dateLT := parquet.NewLogicalType()
	dateLT.DATE = parquet.NewDateType()

	for _, tc := range []struct {
		name   string
		text   string
		pT     parquet.Type
		cT     *parquet.ConvertedType
		want   any
		errMsg string
	}{
		{name: "DATE day count", text: "19723", pT: int32T, cT: dateCT, want: int32(19723)},
		{name: "DATE trailing characters", text: "19723abc", pT: int32T, cT: dateCT, errMsg: "parse DATE"},
		{name: "DATE fractional", text: "1.5", pT: int32T, cT: dateCT, errMsg: "parse DATE"},
		{
			name: "TIMESTAMP_MILLIS tick count", text: "1699999999999", pT: int64T,
			cT: parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS), want: int64(1699999999999),
		},
		{
			name: "TIMESTAMP_MILLIS trailing characters", text: "123abc", pT: int64T,
			cT: parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS), errMsg: "parse TIMESTAMP_MILLIS",
		},
		{
			name: "TIMESTAMP_MICROS trailing characters", text: "123abc", pT: int64T,
			cT: parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS), errMsg: "parse TIMESTAMP_MICROS",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := StrToParquetType(tc.text, &tc.pT, tc.cT, 0, 0)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// The DATE logical type shares the fallback.
	_, err := StrToParquetTypeWithLogical("19723abc", &int32T, nil, dateLT, 0, 0)
	require.ErrorContains(t, err, "parse DATE")
}
