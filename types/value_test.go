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

// TestDayCountsAreStrict covers the bare day count a DATE column accepts alongside its
// text form, which fmt.Sscanf read as far as "19723abc" -> 19723.
func TestDayCountsAreStrict(t *testing.T) {
	int32T := parquet.Type_INT32
	dateCT := parquet.ConvertedTypePtr(parquet.ConvertedType_DATE)
	dateLT := parquet.NewLogicalType()
	dateLT.DATE = parquet.NewDateType()

	got, err := StrToParquetType("19723", &int32T, dateCT, 0, 0)
	require.NoError(t, err)
	require.Equal(t, int32(19723), got)

	for _, text := range []string{"19723abc", "1.5", "NaN"} {
		_, err := StrToParquetType(text, &int32T, dateCT, 0, 0)
		require.ErrorContains(t, err, "parse DATE", text)

		// The DATE logical type shares the fallback.
		_, err = StrToParquetTypeWithLogical(text, &int32T, nil, dateLT, 0, 0)
		require.ErrorContains(t, err, "parse DATE", text)
	}
}

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
				got, err := StrToParquetTypeWithLogical(value, &byteArray, nil, lT, 0, 0)
				require.NoError(t, err, "%s", value)
				require.Equal(t, value, got, "%s", value)
			}
		})
	}
}

// TestStrToParquetTypeWithLogical_UnsupportedTextForms checks that annotations whose text
// form the write path cannot parse are refused rather than stored as their own text, which
// is the silent corruption in #418.
func TestStrToParquetTypeWithLogical_UnsupportedTextForms(t *testing.T) {
	tests := []struct {
		name string
		str  string
		pT   parquet.Type
		cT   *parquet.ConvertedType
		lT   *parquet.LogicalType
	}{
		{
			name: "GEOMETRY",
			str:  "POINT (1 2)",
			pT:   parquet.Type_BYTE_ARRAY,
			lT:   &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()},
		},
		{
			name: "GEOGRAPHY",
			str:  "POINT (1 2)",
			pT:   parquet.Type_BYTE_ARRAY,
			lT:   &parquet.LogicalType{GEOGRAPHY: parquet.NewGeographyType()},
		},
		{
			name: "BSON logical type",
			str:  `{"a": 1}`,
			pT:   parquet.Type_BYTE_ARRAY,
			lT:   &parquet.LogicalType{BSON: parquet.NewBsonType()},
		},
		{
			name: "BSON converted type",
			str:  `{"a": 1}`,
			pT:   parquet.Type_BYTE_ARRAY,
			cT:   parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := StrToParquetTypeWithLogical(tt.str, &tt.pT, tt.cT, tt.lT, 0, 0)
			require.Error(t, err)
			require.Contains(t, err.Error(), "not supported yet")
			require.Contains(t, err.Error(), tt.name[:4])
		})
	}
}

// TestStrToParquetType_NoBase64Sniffing pins criterion 4 of #421: an unannotated
// byte-backed column reads its input as base64, and text that is not base64 is an
// error rather than a silently stored byte string.
func TestStrToParquetType_NoBase64Sniffing(t *testing.T) {
	byteArray := parquet.Type_BYTE_ARRAY
	flba := parquet.Type_FIXED_LEN_BYTE_ARRAY

	t.Run("BYTE_ARRAY decodes base64", func(t *testing.T) {
		got, err := StrToParquetType("aGVsbG8=", &byteArray, nil, 0, 0)
		require.NoError(t, err)
		require.Equal(t, "hello", got)
	})

	t.Run("BYTE_ARRAY rejects non-base64", func(t *testing.T) {
		_, err := StrToParquetType("hello", &byteArray, nil, 0, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not valid base64")
	})

	t.Run("FIXED_LEN_BYTE_ARRAY no longer falls back to the literal string", func(t *testing.T) {
		_, err := StrToParquetType("abcd", &flba, nil, 4, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "column length is 4")
	})
}

// TestJSONStringColumnsRejectOtherShapes covers a column whose text is taken at face
// value. A non-string used to be rendered to text and read as one, so JSON true was
// stored as the three bytes "true" decodes to.
func TestJSONStringColumnsRejectOtherShapes(t *testing.T) {
	byteArray := parquet.Type_BYTE_ARRAY
	utf8CT := parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)

	columns := map[string]*parquet.ConvertedType{"unannotated": nil, "UTF8": utf8CT}
	values := map[string]any{
		"boolean":     true,
		"json.Number": json.Number("1234"),
		"float":       1234.0,
	}

	for colName, cT := range columns {
		for valName, value := range values {
			t.Run(colName+"/"+valName, func(t *testing.T) {
				_, err := JSONTypeToParquetTypeWithLogical(
					reflect.ValueOf(value), &byteArray, cT, nil, 0, 0)
				require.ErrorContains(t, err, "takes a JSON string")
			})
		}
	}

	// json.Number is a string to Go, so the message says what it really is.
	_, err := JSONTypeToParquetTypeWithLogical(
		reflect.ValueOf(json.Number("1234")), &byteArray, nil, nil, 0, 0)
	require.ErrorContains(t, err, "got number")

	// INT96 is not in that set: it reads its timestamp form and falls back to the bare
	// 96-bit integer the column stores, so a number is a value it holds, and the CSV path
	// always took that integer. Note what the fallback means: the low eight bytes are
	// nanoseconds within the day and the high four the Julian day, so 12345 is 12,345
	// nanoseconds on Julian day 0, not day 12,345.
	int96 := parquet.Type_INT96
	fromNumber, err := JSONTypeToParquetTypeWithLogical(
		reflect.ValueOf(json.Number("12345")), &int96, nil, nil, 0, 0)
	require.NoError(t, err)
	fromText, err := StrToParquetTypeWithLogical("12345", &int96, nil, nil, 0, 0)
	require.NoError(t, err)
	require.Equal(t, fromText, fromNumber)

	// A real JSON string still lands, base64-decoded or verbatim as the column asks.
	got, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf("aGk="), &byteArray, nil, nil, 0, 0)
	require.NoError(t, err)
	require.Equal(t, "hi", got)

	got, err = JSONTypeToParquetTypeWithLogical(reflect.ValueOf("aGk="), &byteArray, utf8CT, nil, 0, 0)
	require.NoError(t, err)
	require.Equal(t, "aGk=", got)
}
