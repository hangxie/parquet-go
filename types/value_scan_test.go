package types

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// TestStrToParquetTypeWithLogical_RawMode pins the raw representation of each column
// kind: byte-backed values arrive as base64, number-backed ones as the physical number,
// and columns annotated as text stay verbatim.
func TestStrToParquetTypeWithLogical_RawMode(t *testing.T) {
	b64 := base64.StdEncoding.EncodeToString

	tests := []struct {
		name     string
		str      string
		pT       parquet.Type
		cT       *parquet.ConvertedType
		lT       *parquet.LogicalType
		length   int
		expected any
		errMsg   string
	}{
		{
			name:     "UUID as base64",
			str:      b64([]byte("0123456789abcdef")),
			pT:       parquet.Type_FIXED_LEN_BYTE_ARRAY,
			lT:       &parquet.LogicalType{UUID: parquet.NewUUIDType()},
			length:   common.UUIDByteLen,
			expected: "0123456789abcdef",
		},
		{
			name:   "UUID text is refused in raw mode",
			str:    "550e8400-e29b-41d4-a716-446655440000",
			pT:     parquet.Type_FIXED_LEN_BYTE_ARRAY,
			lT:     &parquet.LogicalType{UUID: parquet.NewUUIDType()},
			length: common.UUIDByteLen,
			errMsg: "not valid base64",
		},
		{
			name:     "FLOAT16 as base64",
			str:      b64([]byte{0x00, 0x49}),
			pT:       parquet.Type_FIXED_LEN_BYTE_ARRAY,
			lT:       &parquet.LogicalType{FLOAT16: parquet.NewFloat16Type()},
			length:   common.Float16ByteLen,
			expected: string([]byte{0x00, 0x49}),
		},
		{
			name:     "INTERVAL as base64",
			str:      b64(make([]byte, common.IntervalByteLen)),
			pT:       parquet.Type_FIXED_LEN_BYTE_ARRAY,
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
			length:   common.IntervalByteLen,
			expected: string(make([]byte, common.IntervalByteLen)),
		},
		{
			name:     "INT96 as base64",
			str:      b64(make([]byte, 12)),
			pT:       parquet.Type_INT96,
			expected: string(make([]byte, 12)),
		},
		{
			name:   "INT96 timestamp text is refused in raw mode",
			str:    "2023-01-01T00:00:00Z",
			pT:     parquet.Type_INT96,
			errMsg: "not valid base64",
		},
		{
			name:     "DATE as day count",
			str:      "19723",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
			expected: int32(19723),
		},
		{
			name:   "DATE text is refused in raw mode",
			str:    "2023-12-25",
			pT:     parquet.Type_INT32,
			cT:     parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
			errMsg: "parse INT32",
		},
		{
			name:     "TIME_MILLIS as tick count",
			str:      "43200000",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			expected: int32(43200000),
		},
		{
			name:     "BSON as base64",
			str:      b64([]byte{0x05, 0x00, 0x00, 0x00, 0x00}),
			pT:       parquet.Type_BYTE_ARRAY,
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			expected: string([]byte{0x05, 0x00, 0x00, 0x00, 0x00}),
		},
		{
			name:     "GEOMETRY as base64 WKB",
			str:      b64([]byte{0x01, 0x02}),
			pT:       parquet.Type_BYTE_ARRAY,
			lT:       &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()},
			expected: string([]byte{0x01, 0x02}),
		},
		{
			name:     "ENUM stays text",
			str:      "TEST",
			pT:       parquet.Type_BYTE_ARRAY,
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM),
			expected: "TEST",
		},
		{
			name:     "UTF8 stays text",
			str:      "hello",
			pT:       parquet.Type_BYTE_ARRAY,
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			expected: "hello",
		},
		{
			name:     "STRING logical type stays text",
			str:      "hello",
			pT:       parquet.Type_BYTE_ARRAY,
			lT:       &parquet.LogicalType{STRING: parquet.NewStringType()},
			expected: "hello",
		},
		{
			name:     "unannotated BYTE_ARRAY as base64",
			str:      b64([]byte{0xff, 0x00}),
			pT:       parquet.Type_BYTE_ARRAY,
			expected: string([]byte{0xff, 0x00}),
		},
		{
			name:   "FIXED_LEN_BYTE_ARRAY length is checked after decoding",
			str:    b64([]byte{0x01, 0x02}),
			pT:     parquet.Type_FIXED_LEN_BYTE_ARRAY,
			length: 4,
			errMsg: "column length is 4",
		},
		{
			name:     "UINT_64 carries the physical int64",
			str:      "-1",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64),
			expected: int64(-1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StrToParquetTypeWithLogical(tt.str, &tt.pT, tt.cT, tt.lT, tt.length, 0, WithValueMode(ValueModeRaw))
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, got)
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
		for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw} {
			t.Run(tt.name+" "+mode.String(), func(t *testing.T) {
				got, err := JSONTypeToParquetTypeWithLogical(
					reflect.ValueOf(tt.value), &tt.pT, nil, nil, 0, 0, WithValueMode(mode),
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
				// Both modes apply the annotation where they can. A narrow annotation
				// rejects values its physical INT32 would hold, so raw mode checks it
				// too; the 32- and 64-bit unsigned widths carry the upper half of their
				// range as a negative physical value and are excluded by maxWidth.
				for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw} {
					assertAnnotatedInteger(t, col, cT, lT, mode, spelling)
				}
			}
		})
	}
}

// annotatedIntegerRejections are values no integer annotation narrower than maxWidth holds,
// in the text and number forms the same document can arrive as.
var annotatedIntegerRejections = []struct {
	name     string
	text     string
	num      any
	maxWidth int
}{
	{name: "fractional", text: "1.5", num: float64(1.5)},
	{name: "trailing characters", text: "123abc"},
	{name: "past the declared width", text: "100000", num: int64(100000), maxWidth: 16},
	{name: "non-finite", text: "NaN", num: math.NaN()},
}

// assertAnnotatedInteger checks one column, one spelling of its annotation and one mode:
// every rejected value is reported in both forms, and a value the column holds reads the
// same whichever form it arrived as.
func assertAnnotatedInteger(t *testing.T, col annotatedIntegerCase, cT *parquet.ConvertedType, lT *parquet.LogicalType, mode ValueMode, spelling string) {
	t.Helper()
	opt := WithValueMode(mode)

	for _, tc := range annotatedIntegerRejections {
		if tc.maxWidth != 0 && col.width > tc.maxWidth {
			continue
		}
		_, err := StrToParquetTypeWithLogical(tc.text, &col.pT, cT, lT, 0, 0, opt)
		require.Error(t, err, "%s text %q as %s in %s mode", col.name, tc.text, spelling, mode)

		if tc.num == nil {
			continue
		}
		_, err = JSONTypeToParquetTypeWithLogical(reflect.ValueOf(tc.num), &col.pT, cT, lT, 0, 0, opt)
		require.Error(t, err, "%s number %v as %s in %s mode", col.name, tc.num, spelling, mode)
	}

	fromText, err := StrToParquetTypeWithLogical("7", &col.pT, cT, lT, 0, 0, opt)
	require.NoError(t, err)
	fromNumber, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(int64(7)), &col.pT, cT, lT, 0, 0, opt)
	require.NoError(t, err)
	require.Equal(t, fromText, fromNumber, "%s as %s in %s mode", col.name, spelling, mode)
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

// TestStrToParquetTypeWithLogicalRefusesWKT keeps #418's damaging case refused. WKT is
// not a form any mode renders, so it is not a form the write path takes; up to v3.8.3 it
// was stored as the eleven bytes of its own text, claiming to be WKB.
func TestStrToParquetTypeWithLogicalRefusesWKT(t *testing.T) {
	pT := parquet.Type_BYTE_ARRAY
	for name, lT := range map[string]*parquet.LogicalType{
		"GEOMETRY":  {GEOMETRY: parquet.NewGeometryType()},
		"GEOGRAPHY": {GEOGRAPHY: parquet.NewGeographyType()},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := StrToParquetTypeWithLogical("POINT (1 2)", &pT, nil, lT, 0, 0)
			require.ErrorContains(t, err, name)
			require.ErrorContains(t, err, "JSON text of its rendering")
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

// TestRawModeSchemaErrors covers the raw path's two refusals that are about the column
// rather than the value: a schema with no physical type, and a text column handed
// something other than text.
func TestRawModeSchemaErrors(t *testing.T) {
	raw := WithValueMode(ValueModeRaw)

	t.Run("a text column takes only a string", func(t *testing.T) {
		_, err := JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf(42), parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), nil, 0, 0, raw,
		)
		require.ErrorContains(t, err, "takes a JSON string")
	})
}

func TestJSONTypeToParquetTypeWithLogical_RawMode(t *testing.T) {
	byteArray := parquet.Type_BYTE_ARRAY
	enum := parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM)

	t.Run("base64 string decodes for an unannotated column", func(t *testing.T) {
		got, err := JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf("//8="), &byteArray, nil, nil, 0, 0, WithValueMode(ValueModeRaw),
		)
		require.NoError(t, err)
		require.Equal(t, string([]byte{0xff, 0xff}), got)
	})

	t.Run("ENUM keeps its text", func(t *testing.T) {
		got, err := JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf("TEST"), &byteArray, enum, nil, 0, 0, WithValueMode(ValueModeRaw),
		)
		require.NoError(t, err)
		require.Equal(t, "TEST", got)
	})

	t.Run("a Go number converts without the string scanner", func(t *testing.T) {
		got, err := JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf(int64(42)), parquet.TypePtr(parquet.Type_INT32), nil, nil, 0, 0,
			WithValueMode(ValueModeRaw),
		)
		require.NoError(t, err)
		require.Equal(t, int32(42), got)
	})

	t.Run("a nil interface stays nil", func(t *testing.T) {
		var v any
		got, err := JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf(&v).Elem(), &byteArray, nil, nil, 0, 0, WithValueMode(ValueModeRaw),
		)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("GEOMETRY is refused in interpreted mode", func(t *testing.T) {
		lT := &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()}
		_, err := JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf("POINT (1 2)"), &byteArray, nil, lT, 0, 0,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "GEOMETRY")
	})
}

// BenchmarkStrToParquetTypeWithLogical pins the cost of resolving the value options, paid
// once per value. The default of no options must not allocate.
func BenchmarkStrToParquetTypeWithLogical(b *testing.B) {
	pT := parquet.Type_INT32
	b.Run("no options", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = StrToParquetTypeWithLogical("42", &pT, nil, nil, 0, 0)
		}
	})
	opts := []ValueOption{WithValueMode(ValueModeRaw)}
	b.Run("raw mode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = StrToParquetTypeWithLogical("42", &pT, nil, nil, 0, 0, opts...)
		}
	})
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
			for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw} {
				t.Run(colName+"/"+valName+"/"+mode.String(), func(t *testing.T) {
					_, err := JSONTypeToParquetTypeWithLogical(
						reflect.ValueOf(value), &byteArray, cT, nil, 0, 0, WithValueMode(mode),
					)
					require.ErrorContains(t, err, "takes a JSON string")
				})
			}
		}
	}

	// INT96 is not in that set. Interpreted mode reads its timestamp form and falls back
	// to the bare 96-bit integer the column stores, so a number is a value it holds; only
	// raw mode carries it as base64. The CSV path always took that integer, and the two
	// must agree. Note what the fallback means: the low eight bytes are nanoseconds
	// within the day and the high four the Julian day, so 12345 is 12,345 nanoseconds on
	// Julian day 0, not day 12,345.
	int96 := parquet.Type_INT96
	fromNumber, err := JSONTypeToParquetTypeWithLogical(
		reflect.ValueOf(json.Number("12345")), &int96, nil, nil, 0, 0,
	)
	require.NoError(t, err)
	fromText, err := StrToParquetTypeWithLogical("12345", &int96, nil, nil, 0, 0)
	require.NoError(t, err)
	require.Equal(t, fromText, fromNumber)

	_, err = JSONTypeToParquetTypeWithLogical(
		reflect.ValueOf(json.Number("12345")), &int96, nil, nil, 0, 0, WithValueMode(ValueModeRaw),
	)
	require.ErrorContains(t, err, "takes a JSON string")

	// json.Number is a string to Go, so the message says what it really is.
	_, err = JSONTypeToParquetTypeWithLogical(
		reflect.ValueOf(json.Number("1234")), &byteArray, nil, nil, 0, 0,
	)
	require.ErrorContains(t, err, "got number")

	// A real JSON string still lands, base64-decoded or verbatim as the column asks.
	for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw} {
		got, err := JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf("aGk="), &byteArray, nil, nil, 0, 0, WithValueMode(mode),
		)
		require.NoError(t, err)
		require.Equal(t, "hi", got)

		got, err = JSONTypeToParquetTypeWithLogical(
			reflect.ValueOf("aGk="), &byteArray, utf8CT, nil, 0, 0, WithValueMode(mode),
		)
		require.NoError(t, err)
		require.Equal(t, "aGk=", got)
	}
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
