package types

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
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

// readSE builds a schema element for the read path; length is set only when non-zero so an
// unannotated BYTE_ARRAY keeps an unset TypeLength.
func readSE(pT parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length int32) *parquet.SchemaElement {
	e := &parquet.SchemaElement{Type: &pT, ConvertedType: cT, LogicalType: lT}
	if length > 0 {
		e.TypeLength = &length
	}
	return e
}

// TestConvertValueReportsWhatItCannotRender is the reason ConvertValue exists.
// ConvertToJSONType returns a value for anything, so a column holding bytes it cannot
// render came back as a substitute: malformed BSON as base64, unparseable WKB as a
// wkb_hex map, a mis-sized UUID or FLOAT16 as the raw bytes. A caller could not tell
// those from a value that really rendered.
func TestConvertValueReportsWhatItCannotRender(t *testing.T) {
	ct := parquet.ConvertedTypePtr
	tests := []struct {
		name string
		val  any
		se   *parquet.SchemaElement
		opts []ValueOption
		// substitute is what the deprecated ConvertToJSONType still returns.
		substitute any
		errMsg     string
	}{
		{
			name: "BSON that does not parse", val: "not-bson",
			se:         readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_BSON), nil, 0),
			substitute: "bm90LWJzb24=", errMsg: "BSON",
		},
		{
			name: "BSON truncated", val: "\x05\x00",
			se:         readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_BSON), nil, 0),
			substitute: "BQA=", errMsg: "BSON",
		},
		{
			// GEOMETRY renders as hex by default, where a wkb_hex map is the rendering
			// asked for rather than a substitute; GeoJSON is the mode that can fail.
			name: "GEOMETRY that is not WKB", val: "not-wkb",
			se:         readSE(parquet.Type_BYTE_ARRAY, nil, &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()}, 0),
			opts:       []ValueOption{WithGeospatialConfig(NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON)))},
			substitute: map[string]any{"wkb_hex": "6e6f742d776b62", "crs": "OGC:CRS84"},
			errMsg:     "GEOMETRY",
		},
		{
			// A geospatial column whose value is not bytes at all cannot render either.
			name: "GEOMETRY that is not bytes", val: 42,
			se:         readSE(parquet.Type_BYTE_ARRAY, nil, &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()}, 0),
			opts:       []ValueOption{WithGeospatialConfig(NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON)))},
			substitute: 42, errMsg: "GEOMETRY",
		},
		{
			name: "GEOGRAPHY that is not WKB", val: "\x01\x99",
			se:         readSE(parquet.Type_BYTE_ARRAY, nil, &parquet.LogicalType{GEOGRAPHY: parquet.NewGeographyType()}, 0),
			substitute: map[string]any{"wkb_hex": "0199", "crs": "OGC:CRS84", "algorithm": "SPHERICAL"},
			errMsg:     "GEOGRAPHY",
		},
		{
			name: "UUID of the wrong width", val: "short",
			se:         readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, &parquet.LogicalType{UUID: parquet.NewUUIDType()}, common.UUIDByteLen),
			substitute: "short", errMsg: "UUID",
		},
		{
			name: "FLOAT16 of the wrong width", val: "x",
			se:         readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, &parquet.LogicalType{FLOAT16: parquet.NewFloat16Type()}, common.Float16ByteLen),
			substitute: "x", errMsg: "FLOAT16",
		},
		{
			name: "INTERVAL of the wrong width", val: "abc",
			se:         readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, ct(parquet.ConvertedType_INTERVAL), nil, common.IntervalByteLen),
			substitute: "abc", errMsg: "INTERVAL",
		},
		{
			name: "INT96 that is too short", val: "abc",
			se:         readSE(parquet.Type_INT96, nil, nil, 0),
			substitute: "abc", errMsg: "INT96",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ConvertValue(tt.val, tt.se, tt.opts...)
			require.ErrorContains(t, err, tt.errMsg)

			// The deprecated wrapper keeps every substitution, so no caller changes.
			//nolint:staticcheck // exercising the deprecated path is the point
			require.Equal(t, tt.substitute, ConvertToJSONType(tt.val, tt.se, tt.opts...))
		})
	}
}

// TestConvertValueRendersWhatItCan pins that the strict path is only strict about failure:
// every value the column really holds renders exactly as it always did.
func TestConvertValueRendersWhatItCan(t *testing.T) {
	ct := parquet.ConvertedTypePtr
	bsonDoc := "\x05\x00\x00\x00\x00"
	tests := []struct {
		name string
		val  any
		se   *parquet.SchemaElement
	}{
		{"BSON", bsonDoc, readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_BSON), nil, 0)},
		{"UUID", string(make([]byte, common.UUIDByteLen)), readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, &parquet.LogicalType{UUID: parquet.NewUUIDType()}, common.UUIDByteLen)},
		{"FLOAT16", "\x00\x3c", readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, &parquet.LogicalType{FLOAT16: parquet.NewFloat16Type()}, common.Float16ByteLen)},
		{"INTERVAL", string(make([]byte, common.IntervalByteLen)), readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, ct(parquet.ConvertedType_INTERVAL), nil, common.IntervalByteLen)},
		{"INT96", string(make([]byte, 12)), readSE(parquet.Type_INT96, nil, nil, 0)},
		{"DATE", int32(19723), readSE(parquet.Type_INT32, ct(parquet.ConvertedType_DATE), nil, 0)},
		{"UTF8", "hello", readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_UTF8), nil, 0)},
		{"unannotated BYTE_ARRAY", "\x00\xff", readSE(parquet.Type_BYTE_ARRAY, nil, nil, 0)},
		{"INT32", int32(42), readSE(parquet.Type_INT32, nil, nil, 0)},
		{"nil", nil, readSE(parquet.Type_INT32, nil, nil, 0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertValue(tt.val, tt.se)
			require.NoError(t, err)
			//nolint:staticcheck // the wrapper must agree wherever the strict path succeeds
			require.Equal(t, ConvertToJSONType(tt.val, tt.se), got)
		})
	}
}

// TestConvertValueEmptyBSON covers a BSON column holding no bytes. bson.Unmarshal rejects
// that as EOF, so it is not a valid empty document and the strict path says so; the empty
// map stays as the value the deprecated path returns.
func TestConvertValueEmptyBSON(t *testing.T) {
	se := readSE(parquet.Type_BYTE_ARRAY, parquet.ConvertedTypePtr(parquet.ConvertedType_BSON), nil, 0)

	got, err := ConvertValue("", se)
	require.ErrorContains(t, err, "document is empty")
	require.Equal(t, map[string]any{}, got)

	//nolint:staticcheck // the substitution must survive
	require.Equal(t, map[string]any{}, ConvertToJSONType("", se))
}

// TestConvertValueGeospatialNonBytes covers a geospatial column whose value is not bytes at
// all. Hex and base64 render any bytes, but a value that has none cannot be rendered in any
// mode, so every mode reports it rather than passing the value through.
func TestConvertValueGeospatialNonBytes(t *testing.T) {
	modes := map[string]GeospatialJSONMode{
		"hex": GeospatialModeHex, "base64": GeospatialModeBase64,
		"geojson": GeospatialModeGeoJSON, "hybrid": GeospatialModeHybrid,
	}
	geometry := readSE(parquet.Type_BYTE_ARRAY, nil, &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()}, 0)
	geography := readSE(parquet.Type_BYTE_ARRAY, nil, &parquet.LogicalType{GEOGRAPHY: parquet.NewGeographyType()}, 0)

	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			_, err := ConvertValue(42, geometry, WithGeospatialConfig(NewGeospatialConfig(WithGeometryJSONMode(mode))))
			require.ErrorContains(t, err, "GEOMETRY")
			_, err = ConvertValue(42, geography, WithGeospatialConfig(NewGeospatialConfig(WithGeographyJSONMode(mode))))
			require.ErrorContains(t, err, "GEOGRAPHY")
		})
	}

	// Real bytes that are not WKB still render in the modes that do not read them.
	for _, mode := range []GeospatialJSONMode{GeospatialModeHex, GeospatialModeBase64} {
		_, err := ConvertValue("not-wkb", geometry, WithGeospatialConfig(NewGeospatialConfig(WithGeometryJSONMode(mode))))
		require.NoError(t, err)
	}
}

// TestConvertValueReturnsSubstituteWithError pins the contract the reader depends on: the
// value returned alongside an error is the substitute ConvertToJSONType produces, not nil.
// Go convention would let a maintainer treat it as meaningless and return nil, which would
// quietly change what the reader hands back for every unrenderable value.
func TestConvertValueReturnsSubstituteWithError(t *testing.T) {
	ct := parquet.ConvertedTypePtr
	cases := []struct {
		name string
		val  any
		se   *parquet.SchemaElement
	}{
		{"BSON", "not-bson", readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_BSON), nil, 0)},
		{"UUID", "short", readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, &parquet.LogicalType{UUID: parquet.NewUUIDType()}, common.UUIDByteLen)},
		{"INT96", "abc", readSE(parquet.Type_INT96, nil, nil, 0)},
		{"GEOGRAPHY", "\x01\x99", readSE(parquet.Type_BYTE_ARRAY, nil, &parquet.LogicalType{GEOGRAPHY: parquet.NewGeographyType()}, 0)},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertValue(tt.val, tt.se)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrUnrenderable)
			//nolint:staticcheck // the value half must equal what the deprecated path returns
			require.Equal(t, ConvertToJSONType(tt.val, tt.se), got)
		})
	}
}

// TestConvertValueNilSchemaElement covers a call with nothing to render against. Raw mode
// makes the point plainest: without a schema element there is no way to know whether the
// value stays text or becomes base64.
func TestConvertValueNilSchemaElement(t *testing.T) {
	got, err := ConvertValue("hello", nil, WithValueMode(ValueModeRaw))
	require.ErrorContains(t, err, "without a schema element")
	require.Equal(t, "hello", got)

	// A nil value needs no schema to render as nil.
	got, err = ConvertValue(nil, nil)
	require.NoError(t, err)
	require.Nil(t, got)

	//nolint:staticcheck // the deprecated path keeps passing the value through
	require.Equal(t, "hello", ConvertToJSONType("hello", nil))
}

// TestConvertValueINT96Width covers an INT96 that is not exactly 12 bytes. INT96ToTime only
// rejects what is too short, so a longer value was rendering as the timestamp its first 12
// bytes spell. Either byte form is accepted, as for every other byte-backed column.
func TestConvertValueINT96Width(t *testing.T) {
	se := readSE(parquet.Type_INT96, nil, nil, 0)

	for _, val := range []any{string(make([]byte, 11)), string(make([]byte, 20))} {
		_, err := ConvertValue(val, se)
		require.ErrorIs(t, err, ErrUnrenderable)
		require.ErrorContains(t, err, "must be 12")
	}

	fromString, err := ConvertValue(string(make([]byte, 12)), se)
	require.NoError(t, err)
	fromBytes, err := ConvertValue(make([]byte, 12), se)
	require.NoError(t, err)
	require.Equal(t, fromString, fromBytes)
}

// TestConvertValueRawMode covers the read side of the value mode. Raw renders the physical
// value the column stores, so that what the reader emits is what the writer takes back in
// the same mode: base64 for a byte-backed column, the underlying number otherwise, and
// text verbatim where the annotation says the column holds text.
func TestConvertValueRawMode(t *testing.T) {
	ct := parquet.ConvertedTypePtr
	raw := WithValueMode(ValueModeRaw)

	tests := []struct {
		name string
		val  any
		se   *parquet.SchemaElement
		want any
	}{
		{
			"UUID", string(make([]byte, common.UUIDByteLen)),
			readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, &parquet.LogicalType{UUID: parquet.NewUUIDType()}, common.UUIDByteLen),
			"AAAAAAAAAAAAAAAAAAAAAA==",
		},
		{
			"FLOAT16", "\x00\x3c",
			readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, &parquet.LogicalType{FLOAT16: parquet.NewFloat16Type()}, common.Float16ByteLen),
			"ADw=",
		},
		{
			"INTERVAL", string(make([]byte, common.IntervalByteLen)),
			readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, ct(parquet.ConvertedType_INTERVAL), nil, common.IntervalByteLen),
			"AAAAAAAAAAAAAAAA",
		},
		{"INT96", string(make([]byte, 12)), readSE(parquet.Type_INT96, nil, nil, 0), "AAAAAAAAAAAAAAAA"},
		{"BSON", "\x05\x00\x00\x00\x00", readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_BSON), nil, 0), "BQAAAAA="},
		{"GEOMETRY", "\x01\x02", readSE(parquet.Type_BYTE_ARRAY, nil, &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()}, 0), "AQI="},
		{"unannotated BYTE_ARRAY", "\x00\xff", readSE(parquet.Type_BYTE_ARRAY, nil, nil, 0), "AP8="},
		{"UTF8 stays text", "hello", readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_UTF8), nil, 0), "hello"},
		{"ENUM stays text", "TEST", readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_ENUM), nil, 0), "TEST"},
		{"DATE as its day count", int32(19723), readSE(parquet.Type_INT32, ct(parquet.ConvertedType_DATE), nil, 0), int32(19723)},
		{
			"TIMESTAMP as its tick count", int64(1703462400000),
			readSE(parquet.Type_INT64, ct(parquet.ConvertedType_TIMESTAMP_MILLIS), nil, 0), int64(1703462400000),
		},
		{
			"UINT_64 as the physical int64", int64(-1),
			readSE(parquet.Type_INT64, ct(parquet.ConvertedType_UINT_64), nil, 0), int64(-1),
		},
		{"BOOLEAN", true, readSE(parquet.Type_BOOLEAN, nil, nil, 0), true},
		{"DOUBLE", 1.5, readSE(parquet.Type_DOUBLE, nil, nil, 0), 1.5},
		{"FLOAT", float32(1.5), readSE(parquet.Type_FLOAT, nil, nil, 0), float32(1.5)},
		// NaN and the infinities have no JSON number form, so raw quotes them the way
		// the interpreted path does; a bare Go NaN is a value json.Marshal refuses.
		{"DOUBLE NaN", math.NaN(), readSE(parquet.Type_DOUBLE, nil, nil, 0), "NaN"},
		{"DOUBLE +Inf", math.Inf(1), readSE(parquet.Type_DOUBLE, nil, nil, 0), "Infinity"},
		{"DOUBLE -Inf", math.Inf(-1), readSE(parquet.Type_DOUBLE, nil, nil, 0), "-Infinity"},
		{"FLOAT NaN", float32(math.NaN()), readSE(parquet.Type_FLOAT, nil, nil, 0), "NaN"},
		{"nil", nil, readSE(parquet.Type_INT32, nil, nil, 0), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertValue(tt.val, tt.se, raw)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestConvertValueRawModeErrors covers the two things raw rendering cannot do: a schema
// with no physical type says nothing about what the column holds, and a byte-backed column
// whose value is not bytes has none to encode.

// TestConvertValueRawModeErrors covers the two things raw rendering cannot do: a schema
// with no physical type says nothing about what the column holds, and a byte-backed column
// whose value is not bytes has none to encode.
func TestConvertValueRawModeErrors(t *testing.T) {
	raw := WithValueMode(ValueModeRaw)

	_, err := ConvertValue("x", &parquet.SchemaElement{}, raw)
	require.ErrorContains(t, err, "without a physical type")

	_, err = ConvertValue(42, readSE(parquet.Type_BYTE_ARRAY, nil, nil, 0), raw)
	require.ErrorIs(t, err, ErrUnrenderable)
	require.ErrorContains(t, err, "not bytes")
}

// TestConvertValueRawWidths covers the widths raw mode checks. A byte-backed column whose
// width the format or the schema fixes is checked against it, as the interpreted path
// checks UUID, FLOAT16 and INTERVAL; base64 of the wrong number of bytes would otherwise
// read back as a value the column cannot hold.

// TestConvertValueRawWidths covers the widths raw mode checks. A byte-backed column whose
// width the format or the schema fixes is checked against it, as the interpreted path
// checks UUID, FLOAT16 and INTERVAL; base64 of the wrong number of bytes would otherwise
// read back as a value the column cannot hold.
func TestConvertValueRawWidths(t *testing.T) {
	raw := WithValueMode(ValueModeRaw)
	ct := parquet.ConvertedTypePtr

	tests := []struct {
		name string
		se   *parquet.SchemaElement
		good int
	}{
		{"FIXED_LEN_BYTE_ARRAY", readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, nil, 8), 8},
		{"INT96", readSE(parquet.Type_INT96, nil, nil, 0), 12},
		{"UUID", readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil,
			&parquet.LogicalType{UUID: parquet.NewUUIDType()}, common.UUIDByteLen), common.UUIDByteLen},
		{"FLOAT16", readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil,
			&parquet.LogicalType{FLOAT16: parquet.NewFloat16Type()}, common.Float16ByteLen), common.Float16ByteLen},
		{"INTERVAL", readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, ct(parquet.ConvertedType_INTERVAL),
			nil, common.IntervalByteLen), common.IntervalByteLen},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ConvertValue(string(make([]byte, tt.good)), tt.se, raw)
			require.NoError(t, err)

			for _, n := range []int{tt.good - 1, tt.good + 1} {
				_, err := ConvertValue(string(make([]byte, n)), tt.se, raw)
				require.ErrorIs(t, err, ErrUnrenderable, "%d bytes", n)
				require.ErrorContains(t, err, "must be", "%d bytes", n)
			}
		})
	}

	// An unannotated BYTE_ARRAY has no fixed width, so any length renders.
	for _, n := range []int{0, 1, 100} {
		_, err := ConvertValue(string(make([]byte, n)), readSE(parquet.Type_BYTE_ARRAY, nil, nil, 0), raw)
		require.NoError(t, err, "%d bytes", n)
	}

	// Only raw mode reports a width the schema fixes but the annotation does not: it
	// promises the value writes back as it was read, and the raw write path rejects
	// base64 that decodes to the wrong width. Interpreted mode promises no such thing and
	// renders the same value.
	for _, se := range []*parquet.SchemaElement{
		readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, nil, 8),
		readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, ct(parquet.ConvertedType_DECIMAL), nil, 8),
	} {
		_, err := ConvertValue("short", se, raw)
		require.ErrorIs(t, err, ErrUnrenderable)

		_, err = ConvertValue("short", se)
		require.NoError(t, err)
	}
}

// TestRawModeRoundTrip is the symmetry the mode exists for: what the read path renders in
// raw mode is what the write path takes back, for every column kind, including the ones
// with no interpreted write form at all.

// TestRawModeRoundTrip is the symmetry the mode exists for: what the read path renders in
// raw mode is what the write path takes back, for every column kind, including the ones
// with no interpreted write form at all.
func TestRawModeRoundTrip(t *testing.T) {
	ct := parquet.ConvertedTypePtr
	raw := WithValueMode(ValueModeRaw)

	tests := []struct {
		name   string
		values []any
		se     *parquet.SchemaElement
	}{
		{"unannotated BYTE_ARRAY", []any{"", "\x00\xff", "hello"}, readSE(parquet.Type_BYTE_ARRAY, nil, nil, 0)},
		{"UTF8", []any{"hello", "TEST"}, readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_UTF8), nil, 0)},
		{"ENUM", []any{"ACTIVE", "AAAA"}, readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_ENUM), nil, 0)},
		{
			"UUID",
			[]any{string(make([]byte, common.UUIDByteLen))},
			readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, &parquet.LogicalType{UUID: parquet.NewUUIDType()}, common.UUIDByteLen),
		},
		{
			"FLOAT16",
			[]any{"\x00\x3c", "\xff\x7b"},
			readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, &parquet.LogicalType{FLOAT16: parquet.NewFloat16Type()}, common.Float16ByteLen),
		},
		{
			"INTERVAL",
			[]any{string(make([]byte, common.IntervalByteLen))},
			readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, ct(parquet.ConvertedType_INTERVAL), nil, common.IntervalByteLen),
		},
		{"INT96", []any{string(make([]byte, 12))}, readSE(parquet.Type_INT96, nil, nil, 0)},
		{"BSON", []any{"\x05\x00\x00\x00\x00"}, readSE(parquet.Type_BYTE_ARRAY, ct(parquet.ConvertedType_BSON), nil, 0)},
		{"GEOMETRY", []any{"\x01\x02\x03"}, readSE(parquet.Type_BYTE_ARRAY, nil, &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()}, 0)},
		{"DATE", []any{int32(0), int32(-1), int32(19723)}, readSE(parquet.Type_INT32, ct(parquet.ConvertedType_DATE), nil, 0)},
		{
			"TIMESTAMP_MILLIS",
			[]any{int64(0), int64(-1), int64(1703462400000)},
			readSE(parquet.Type_INT64, ct(parquet.ConvertedType_TIMESTAMP_MILLIS), nil, 0),
		},
		{"INT32", []any{int32(0), int32(-2147483648)}, readSE(parquet.Type_INT32, nil, nil, 0)},
		{"BOOLEAN", []any{true, false}, readSE(parquet.Type_BOOLEAN, nil, nil, 0)},
		{"DOUBLE", []any{0.0, -1.5, math.MaxFloat64}, readSE(parquet.Type_DOUBLE, nil, nil, 0)},
		{"FLOAT", []any{float32(0), float32(-1.5)}, readSE(parquet.Type_FLOAT, nil, nil, 0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, want := range tt.values {
				rendered, err := ConvertValue(want, tt.se, raw)
				require.NoError(t, err, "%#v", want)

				// Whatever raw renders has to be encodable; a bare NaN is not.
				_, jsonErr := json.Marshal(rendered)
				require.NoError(t, jsonErr, "%#v renders as %#v", want, rendered)

				text, ok := rendered.(string)
				if !ok {
					text = fmt.Sprintf("%v", rendered)
				}
				got, err := StrToParquetTypeWithLogical(text, tt.se.Type, tt.se.ConvertedType,
					tt.se.LogicalType, int(tt.se.GetTypeLength()), int(tt.se.GetScale()), raw)
				require.NoError(t, err, "%#v rendered as %q", want, text)
				require.Equal(t, want, got, "rendered as %q", text)
			}
		})
	}
}

// TestConvertValueRejectsUnsupportedMode pins that the read path refuses a mode outside the
// two defined ones, as the write helpers do, rather than quietly rendering interpreted.

// TestConvertValueRejectsUnsupportedMode pins that the read path refuses a mode outside the
// two defined ones, as the write helpers do, rather than quietly rendering interpreted.
func TestConvertValueRejectsUnsupportedMode(t *testing.T) {
	se := readSE(parquet.Type_BYTE_ARRAY, parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), nil, 0)
	_, err := ConvertValue("x", se, WithValueMode(ValueMode(99)))
	require.ErrorIs(t, err, ErrUnsupportedValueMode)
	require.ErrorContains(t, err, "unsupported value mode 99")
}

// TestConvertToJSONTypeIgnoresMode pins the deprecated wrapper's contract: it renders what
// it always rendered. Forwarding the mode would have changed its output for a caller that
// passed one while the doc said it was not honoured, which is the opposite of what a
// compatibility wrapper is for.

// TestConvertToJSONTypeIgnoresMode pins the deprecated wrapper's contract: it renders what
// it always rendered. Forwarding the mode would have changed its output for a caller that
// passed one while the doc said it was not honoured, which is the opposite of what a
// compatibility wrapper is for.
func TestConvertToJSONTypeIgnoresMode(t *testing.T) {
	uuidSE := readSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil,
		&parquet.LogicalType{UUID: parquet.NewUUIDType()}, common.UUIDByteLen)
	val := string(make([]byte, common.UUIDByteLen))

	for _, mode := range []ValueMode{ValueModeInterpreted, ValueModeRaw, ValueMode(99)} {
		//nolint:staticcheck // the deprecated path's stability is the point
		got := ConvertToJSONType(val, uuidSE, WithValueMode(mode))
		require.Equal(t, "00000000-0000-0000-0000-000000000000", got, "%s", mode)
	}
}

// TestConvertValueEmptyBSON covers a BSON column holding no bytes. bson.Unmarshal rejects
// that as EOF, so it is not a valid empty document and the strict path says so; the empty
// map stays as the value the deprecated path returns.

// TestConvertValueChecksModeBeforeValue pins that an unusable mode is reported even where
// there is nothing to render, since it describes the call rather than the value.
func TestConvertValueChecksModeBeforeValue(t *testing.T) {
	se := readSE(parquet.Type_INT32, nil, nil, 0)
	for _, val := range []any{nil, int32(1)} {
		_, err := ConvertValue(val, se, WithValueMode(ValueMode(99)))
		require.ErrorIs(t, err, ErrUnsupportedValueMode, "%#v", val)
	}
}

// TestConvertValueINT96Width covers an INT96 that is not exactly 12 bytes. INT96ToTime only
// rejects what is too short, so a longer value was rendering as the timestamp its first 12
// bytes spell. Either byte form is accepted, as for every other byte-backed column.
