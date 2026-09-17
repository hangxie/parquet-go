package types

import (
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
