package types

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// renderedInterval is convertIntervalValue's value half, for use in struct literals.
func renderedInterval(s string) any {
	v, _ := convertIntervalValue(s)
	return v
}

// renderedINT96 is convertINT96Value's value half, for use in struct literals.
func renderedINT96(s string) any {
	v, _ := convertINT96Value(s)
	return v
}

func TestConvertToJSONType_AllConvertedTypes(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		pT        *parquet.Type
		cT        *parquet.ConvertedType
		precision int
		scale     int
		expected  any
	}{
		{
			name:      "int32_decimal_convertedtype",
			value:     int32(12345),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     2,
			expected:  json.Number("123.45"),
		},
		{
			name:      "int64_decimal_convertedtype",
			value:     int64(98765),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     3,
			expected:  json.Number("98.765"),
		},
		{
			name:      "string_decimal_byte_array",
			value:     StrIntToBinary("1234567890", "BigEndian", 0, true),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 12,
			scale:     4,
			expected:  json.Number("123456.7890"),
		},
		{
			name:      "string_decimal_fixed_len_byte_array",
			value:     StrIntToBinary("9876543210", "BigEndian", 12, true),
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 12,
			scale:     2,
			expected:  json.Number("98765432.10"),
		},
		{
			name:      "non_decimal_convertedtype",
			value:     "hello",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			precision: 0,
			scale:     0,
			expected:  "hello",
		},
		{
			name:      "no_convertedtype",
			value:     int32(42),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			precision: 0,
			scale:     0,
			expected:  int32(42),
		},
		{
			name:      "negative_decimal",
			value:     int32(-12345),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     2,
			expected:  json.Number("-123.45"),
		},
		{
			name:      "zero_scale_decimal",
			value:     int32(123),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     0,
			expected:  json.Number("123"),
		},
		{
			name:      "nil_value",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     2,
			expected:  nil,
		},
		{
			name:      "int96_timestamp",
			value:     "\x00\x80\xa7HJ'\x00\x00*\x89%\x00", // INT96 binary data for 2023-01-01T12:00:00Z
			pT:        parquet.TypePtr(parquet.Type_INT96),
			cT:        nil,
			precision: 0,
			scale:     0,
			expected:  "2023-01-01T12:00:00.000000000Z",
		},
		{
			name:      "interval_converted_type",
			value:     string([]byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 221, 109, 0}),
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
			precision: 0,
			scale:     0,
			expected:  "1 day 7200.000 sec", // 1 day + 2 hours = 1 day + 7200 seconds
		},
		{
			name:      "timestamp_millis_converted_type",
			value:     int64(1640995200000), // 2022-01-01T00:00:00Z in milliseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "timestamp_micros_converted_type",
			value:     int64(1640995200000000), // 2022-01-01T00:00:00Z in microseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS),
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000000Z",
		},
		{
			name:      "byte_array_without_converted_type",
			value:     "Hello World!",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			precision: 0,
			scale:     0,
			expected:  base64.StdEncoding.EncodeToString([]byte("Hello World!")), // "SGVsbG8gV29ybGQh"
		},
		{
			name:      "fixed_len_byte_array_without_converted_type",
			value:     []byte{0x01, 0x02, 0x03, 0x04, 0xFF},
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			precision: 0,
			scale:     0,
			expected:  base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03, 0x04, 0xFF}), // "AQIDBP8="
		},
		{
			name:      "byte_array_with_utf8_converted_type",
			value:     "Hello World!",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			precision: 0,
			scale:     0,
			expected:  "Hello World!", // Should remain as string, not base64 encoded
		},
		{
			name:      "time_millis_converted_type",
			value:     int32(45296789), // 12:34:56.789
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789",
		},
		{
			name:      "time_micros_converted_type",
			value:     int64(45296789012), // 12:34:56.789012
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789012",
		},
		{
			name:      "time_millis_zero",
			value:     int32(0),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			precision: 0,
			scale:     0,
			expected:  "00:00:00.000",
		},
		{
			name:      "time_micros_zero",
			value:     int64(0),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
			precision: 0,
			scale:     0,
			expected:  "00:00:00.000000",
		},
		{
			name:      "time_millis_wrong_type",
			value:     "not_an_int",
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			precision: 0,
			scale:     0,
			expected:  "not_an_int", // Should return original value if type assertion fails
		},
		{
			name:      "bson_converted_type_bytes_input",
			value:     []byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			precision: 0,
			scale:     0,
			expected:  "FgAAABBpAAEAAAAA", // Base64 encoded BSON data
		},
		{
			name:      "bson_converted_type_nil_value",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			precision: 0,
			scale:     0,
			expected:  nil,
		},
		// Additional conversion cases
		{
			name:     "int64_conversion",
			value:    int64(9223372036854775807),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
			expected: int64(9223372036854775807),
		},
		{
			name:     "list_conversion",
			value:    []string{"a", "b", "c"},
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "map_conversion",
			value:    map[string]any{"key": "value"},
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_MAP),
			expected: map[string]any{"key": "value"},
		},
		{
			name:     "json_conversion",
			value:    `{"json": "data"}`,
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_JSON),
			expected: `{"json": "data"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se := parquet.NewSchemaElement()
			se.Type = tt.pT
			se.ConvertedType = tt.cT
			if tt.precision != 0 {
				p := int32(tt.precision)
				se.Precision = &p
			}
			if tt.scale != 0 {
				s := int32(tt.scale)
				se.Scale = &s
			}
			result := ConvertToJSONType(tt.value, se)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertToJSONType_AllLogicalTypes(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		pT        *parquet.Type
		cT        *parquet.ConvertedType
		lT        *parquet.LogicalType
		precision int
		scale     int
		expected  any
	}{
		{
			name:      "int32_logicaltype_decimal",
			value:     int32(44444),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createDecimalLogicalType(9, 2),
			precision: 9,
			scale:     2,
			expected:  json.Number("444.44"),
		},
		{
			name:      "int64_logicaltype_decimal",
			value:     int64(-12345),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createDecimalLogicalType(18, 3),
			precision: 18,
			scale:     3,
			expected:  json.Number("-12.345"),
		},
		{
			name:      "convertedtype_fallback",
			value:     int32(12345),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			lT:        nil,
			precision: 5,
			scale:     2,
			expected:  json.Number("123.45"),
		},
		{
			name:      "both_types_prefer_logical",
			value:     int32(98765),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			lT:        createDecimalLogicalType(7, 3),
			precision: 7,
			scale:     3,
			expected:  json.Number("98.765"),
		},
		{
			name:      "non_decimal_logical_type",
			value:     "hello",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        createStringLogicalType(),
			precision: 0,
			scale:     0,
			expected:  "hello",
		},
		{
			name:      "no_types",
			value:     int32(42),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        nil,
			precision: 0,
			scale:     0,
			expected:  int32(42),
		},
		{
			name:      "string_decimal_logicaltype",
			value:     StrIntToBinary("123456789", "BigEndian", 0, true),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        createDecimalLogicalType(15, 4),
			precision: 15,
			scale:     4,
			expected:  json.Number("12345.6789"),
		},
		{
			name:      "int96_timestamp_with_logical",
			value:     "\x00\x80\xa7HJ'\x00\x00*\x89%\x00", // INT96 binary data for 2023-01-01T12:00:00Z
			pT:        parquet.TypePtr(parquet.Type_INT96),
			cT:        nil,
			lT:        createStringLogicalType(), // Non-relevant logical type
			precision: 0,
			scale:     0,
			expected:  "2023-01-01T12:00:00.000000000Z", // Should still convert INT96
		},
		{
			name:      "timestamp_logical_type_millis",
			value:     int64(1640995200000), // 2022-01-01T00:00:00Z in milliseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimestampLogicalType(true, true, false, false), // millis, UTC adjusted
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "timestamp_logical_type_micros",
			value:     int64(1640995200000000), // 2022-01-01T00:00:00Z in microseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimestampLogicalType(false, true, false, true), // micros, UTC adjusted
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000000Z",
		},
		{
			name:      "timestamp_logical_type_nanos",
			value:     int64(1640995200000000000), // 2022-01-01T00:00:00Z in nanoseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimestampLogicalType(false, false, true, true), // nanos, UTC adjusted
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000000000Z",
		},
		{
			name:      "timestamp_logical_type_not_utc_adjusted",
			value:     int64(1640995200000), // 2022-01-01T00:00:00Z in milliseconds
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimestampLogicalType(true, false, false, false), // millis, not UTC adjusted
			precision: 0,
			scale:     0,
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "byte_array_without_logical_or_converted_type",
			value:     "Binary Data",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        nil,
			precision: 0,
			scale:     0,
			expected:  base64.StdEncoding.EncodeToString([]byte("Binary Data")), // "QmluYXJ5IERhdGE="
		},
		{
			name:      "fixed_len_byte_array_without_logical_or_converted_type",
			value:     []byte{0xDE, 0xAD, 0xBE, 0xEF},
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        nil,
			precision: 0,
			scale:     0,
			expected:  base64.StdEncoding.EncodeToString([]byte{0xDE, 0xAD, 0xBE, 0xEF}), // "3q2+7w=="
		},
		{
			name:      "byte_array_with_string_logical_type",
			value:     "String Data",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        createStringLogicalType(),
			precision: 0,
			scale:     0,
			expected:  "String Data", // Should remain as string, not base64 encoded
		},
		{
			name: "uuid_logical_type_bytes",
			value: []byte{
				0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
				0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
			}, // 16-byte UUID in big-endian format
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			precision: 0,
			scale:     0,
			expected:  "01234567-89ab-cdef-0123-456789abcdef", // Standard UUID string format
		},
		{
			name: "uuid_logical_type_string_input",
			value: string([]byte{
				0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
				0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
			}), // UUID as string bytes
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			precision: 0,
			scale:     0,
			expected:  "6ba7b810-9dad-11d1-80b4-00c04fd430c8", // Standard UUID string format
		},
		{
			name:      "time_logical_type_millis",
			value:     int32(45296789), // 12:34:56.789
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createTimeLogicalType(true, false, false), // millis
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789",
		},
		{
			name:      "time_logical_type_micros",
			value:     int64(45296789012), // 12:34:56.789012
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimeLogicalType(false, true, false), // micros
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789012",
		},
		{
			name:      "time_logical_type_nanos",
			value:     int64(45296789012345), // 12:34:56.789012345
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createTimeLogicalType(false, false, true), // nanos
			precision: 0,
			scale:     0,
			expected:  "12:34:56.789012345",
		},
		{
			name:      "time_logical_type_zero_millis",
			value:     int32(0),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createTimeLogicalType(true, false, false), // millis
			precision: 0,
			scale:     0,
			expected:  "00:00:00.000",
		},
		{
			name:      "time_logical_type_wrong_type",
			value:     "not_an_int",
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createTimeLogicalType(true, false, false), // millis
			precision: 0,
			scale:     0,
			expected:  "not_an_int", // Should return original value if type assertion fails
		},
		{
			name:      "bson_logical_type_string_input",
			value:     string([]byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{BSON: &parquet.BsonType{}},
			precision: 0,
			scale:     0,
			expected:  "FgAAABBpAAEAAAAA", // Base64 encoded BSON data
		},
		{
			name:      "bson_logical_type_bytes_input",
			value:     []byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x02, 0x00, 0x00, 0x00, 0x00},
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{BSON: &parquet.BsonType{}},
			precision: 0,
			scale:     0,
			expected:  "FgAAABBpAAIAAAAA", // Base64 encoded BSON data
		},
		{
			name:      "bson_converted_type_string_input",
			value:     string([]byte{0x16, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x03, 0x00, 0x00, 0x00, 0x00}),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			lT:        nil,
			precision: 0,
			scale:     0,
			expected:  "FgAAABBpAAMAAAAA", // Base64 encoded BSON data
		},
		{
			name:      "bson_logical_type_nil_value",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{BSON: &parquet.BsonType{}},
			precision: 0,
			scale:     0,
			expected:  nil,
		},
		// FLOAT16 tests
		{
			name:      "float16_positive",
			value:     []byte{0xC0, 0x48}, // 9.5 in half-precision (little-endian: 0x48C0)
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			precision: 0,
			scale:     0,
			expected:  float32(9.5),
		},
		{
			name:      "float16_negative",
			value:     string([]byte{0xC0, 0xC8}), // -9.5 in half-precision (little-endian: 0xC8C0)
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			precision: 0,
			scale:     0,
			expected:  float32(-9.5),
		},
		{
			name:      "float16_zero",
			value:     []byte{0x00, 0x00},
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			precision: 0,
			scale:     0,
			expected:  float32(0),
		},
		{
			name:      "float16_nil",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        nil,
			lT:        &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			precision: 0,
			scale:     0,
			expected:  nil,
		},
		// DATE tests
		{
			name:      "date_positive_days",
			value:     int32(9000), // 1994-08-23
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        &parquet.LogicalType{DATE: &parquet.DateType{}},
			precision: 0,
			scale:     0,
			expected:  "1994-08-23",
		},
		{
			name:      "date_zero",
			value:     int32(0), // 1970-01-01
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        &parquet.LogicalType{DATE: &parquet.DateType{}},
			precision: 0,
			scale:     0,
			expected:  "1970-01-01",
		},
		{
			name:      "date_negative_days",
			value:     int32(-365), // 1969-01-01
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        &parquet.LogicalType{DATE: &parquet.DateType{}},
			precision: 0,
			scale:     0,
			expected:  "1969-01-01",
		},
		{
			name:      "date_nil",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        &parquet.LogicalType{DATE: &parquet.DateType{}},
			precision: 0,
			scale:     0,
			expected:  nil,
		},
		// INTEGER tests
		{
			name:      "integer_int8_signed",
			value:     int32(-42),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createIntegerLogicalType(8, true),
			precision: 0,
			scale:     0,
			expected:  int8(-42),
		},
		{
			name:      "integer_int16_signed",
			value:     int32(-1000),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createIntegerLogicalType(16, true),
			precision: 0,
			scale:     0,
			expected:  int16(-1000),
		},
		{
			name:      "integer_int32_signed",
			value:     int32(-100000),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createIntegerLogicalType(32, true),
			precision: 0,
			scale:     0,
			expected:  int32(-100000),
		},
		{
			name:      "integer_int64_signed",
			value:     int64(-1000000000),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createIntegerLogicalType(64, true),
			precision: 0,
			scale:     0,
			expected:  int64(-1000000000),
		},
		{
			name:      "integer_uint8_unsigned",
			value:     int32(200),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createIntegerLogicalType(8, false),
			precision: 0,
			scale:     0,
			expected:  uint8(200),
		},
		{
			name:      "integer_uint16_unsigned",
			value:     int32(50000),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createIntegerLogicalType(16, false),
			precision: 0,
			scale:     0,
			expected:  uint16(50000),
		},
		{
			name:      "integer_uint32_unsigned",
			value:     int32(100000),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createIntegerLogicalType(32, false),
			precision: 0,
			scale:     0,
			expected:  uint32(100000),
		},
		{
			name:      "integer_uint64_unsigned",
			value:     int64(1000000000),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			cT:        nil,
			lT:        createIntegerLogicalType(64, false),
			precision: 0,
			scale:     0,
			expected:  uint64(1000000000),
		},
		{
			name:      "integer_nil",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_INT32),
			cT:        nil,
			lT:        createIntegerLogicalType(32, true),
			precision: 0,
			scale:     0,
			expected:  nil,
		},
		// GEOMETRY tests
		{
			name:      "geometry_wkb_point",
			value:     []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x49, 0x40}, // POINT(100 50) in WKB
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        createGeometryLogicalType("OGC:CRS84"),
			precision: 0,
			scale:     0,
			expected:  map[string]any{"wkb_hex": "010100000000000000000059400000000000004940", "crs": "OGC:CRS84"},
		},
		{
			name:      "geometry_nil",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        createGeometryLogicalType("OGC:CRS84"),
			precision: 0,
			scale:     0,
			expected:  nil,
		},
		// GEOGRAPHY tests
		{
			name:      "geography_nil",
			value:     nil,
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        nil,
			lT:        createGeographyLogicalType("OGC:CRS84", parquet.EdgeInterpolationAlgorithm_SPHERICAL),
			precision: 0,
			scale:     0,
			expected:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se := parquet.NewSchemaElement()
			se.Type = tt.pT
			se.ConvertedType = tt.cT
			se.LogicalType = tt.lT
			if tt.precision != 0 {
				p := int32(tt.precision)
				se.Precision = &p
			}
			if tt.scale != 0 {
				s := int32(tt.scale)
				se.Scale = &s
			}
			result := ConvertToJSONType(tt.value, se)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertToJSONType(t *testing.T) {
	wkbPoint := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x49, 0x40} // POINT(100 50)
	geomLT := createGeometryLogicalType("OGC:CRS84")
	geogLT := createGeographyLogicalType("OGC:CRS84", parquet.EdgeInterpolationAlgorithm_SPHERICAL)

	makeSchemaElement := func(lT *parquet.LogicalType) *parquet.SchemaElement {
		se := parquet.NewSchemaElement()
		se.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		se.LogicalType = lT
		return se
	}

	tests := []struct {
		name     string
		value    any
		se       *parquet.SchemaElement
		opts     []JSONTypeOption
		expected any
	}{
		{
			name:     "nil_schema_element_returns_val",
			value:    wkbPoint,
			se:       nil,
			expected: wkbPoint,
		},
		{
			name:  "geometry_default_uses_hex",
			value: wkbPoint,
			se:    makeSchemaElement(geomLT),
			expected: map[string]any{
				"wkb_hex": "010100000000000000000059400000000000004940",
				"crs":     "OGC:CRS84",
			},
		},
		{
			name:  "geometry_geojson_mode",
			value: wkbPoint,
			se:    makeSchemaElement(geomLT),
			opts:  []JSONTypeOption{WithGeospatialConfig(NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON)))},
			expected: map[string]any{
				"type":       "Feature",
				"geometry":   map[string]any{"type": "Point", "coordinates": []float64{100, 50}},
				"properties": map[string]any{"crs": "OGC:CRS84"},
			},
		},
		{
			name:  "geometry_base64_mode",
			value: wkbPoint,
			se:    makeSchemaElement(geomLT),
			opts:  []JSONTypeOption{WithGeospatialConfig(NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeBase64)))},
			expected: map[string]any{
				"wkb_b64": "AQEAAAAAAAAAAABZQAAAAAAAAElA",
				"crs":     "OGC:CRS84",
			},
		},
		{
			name:  "geography_default_uses_geojson",
			value: wkbPoint,
			se:    makeSchemaElement(geogLT),
			expected: map[string]any{
				"type":       "Feature",
				"geometry":   map[string]any{"type": "Point", "coordinates": []float64{100, 50}},
				"properties": map[string]any{"crs": "OGC:CRS84", "algorithm": "SPHERICAL"},
			},
		},
		{
			name:  "geography_hex_mode",
			value: wkbPoint,
			se:    makeSchemaElement(geogLT),
			opts:  []JSONTypeOption{WithGeospatialConfig(NewGeospatialConfig(WithGeographyJSONMode(GeospatialModeHex)))},
			expected: map[string]any{
				"wkb_hex":   "010100000000000000000059400000000000004940",
				"crs":       "OGC:CRS84",
				"algorithm": "SPHERICAL",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertToJSONType(tt.value, tt.se, tt.opts...)
			require.Equal(t, tt.expected, result)
		})
	}
}

// named floating-point types, which the library supports through reflection
type (
	namedFloat32 float32
	namedFloat64 float64
)

// TestConvertToJSONType_NonFiniteFloat verifies that NaN/+Inf/-Inf values are quoted as
// strings so the result can always be passed to encoding/json.Marshal, matching the strings
// JSONWriter already accepts on input for FLOAT/DOUBLE/FLOAT16 fields.
func TestConvertToJSONType_NonFiniteFloat(t *testing.T) {
	// little-endian FLOAT16 bit patterns
	float16NaN := []byte{0x00, 0x7e}
	float16PosInf := []byte{0x00, 0x7c}
	float16NegInf := []byte{0x00, 0xfc}

	tests := []struct {
		name     string
		value    any
		se       *parquet.SchemaElement
		expected any
	}{
		{
			name:     "float_nan",
			value:    float32(math.NaN()),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_FLOAT)},
			expected: "NaN",
		},
		{
			name:     "float_pos_inf",
			value:    float32(math.Inf(1)),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_FLOAT)},
			expected: "Infinity",
		},
		{
			name:     "float_neg_inf",
			value:    float32(math.Inf(-1)),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_FLOAT)},
			expected: "-Infinity",
		},
		{
			name:     "float_finite_unchanged",
			value:    float32(1.5),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_FLOAT)},
			expected: float32(1.5),
		},
		{
			name:     "double_nan",
			value:    math.NaN(),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_DOUBLE)},
			expected: "NaN",
		},
		{
			name:     "double_pos_inf",
			value:    math.Inf(1),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_DOUBLE)},
			expected: "Infinity",
		},
		{
			name:     "double_neg_inf",
			value:    math.Inf(-1),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_DOUBLE)},
			expected: "-Infinity",
		},
		{
			name:     "double_finite_unchanged",
			value:    2.5,
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_DOUBLE)},
			expected: 2.5,
		},
		{
			name:     "named_float32_nan",
			value:    namedFloat32(math.NaN()),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_FLOAT)},
			expected: "NaN",
		},
		{
			name:     "named_float32_neg_inf",
			value:    namedFloat32(math.Inf(-1)),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_FLOAT)},
			expected: "-Infinity",
		},
		{
			name:     "named_float32_finite_unchanged",
			value:    namedFloat32(1.5),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_FLOAT)},
			expected: namedFloat32(1.5),
		},
		{
			name:     "named_float64_nan",
			value:    namedFloat64(math.NaN()),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_DOUBLE)},
			expected: "NaN",
		},
		{
			name:     "named_float64_pos_inf",
			value:    namedFloat64(math.Inf(1)),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_DOUBLE)},
			expected: "Infinity",
		},
		{
			name:     "named_float64_finite_unchanged",
			value:    namedFloat64(2.5),
			se:       &parquet.SchemaElement{Type: parquet.TypePtr(parquet.Type_DOUBLE)},
			expected: namedFloat64(2.5),
		},
		{
			name:  "float16_nan",
			value: float16NaN,
			se: &parquet.SchemaElement{
				Type:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			},
			expected: "NaN",
		},
		{
			name:  "float16_pos_inf",
			value: float16PosInf,
			se: &parquet.SchemaElement{
				Type:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			},
			expected: "Infinity",
		},
		{
			name:  "float16_neg_inf",
			value: float16NegInf,
			se: &parquet.SchemaElement{
				Type:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			},
			expected: "-Infinity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertToJSONType(tt.value, tt.se)
			require.Equal(t, tt.expected, result)
			_, err := json.Marshal(result)
			require.NoError(t, err)
		})
	}
}

// Helper function to create a decimal logical type for testing
func createDecimalLogicalType(precision, scale int32) *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.DECIMAL = parquet.NewDecimalType()
	lt.DECIMAL.Precision = precision
	lt.DECIMAL.Scale = scale
	return lt
}

// Helper function to create a string logical type for testing
func createStringLogicalType() *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.STRING = parquet.NewStringType()
	return lt
}

// Helper function to create a timestamp logical type for testing
func createTimestampLogicalType(millis, micros, nanos, utcAdjusted bool) *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.TIMESTAMP = parquet.NewTimestampType()

	// Set UTC adjusted flag
	lt.TIMESTAMP.IsAdjustedToUTC = utcAdjusted

	// Set time unit
	lt.TIMESTAMP.Unit = parquet.NewTimeUnit()
	if millis {
		lt.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()
	} else if micros {
		lt.TIMESTAMP.Unit.MICROS = parquet.NewMicroSeconds()
	} else if nanos {
		lt.TIMESTAMP.Unit.NANOS = parquet.NewNanoSeconds()
	} else {
		// Default to millis if none specified
		lt.TIMESTAMP.Unit.MILLIS = parquet.NewMilliSeconds()
	}

	return lt
}

// Helper function to create a time logical type for testing
func createTimeLogicalType(millis, micros, nanos bool) *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.TIME = parquet.NewTimeType()

	// Set time unit
	lt.TIME.Unit = parquet.NewTimeUnit()
	if millis {
		lt.TIME.Unit.MILLIS = parquet.NewMilliSeconds()
	} else if micros {
		lt.TIME.Unit.MICROS = parquet.NewMicroSeconds()
	} else if nanos {
		lt.TIME.Unit.NANOS = parquet.NewNanoSeconds()
	} else {
		// Default to millis if none specified
		lt.TIME.Unit.MILLIS = parquet.NewMilliSeconds()
	}

	// For TIME, the isAdjustedToUTC is not relevant (it's always local time)
	lt.TIME.IsAdjustedToUTC = false

	return lt
}

// Helper function to create an integer logical type for testing
func createIntegerLogicalType(bitWidth int8, isSigned bool) *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.INTEGER = parquet.NewIntType()
	lt.INTEGER.BitWidth = bitWidth
	lt.INTEGER.IsSigned = isSigned
	return lt
}

// Helper function to create a geometry logical type for testing
func createGeometryLogicalType(crs string) *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.GEOMETRY = parquet.NewGeometryType()
	lt.GEOMETRY.CRS = &crs
	return lt
}

// Helper function to create a geography logical type for testing
func createGeographyLogicalType(crs string, algo parquet.EdgeInterpolationAlgorithm) *parquet.LogicalType {
	lt := parquet.NewLogicalType()
	lt.GEOGRAPHY = parquet.NewGeographyType()
	lt.GEOGRAPHY.CRS = &crs
	lt.GEOGRAPHY.Algorithm = &algo
	return lt
}

func TestConvertIntervalValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name:     "valid_interval_bytes",
			val:      []byte{0, 0, 0, 0, 1, 0, 0, 0, 128, 238, 54, 0},
			expected: "1 day 3600.000 sec",
		},
		{
			name:     "valid_interval_string",
			val:      string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 232, 3, 0, 0}),
			expected: "1.000 sec",
		},
		{
			name:     "interval_with_months_days_seconds",
			val:      []byte{2, 0, 0, 0, 15, 0, 0, 0, 220, 5, 0, 0},
			expected: "2 mon 15 day 1.500 sec",
		},
		{
			name:     "invalid_length_string",
			val:      "short",
			expected: "short",
		},
		{
			name:     "invalid_length_bytes",
			val:      []byte{1, 2, 3},
			expected: []byte{1, 2, 3},
		},
		{
			name:     "non_interval_value",
			val:      42,
			expected: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := convertIntervalValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertTimestampValue(t *testing.T) {
	tests := []struct {
		name          string
		val           any
		convertedType parquet.ConvertedType
		expected      any
	}{
		{
			name:          "nil_value",
			val:           nil,
			convertedType: parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected:      nil,
		},
		{
			name:          "timestamp_millis_valid",
			val:           int64(1640995200000), // 2022-01-01T00:00:00Z
			convertedType: parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected:      "2022-01-01T00:00:00.000Z",
		},
		{
			name:          "timestamp_micros_valid",
			val:           int64(1640995200000000), // 2022-01-01T00:00:00Z
			convertedType: parquet.ConvertedType_TIMESTAMP_MICROS,
			expected:      "2022-01-01T00:00:00.000000Z",
		},
		{
			name:          "non_int64_value",
			val:           int32(123),
			convertedType: parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected:      int32(123), // Should return unchanged
		},
		{
			name:          "unsupported_converted_type",
			val:           int64(1640995200000),
			convertedType: parquet.ConvertedType_UTF8,
			expected:      int64(1640995200000), // Should return unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertTimestampValue(tt.val, tt.convertedType)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertTimestampLogicalValue(t *testing.T) {
	tests := []struct {
		name      string
		val       any
		timestamp *parquet.TimestampType
		expected  any
	}{
		{
			name:      "nil_value",
			val:       nil,
			timestamp: createTimestampLogicalType(true, false, false, true).GetTIMESTAMP(),
			expected:  nil,
		},
		{
			name:      "nil_timestamp",
			val:       int64(1640995200000),
			timestamp: nil,
			expected:  nil,
		},
		{
			name:      "timestamp_millis_utc_adjusted",
			val:       int64(1640995200000),                                                // 2022-01-01T00:00:00Z
			timestamp: createTimestampLogicalType(true, false, false, true).GetTIMESTAMP(), // millis, UTC adjusted
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "timestamp_micros_utc_adjusted",
			val:       int64(1640995200000000),                                             // 2022-01-01T00:00:00Z
			timestamp: createTimestampLogicalType(false, true, false, true).GetTIMESTAMP(), // micros, UTC adjusted
			expected:  "2022-01-01T00:00:00.000000Z",
		},
		{
			name:      "timestamp_nanos_utc_adjusted",
			val:       int64(1640995200000000000),                                          // 2022-01-01T00:00:00Z
			timestamp: createTimestampLogicalType(false, false, true, true).GetTIMESTAMP(), // nanos, UTC adjusted
			expected:  "2022-01-01T00:00:00.000000000Z",
		},
		{
			name:      "timestamp_millis_not_utc_adjusted",
			val:       int64(1640995200000),                                                 // 2022-01-01T00:00:00Z
			timestamp: createTimestampLogicalType(true, false, false, false).GetTIMESTAMP(), // millis, not UTC adjusted
			expected:  "2022-01-01T00:00:00.000Z",
		},
		{
			name:      "non_int64_value",
			val:       int32(123),
			timestamp: createTimestampLogicalType(true, false, false, true).GetTIMESTAMP(),
			expected:  int32(123), // Should return unchanged
		},
		{
			name: "default_to_millis_when_no_unit",
			val:  int64(1640995200000),
			timestamp: func() *parquet.TimestampType {
				ts := parquet.NewTimestampType()
				ts.IsAdjustedToUTC = true
				// Don't set Unit to test default behavior
				return ts
			}(),
			expected: "2022-01-01T00:00:00.000Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertTimestampLogicalValue(tt.val, tt.timestamp)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertBinaryValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name:     "string_input",
			val:      "Hello, World!",
			expected: base64.StdEncoding.EncodeToString([]byte("Hello, World!")), // "SGVsbG8sIFdvcmxkIQ=="
		},
		{
			name:     "byte_slice_input",
			val:      []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F},                                    // "Hello" in bytes
			expected: base64.StdEncoding.EncodeToString([]byte{0x48, 0x65, 0x6C, 0x6C, 0x6F}), // "SGVsbG8="
		},
		{
			name:     "binary_data",
			val:      []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD},
			expected: base64.StdEncoding.EncodeToString([]byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD}), // "AAECAz/+/Q=="
		},
		{
			name:     "empty_string",
			val:      "",
			expected: base64.StdEncoding.EncodeToString([]byte("")), // ""
		},
		{
			name:     "empty_byte_slice",
			val:      []byte{},
			expected: base64.StdEncoding.EncodeToString([]byte{}), // ""
		},
		{
			name:     "non_binary_value",
			val:      42,
			expected: 42, // Should return unchanged
		},
		{
			name:     "float_value",
			val:      3.14,
			expected: 3.14, // Should return unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertBinaryValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertINT96Value(t *testing.T) {
	// nil
	res, _ := convertINT96Value(nil)
	require.Nil(t, res)

	// string
	timeStr := "2023-01-01T12:00:00.000000000Z"
	ts, _ := time.Parse(time.RFC3339Nano, timeStr)
	int96 := TimeToINT96(ts)
	res, _ = convertINT96Value(int96)
	require.Equal(t, timeStr, res)

	// error path: string shorter than 12 bytes causes INT96ToTime to fail
	short := "tooshort"
	res, _ = convertINT96Value(short)
	require.Equal(t, short, res)

	// default
	res, _ = convertINT96Value(123)
	require.Equal(t, 123, res)
}

func TestConvertDateLogicalValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name:     "epoch_day_zero",
			val:      int32(0), // 1970-01-01
			expected: "1970-01-01",
		},
		{
			name:     "positive_days",
			val:      int32(19358), // 2023-01-01
			expected: "2023-01-01",
		},
		{
			name:     "negative_days",
			val:      int32(-1), // 1969-12-31
			expected: "1969-12-31",
		},
		{
			name:     "leap_year_date",
			val:      int32(19417), // 2023-02-29 would be invalid, using 2023-03-01
			expected: "2023-03-01",
		},
		{
			name:     "non_int32_value",
			val:      "not_an_int32",
			expected: "not_an_int32", // Should return unchanged
		},
		{
			name:     "int64_value",
			val:      int64(19358),
			expected: int64(19358), // Should return unchanged as it's not int32
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertDateLogicalValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}

// moved to geospatial_test.go: Test_ConvertGeometryAndGeographyLogicalValue

// moved to geospatial_test.go: Test_GeometryAndGeography_MoreModes and helpers

func TestConvertFloat16LogicalValue(t *testing.T) {
	// Note: Float16 values in Parquet are stored in little-endian format
	// float16 1.0 = 0x3c00, little-endian bytes: [0x00, 0x3c]
	// float16 0.5 = 0x3800, little-endian bytes: [0x00, 0x38]
	// float16 -2.0 = 0xc000, little-endian bytes: [0x00, 0xc0]
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"one", string([]byte{0x00, 0x3c}), float32(1.0)},
		{"half", string([]byte{0x00, 0x38}), float32(0.5)},
		{"neg_two", string([]byte{0x00, 0xc0}), float32(-2.0)},
		{"wrong_len", string([]byte{0x00}), string([]byte{0x00})},
		{"nil", nil, nil},
		// []byte input path
		{"bytes_input", []byte{0x00, 0x3c}, float32(1.0)},
		// unsupported type returns unchanged
		{"unsupported_type", 123, 123},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ConvertFloat16LogicalValue(tc.in)
			require.Equal(t, tc.want, got)
		})
	}

	// Additional epsilon checks for subnormals and infinities
	// subnormal 0x0001, little-endian: [0x01, 0x00]
	t.Run("subnormal_pos", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x01, 0x00})).(float32)
		expected := float32(1.0/1024.0) / float32(1<<14)
		require.InEpsilon(t, expected, got, 1e-6)
	})
	// subnormal negative 0x8001, little-endian: [0x01, 0x80]
	t.Run("subnormal_neg", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x01, 0x80})).(float32)
		expected := -float32(1.0/1024.0) / float32(1<<14)
		require.InEpsilon(t, expected, got, 1e-6)
	})
	// +inf 0x7c00, little-endian: [0x00, 0x7c]
	t.Run("pos_inf", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x00, 0x7c})).(float32)
		require.True(t, math.IsInf(float64(got), 1))
	})
	// -inf 0xfc00, little-endian: [0x00, 0xfc]
	t.Run("neg_inf", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x00, 0xfc})).(float32)
		require.True(t, math.IsInf(float64(got), -1))
	})
	t.Run("zero", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x00, 0x00})).(float32)
		require.Equal(t, float32(0.0), got)
	})
	t.Run("wrong_len_bytes", func(t *testing.T) {
		in := []byte{0x00}
		got := ConvertFloat16LogicalValue(in)
		require.Equal(t, in, got)
	})
	// NaN as string, 0x7e00 little-endian: [0x00, 0x7e]
	t.Run("nan_string", func(t *testing.T) {
		got := ConvertFloat16LogicalValue(string([]byte{0x00, 0x7e})).(float32)
		require.True(t, math.IsNaN(float64(got)))
	})
	// NaN as []byte, 0x7e00 little-endian: [0x00, 0x7e]
	t.Run("nan_bytes", func(t *testing.T) {
		got := ConvertFloat16LogicalValue([]byte{0x00, 0x7e}).(float32)
		require.True(t, math.IsNaN(float64(got)))
	})
}

// Test JSONTypeToParquetType function comprehensively to improve its 30.8% coverage

// Test parquetTypeToJSONTypeWithConverted comprehensively via ConvertToJSONType
func TestConvertToJSONType_ConvertedTypes_Comprehensive(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		pT        *parquet.Type
		cT        *parquet.ConvertedType
		precision int
		scale     int
		expected  any
	}{
		// Nil values
		{
			name:     "nil_value",
			value:    nil,
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: nil,
		},
		// INT96 timestamp conversion (before checking ConvertedType) - need proper 12-byte data
		{
			name:     "int96_timestamp",
			value:    string(make([]byte, 12)), // Proper 12-byte INT96 data
			pT:       parquet.TypePtr(parquet.Type_INT96),
			expected: renderedINT96(string(make([]byte, 12))),
		},
		// Binary types without converted type
		{
			name:     "byte_array_without_converted_type",
			value:    "binary_data",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: convertBinaryValue("binary_data"),
		},
		{
			name:     "fixed_len_byte_array_without_converted_type",
			value:    "fixed_binary_data",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			expected: convertBinaryValue("fixed_binary_data"),
		},
		// No converted type, return as-is
		{
			name:     "no_converted_type_int32",
			value:    int32(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			expected: int32(12345),
		},
		// Decimal conversions
		{
			name:     "decimal_int32",
			value:    int32(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: json.Number("123.45"),
		},
		{
			name:     "decimal_int64",
			value:    int64(123456789),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    3,
			expected: json.Number("123456.789"),
		},
		{
			name:      "decimal_byte_array",
			value:     "12345",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     2,
			expected:  json.Number("2112956140.05"),
		},
		{
			name:      "decimal_fixed_len_byte_array",
			value:     "12345",
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:        parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			precision: 5,
			scale:     2,
			expected:  json.Number("2112956140.05"),
		},
		{
			name:     "decimal_wrong_type",
			value:    "wrong_type",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: "wrong_type", // fallback to original value
		},
		// UTF8 string
		{
			name:     "utf8_string",
			value:    "utf8_string",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			expected: "utf8_string",
		},
		// DATE conversion
		{
			name:     "date_conversion",
			value:    int32(18628), // Days since epoch
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
			expected: int32(18628),
		},
		// TIME_MILLIS conversion
		{
			name:     "time_millis_conversion",
			value:    int32(43200000), // 12:00:00 in milliseconds
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			expected: TIME_MILLISToTimeFormat(43200000),
		},
		{
			name:     "time_millis_wrong_type",
			value:    "wrong_type",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			expected: "wrong_type",
		},
		// TIME_MICROS conversion
		{
			name:     "time_micros_conversion",
			value:    int64(43200000000), // 12:00:00 in microseconds
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
			expected: TIME_MICROSToTimeFormat(43200000000),
		},
		{
			name:     "time_micros_wrong_type",
			value:    "wrong_type",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
			expected: "wrong_type",
		},
		// TIMESTAMP conversions
		{
			name:     "timestamp_millis",
			value:    int64(1609459200000), // 2021-01-01 00:00:00 UTC in milliseconds
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
			expected: ConvertTimestampValue(int64(1609459200000), parquet.ConvertedType_TIMESTAMP_MILLIS),
		},
		{
			name:     "timestamp_micros",
			value:    int64(1609459200000000), // 2021-01-01 00:00:00 UTC in microseconds
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS),
			expected: ConvertTimestampValue(int64(1609459200000000), parquet.ConvertedType_TIMESTAMP_MICROS),
		},
		// Integer conversions
		{
			name:     "int8_conversion",
			value:    int32(127),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			expected: int8(127),
		},
		{
			name:     "int8_wrong_type",
			value:    "wrong_type",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			expected: "wrong_type",
		},
		{
			name:     "int16_conversion",
			value:    int32(32767),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
			expected: int16(32767),
		},
		{
			name:     "int16_wrong_type",
			value:    "wrong_type",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
			expected: "wrong_type",
		},
		// INT_32 (already int32)
		{
			name:     "int32_conversion",
			value:    int32(2147483647),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32),
			expected: int32(2147483647),
		},
		// UINT conversions
		{
			name:     "uint8_conversion",
			value:    int32(255),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8),
			expected: uint8(255),
		},
		{
			name:     "uint8_wrong_type",
			value:    "wrong_type",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8),
			expected: "wrong_type",
		},
		{
			name:     "uint16_conversion",
			value:    int32(65535),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16),
			expected: uint16(65535),
		},
		{
			name:     "uint16_wrong_type",
			value:    "wrong_type",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16),
			expected: "wrong_type",
		},
		{
			name:     "uint32_conversion",
			value:    int32(-1), // Will be interpreted as uint32(4294967295)
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
			expected: uint32(4294967295),
		},
		{
			name:     "uint32_wrong_type",
			value:    "wrong_type",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
			expected: "wrong_type",
		},
		{
			name:     "uint64_conversion",
			value:    int64(-1), // Will be interpreted as uint64(18446744073709551615)
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64),
			expected: uint64(18446744073709551615),
		},
		{
			name:     "uint64_wrong_type",
			value:    "wrong_type",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64),
			expected: "wrong_type",
		},
		// INTERVAL conversion
		{
			name:     "interval_conversion",
			value:    "interval_data",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
			expected: renderedInterval("interval_data"),
		},
		// BSON conversion
		{
			name:     "bson_conversion",
			value:    "bson_data",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			expected: ConvertBSONLogicalValue("bson_data"),
		},
		// Default fallback
		{
			name:     "unknown_converted_type",
			value:    "unknown_data",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_JSON), // Unknown type
			expected: "unknown_data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se := parquet.NewSchemaElement()
			se.Type = tt.pT
			se.ConvertedType = tt.cT
			if tt.precision != 0 {
				p := int32(tt.precision)
				se.Precision = &p
			}
			if tt.scale != 0 {
				s := int32(tt.scale)
				se.Scale = &s
			}
			result := ConvertToJSONType(tt.value, se)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertTimeLogicalValue(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		timeType *parquet.TimeType
		expected any
	}{
		{
			name:  "time_millis_zero",
			value: int32(0),
			timeType: &parquet.TimeType{
				Unit: &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
			expected: "00:00:00.000",
		},
		{
			name:  "time_micros_zero",
			value: int64(0),
			timeType: &parquet.TimeType{
				Unit: &parquet.TimeUnit{MICROS: &parquet.MicroSeconds{}},
			},
			expected: "00:00:00.000000",
		},
		{
			name:  "time_millis_wrong_type",
			value: "wrong_type",
			timeType: &parquet.TimeType{
				Unit: &parquet.TimeUnit{MILLIS: &parquet.MilliSeconds{}},
			},
			expected: "wrong_type", // Should return original value
		},
		{
			name:  "time_nanos",
			value: int64(123456789000), // 123.456789 seconds in nanoseconds
			timeType: &parquet.TimeType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			},
			expected: "00:02:03.456789000", // Should convert nanoseconds
		},
		{
			name:  "time_unknown_unit",
			value: int64(12345),
			timeType: &parquet.TimeType{
				Unit: &parquet.TimeUnit{}, // No unit specified
			},
			expected: int64(12345), // Should return original value
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertTimeLogicalValue(tt.value, tt.timeType)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestParquetTypeToJSONTypeWithLogical_Fallthrough(t *testing.T) {
	// Logical types not explicitly handled (LIST, MAP, ENUM, NULL, JSON) fall through
	// to "return val", leaving the value unchanged.
	tests := []struct {
		name  string
		value any
		lT    *parquet.LogicalType
	}{
		{
			name:  "list_logical_type",
			value: int32(42),
			lT:    &parquet.LogicalType{LIST: parquet.NewListType()},
		},
		{
			name:  "map_logical_type",
			value: "data",
			lT:    &parquet.LogicalType{MAP: parquet.NewMapType()},
		},
		{
			name:  "enum_logical_type",
			value: "RED",
			lT:    &parquet.LogicalType{ENUM: parquet.NewEnumType()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se := parquet.NewSchemaElement()
			se.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
			se.LogicalType = tt.lT
			result := ConvertToJSONType(tt.value, se)
			require.Equal(t, tt.value, result)
		})
	}
}

func TestJSONTypeToParquetTypeWithLogical(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		cT          *parquet.ConvertedType
		lT          *parquet.LogicalType
		length      int
		scale       int
		expected    any
		expectError bool
	}{
		// Nil interface value returns nil, nil
		{
			name:        "nil_interface_value",
			value:       (*interface{})(nil),
			pT:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expectError: true,
		},
		// Decimal with LogicalType.DECIMAL - scale from lT overrides scale param
		{
			name:     "decimal_logical_float64_scale_override",
			value:    float64(123.45),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createDecimalLogicalType(9, 2),
			scale:    0, // lT scale=2 should override this
			expected: int32(12345),
		},
		// Decimal with LogicalType.DECIMAL and integer input (int64 path)
		{
			name:     "decimal_logical_int64_input",
			value:    int64(123),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createDecimalLogicalType(18, 3),
			expected: int64(123000),
		},
		// Decimal with LogicalType.DECIMAL and string input (json.Number-like strings)
		{
			name:     "decimal_logical_string_input",
			value:    "456.78",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createDecimalLogicalType(9, 2),
			expected: int32(45678),
		},
		// Non-decimal falls through to jsonValueToParquetDirect
		{
			name:     "non_decimal_bool_boolean_type",
			value:    true,
			pT:       parquet.TypePtr(parquet.Type_BOOLEAN),
			expected: true,
		},
		// Non-decimal float64 → DOUBLE direct conversion
		{
			name:     "non_decimal_float64_double_type",
			value:    float64(3.14),
			pT:       parquet.TypePtr(parquet.Type_DOUBLE),
			expected: float64(3.14),
		},
		// Non-decimal string → BYTE_ARRAY (UTF8) direct conversion
		{
			name:     "non_decimal_string_utf8",
			value:    "hello world",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			expected: "hello world",
		},
		// DATE converted type forces fallback to string-based conversion
		{
			name:     "date_ct_string_fallback",
			value:    "2024-01-15",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
			expected: int32(19737),
		},
		// TIME_MILLIS converted type forces fallback to string-based conversion
		{
			name:     "time_millis_ct_string_fallback",
			value:    "10:30:00.123",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			expected: int32(37800123),
		},
		// TIMESTAMP_MILLIS converted type forces fallback to string-based conversion
		{
			name:     "timestamp_millis_ct_string_fallback",
			value:    "2024-01-15T10:30:00Z",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
			expected: int64(1705314600000),
		},
		// Decimal with ConvertedType (no LogicalType) - float64 path
		{
			name:     "decimal_converted_float64",
			value:    float64(99.99),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(9999),
		},
		// Decimal with ConvertedType and uint input
		{
			name:     "decimal_converted_uint32",
			value:    uint32(500),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(5000),
		},
		// Decimal with int64 via ConvertedType
		{
			name:     "decimal_converted_int64",
			value:    int64(42),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int64(4200),
		},
		// Fallback to string-based conversion for complex types (map)
		{
			// A value with no byte-backed form reaches the string path as its %v
			// rendering, which a BYTE_ARRAY column now reads as base64 and rejects
			// rather than storing the text of \"map[key:val]\".
			name:        "map_fallback_to_string_based",
			value:       map[string]any{"key": "val"},
			pT:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expectError: true,
		},
		// FLOAT16 logical type with string input must use ParseFloat16String, not the raw
		// byte-array direct path (a naive direct conversion would keep the first two bytes
		// of the string itself instead of the parsed IEEE 754 half-precision value).
		{
			name:     "float16_logical_string_input",
			value:    "9.5",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			length:   2,
			expected: Float32ToFloat16(9.5),
		},
		{
			name:     "float16_logical_nan_string_input",
			value:    "NaN",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			length:   2,
			expected: Float32ToFloat16(float32(math.NaN())),
		},
		// UUID logical type with string input must use uuid.Parse, not the raw
		// byte-array direct path (which would keep the dashed string as-is).
		{
			name:   "uuid_logical_string_input",
			value:  "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 16,
			expected: string([]byte{
				0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1,
				0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
			}),
		},
		// Text logical types with no converted type: a value that happens to be valid
		// base64 must still be stored verbatim rather than decoded as binary.
		{
			name:     "string_logical_base64_looking",
			value:    "TEST",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			lT:       &parquet.LogicalType{STRING: parquet.NewStringType()},
			expected: "TEST",
		},
		{
			name:     "enum_logical_base64_looking",
			value:    "TEST",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			lT:       &parquet.LogicalType{ENUM: parquet.NewEnumType()},
			expected: "TEST",
		},
		{
			name:     "json_logical_base64_looking",
			value:    "null",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			lT:       &parquet.LogicalType{JSON: parquet.NewJsonType()},
			expected: "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var val reflect.Value
			if tt.value == nil {
				val = reflect.ValueOf((*interface{})(nil))
			} else {
				val = reflect.ValueOf(tt.value)
			}

			result, err := JSONTypeToParquetTypeWithLogical(val, tt.pT, tt.cT, tt.lT, tt.length, tt.scale)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expected != nil {
					require.Equal(t, tt.expected, result)
				}
			}
		})
	}
}

func TestWithGeospatialConfig(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *GeospatialConfig
		expectNil bool
	}{
		{
			name: "sets_geospatial_field",
			cfg:  &GeospatialConfig{GeometryJSONMode: GeospatialModeGeoJSON},
		},
		{
			name:      "nil_config_sets_nil_geospatial",
			cfg:       nil,
			expectNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &JSONTypeConfig{}
			opt := WithGeospatialConfig(tt.cfg)
			opt(c)

			if tt.expectNil {
				require.Nil(t, c.Geospatial)
			} else {
				require.Equal(t, tt.cfg, c.Geospatial)
			}
		})
	}
}

func TestJSONTypeToParquetType(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		cT          *parquet.ConvertedType
		length      int
		scale       int
		expected    any
		expectError bool
	}{
		{
			name:   "json_boolean_true",
			value:  true,
			pT:     parquet.TypePtr(parquet.Type_BOOLEAN),
			length: 0,
			scale:  0,
		},
		{
			name:   "json_number_int32",
			value:  float64(42), // JSON numbers are float64
			pT:     parquet.TypePtr(parquet.Type_INT32),
			length: 0,
			scale:  0,
		},
		{
			name:   "json_number_int64",
			value:  float64(42),
			pT:     parquet.TypePtr(parquet.Type_INT64),
			length: 0,
			scale:  0,
		},
		{
			name:   "json_number_float",
			value:  float64(3.14),
			pT:     parquet.TypePtr(parquet.Type_FLOAT),
			length: 0,
			scale:  0,
		},
		{
			name:   "json_number_double",
			value:  float64(3.14),
			pT:     parquet.TypePtr(parquet.Type_DOUBLE),
			length: 0,
			scale:  0,
		},
		{
			name:     "json_string",
			value:    "aGVsbG8=",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			length:   0,
			scale:    0,
			expected: "hello",
		},
		{
			name:        "invalid_json_type",
			value:       make(map[string]any), // Unsupported type
			pT:          parquet.TypePtr(parquet.Type_BOOLEAN),
			expectError: true,
		},
		// Comprehensive decimal tests with all numeric types
		{
			name:        "nil_interface_value",
			value:       (*interface{})(nil),
			pT:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expectError: true,
		},
		{
			name:     "decimal_float32",
			value:    float32(123.45),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(12345),
		},
		{
			name:     "decimal_float64",
			value:    123.456,
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    3,
			expected: int64(123456),
		},
		{
			name:     "decimal_int",
			value:    int(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(1234500),
		},
		{
			name:     "decimal_int8",
			value:    int8(123),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(1230),
		},
		{
			name:     "decimal_int16",
			value:    int16(1234),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(12340),
		},
		{
			name:     "decimal_int32",
			value:    int32(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(1234500),
		},
		{
			name:     "decimal_int64",
			value:    int64(123456789),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    3,
			expected: int64(123456789000),
		},
		{
			name:     "decimal_uint",
			value:    uint(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(1234500),
		},
		{
			name:     "decimal_uint8",
			value:    uint8(123),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(1230),
		},
		{
			name:     "decimal_uint16",
			value:    uint16(1234),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    1,
			expected: int32(12340),
		},
		{
			name:     "decimal_uint32",
			value:    uint32(12345),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(1234500),
		},
		{
			name:     "decimal_uint64",
			value:    uint64(123456789),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    3,
			expected: int64(123456789000),
		},
		{
			name:     "decimal_string",
			value:    "123.45",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:    2,
			expected: int32(12345),
		},
		{
			name:     "non_decimal_string",
			value:    "aGVsbG8gd29ybGQ=",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: "hello world",
		},
		{
			name:     "non_decimal_boolean",
			value:    true,
			pT:       parquet.TypePtr(parquet.Type_BOOLEAN),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := reflect.ValueOf(tt.value)
			if tt.value == nil {
				val = reflect.ValueOf((*interface{})(nil))
			}

			result, err := JSONTypeToParquetType(val, tt.pT, tt.cT, tt.length, tt.scale)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expected != nil {
					require.Equal(t, tt.expected, result)
				}
			}
		})
	}
}

func TestJSONValueToParquetDirect(t *testing.T) {
	tests := []struct {
		name       string
		value      any
		pT         *parquet.Type
		cT         *parquet.ConvertedType
		expected   any
		usesDirect bool // whether direct conversion should be used
	}{
		// Boolean
		{
			name:       "bool_true",
			value:      true,
			pT:         parquet.TypePtr(parquet.Type_BOOLEAN),
			expected:   true,
			usesDirect: true,
		},
		{
			name:       "bool_false",
			value:      false,
			pT:         parquet.TypePtr(parquet.Type_BOOLEAN),
			expected:   false,
			usesDirect: true,
		},
		// INT32
		{
			name:       "int32_from_float64",
			value:      float64(42),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			expected:   int32(42),
			usesDirect: true,
		},
		{
			name:       "int32_from_int",
			value:      int(100),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			expected:   int32(100),
			usesDirect: true,
		},
		// INT64
		{
			name:       "int64_from_float64",
			value:      float64(9999999999),
			pT:         parquet.TypePtr(parquet.Type_INT64),
			expected:   int64(9999999999),
			usesDirect: true,
		},
		// FLOAT
		{
			name:       "float_from_float64",
			value:      float64(3.14),
			pT:         parquet.TypePtr(parquet.Type_FLOAT),
			expected:   float32(3.14),
			usesDirect: true,
		},
		// DOUBLE
		{
			name:       "double_from_float64",
			value:      float64(3.14159265359),
			pT:         parquet.TypePtr(parquet.Type_DOUBLE),
			expected:   float64(3.14159265359),
			usesDirect: true,
		},
		// BYTE_ARRAY (string)
		{
			// Byte-backed columns are no longer handled directly: their JSON form is
			// base64, which the string path decodes.
			name:       "string_direct",
			value:      "aGVsbG8gd29ybGQ=",
			pT:         parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected:   "hello world",
			usesDirect: false,
		},
		// UTF8 converted type
		{
			name:       "utf8_string",
			value:      "unicode: 你好",
			pT:         parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			expected:   "unicode: 你好",
			usesDirect: true,
		},
		// INT_8 converted type
		{
			name:       "int8_converted",
			value:      float64(127),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
			expected:   int32(127),
			usesDirect: true,
		},
		// UINT_32 converted type
		{
			name:       "uint32_converted",
			value:      float64(4294967295),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
			expected:   int32(-1), // uint32 max wraps to -1 in int32
			usesDirect: true,
		},
		// INT_16 converted type
		{
			name:       "int16_converted",
			value:      float64(32767),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
			expected:   int32(32767),
			usesDirect: true,
		},
		// INT_32 converted type
		{
			name:       "int32_converted_type",
			value:      float64(2147483647),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32),
			expected:   int32(2147483647),
			usesDirect: true,
		},
		// INT_64 converted type
		{
			name:       "int64_converted_type",
			value:      float64(999999999999),
			pT:         parquet.TypePtr(parquet.Type_INT64),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
			expected:   int64(999999999999),
			usesDirect: true,
		},
		// UINT_8 converted type
		{
			name:       "uint8_converted",
			value:      float64(255),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8),
			expected:   int32(255),
			usesDirect: true,
		},
		// UINT_16 converted type
		{
			name:       "uint16_converted",
			value:      float64(65535),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16),
			expected:   int32(65535),
			usesDirect: true,
		},
		// UINT_64 converted type
		{
			name:       "uint64_converted",
			value:      uint64(18446744073709551615),
			pT:         parquet.TypePtr(parquet.Type_INT64),
			cT:         parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64),
			expected:   int64(-1), // uint64 max wraps to -1 in int64
			usesDirect: true,
		},
		// Test with uint input for int target
		{
			name:       "int32_from_uint",
			value:      uint(200),
			pT:         parquet.TypePtr(parquet.Type_INT32),
			expected:   int32(200),
			usesDirect: true,
		},
		// Test float from int
		{
			name:       "float_from_int",
			value:      int(42),
			pT:         parquet.TypePtr(parquet.Type_FLOAT),
			expected:   float32(42),
			usesDirect: true,
		},
		// Test double from uint
		{
			name:       "double_from_uint",
			value:      uint64(12345),
			pT:         parquet.TypePtr(parquet.Type_DOUBLE),
			expected:   float64(12345),
			usesDirect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := reflect.ValueOf(tt.value)
			result, err := JSONTypeToParquetType(val, tt.pT, tt.cT, 0, 0)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func BenchmarkJSONTypeToParquetType_DirectConversion(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_INT32)
	val := reflect.ValueOf(float64(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkJSONTypeToParquetType_Boolean(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BOOLEAN)
	val := reflect.ValueOf(true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkJSONTypeToParquetType_String(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	val := reflect.ValueOf("aGVsbG8gd29ybGQ=") // base64, the form a byte-backed column takes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkComparison_Int32_Direct(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_INT32)
	val := reflect.ValueOf(float64(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkComparison_Int32_StringBased(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_INT32)
	val := reflect.ValueOf(float64(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stringBasedConversion(val, pT)
	}
}

func BenchmarkComparison_Bool_Direct(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BOOLEAN)
	val := reflect.ValueOf(true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkComparison_Bool_StringBased(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BOOLEAN)
	val := reflect.ValueOf(true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stringBasedConversion(val, pT)
	}
}

func BenchmarkComparison_Double_Direct(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_DOUBLE)
	val := reflect.ValueOf(float64(3.14159265359))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkComparison_Double_StringBased(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_DOUBLE)
	val := reflect.ValueOf(float64(3.14159265359))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stringBasedConversion(val, pT)
	}
}

func BenchmarkComparison_String_Direct(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	val := reflect.ValueOf("aGVsbG8gd29ybGQ=") // base64, the form a byte-backed column takes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = JSONTypeToParquetType(val, pT, nil, 0, 0)
	}
}

func BenchmarkComparison_String_StringBased(b *testing.B) {
	pT := parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	val := reflect.ValueOf("aGVsbG8gd29ybGQ=") // base64, the form a byte-backed column takes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = stringBasedConversion(val, pT)
	}
}

// stringBasedConversion simulates the old string-based approach for benchmarking
func stringBasedConversion(val reflect.Value, pT *parquet.Type) (any, error) {
	s := fmt.Sprintf("%v", val)
	return StrToParquetType(s, pT, nil, 0, 0)
}

func TestJSONValueToParquetDirect_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		cT          *parquet.ConvertedType
		expected    any
		expectError bool
	}{
		// A schema element with no physical type says nothing about what the column
		// holds, so the value is refused whatever the annotation.
		{
			name:        "nil_pT_with_utf8",
			value:       "hello",
			pT:          nil,
			cT:          parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
			expectError: true,
		},
		// BYTE_ARRAY with valid base64 string: jsonPhysicalTypeDirect decodes it.
		{
			name:     "byte_array_base64_direct",
			value:    "SGVsbG8=", // base64(\"Hello\")
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: "Hello",
		},
		// BSON has no interpreted write form yet, so the value is refused rather than
		// stored as the bytes of its own text.
		{
			name:        "bson_ct_fallthrough",
			value:       "somedata",
			pT:          parquet.TypePtr(parquet.Type_INT32),
			cT:          parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
			expectError: true,
		},
		// bool value for FLOAT type: getNumericValue returns (0,false),
		// jsonPhysicalTypeDirect returns false, StrToParquetType fails to parse \"true\" as float.
		{
			name:        "bool_for_float_type",
			value:       true,
			pT:          parquet.TypePtr(parquet.Type_FLOAT),
			expectError: true,
		},
		// DATE cT: jsonValueToParquetDirect returns (nil,false), falls back to StrToParquetType.
		{
			name:     "date_ct_numeric_fallback",
			value:    int64(1000),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
			expected: int32(1000),
		},
		// INTERVAL cT: jsonValueToParquetDirect returns (nil,false) so the interval string is
		// parsed into its 12-byte little-endian form instead of being kept verbatim.
		{
			name:     "interval_ct_string_parsed",
			value:    "2 mon 3 day 4.500 sec",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
			expected: string([]byte{2, 0, 0, 0, 3, 0, 0, 0, 0x94, 0x11, 0, 0}),
		},
		{
			name:     "interval_ct_digits_fallback",
			value:    "12345",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
			expected: StrIntToBinary("12345", "LittleEndian", common.IntervalByteLen, false),
		},
		// ENUM and JSON are text on the wire: a value that happens to be valid base64 must
		// still be stored verbatim rather than decoded as binary.
		{
			name:     "enum_ct_base64_looking_string",
			value:    "TEST",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM),
			expected: "TEST",
		},
		{
			name:     "json_ct_base64_looking_string",
			value:    "null",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_JSON),
			expected: "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := reflect.ValueOf(tt.value)
			result, err := JSONTypeToParquetType(val, tt.pT, tt.cT, 0, 0)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestJSONTypeToParquetType_TimeNumber covers a TIME carried as a JSON number, which skips
// the string parser and so needs its own [0, 24h) check.
func TestJSONTypeToParquetType_TimeNumber(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		pT          *parquet.Type
		cT          *parquet.ConvertedType
		lT          *parquet.LogicalType
		expected    any
		expectError bool
		errMsg      string
	}{
		{
			name:     "millis_in_range",
			value:    float64(45296789),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "micros_in_range",
			value:    int64(86399999999),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, true, false),
			expected: int64(86399999999),
		},
		{
			name:     "nanos_in_range",
			value:    int64(45296789012345),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, false, true),
			expected: int64(45296789012345),
		},
		{
			name:        "millis_negative",
			value:       float64(-1000),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			name:        "millis_past_midnight",
			value:       float64(90000000),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			name:        "micros_full_day",
			value:       int64(86400000000),
			pT:          parquet.TypePtr(parquet.Type_INT64),
			lT:          createTimeLogicalType(false, true, false),
			expectError: true,
		},
		{
			name:        "millis_fractional",
			value:       1.9,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			// Truncating to int64 first made this a legal 0 instead of a negative TIME.
			name:        "millis_negative_fraction",
			value:       -0.5,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			name:        "millis_nan",
			value:       math.NaN(),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			name:        "millis_inf",
			value:       math.Inf(1),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			// Past what int64 can hold, where the conversion itself is undefined in Go.
			name:        "millis_astronomical",
			value:       1e30,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			name:        "millis_negative_whole_float",
			value:       -1.0,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			// Not a number at all: the clock string goes through the string parser.
			name:     "millis_clock_string",
			value:    "12:34:56.789",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "millis_integral_float",
			value:    float64(45296789),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "micros_unsigned",
			value:    uint64(45296789012),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, true, false),
			expected: int64(45296789012),
		},
		{
			// A uint64 this large is not representable as int64 at all.
			name:        "micros_unsigned_astronomical",
			value:       uint64(math.MaxUint64),
			pT:          parquet.TypePtr(parquet.Type_INT64),
			lT:          createTimeLogicalType(false, true, false),
			expectError: true,
		},
		{
			// Schema building backfills TIME_MILLIS next to the logical type, which is how
			// every real MILLIS column reaches here.
			name:     "millis_with_converted_type",
			value:    float64(45296789),
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:        "millis_with_converted_type_out_of_range",
			value:       float64(90000000),
			pT:          parquet.TypePtr(parquet.Type_INT32),
			cT:          parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			lT:          createTimeLogicalType(true, false, false),
			expectError: true,
		},
		{
			name:     "micros_converted_type_only",
			value:    float64(45296789012),
			pT:       parquet.TypePtr(parquet.Type_INT64),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
			expected: int64(45296789012),
		},
		{
			name:        "millis_converted_type_only_fractional",
			value:       1.9,
			pT:          parquet.TypePtr(parquet.Type_INT32),
			cT:          parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			expectError: true,
			errMsg:      "not a whole number of ticks",
		},
		{
			name:     "millis_converted_type_only_clock_string",
			value:    "12:34:56.789",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
			expected: int32(45296789),
		},
		{
			name:        "empty_unit_rejected",
			value:       int64(123456789),
			pT:          parquet.TypePtr(parquet.Type_INT64),
			lT:          &parquet.LogicalType{TIME: &parquet.TimeType{Unit: parquet.NewTimeUnit()}},
			expectError: true,
			errMsg:      "time unit not set",
		},
		{
			name:        "nil_unit_rejected",
			value:       int64(123456789),
			pT:          parquet.TypePtr(parquet.Type_INT64),
			lT:          &parquet.LogicalType{TIME: &parquet.TimeType{}},
			expectError: true,
			errMsg:      "time unit not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(tt.value), tt.pT, tt.cT, tt.lT, 0, 0)
			if tt.expectError {
				require.Error(t, err)
				errMsg := tt.errMsg
				if errMsg == "" {
					errMsg = "outside [0, 24h)"
				}
				require.Contains(t, err.Error(), errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestJSONTypeToParquetType_FixedLenByteArrayWidth(t *testing.T) {
	testCases := map[string]struct {
		value    string
		length   int
		expected string
		errMsg   string
	}{
		"base64-matches-width": {
			"SGVsbG8gV29ybGQ=", 11, "Hello World", "",
		},
		"decoded-width-decides": {
			"0123456789abcdef", 16, "",
			`FIXED_LEN_BYTE_ARRAY "0123456789abcdef" decodes to 12 bytes, column length is 16`,
		},
		"not-base64": {
			"abc", 5, "", `FIXED_LEN_BYTE_ARRAY "abc" is not valid base64`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			res, err := JSONTypeToParquetTypeWithLogical(
				reflect.ValueOf(tc.value),
				parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				nil,
				nil,
				tc.length,
				0,
			)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, res)
		})
	}
}

// physicalRoundTripCase is one column of the read, JSON, write sweep. Every physical value
// listed must come back unchanged after being rendered for JSON output and written again.
type physicalRoundTripCase struct {
	name   string
	se     *parquet.SchemaElement
	values []any
	issue  string // open defect that keeps this column from round-tripping yet
}

// roundTripSE builds a schema element for the sweep; length is only set when non-zero so
// unannotated BYTE_ARRAY columns keep an unset TypeLength.
func roundTripSE(pT parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length int32) *parquet.SchemaElement {
	se := parquet.NewSchemaElement()
	se.Type = parquet.TypePtr(pT)
	se.ConvertedType = cT
	se.LogicalType = lT
	if length != 0 {
		se.TypeLength = &length
	}
	return se
}

// roundTripPhysical runs one physical value through the full read and write paths the JSON
// writer uses: render for output, marshal, decode the way marshal/json.go does, write back.
func roundTripPhysical(se *parquet.SchemaElement, val any) (any, error) {
	encoded, err := json.Marshal(ConvertToJSONType(val, se))
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(encoded))
	dec.UseNumber()
	var decoded any
	if err := dec.Decode(&decoded); err != nil {
		return nil, err
	}

	got, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(decoded), se.Type, se.ConvertedType,
		se.LogicalType, int(se.GetTypeLength()), int(se.GetScale()))
	if err != nil {
		return nil, fmt.Errorf("rendered as %s: %w", encoded, err)
	}
	return got, nil
}

// TestPhysicalRoundTrip is the cross-type sweep of the exact physical round trip
// encode(decode(v)) == v. The per-type round trips elsewhere in this package each cover only
// the type whose own fix added them; this one covers every logical and converted type at the
// entry points the JSON writer actually uses.
//
// Values that are not uniquely recoverable from their rendered form are out of scope by
// definition and are excluded below: distinct NaN payloads collapse into one, FLOAT16 negative
// zero renders as 0, and a BYTE_ARRAY DECIMAL padded with redundant sign-extension bytes comes
// back in its minimal encoding (see TestDecimalByteArrayPaddingCanonicalizes). Columns whose
// round trip is still broken carry the issue that tracks the break.
func TestPhysicalRoundTrip(t *testing.T) {
	decimalSE := func(pT parquet.Type, precision, scale, length int32) *parquet.SchemaElement {
		se := roundTripSE(pT, nil, createDecimalLogicalType(precision, scale), length)
		se.Precision, se.Scale = &precision, &scale
		return se
	}
	// A DECIMAL column may carry only the legacy annotation, which keeps precision and
	// scale on the schema element rather than in a logical type.
	convertedDecimalSE := func(pT parquet.Type, precision, scale, length int32) *parquet.SchemaElement {
		se := roundTripSE(pT, parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), nil, length)
		se.Precision, se.Scale = &precision, &scale
		return se
	}
	float16 := func(bits uint16) string {
		return string([]byte{byte(bits), byte(bits >> 8)})
	}
	interval := func(months, days, millis uint32) string {
		b := make([]byte, common.IntervalByteLen)
		binary.LittleEndian.PutUint32(b[0:4], months)
		binary.LittleEndian.PutUint32(b[4:8], days)
		binary.LittleEndian.PutUint32(b[8:12], millis)
		return string(b)
	}
	bsonDoc, err := bson.Marshal(bson.D{{Key: "a", Value: int32(1)}})
	require.NoError(t, err)

	uuidLT := parquet.NewLogicalType()
	uuidLT.UUID = parquet.NewUUIDType()
	float16LT := parquet.NewLogicalType()
	float16LT.FLOAT16 = parquet.NewFloat16Type()
	enumLT := parquet.NewLogicalType()
	enumLT.ENUM = parquet.NewEnumType()
	jsonLT := parquet.NewLogicalType()
	jsonLT.JSON = parquet.NewJsonType()
	bsonLT := parquet.NewLogicalType()
	bsonLT.BSON = parquet.NewBsonType()
	dateLT := parquet.NewLogicalType()
	dateLT.DATE = parquet.NewDateType()

	tests := []physicalRoundTripCase{
		{
			name:   "BOOLEAN",
			se:     roundTripSE(parquet.Type_BOOLEAN, nil, nil, 0),
			values: []any{true, false},
		},
		{
			name:   "INT32",
			se:     roundTripSE(parquet.Type_INT32, nil, nil, 0),
			values: []any{int32(0), int32(-1), int32(math.MaxInt32), int32(math.MinInt32)},
		},
		{
			name:   "INT64",
			se:     roundTripSE(parquet.Type_INT64, nil, nil, 0),
			values: []any{int64(0), int64(-1), int64(math.MaxInt64), int64(math.MinInt64)},
		},
		{
			name: "FLOAT",
			se:   roundTripSE(parquet.Type_FLOAT, nil, nil, 0),
			values: []any{
				float32(0), float32(-0.5), float32(math.MaxFloat32), float32(math.SmallestNonzeroFloat32),
				float32(math.Inf(1)), float32(math.Inf(-1)),
			},
		},
		{
			name: "DOUBLE",
			se:   roundTripSE(parquet.Type_DOUBLE, nil, nil, 0),
			values: []any{
				float64(0), -0.5, math.MaxFloat64, math.SmallestNonzeroFloat64,
				math.Inf(1), math.Inf(-1),
			},
		},
		{
			name:   "BYTE_ARRAY unannotated",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, nil, nil, 0),
			values: []any{"", "hello", "\x00\xff\x01\xfe", "TEST"},
		},
		{
			name:   "FIXED_LEN_BYTE_ARRAY unannotated",
			se:     roundTripSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, nil, 4),
			values: []any{"\x00\xff\x01\xfe", "abcd", "\x00\x00\x00\x00"},
		},
		{
			name:   "UTF8",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8), createStringLogicalType(), 0),
			values: []any{"", "hello", "TEST", "AAAA", "日本語", `{"a":1}`},
		},
		{
			name:   "ENUM",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM), enumLT, 0),
			values: []any{"ACTIVE", "TEST", "AAAA"},
		},
		{
			name:   "JSON",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, parquet.ConvertedTypePtr(parquet.ConvertedType_JSON), jsonLT, 0),
			values: []any{`{"a":1}`, `null`, `"TEST"`},
		},
		{
			name:   "STRING logical only",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, nil, createStringLogicalType(), 0),
			values: []any{"", "hello", "TEST", "AAAA", "日本語"},
		},
		{
			name:   "ENUM logical only",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, nil, enumLT, 0),
			values: []any{"ACTIVE", "TEST", "AAAA"},
		},
		{
			name:   "JSON logical only",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, nil, jsonLT, 0),
			values: []any{`{"a":1}`, `null`, `"TEST"`},
		},
		{
			name:   "UUID",
			se:     roundTripSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, uuidLT, common.UUIDByteLen),
			values: []any{string(make([]byte, common.UUIDByteLen)), "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00"},
		},
		{
			name: "FLOAT16",
			se:   roundTripSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, nil, float16LT, common.Float16ByteLen),
			values: []any{
				float16(0x0000), float16(0x3c00), float16(0xc900), float16(0x0001),
				float16(0x7bff), float16(0x7c00), float16(0xfc00),
			},
		},
		{
			name:   "DATE converted",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_DATE), nil, 0),
			values: []any{int32(0), int32(-1), int32(19723), int32(-719162)},
		},
		{
			name:   "DATE logical",
			se:     roundTripSE(parquet.Type_INT32, nil, dateLT, 0),
			values: []any{int32(0), int32(-1), int32(19723)},
		},
		{
			name:   "TIME_MILLIS converted",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS), nil, 0),
			values: []any{int32(0), int32(1), int32(45296789), int32(86399999)},
		},
		{
			name:   "TIME_MICROS converted",
			se:     roundTripSE(parquet.Type_INT64, parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS), nil, 0),
			values: []any{int64(0), int64(1), int64(45296789012), int64(86399999999)},
		},
		{
			name:   "TIME MILLIS logical",
			se:     roundTripSE(parquet.Type_INT32, nil, createTimeLogicalType(true, false, false), 0),
			values: []any{int32(0), int32(45296789), int32(86399999)},
		},
		{
			name:   "TIME MICROS logical",
			se:     roundTripSE(parquet.Type_INT64, nil, createTimeLogicalType(false, true, false), 0),
			values: []any{int64(0), int64(45296789012), int64(86399999999)},
		},
		{
			name:   "TIME NANOS logical",
			se:     roundTripSE(parquet.Type_INT64, nil, createTimeLogicalType(false, false, true), 0),
			values: []any{int64(0), int64(45296789012345), int64(86399999999999)},
		},
		{
			name:   "TIMESTAMP_MILLIS converted",
			se:     roundTripSE(parquet.Type_INT64, parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS), nil, 0),
			values: []any{int64(0), int64(-1), int64(1699999999999)},
		},
		{
			name:   "TIMESTAMP_MICROS converted",
			se:     roundTripSE(parquet.Type_INT64, parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS), nil, 0),
			values: []any{int64(0), int64(-1), int64(1699999999999999)},
		},
		{
			name:   "TIMESTAMP MILLIS logical",
			se:     roundTripSE(parquet.Type_INT64, nil, createTimestampLogicalType(true, false, false, true), 0),
			values: []any{int64(0), int64(-1), int64(1699999999999)},
		},
		{
			name:   "TIMESTAMP MICROS logical",
			se:     roundTripSE(parquet.Type_INT64, nil, createTimestampLogicalType(false, true, false, true), 0),
			values: []any{int64(0), int64(-1), int64(1699999999999999)},
		},
		{
			name:   "TIMESTAMP NANOS logical",
			se:     roundTripSE(parquet.Type_INT64, nil, createTimestampLogicalType(false, false, true, true), 0),
			values: []any{int64(0), int64(-1), int64(1699999999999999999)},
		},
		{
			name:   "TIMESTAMP MILLIS logical local",
			se:     roundTripSE(parquet.Type_INT64, nil, createTimestampLogicalType(true, false, false, false), 0),
			values: []any{int64(0), int64(-1), int64(1699999999999)},
		},
		{
			name:   "TIMESTAMP MICROS logical local",
			se:     roundTripSE(parquet.Type_INT64, nil, createTimestampLogicalType(false, true, false, false), 0),
			values: []any{int64(0), int64(-1), int64(1699999999999999)},
		},
		{
			name:   "TIMESTAMP NANOS logical local",
			se:     roundTripSE(parquet.Type_INT64, nil, createTimestampLogicalType(false, false, true, false), 0),
			values: []any{int64(0), int64(-1), int64(1699999999999999999)},
		},
		{
			name: "INTERVAL",
			se:   roundTripSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL), nil, common.IntervalByteLen),
			values: []any{
				interval(0, 0, 0), interval(2, 3, 4500), interval(0, 0, 999),
				interval(math.MaxUint32, math.MaxUint32, math.MaxUint32),
			},
		},
		{
			// Raw Julian days rather than only what TimeToINT96 produces: a file from
			// another writer can carry any day in the range and any nanosecond within it,
			// which is where the rendering used to overflow (TestINT96PhysicalRoundTrip).
			name: "INT96",
			se:   roundTripSE(parquet.Type_INT96, nil, nil, 0),
			values: []any{
				TimeToINT96(time.Unix(0, 0).UTC()),
				TimeToINT96(time.Date(2023, 1, 2, 3, 4, 5, 123456789, time.UTC)),
				rawINT96(0, 0), rawINT96(0, math.MaxInt32),
				rawINT96(123456789, uint32(JULIAN_DAY_OF_EPOCH)),
				rawINT96(uint64(24*time.Hour.Nanoseconds()-1), uint32(JULIAN_DAY_OF_EPOCH)),
			},
		},
		{
			name:   "DECIMAL INT32",
			se:     decimalSE(parquet.Type_INT32, 9, 2, 0),
			values: []any{int32(0), int32(-1), int32(999999999), int32(-999999999)},
		},
		{
			name:   "DECIMAL INT64",
			se:     decimalSE(parquet.Type_INT64, 18, 2, 0),
			values: []any{int64(0), int64(-1), int64(999999999999999999), int64(-999999999999999999)},
		},
		{
			name:   "DECIMAL FIXED_LEN_BYTE_ARRAY",
			se:     decimalSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, 38, 2, 16),
			values: []any{StrIntToBinary("12345678901234567890123456", "BigEndian", 16, true), StrIntToBinary("-1", "BigEndian", 16, true)},
		},
		{
			name:   "DECIMAL BYTE_ARRAY",
			se:     decimalSE(parquet.Type_BYTE_ARRAY, 38, 3, 0),
			values: []any{StrIntToBinary("-99999999999999999999999999999999999999", "BigEndian", 16, true), StrIntToBinary("1", "BigEndian", 1, true)},
		},
		{
			name:   "DECIMAL converted INT32",
			se:     convertedDecimalSE(parquet.Type_INT32, 9, 2, 0),
			values: []any{int32(0), int32(-1), int32(999999999), int32(-999999999)},
		},
		{
			name:   "DECIMAL converted INT64",
			se:     convertedDecimalSE(parquet.Type_INT64, 18, 2, 0),
			values: []any{int64(0), int64(-1), int64(999999999999999999), int64(-999999999999999999)},
		},
		{
			name:   "DECIMAL converted FIXED_LEN_BYTE_ARRAY",
			se:     convertedDecimalSE(parquet.Type_FIXED_LEN_BYTE_ARRAY, 38, 2, 16),
			values: []any{StrIntToBinary("12345678901234567890123456", "BigEndian", 16, true), StrIntToBinary("-1", "BigEndian", 16, true)},
		},
		{
			name:   "DECIMAL converted BYTE_ARRAY",
			se:     convertedDecimalSE(parquet.Type_BYTE_ARRAY, 38, 3, 0),
			values: []any{StrIntToBinary("-99999999999999999999999999999999999999", "BigEndian", 16, true), StrIntToBinary("1", "BigEndian", 1, true)},
		},
		{
			name:   "INT_8",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8), nil, 0),
			values: []any{int32(0), int32(-128), int32(127)},
		},
		{
			name:   "INT_16",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16), nil, 0),
			values: []any{int32(0), int32(-32768), int32(32767)},
		},
		{
			name:   "INT_32",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32), nil, 0),
			values: []any{int32(math.MinInt32), int32(math.MaxInt32)},
		},
		{
			name:   "INT_64",
			se:     roundTripSE(parquet.Type_INT64, parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64), nil, 0),
			values: []any{int64(math.MinInt64), int64(math.MaxInt64)},
		},
		{
			name:   "UINT_8",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8), nil, 0),
			values: []any{int32(0), int32(255)},
		},
		{
			name:   "UINT_16",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16), nil, 0),
			values: []any{int32(0), int32(65535)},
		},
		{
			name:   "UINT_32",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32), nil, 0),
			values: []any{int32(0), int32(-1)},
		},
		{
			name:   "UINT_64",
			se:     roundTripSE(parquet.Type_INT64, parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64), nil, 0),
			values: []any{int64(0), int64(-1)},
		},
		{
			// The shape parquet-go's own schema builder emits: common/logicaltype.go
			// backfills an INTEGER logical type next to every INT_*/UINT_* converted type,
			// so this pairing, not either annotation alone, is what the writers usually see.
			name:   "INT_32 with backfilled INTEGER",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32), createIntegerLogicalType(32, true), 0),
			values: []any{int32(0), int32(math.MinInt32), int32(math.MaxInt32)},
		},
		{
			name:   "UINT_32 with backfilled INTEGER",
			se:     roundTripSE(parquet.Type_INT32, parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32), createIntegerLogicalType(32, false), 0),
			values: []any{int32(0), int32(math.MaxInt32), int32(math.MinInt32), int32(-1)},
		},
		{
			name:   "INT_64 with backfilled INTEGER",
			se:     roundTripSE(parquet.Type_INT64, parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64), createIntegerLogicalType(64, true), 0),
			values: []any{int64(0), int64(math.MinInt64), int64(math.MaxInt64)},
		},
		{
			name:   "UINT_64 with backfilled INTEGER",
			se:     roundTripSE(parquet.Type_INT64, parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64), createIntegerLogicalType(64, false), 0),
			values: []any{int64(0), int64(math.MaxInt64), int64(math.MinInt64), int64(-1)},
		},
		{
			name:   "INTEGER logical signed 8",
			se:     roundTripSE(parquet.Type_INT32, nil, createIntegerLogicalType(8, true), 0),
			values: []any{int32(0), int32(-128), int32(127)},
		},
		{
			name:   "INTEGER logical signed 16",
			se:     roundTripSE(parquet.Type_INT32, nil, createIntegerLogicalType(16, true), 0),
			values: []any{int32(0), int32(-32768), int32(32767)},
		},
		{
			name:   "INTEGER logical signed 32",
			se:     roundTripSE(parquet.Type_INT32, nil, createIntegerLogicalType(32, true), 0),
			values: []any{int32(math.MinInt32), int32(math.MaxInt32)},
		},
		{
			name:   "INTEGER logical signed 64",
			se:     roundTripSE(parquet.Type_INT64, nil, createIntegerLogicalType(64, true), 0),
			values: []any{int64(math.MinInt64), int64(math.MaxInt64)},
		},
		{
			// The unsigned values straddle the sign bit of the physical column, where the
			// rendered number stops fitting the signed type that carries it.
			name:   "INTEGER logical unsigned 8",
			se:     roundTripSE(parquet.Type_INT32, nil, createIntegerLogicalType(8, false), 0),
			values: []any{int32(0), int32(127), int32(128), int32(255)},
		},
		{
			name:   "INTEGER logical unsigned 16",
			se:     roundTripSE(parquet.Type_INT32, nil, createIntegerLogicalType(16, false), 0),
			values: []any{int32(0), int32(32767), int32(32768), int32(65535)},
		},
		{
			name:   "INTEGER logical unsigned 32",
			se:     roundTripSE(parquet.Type_INT32, nil, createIntegerLogicalType(32, false), 0),
			values: []any{int32(0), int32(math.MaxInt32), int32(math.MinInt32), int32(-1)},
		},
		{
			name:   "INTEGER logical unsigned 64",
			se:     roundTripSE(parquet.Type_INT64, nil, createIntegerLogicalType(64, false), 0),
			values: []any{int64(0), int64(math.MaxInt64), int64(math.MinInt64), int64(-1)},
		},
		{
			name:   "BSON",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, parquet.ConvertedTypePtr(parquet.ConvertedType_BSON), bsonLT, 0),
			values: []any{string(bsonDoc)},
			issue:  "#417: BSON has no interpreted write form, the rendered document is not parsed back",
		},
		{
			name:   "GEOMETRY",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, nil, createGeometryLogicalType("OGC:CRS84"), 0),
			values: []any{string(createSimpleWKBPoint(1, 2, true))},
			issue:  "#418: geospatial has no textual write form, GeoJSON output cannot be written back",
		},
		{
			name:   "GEOGRAPHY",
			se:     roundTripSE(parquet.Type_BYTE_ARRAY, nil, createGeographyLogicalType("OGC:CRS84", parquet.EdgeInterpolationAlgorithm_SPHERICAL), 0),
			values: []any{string(createSimpleWKBPoint(1, 2, true))},
			issue:  "#418: geospatial has no textual write form, GeoJSON output cannot be written back",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, val := range tt.values {
				got, err := roundTripPhysical(tt.se, val)
				if tt.issue != "" {
					// Asserting the break rather than skipping it: a skip stays quiet
					// forever, while this fails the day the issue lands and says so.
					require.False(t, err == nil && reflect.DeepEqual(val, got),
						"%s\nvalue %#v now round-trips; drop the issue from this case", tt.issue, val)
					continue
				}
				require.NoError(t, err, "value %#v", val)
				require.Equal(t, val, got, "value %#v", val)
			}
		})
	}
}
