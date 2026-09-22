package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestStrToParquetType(t *testing.T) {
	testCases := []struct {
		name           string
		inputStr       string
		expectedGoData any
		parquetType    *parquet.Type
		convertedType  *parquet.ConvertedType
		length         int
		scale          int
	}{
		// Basic primitive types
		{
			name:           "boolean-false",
			inputStr:       "false",
			expectedGoData: bool(false),
			parquetType:    parquet.TypePtr(parquet.Type_BOOLEAN),
		},
		{
			name:           "int32-positive",
			inputStr:       "1",
			expectedGoData: int32(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
		},
		{
			name:           "int64-zero",
			inputStr:       "0",
			expectedGoData: int64(0),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
		},
		{
			name:           "int96-little-endian",
			inputStr:       "12345",
			expectedGoData: StrIntToBinary("12345", "LittleEndian", 12, true),
			parquetType:    parquet.TypePtr(parquet.Type_INT96),
		},
		{
			name:           "float32",
			inputStr:       "0.1",
			expectedGoData: float32(0.1),
			parquetType:    parquet.TypePtr(parquet.Type_FLOAT),
		},
		{
			name:           "float64",
			inputStr:       "0.1",
			expectedGoData: float64(0.1),
			parquetType:    parquet.TypePtr(parquet.Type_DOUBLE),
		},
		{
			name:           "byte-array-base64",
			inputStr:       "SGVsbG8gV29ybGQ=", // "Hello World" in base64
			expectedGoData: string("Hello World"),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
		},
		{
			name:           "fixed-len-byte-array-base64",
			inputStr:       "SGVsbG8gV29ybGQ=", // "Hello World" in base64
			expectedGoData: string("Hello World"),
			parquetType:    parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
		},

		// Converted types
		{
			name:           "utf8-string",
			inputStr:       "abc bcd",
			expectedGoData: string("abc bcd"),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		},
		{
			name:           "int8-converted",
			inputStr:       "1",
			expectedGoData: int32(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_8),
		},
		{
			name:           "uint64-converted",
			inputStr:       "1",
			expectedGoData: uint64(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_64),
		},
		{
			name:           "date-converted",
			inputStr:       "1",
			expectedGoData: int32(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
		},
		{
			name:           "timestamp-millis",
			inputStr:       "1",
			expectedGoData: int64(1),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
		},

		// Decimal types
		{
			name:           "decimal-int32",
			inputStr:       "123.45",
			expectedGoData: int32(12345),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			length:         12,
			scale:          2,
		},
		{
			name:           "decimal-fixed-len-byte-array",
			inputStr:       "123.45",
			expectedGoData: StrIntToBinary("12345", "BigEndian", 12, true),
			parquetType:    parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			length:         12,
			scale:          2,
		},
		{
			name:           "decimal-byte-array-high-precision",
			inputStr:       "373.1145",
			expectedGoData: StrIntToBinary("373114500000000000000", "BigEndian", 0, true),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			length:         16,
			scale:          18,
		},
		{
			name:           "unknown-basic-type",
			inputStr:       "test",
			expectedGoData: nil,
			parquetType:    parquet.TypePtr(parquet.Type(-1)), // Unknown type
		},
		{
			name:           "unknown-converted-type",
			inputStr:       "test",
			expectedGoData: nil,
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType(-1)), // Unknown converted type
		},
		{
			name:           "int16-converted",
			inputStr:       "32767",
			expectedGoData: int32(32767),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_16),
		},
		{
			name:           "int32-converted",
			inputStr:       "123456",
			expectedGoData: int32(123456),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32),
		},
		{
			name:           "uint8-converted",
			inputStr:       "255",
			expectedGoData: int32(255),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_8),
		},
		{
			name:           "uint16-converted",
			inputStr:       "65535",
			expectedGoData: int32(65535),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_16),
		},
		{
			name:           "uint32-converted",
			inputStr:       "4294967295",
			expectedGoData: int32(-1), // Overflow behavior
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UINT_32),
		},
		{
			name:           "time-millis-converted",
			inputStr:       "86399999",
			expectedGoData: int32(86399999),
			parquetType:    parquet.TypePtr(parquet.Type_INT32),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MILLIS),
		},
		{
			name:           "int64-converted",
			inputStr:       "9223372036854775807",
			expectedGoData: int64(9223372036854775807),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64),
		},
		{
			name:           "time-micros-converted",
			inputStr:       "86399999999",
			expectedGoData: int64(86399999999),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS),
		},
		{
			name:           "timestamp-micros-converted",
			inputStr:       "1640995200000000",
			expectedGoData: int64(1640995200000000),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MICROS),
		},
		{
			name:           "interval-converted",
			inputStr:       "1234567890123",
			expectedGoData: StrIntToBinary("1234567890123", "LittleEndian", 12, false),
			parquetType:    parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL),
		},
		{
			name:           "decimal-int64",
			inputStr:       "123.45",
			expectedGoData: int64(12345),
			parquetType:    parquet.TypePtr(parquet.Type_INT64),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:          2,
		},
		{
			name:           "decimal-byte-array-default",
			inputStr:       "999.999",
			expectedGoData: StrIntToBinary("999999", "BigEndian", 0, true),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY), // Will hit default case in decimal
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
			scale:          3,
		},
		{
			name:           "bson-converted",
			inputStr:       `{"i":1}`,
			expectedGoData: string([]byte{0x0c, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_BSON),
		},
		{
			name:           "json-converted",
			inputStr:       `{"key":"value"}`,
			expectedGoData: string(`{"key":"value"}`),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_JSON),
		},
		{
			name:           "enum-converted",
			inputStr:       "RED",
			expectedGoData: string("RED"),
			parquetType:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			convertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actualResult, _ := StrToParquetType(
				testCase.inputStr,
				testCase.parquetType,
				testCase.convertedType,
				testCase.length,
				testCase.scale,
			)

			actualStr := fmt.Sprintf("%v", actualResult)
			expectedStr := fmt.Sprintf("%v", testCase.expectedGoData)

			require.Equal(t, expectedStr, actualStr,
				"StrToParquetType conversion failed for input: %s with Type: %v, ConvertedType: %v\nExpected: %s\nGot: %s",
				testCase.inputStr, testCase.parquetType, testCase.convertedType, expectedStr, actualStr)
		})
	}
}

func TestStrToParquetTypeWithLogical(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		pT       *parquet.Type
		cT       *parquet.ConvertedType
		lT       *parquet.LogicalType
		length   int
		scale    int
		expected any
	}{
		// FLOAT16 tests
		{
			name:   "float16_human_readable",
			s:      "3.14",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			length: 2,
			// expected is 2-byte float16 representation
		},
		{
			name:   "float16_negative",
			s:      "-1.5",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			length: 2,
		},
		// UUID tests
		{
			name:     "uuid_human_readable",
			s:        "550e8400-e29b-41d4-a716-446655440000",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length:   16,
			expected: "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00",
		},
		{
			name:     "uuid_urn_prefixed",
			s:        "urn:uuid:550e8400-e29b-41d4-a716-446655440000",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length:   16,
			expected: "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00",
		},
		{
			name:     "uuid_undashed_hex",
			s:        "550e8400e29b41d4a716446655440000",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length:   16,
			expected: "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00",
		},
		{
			name:     "uuid_braced",
			s:        "{550e8400-e29b-41d4-a716-446655440000}",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length:   16,
			expected: "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00",
		},
		// TIMESTAMP nanos tests
		{
			name: "timestamp_nanos_human_readable",
			s:    "2024-01-15T10:30:00Z",
			pT:   parquet.TypePtr(parquet.Type_INT64),
			lT: &parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			}},
			expected: int64(1705314600000000000),
		},
		{
			name: "timestamp_nanos_raw_integer",
			s:    "1705314600000000000",
			pT:   parquet.TypePtr(parquet.Type_INT64),
			lT: &parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			}},
			expected: int64(1705314600000000000),
		},
		// TIME nanos tests
		{
			name: "time_nanos_human_readable",
			s:    "10:30:00.123456789",
			pT:   parquet.TypePtr(parquet.Type_INT64),
			lT: &parquet.LogicalType{TIME: &parquet.TimeType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			}},
			expected: int64(37800123456789),
		},
		{
			name: "time_nanos_raw_integer",
			s:    "37800123456789",
			pT:   parquet.TypePtr(parquet.Type_INT64),
			lT: &parquet.LogicalType{TIME: &parquet.TimeType{
				Unit: &parquet.TimeUnit{NANOS: &parquet.NanoSeconds{}},
			}},
			expected: int64(37800123456789),
		},
		// DATE tests via LogicalType
		{
			name:     "date_human_readable_logical",
			s:        "2024-01-15",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected: int32(19737),
		},
		{
			name:     "date_raw_integer_logical",
			s:        "19737",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected: int32(19737),
		},
		// Fallback to ConvertedType
		{
			name:     "fallback_to_converted_type",
			s:        "2024-01-15",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
			lT:       nil,
			expected: int32(19737),
		},
		// Nil LogicalType
		{
			name:     "nil_logical_type",
			s:        "42",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			cT:       parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32),
			lT:       nil,
			expected: int32(42),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := StrToParquetTypeWithLogical(tt.s, tt.pT, tt.cT, tt.lT, tt.length, tt.scale)
			require.NoError(t, err)
			if tt.expected != nil {
				require.Equal(t, tt.expected, result)
			} else {
				require.NotNil(t, result)
			}
		})
	}
}

func TestStrToParquetType_HumanReadable(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		pT       parquet.Type
		cT       parquet.ConvertedType
		expected any
	}{
		// DATE
		{
			name:     "date_human_readable",
			s:        "2024-01-15",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedType_DATE,
			expected: int32(19737),
		},
		{
			name:     "date_raw_integer",
			s:        "19737",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedType_DATE,
			expected: int32(19737),
		},
		// TIME_MILLIS
		{
			name:     "time_millis_human_readable",
			s:        "10:30:00.123",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedType_TIME_MILLIS,
			expected: int32(37800123),
		},
		{
			name:     "time_millis_raw_integer",
			s:        "37800123",
			pT:       parquet.Type_INT32,
			cT:       parquet.ConvertedType_TIME_MILLIS,
			expected: int32(37800123),
		},
		// TIME_MICROS
		{
			name:     "time_micros_human_readable",
			s:        "10:30:00.123456",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIME_MICROS,
			expected: int64(37800123456),
		},
		{
			name:     "time_micros_raw_integer",
			s:        "37800123456",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIME_MICROS,
			expected: int64(37800123456),
		},
		// TIMESTAMP_MILLIS
		{
			name:     "timestamp_millis_human_readable",
			s:        "2024-01-15T10:30:00Z",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected: int64(1705314600000),
		},
		{
			name:     "timestamp_millis_raw_integer",
			s:        "1705314600000",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIMESTAMP_MILLIS,
			expected: int64(1705314600000),
		},
		// TIMESTAMP_MICROS
		{
			name:     "timestamp_micros_human_readable",
			s:        "2024-01-15T10:30:00Z",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIMESTAMP_MICROS,
			expected: int64(1705314600000000),
		},
		{
			name:     "timestamp_micros_raw_integer",
			s:        "1705314600000000",
			pT:       parquet.Type_INT64,
			cT:       parquet.ConvertedType_TIMESTAMP_MICROS,
			expected: int64(1705314600000000),
		},
		// INTERVAL
		{
			name: "interval_human_readable",
			s:    "1 mon 2 day 3.456 sec",
			pT:   parquet.Type_FIXED_LEN_BYTE_ARRAY,
			cT:   parquet.ConvertedType_INTERVAL,
		},
		{
			name: "interval_raw_integer",
			s:    "123456",
			pT:   parquet.Type_FIXED_LEN_BYTE_ARRAY,
			cT:   parquet.ConvertedType_INTERVAL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pT := tt.pT
			cT := tt.cT
			result, err := StrToParquetType(tt.s, &pT, &cT, 0, 0)
			require.NoError(t, err)
			if tt.expected != nil {
				require.Equal(t, tt.expected, result)
			} else {
				require.NotNil(t, result)
			}
		})
	}
}

func TestStrToParquetType_INT96(t *testing.T) {
	// INT96 human readable
	pT := parquet.Type_INT96
	result, err := StrToParquetType("2024-01-15T10:30:00Z", &pT, nil, 0, 0)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 12, len(result.(string)))

	// INT96 raw integer fallback
	result2, err := StrToParquetType("123456789", &pT, nil, 0, 0)
	require.NoError(t, err)
	require.NotNil(t, result2)
}

func TestStrToParquetTypeWithLogical_Comprehensive(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		pT       *parquet.Type
		lT       *parquet.LogicalType
		length   int
		expected any
	}{
		{
			name:     "uuid",
			s:        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length:   16,
			expected: string([]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}),
		},
		{
			name:     "float16",
			s:        "9.5",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			length:   2,
			expected: []byte{0xC0, 0x48},
		},
		{
			name:     "date_string",
			s:        "2024-01-15",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected: int32(19737),
		},
		{
			name:     "date_int",
			s:        "19737",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       &parquet.LogicalType{DATE: &parquet.DateType{}},
			expected: int32(19737),
		},
		{
			name:     "timestamp_nanos",
			s:        "2024-01-15T14:30:45.123456789Z",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimestampLogicalType(false, false, true, true),
			expected: int64(1705329045123456789),
		},
		{
			name:     "timestamp_micros",
			s:        "2024-01-15T14:30:45.123456Z",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimestampLogicalType(false, true, false, true),
			expected: int64(1705329045123456),
		},
		{
			name:     "timestamp_millis",
			s:        "2024-01-15T14:30:45.123Z",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimestampLogicalType(true, false, false, true),
			expected: int64(1705329045123),
		},
		{
			name:     "timestamp_int",
			s:        "1705329045123456789",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimestampLogicalType(false, false, true, true),
			expected: int64(1705329045123456789),
		},
		{
			name:     "time_nanos",
			s:        "12:34:56.789012345",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, false, true),
			expected: int64(45296789012345),
		},
		{
			name:     "time_micros",
			s:        "12:34:56.789012",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, true, false),
			expected: int64(45296789012),
		},
		{
			name:     "time_millis",
			s:        "12:34:56.789",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "time_int32",
			s:        "45296789",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createTimeLogicalType(true, false, false),
			expected: int32(45296789),
		},
		{
			name:     "time_int64",
			s:        "45296789012345",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createTimeLogicalType(false, false, true),
			expected: int64(45296789012345),
		},
		{
			name:     "decimal_int32",
			s:        "123.45",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createDecimalLogicalType(9, 2),
			expected: int32(12345),
		},
		{
			name:     "decimal_int64",
			s:        "123.45",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createDecimalLogicalType(18, 2),
			expected: int64(12345),
		},
		{
			name:     "decimal_fixed_len",
			length:   12,
			s:        "123.45",
			pT:       parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:       createDecimalLogicalType(9, 2),
			expected: StrIntToBinary("12345", "BigEndian", 12, true),
		},
		{
			name:     "decimal_byte_array",
			s:        "123.45",
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			lT:       createDecimalLogicalType(9, 2),
			expected: StrIntToBinary("12345", "BigEndian", 0, true),
		},
		// Nil unit: strToTimestampLogical returns error, falls back to StrToParquetType
		{
			name:     "timestamp_nil_unit_fallback",
			s:        "123456789",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       &parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{}},
			expected: int64(123456789),
		},
		{
			name:     "integer_int8",
			s:        "123",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(8, true),
			expected: int8(123),
		},
		{
			name:     "integer_uint8",
			s:        "200",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(8, false),
			expected: uint8(200),
		},
		{
			name:     "integer_int16",
			s:        "12345",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(16, true),
			expected: int16(12345),
		},
		{
			name:     "integer_uint16",
			s:        "50000",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(16, false),
			expected: uint16(50000),
		},
		{
			name:     "integer_int32",
			s:        "1234567",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(32, true),
			expected: int32(1234567),
		},
		{
			name:     "integer_uint32",
			s:        "1234567",
			pT:       parquet.TypePtr(parquet.Type_INT32),
			lT:       createIntegerLogicalType(32, false),
			expected: uint32(1234567),
		},
		{
			name:     "integer_int64",
			s:        "1234567890",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createIntegerLogicalType(64, true),
			expected: int64(1234567890),
		},
		{
			name:     "integer_uint64",
			s:        "1234567890",
			pT:       parquet.TypePtr(parquet.Type_INT64),
			lT:       createIntegerLogicalType(64, false),
			expected: uint64(1234567890),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := StrToParquetTypeWithLogical(tt.s, tt.pT, nil, tt.lT, tt.length, 0)
			require.NoError(t, err)

			switch exp := tt.expected.(type) {
			case []byte:
				require.Equal(t, string(exp), result)
			case int8:
				require.Equal(t, exp, int8(result.(int32)))
			case uint8:
				require.Equal(t, exp, uint8(result.(int32)))
			case int16:
				require.Equal(t, exp, int16(result.(int32)))
			case uint16:
				require.Equal(t, exp, uint16(result.(int32)))
			case uint32:
				require.Equal(t, exp, uint32(result.(int32)))
			case uint64:
				require.Equal(t, exp, uint64(result.(int64)))
			default:
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestStrToParquetTypeWithLogical_Errors covers wrap paths reachable via the
// public API: FLOAT16 and DATE in strToLogicalType propagate errors with
// handled=true.
func TestStrToParquetTypeWithLogical_Errors(t *testing.T) {
	tests := []struct {
		name   string
		s      string
		pT     *parquet.Type
		lT     *parquet.LogicalType
		length int
		errMsg string
	}{
		{
			name:   "float16_invalid",
			s:      "not-a-float",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			length: 2,
			errMsg: "parse FLOAT16",
		},
		{
			name:   "date_invalid",
			s:      "not-a-date",
			pT:     parquet.TypePtr(parquet.Type_INT32),
			lT:     &parquet.LogicalType{DATE: &parquet.DateType{}},
			errMsg: "parse DATE",
		},
		{
			name:   "uuid_invalid",
			s:      "not-a-uuid",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 16,
			errMsg: "parse UUID",
		},
		{
			name:   "uuid_truncated_string",
			s:      "550e8400-e29b-41d4-a716-44665544",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 16,
			errMsg: "parse UUID",
		},
		{
			name:   "uuid_non_hex",
			s:      "550e8400-e29b-41d4-a716-44665544zzzz",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 16,
			errMsg: "parse UUID",
		},
		{
			name:   "uuid_square_brackets",
			s:      "[550e8400-e29b-41d4-a716-446655440000]",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 16,
			errMsg: "parse UUID",
		},
		{
			name:   "uuid_arbitrary_wrapper",
			s:      "X550e8400-e29b-41d4-a716-446655440000Y",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 16,
			errMsg: "parse UUID",
		},
		{
			name:   "uuid_raw_binary",
			s:      "\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 16,
			errMsg: "parse UUID",
		},
		{
			name:   "uuid_column_too_short",
			s:      "550e8400-e29b-41d4-a716-446655440000",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 8,
			errMsg: "UUID requires length 16",
		},
		{
			name:   "uuid_column_length_unset",
			s:      "550e8400-e29b-41d4-a716-446655440000",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			length: 0,
			errMsg: "UUID requires length 16",
		},
		{
			name:   "float16_column_wrong_length",
			s:      "9.5",
			pT:     parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			lT:     &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			length: 8,
			errMsg: "FLOAT16 requires length 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := StrToParquetTypeWithLogical(tt.s, tt.pT, nil, tt.lT, tt.length, 0)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

// TestStrToTimeLogical_Errors covers the parse and range failures in strToTimeLogical.
// A TIME string that fails here is reported rather than handed to the physical scan,
// which would read "25:00:00.000" as 25.
func TestStrToTimeLogical_Errors(t *testing.T) {
	tests := []struct {
		name   string
		s      string
		t      *parquet.TimeType
		errMsg string
	}{
		{
			name:   "millis_invalid",
			s:      "not-a-time",
			t:      createTimeLogicalType(true, false, false).GetTIME(),
			errMsg: "parse TIME_MILLIS",
		},
		{
			name:   "micros_invalid",
			s:      "not-a-time",
			t:      createTimeLogicalType(false, true, false).GetTIME(),
			errMsg: "parse TIME_MICROS",
		},
		{
			name:   "nanos_invalid",
			s:      "not-a-time",
			t:      createTimeLogicalType(false, false, true).GetTIME(),
			errMsg: "parse TIME_NANOS",
		},
		{
			name:   "nil_unit",
			s:      "123456789",
			t:      &parquet.TimeType{},
			errMsg: "time unit not set",
		},
		{
			name:   "empty_unit",
			s:      "123456789",
			t:      &parquet.TimeType{Unit: parquet.NewTimeUnit()},
			errMsg: "time unit not set",
		},
		{
			name:   "millis_negative",
			s:      "-1000",
			t:      createTimeLogicalType(true, false, false).GetTIME(),
			errMsg: "outside [0, 24h)",
		},
		{
			name:   "micros_full_day",
			s:      "86400000000",
			t:      createTimeLogicalType(false, true, false).GetTIME(),
			errMsg: "outside [0, 24h)",
		},
		{
			name:   "nanos_full_day",
			s:      "86400000000000",
			t:      createTimeLogicalType(false, false, true).GetTIME(),
			errMsg: "outside [0, 24h)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := strToTimeLogical(tt.s, tt.t)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

// TestStrToTimestampLogical_Errors covers the failed tick-count scan in
// strToTimestampLogical. Reached directly for the same reason as
// TestStrToTimeLogical_Errors. The error names the column's unit, so the logical and
// converted spellings of the same column report a bad value identically.
func TestStrToTimestampLogical_Errors(t *testing.T) {
	ts := createTimestampLogicalType(true, false, false, true).GetTIMESTAMP()
	_, err := strToTimestampLogical("not-a-timestamp", ts)
	require.Error(t, err)
	require.Contains(t, err.Error(), `parse TIMESTAMP_MILLIS "not-a-timestamp"`)
}

func TestStrToParquetType_IntervalErrors(t *testing.T) {
	// A bare unsigned integer is the legacy little-endian form and stays accepted; anything
	// else used to be handed to StrIntToBinary, which silently produced zeros or a partial
	// value instead of reporting the failure.
	tests := []struct {
		name   string
		s      string
		errMsg string
	}{
		{name: "not_an_interval", s: "garbage", errMsg: "parse INTERVAL"},
		{name: "unknown_unit", s: "2 mon 3 zzz", errMsg: "parse INTERVAL"},
		{name: "negative_seconds", s: "-1 sec", errMsg: "parse INTERVAL"},
		{name: "overflowing_seconds", s: "5000000 sec", errMsg: "parse INTERVAL"},
		{name: "negative_integer", s: "-1", errMsg: "parse INTERVAL"},
		{name: "integer_too_wide", s: "79228162514264337593543950336", errMsg: "exceeds 12 bytes"},
	}

	pT := parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
	cT := parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := StrToParquetType(tt.s, pT, cT, common.IntervalByteLen, 0)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}

	t.Run("legacy_integer_accepted", func(t *testing.T) {
		res, err := StrToParquetType("1234567890123", pT, cT, common.IntervalByteLen, 0)
		require.NoError(t, err)
		require.Equal(t, StrIntToBinary("1234567890123", "LittleEndian", common.IntervalByteLen, false), res)
	})
}

func TestStrToParquetType_FixedLenByteArrayWidth(t *testing.T) {
	testCases := map[string]struct {
		inputStr string
		length   int
		expected string
		errMsg   string
	}{
		"no-length-base64": {
			"SGVsbG8gV29ybGQ=", 0, "Hello World", "",
		},
		"base64-matches-width": {
			"SGVsbG8gV29ybGQ=", 11, "Hello World", "",
		},
		"decoded-width-decides": {
			// Valid base64 that decodes to 12 bytes; the 16 characters themselves are
			// no longer a second reading the width could pick.
			"0123456789abcdef", 16, "",
			`FIXED_LEN_BYTE_ARRAY "0123456789abcdef" decodes to 12 bytes, column length is 16`,
		},
		"width-mismatch": {
			"SGVsbG8gV29ybGQ=", 5, "",
			`FIXED_LEN_BYTE_ARRAY "SGVsbG8gV29ybGQ=" decodes to 11 bytes, column length is 5`,
		},
		"not-base64": {
			"abc", 5, "",
			`FIXED_LEN_BYTE_ARRAY "abc" is not valid base64`,
		},
		"empty-string": {
			"", 5, "",
			`FIXED_LEN_BYTE_ARRAY "" decodes to 0 bytes, column length is 5`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			res, err := StrToParquetType(
				tc.inputStr,
				parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
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

func TestStrToParquetTypeTextAllocations(t *testing.T) {
	pT := parquet.Type_BYTE_ARRAY
	cT := parquet.ConvertedType_UTF8
	for _, tc := range []struct {
		name string
		opts []ValueOption
		max  float64
	}{
		{"default", nil, 1}, {"explicit off", []ValueOption{WithEnforceUTF8(false)}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got any
			var err error
			allocations := testing.AllocsPerRun(100, func() {
				got, err = StrToParquetTypeWithLogical("text", &pT, &cT, nil, 0, 0, tc.opts...)
			})
			require.NoError(t, err)
			require.Equal(t, "text", got)
			require.LessOrEqual(t, allocations, tc.max, "disabled validation must not allocate a boxed string")
		})
	}
}
