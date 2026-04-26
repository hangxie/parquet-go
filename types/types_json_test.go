package types

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

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
			name:     "nil_interface_value",
			value:    (*interface{})(nil),
			pT:       parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			expected: nil,
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
			name:  "map_fallback_to_string_based",
			value: map[string]any{"key": "val"},
			pT:    parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			// fmt.Sprintf("%v", val) produces "map[key:val]" - returned as string
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
