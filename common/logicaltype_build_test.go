package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestNewLogicalTypeFromFieldsMap(t *testing.T) {
	v1 := int8(1)
	crs := "OGC:CRS84"
	testCases := map[string]struct {
		fields   map[string]string
		expected parquet.LogicalType
		errMsg   string
	}{
		"missing-logicaltype": {map[string]string{}, parquet.LogicalType{}, "missing logicaltype"},
		"string": {
			map[string]string{"logicaltype": "STRING"},
			parquet.LogicalType{STRING: &parquet.StringType{}},
			"",
		},
		"list": {
			map[string]string{"logicaltype": "LIST"},
			parquet.LogicalType{LIST: &parquet.ListType{}},
			"",
		},
		"map": {
			map[string]string{"logicaltype": "MAP"},
			parquet.LogicalType{MAP: &parquet.MapType{}},
			"",
		},
		"enum": {
			map[string]string{"logicaltype": "ENUM"},
			parquet.LogicalType{ENUM: &parquet.EnumType{}},
			"",
		},
		"date": {
			map[string]string{"logicaltype": "DATE"},
			parquet.LogicalType{DATE: &parquet.DateType{}},
			"",
		},
		"json": {
			map[string]string{"logicaltype": "JSON"},
			parquet.LogicalType{JSON: &parquet.JsonType{}},
			"",
		},
		"bson": {
			map[string]string{"logicaltype": "BSON"},
			parquet.LogicalType{BSON: &parquet.BsonType{}},
			"",
		},
		"uuid": {
			map[string]string{"logicaltype": "UUID"},
			parquet.LogicalType{UUID: &parquet.UUIDType{}},
			"",
		},
		"decimal-bad-precision": {
			map[string]string{"logicaltype": "DECIMAL"},
			parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			"parse logicaltype.precision value",
		},
		"decimal-bad-scale": {
			map[string]string{"logicaltype": "DECIMAL", "logicaltype.precision": "10"},
			parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			"parse logicaltype.scale value",
		},
		"decimal-good": {
			map[string]string{"logicaltype": "DECIMAL", "logicaltype.precision": "10", "logicaltype.scale": "2"},
			parquet.LogicalType{DECIMAL: &parquet.DecimalType{Precision: 10, Scale: 2}},
			"",
		},
		"time-bad-adjustutc": {
			map[string]string{"logicaltype": "TIME"},
			parquet.LogicalType{TIME: &parquet.TimeType{}},
			"parse logicaltype.isadjustedtoutc as bool",
		},
		"time-bad-unit": {
			map[string]string{"logicaltype": "TIME", "logicaltype.isadjustedtoutc": "true"},
			parquet.LogicalType{TIME: &parquet.TimeType{}},
			"logicaltype time error, unknown unit:",
		},
		"time-good": {
			map[string]string{"logicaltype": "TIME", "logicaltype.isadjustedtoutc": "true", "logicaltype.unit": "MILLIS"},
			parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			"",
		},
		"timestamp-bad-adjustutc": {
			map[string]string{"logicaltype": "TIMESTAMP"},
			parquet.LogicalType{TIME: &parquet.TimeType{}},
			"parse logicaltype.isadjustedtoutc as bool",
		},
		"timestamp-bad-unit": {
			map[string]string{"logicaltype": "TIMESTAMP", "logicaltype.isadjustedtoutc": "true"},
			parquet.LogicalType{TIME: &parquet.TimeType{}},
			"logicaltype time error, unknown unit:",
		},
		"timestamp-good": {
			map[string]string{"logicaltype": "TIMESTAMP", "logicaltype.isadjustedtoutc": "true", "logicaltype.unit": "MILLIS"},
			parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			"",
		},
		"integer-bad-bitwidth": {
			map[string]string{"logicaltype": "INTEGER"},
			parquet.LogicalType{INTEGER: &parquet.IntType{}},
			"parse logicaltype.bitwidth as int32",
		},
		"integer-bad-signed": {
			map[string]string{"logicaltype": "INTEGER", "logicaltype.bitwidth": "64"},
			parquet.LogicalType{INTEGER: &parquet.IntType{}},
			"parse logicaltype.issigned as boolean:",
		},
		"integer-good": {
			map[string]string{"logicaltype": "INTEGER", "logicaltype.bitwidth": "64", "logicaltype.issigned": "true"},
			parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: true}},
			"",
		},
		"bad-logicaltype": {
			map[string]string{"logicaltype": "foobar"},
			parquet.LogicalType{STRING: &parquet.StringType{}},
			"unknown logicaltype:",
		},
		// Newly added logical types
		"float16": {
			map[string]string{"logicaltype": "FLOAT16"},
			parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			"",
		},
		"variant-with-version": {
			map[string]string{"logicaltype": "VARIANT", "logicaltype.specification_version": "1"},
			parquet.LogicalType{VARIANT: &parquet.VariantType{SpecificationVersion: &v1}},
			"",
		},
		"geometry-with-crs": {
			map[string]string{"logicaltype": "GEOMETRY", "logicaltype.crs": "OGC:CRS84"},
			parquet.LogicalType{GEOMETRY: &parquet.GeometryType{CRS: &crs}},
			"",
		},
		"geography-with-crs-and-algo": {
			map[string]string{"logicaltype": "GEOGRAPHY", "logicaltype.crs": "OGC:CRS84", "logicaltype.algorithm": "VINCENTY"},
			parquet.LogicalType{GEOGRAPHY: &parquet.GeographyType{CRS: &crs, Algorithm: parquet.EdgeInterpolationAlgorithmPtr(parquet.EdgeInterpolationAlgorithm_VINCENTY)}},
			"",
		},
		"geography-bad-algo": {
			map[string]string{"logicaltype": "GEOGRAPHY", "logicaltype.algorithm": "UNKNOWN"},
			parquet.LogicalType{GEOGRAPHY: &parquet.GeographyType{}},
			"logicaltype geography error, unknown algorithm:",
		},
		"unknown": {
			map[string]string{"logicaltype": "UNKNOWN"},
			parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			"",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := newLogicalTypeFromFieldsMap(tc.fields)
			if tc.errMsg == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, *actual)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func TestNewTimeUnitFromString(t *testing.T) {
	testCases := map[string]struct {
		unit     string
		expected parquet.TimeUnit
		errMsg   string
	}{
		"MILLIS": {"MILLIS", parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}, ""},
		"MICROS": {"MICROS", parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}, ""},
		"NANOS":  {"NANOS", parquet.TimeUnit{NANOS: parquet.NewNanoSeconds()}, ""},
		"foobar": {"foobar", parquet.TimeUnit{}, "logicaltype time error, unknown unit:"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := newTimeUnitFromString(tc.unit)
			if tc.errMsg == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, *actual)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func TestNewEdgeInterpolationAlgorithmFromString(t *testing.T) {
	tests := []struct {
		in     string
		want   parquet.EdgeInterpolationAlgorithm
		errStr string
	}{
		{"SPHERICAL", parquet.EdgeInterpolationAlgorithm_SPHERICAL, ""},
		{"VINCENTY", parquet.EdgeInterpolationAlgorithm_VINCENTY, ""},
		{"THOMAS", parquet.EdgeInterpolationAlgorithm_THOMAS, ""},
		{"ANDOYER", parquet.EdgeInterpolationAlgorithm_ANDOYER, ""},
		{"KARNEY", parquet.EdgeInterpolationAlgorithm_KARNEY, ""},
		{"", 0, ""},
		{"bad", 0, "unknown algorithm:"},
	}
	for _, tc := range tests {
		got, err := newEdgeInterpolationAlgorithmFromString(tc.in)
		if tc.errStr == "" {
			require.NoError(t, err)
			if tc.in == "" {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
				require.Equal(t, tc.want, *got)
			}
		} else {
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errStr)
		}
	}
}
