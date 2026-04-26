package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestNewLogicalTypeFromConvertedType(t *testing.T) {
	testCases := map[string]struct {
		schema   parquet.SchemaElement
		tag      Tag
		expected *parquet.LogicalType
	}{
		"nil-schema": {parquet.SchemaElement{}, Tag{}, nil},
		"int8": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_INT_8)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 8, IsSigned: true}},
		},
		"int16": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_INT_16)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 16, IsSigned: true}},
		},
		"int32": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_INT_32)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: true}},
		},
		"int64": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_INT_64)},
			Tag{fieldAttr: fieldAttr{Type: "INT64"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: true}},
		},
		"uint8": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_UINT_8)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 8, IsSigned: false}},
		},
		"uint16": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_UINT_16)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 16, IsSigned: false}},
		},
		"uint32": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_UINT_32)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: false}},
		},
		"uint64": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_UINT_64)},
			Tag{fieldAttr: fieldAttr{Type: "INT64"}},
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: false}},
		},
		"decimal": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_DECIMAL)},
			Tag{fieldAttr: fieldAttr{Type: "INT32", Precision: 10, Scale: 9}},
			&parquet.LogicalType{DECIMAL: &parquet.DecimalType{Precision: 10, Scale: 9}},
		},
		"date": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_DATE)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{DATE: &parquet.DateType{}},
		},
		"time-millis": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_TIME_MILLIS)},
			Tag{fieldAttr: fieldAttr{Type: "INT64", isAdjustedToUTC: true}},
			&parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
		},
		"time-micros": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_TIME_MICROS)},
			Tag{fieldAttr: fieldAttr{Type: "INT64", isAdjustedToUTC: false}},
			&parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: false, Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
		},
		"timestamp-millis": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_TIMESTAMP_MILLIS)},
			Tag{fieldAttr: fieldAttr{Type: "INT64", isAdjustedToUTC: true}},
			&parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
		},
		"timestamp-micros": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT64), ConvertedType: ToPtr(parquet.ConvertedType_TIMESTAMP_MICROS)},
			Tag{fieldAttr: fieldAttr{Type: "INT64", isAdjustedToUTC: false}},
			&parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: false, Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
		},
		"bson": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_BYTE_ARRAY), ConvertedType: ToPtr(parquet.ConvertedType_BSON)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{BSON: &parquet.BsonType{}},
		},
		"enum": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_INT32), ConvertedType: ToPtr(parquet.ConvertedType_ENUM)},
			Tag{fieldAttr: fieldAttr{Type: "INT32"}},
			&parquet.LogicalType{ENUM: &parquet.EnumType{}},
		},
		"json": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_BYTE_ARRAY), ConvertedType: ToPtr(parquet.ConvertedType_JSON)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{JSON: &parquet.JsonType{}},
		},
		"list": {
			parquet.SchemaElement{Type: nil, ConvertedType: ToPtr(parquet.ConvertedType_LIST)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{LIST: &parquet.ListType{}},
		},
		"map": {
			parquet.SchemaElement{Type: nil, ConvertedType: ToPtr(parquet.ConvertedType_MAP)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{MAP: &parquet.MapType{}},
		},
		"utf8": {
			parquet.SchemaElement{Type: ToPtr(parquet.Type_BYTE_ARRAY), ConvertedType: ToPtr(parquet.ConvertedType_UTF8)},
			Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY"}},
			&parquet.LogicalType{STRING: &parquet.StringType{}},
		},
		"interval": {
			parquet.SchemaElement{Type: nil, ConvertedType: ToPtr(parquet.ConvertedType_INTERVAL)},
			Tag{fieldAttr: fieldAttr{}},
			nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := newLogicalTypeFromConvertedType(&tc.schema, &tc.tag)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestConvertedTypeFromLogicalType(t *testing.T) {
	testCases := map[string]struct {
		logicalType *parquet.LogicalType
		expected    *parquet.ConvertedType
	}{
		"nil": {nil, nil},
		"string": {
			&parquet.LogicalType{STRING: &parquet.StringType{}},
			ToPtr(parquet.ConvertedType_UTF8),
		},
		"map": {
			&parquet.LogicalType{MAP: &parquet.MapType{}},
			ToPtr(parquet.ConvertedType_MAP),
		},
		"list": {
			&parquet.LogicalType{LIST: &parquet.ListType{}},
			ToPtr(parquet.ConvertedType_LIST),
		},
		"enum": {
			&parquet.LogicalType{ENUM: &parquet.EnumType{}},
			ToPtr(parquet.ConvertedType_ENUM),
		},
		"decimal": {
			&parquet.LogicalType{DECIMAL: &parquet.DecimalType{Precision: 10, Scale: 2}},
			ToPtr(parquet.ConvertedType_DECIMAL),
		},
		"date": {
			&parquet.LogicalType{DATE: &parquet.DateType{}},
			ToPtr(parquet.ConvertedType_DATE),
		},
		"time-millis": {
			&parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			ToPtr(parquet.ConvertedType_TIME_MILLIS),
		},
		"time-micros": {
			&parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
			ToPtr(parquet.ConvertedType_TIME_MICROS),
		},
		"time-nanos": {
			&parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{NANOS: parquet.NewNanoSeconds()}}},
			nil, // NANOS has no corresponding ConvertedType
		},
		"time-nil-unit": {
			&parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: true}},
			nil,
		},
		"timestamp-millis": {
			&parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			ToPtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
		},
		"timestamp-micros": {
			&parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
			ToPtr(parquet.ConvertedType_TIMESTAMP_MICROS),
		},
		"timestamp-nanos": {
			&parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{NANOS: parquet.NewNanoSeconds()}}},
			nil, // NANOS has no corresponding ConvertedType
		},
		"timestamp-nil-unit": {
			&parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: true}},
			nil,
		},
		"int8": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 8, IsSigned: true}},
			ToPtr(parquet.ConvertedType_INT_8),
		},
		"int16": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 16, IsSigned: true}},
			ToPtr(parquet.ConvertedType_INT_16),
		},
		"int32": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: true}},
			ToPtr(parquet.ConvertedType_INT_32),
		},
		"int64": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: true}},
			ToPtr(parquet.ConvertedType_INT_64),
		},
		"uint8": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 8, IsSigned: false}},
			ToPtr(parquet.ConvertedType_UINT_8),
		},
		"uint16": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 16, IsSigned: false}},
			ToPtr(parquet.ConvertedType_UINT_16),
		},
		"uint32": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: false}},
			ToPtr(parquet.ConvertedType_UINT_32),
		},
		"uint64": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: false}},
			ToPtr(parquet.ConvertedType_UINT_64),
		},
		"integer-invalid-bitwidth": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 4, IsSigned: true}},
			nil,
		},
		"json": {
			&parquet.LogicalType{JSON: &parquet.JsonType{}},
			ToPtr(parquet.ConvertedType_JSON),
		},
		"bson": {
			&parquet.LogicalType{BSON: &parquet.BsonType{}},
			ToPtr(parquet.ConvertedType_BSON),
		},
		"uuid": {
			&parquet.LogicalType{UUID: &parquet.UUIDType{}},
			nil, // UUID has no corresponding ConvertedType
		},
		"float16": {
			&parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			nil, // FLOAT16 has no corresponding ConvertedType
		},
		"variant": {
			&parquet.LogicalType{VARIANT: &parquet.VariantType{}},
			nil, // VARIANT has no corresponding ConvertedType
		},
		"geometry": {
			&parquet.LogicalType{GEOMETRY: &parquet.GeometryType{}},
			nil, // GEOMETRY has no corresponding ConvertedType
		},
		"geography": {
			&parquet.LogicalType{GEOGRAPHY: &parquet.GeographyType{}},
			nil, // GEOGRAPHY has no corresponding ConvertedType
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := convertedTypeFromLogicalType(tc.logicalType)
			require.Equal(t, tc.expected, actual)
		})
	}
}

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

func TestGetLogicalTypeFromTag(t *testing.T) {
	tests := []struct {
		name     string
		tag      *Tag
		hasType  bool
		validate func(t *testing.T, lt *parquet.LogicalType)
	}{
		{
			name:    "nil_logicaltype_fields",
			tag:     &Tag{},
			hasType: false,
		},
		{
			name: "empty_logicaltype_fields",
			tag: &Tag{
				fieldAttr: fieldAttr{
					logicalTypeFields: map[string]string{},
				},
			},
			hasType: false,
		},
		{
			name: "variant_logicaltype",
			tag: &Tag{
				fieldAttr: fieldAttr{
					logicalTypeFields: map[string]string{
						"logicaltype": "VARIANT",
					},
				},
			},
			hasType: true,
			validate: func(t *testing.T, lt *parquet.LogicalType) {
				require.True(t, lt.IsSetVARIANT())
			},
		},
		{
			name: "variant_with_specification_version",
			tag: &Tag{
				fieldAttr: fieldAttr{
					logicalTypeFields: map[string]string{
						"logicaltype":                       "VARIANT",
						"logicaltype.specification_version": "1",
					},
				},
			},
			hasType: true,
			validate: func(t *testing.T, lt *parquet.LogicalType) {
				require.True(t, lt.IsSetVARIANT())
				require.Equal(t, int8(1), lt.GetVARIANT().GetSpecificationVersion())
			},
		},
		{
			name: "string_logicaltype",
			tag: &Tag{
				fieldAttr: fieldAttr{
					logicalTypeFields: map[string]string{
						"logicaltype": "STRING",
					},
				},
			},
			hasType: true,
			validate: func(t *testing.T, lt *parquet.LogicalType) {
				require.True(t, lt.IsSetSTRING())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := GetLogicalTypeFromTag(tc.tag)
			require.NoError(t, err)
			if tc.hasType {
				require.NotNil(t, result)
				if tc.validate != nil {
					tc.validate(t, result)
				}
			} else {
				require.Nil(t, result)
			}
		})
	}
}
