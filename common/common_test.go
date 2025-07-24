package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_FieldAttr_Update(t *testing.T) {
	testCases := map[string]struct {
		key, val string
		expected fieldAttr
		errMsg   string
	}{
		"type":                 {"type", "BOOLEAN", fieldAttr{Type: "BOOLEAN"}, ""},
		"convertedtype":        {"convertedtype", "UTF8", fieldAttr{convertedType: "UTF8"}, ""},
		"length-good":          {"length", "123", fieldAttr{Length: 123}, ""},
		"length-bad":           {"length", "abc", fieldAttr{}, "failed to parse length:"},
		"scale-good":           {"scale", "123", fieldAttr{Scale: 123}, ""},
		"scale-bad":            {"scale", "abc", fieldAttr{}, "failed to parse scale:"},
		"precision-good":       {"precision", "123", fieldAttr{Precision: 123}, ""},
		"precision-bad":        {"precision", "abc", fieldAttr{}, "failed to parse precision:"},
		"fieldid-good":         {"fieldid", "123", fieldAttr{fieldID: 123}, ""},
		"fieldid-bad":          {"fieldid", "abc", fieldAttr{}, "failed to parse fieldid:"},
		"isadjustedtoutc-good": {"isadjustedtoutc", "true", fieldAttr{isAdjustedToUTC: true}, ""},
		"isadjustedtoutc-bad":  {"isadjustedtoutc", "abc", fieldAttr{}, "failed to parse isadjustedtoutc:"},
		"omitstats-good":       {"omitstats", "true", fieldAttr{OmitStats: true}, ""},
		"omitstats-bad":        {"omitstats", "abc", fieldAttr{}, "failed to parse omitstats:"},
		"repetitiontype-good":  {"repetitiontype", "repeated", fieldAttr{RepetitionType: parquet.FieldRepetitionType_REPEATED}, ""},
		"repetitiontype-bad":   {"repetitiontype", "foobar", fieldAttr{}, "failed to parse repetitiontype:"},
		"encoding-good":        {"encoding", "plain", fieldAttr{Encoding: parquet.Encoding_PLAIN}, ""},
		"encoding-bad":         {"encoding", "foobar", fieldAttr{}, "failed to parse encoding:"},
		"logicaltype":          {"logicaltype.foo", "bar", fieldAttr{logicalTypeFields: map[string]string{"logicaltype.foo": "bar"}}, ""},
		"unknown-tag":          {"unknown-tag.foo", "foobar", fieldAttr{}, "unrecognized tag"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := fieldAttr{}
			err := actual.update(tc.key, tc.val)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_NewTag(t *testing.T) {
	actual := NewTag()
	require.NotNil(t, actual)
	require.Equal(t, Tag{}, *actual)
}

func Test_StringToTag(t *testing.T) {
	testCases := map[string]struct {
		tag      string
		expected Tag
		errMsg   string
	}{
		"missing=":         {" name ", Tag{}, "expect 'key=value' but got"},
		"name-only":        {"NAME = John", Tag{InName: "John", ExName: "John"}, ""},
		"inname-only":      {" inname = John ", Tag{InName: "John"}, ""},
		"name-then-inname": {" name=John,inname = Jane ", Tag{InName: "Jane", ExName: "John"}, ""},
		"inname-then-name": {" inname=John,name = Jane ", Tag{InName: "John", ExName: "Jane"}, ""},
		"tag-good":         {"type=BYTE_ARRAY,convertedtype=UTF8", Tag{fieldAttr: fieldAttr{Type: "BYTE_ARRAY", convertedType: "UTF8"}}, ""},
		"tag-bad":          {"foo=bar", Tag{}, "failed to parse tag"},
		"key-good":         {"keytype=INT32,KeyConvertedtype=TIME", Tag{Key: fieldAttr{Type: "INT32", convertedType: "TIME"}}, ""},
		"key-bad":          {"keyfoo=bar", Tag{}, "failed to parse tag"},
		"value-good":       {"valuetype=INT32, valuerepetitiontype=REPEATED", Tag{Value: fieldAttr{Type: "INT32", RepetitionType: parquet.FieldRepetitionType_REPEATED}}, ""},
		"value-bad":        {"valuefoo=bar", Tag{}, "failed to parse tag"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := StringToTag(tc.tag)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, *actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_NewSchemaElementFromTagMap(t *testing.T) {
	testCases := map[string]struct {
		tag      Tag
		expected parquet.SchemaElement
		errMsg   string
	}{
		"missing-type": {Tag{}, parquet.SchemaElement{}, "not a valid Type string"},
		"logicaltype-bad": {
			Tag{
				fieldAttr: fieldAttr{
					Type:              "BYTE_ARRAY",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
			},
			parquet.SchemaElement{},
			"failed to create logicaltype from field map",
		},
		"all-good": {
			Tag{
				fieldAttr: fieldAttr{
					Type:           "BYTE_ARRAY",
					convertedType:  "UTF8",
					RepetitionType: parquet.FieldRepetitionType_REQUIRED,
					Length:         10,
					Scale:          9,
					Precision:      8,
					fieldID:        7,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(10)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(9)),
				Precision:      ToPtr(int32(8)),
				FieldID:        ToPtr(int32(7)),
				ConvertedType:  ToPtr(parquet.ConvertedType_UTF8),
				LogicalType:    ToPtr(parquet.LogicalType{STRING: &parquet.StringType{}}),
			},
			"",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := NewSchemaElementFromTagMap(&tc.tag)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, *actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_newTimeUnitFromString(t *testing.T) {
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
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, *actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_newLogicalTypeFromFieldsMap(t *testing.T) {
	testCases := map[string]struct {
		fields   map[string]string
		expected parquet.LogicalType
		errMsg   string
	}{
		"missing-logicaltype": {map[string]string{}, parquet.LogicalType{}, "does not have logicaltype"},
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
			"cannot parse logicaltype.precision as int32",
		},
		"decimal-bad-scale": {
			map[string]string{"logicaltype": "DECIMAL", "logicaltype.precision": "10"},
			parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			"cannot parse logicaltype.scale as int32",
		},
		"decimal-good": {
			map[string]string{"logicaltype": "DECIMAL", "logicaltype.precision": "10", "logicaltype.scale": "2"},
			parquet.LogicalType{DECIMAL: &parquet.DecimalType{Precision: 10, Scale: 2}},
			"",
		},
		"time-bad-adjustutc": {
			map[string]string{"logicaltype": "TIME"},
			parquet.LogicalType{TIME: &parquet.TimeType{}},
			"cannot parse logicaltype.isadjustedtoutc as bool",
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
			"cannot parse logicaltype.isadjustedtoutc as bool",
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
			"cannot parse logicaltype.bitwidth as int32",
		},
		"integer-bad-signed": {
			map[string]string{"logicaltype": "INTEGER", "logicaltype.bitwidth": "64"},
			parquet.LogicalType{INTEGER: &parquet.IntType{}},
			"cannot parse logicaltype.issigned as boolean:",
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
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := newLogicalTypeFromFieldsMap(tc.fields)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, *actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_newLogicalTypeFromConvertedType(t *testing.T) {
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

func Test_DeepCopy(t *testing.T) {
	testCases := map[string]struct {
		src      Tag
		expected Tag
	}{
		"empty": {Tag{}, Tag{}},
		"with-logicaltype": {
			Tag{
				InName: "inname",
				ExName: "exname",
				fieldAttr: fieldAttr{
					Type:              "BOOLEAN",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
				Key: fieldAttr{
					Type:              "BYTE_ARRAY",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
				Value: fieldAttr{
					Type:              "INT32",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
			},
			Tag{
				InName: "inname",
				ExName: "exname",
				fieldAttr: fieldAttr{
					Type: "BOOLEAN",
				},
				Key: fieldAttr{
					Type: "BYTE_ARRAY",
				},
				Value: fieldAttr{
					Type: "INT32",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			dst := NewTag()
			DeepCopy(&tc.src, dst)
			require.Equal(t, tc.expected, *dst)
		})
	}
}

func Test_GetKeyTagMap(t *testing.T) {
	testCases := map[string]struct {
		src      Tag
		expected Tag
	}{
		"empty": {Tag{}, Tag{InName: "Key", ExName: "key"}},
		"with-logicaltype": {
			Tag{
				Key: fieldAttr{
					Type:              "UNT32",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
			},
			Tag{
				InName: "Key",
				ExName: "key",
				fieldAttr: fieldAttr{
					Type: "UNT32",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			dst := GetKeyTagMap(&tc.src)
			require.Equal(t, tc.expected, *dst)
		})
	}
}

func Test_GetValueTagMap(t *testing.T) {
	testCases := map[string]struct {
		src      Tag
		expected Tag
	}{
		"empty": {Tag{}, Tag{InName: "Value", ExName: "value"}},
		"with-logicaltype": {
			Tag{
				Value: fieldAttr{
					Type:              "UNT32",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
				},
			},
			Tag{
				InName: "Value",
				ExName: "value",
				fieldAttr: fieldAttr{
					Type: "UNT32",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			dst := GetValueTagMap(&tc.src)
			require.Equal(t, tc.expected, *dst)
		})
	}
}

func Test_StringToVariableName(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected string
	}{
		"empty":        {"", ""},
		"invalid-char": {"!@#", "PARGO_PREFIX_336435"},
		"no-change":    {"Name", "Name"},
		"title":        {"name", "Name"},
		"prefix":       {"12", "PARGO_PREFIX_12"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			varName := StringToVariableName(tc.str)
			require.Equal(t, tc.expected, varName)
		})
	}
}

func Test_headToUpper(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected string
	}{
		"empty":          {"", ""},
		"lowercase":      {"hello", "Hello"},
		"uppercase":      {"HeHH", "HeHH"},
		"not-alphabetic": {"123", "PARGO_PREFIX_123"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := headToUpper(tc.str)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func Test_str2Int32(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected int32
		errMsg   string
	}{
		"bad":  {"abc", 0, "strconv.Atoi: parsing"},
		"good": {"123", 123, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := str2Int32(tc.str)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_str2Bool(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected bool
		errMsg   string
	}{
		"bad":  {"abc", false, "strconv.ParseBool: parsing"},
		"good": {"true", true, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := str2Bool(tc.str)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_ReformPathStr(t *testing.T) {
	testCases := map[string]struct {
		path     string
		expected string
	}{
		"test-case-1": {"a.b.c", "a\x01b\x01c"},
		"test-case-2": {"a..c", "a\x01\x01c"},
		"test-case-3": {"", ""},
		"test-case-4": {"abc", "abc"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, ReformPathStr(tc.path))
		})
	}
}

func Test_PathToStr(t *testing.T) {
	testCases := map[string]struct {
		path     []string
		expected string
	}{
		"test-case-1": {[]string{"a", "b", "c"}, "a\x01b\x01c"},
		"test-case-2": {[]string{"a", "", "c"}, "a\x01\x01c"},
		"test-case-3": {[]string{}, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, PathToStr(tc.path))
		})
	}
}

func Test_StrToPath(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected []string
	}{
		"test-case-1": {"a\x01b\x01c", []string{"a", "b", "c"}},
		"test-case-2": {"a\x01\x01c", []string{"a", "", "c"}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, StrToPath(tc.str))
		})
	}
}

func Test_PathStrIndex(t *testing.T) {
	testCases := map[string]struct {
		path     string
		expected int
	}{
		"test-case-1": {"a\x01b\x01c", 3},
		"test-case-2": {"a\x01\x01c", 3},
		"test-case-3": {"", 1},
		"test-case-4": {"abc", 1},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, PathStrIndex(tc.path))
		})
	}
}

func Test_IsChildPath(t *testing.T) {
	testCases := map[string]struct {
		parent   string
		child    string
		expected bool
	}{
		"test-case-1": {"a\x01b\x01c", "a\x01b\x01c", true},
		"test-case-2": {"a\x01b", "a\x01b\x01c", true},
		"test-case-3": {"a\x01b\x01", "a\x01b\x01c", false},
		"test-case-4": {"x\x01b\x01c", "a\x01b\x01c", false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsChildPath(tc.parent, tc.child))
		})
	}
}

func Test_ToPtr(t *testing.T) {
	testCases := map[string]struct {
		val any
	}{
		"bool":    {true},
		"int32":   {int32(1)},
		"int64":   {int64(1)},
		"string":  {"012345678901"},
		"float32": {float32(0.1)},
		"float64": {float64(0.1)},
		"slice":   {[]int32{1, 2, 3}},
		"map":     {map[string]int32{"a": 1, "b": 2, "c": 3}},
		"struct": {
			struct {
				id   uint64
				name string
			}{123, "abc"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ptr := ToPtr(tc.val)
			require.NotNil(t, ptr)
			require.Equal(t, tc.val, *ptr)
		})
	}
}
