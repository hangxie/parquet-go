package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func TestDeepCopy(t *testing.T) {
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
			dst := &Tag{}
			DeepCopy(&tc.src, dst)
			require.Equal(t, tc.expected, *dst)
		})
	}
}

func TestFieldAttr_Update(t *testing.T) {
	testCases := map[string]struct {
		key, val string
		expected fieldAttr
		errMsg   string
	}{
		"type":                 {"type", "BOOLEAN", fieldAttr{Type: "BOOLEAN"}, ""},
		"convertedtype":        {"convertedtype", "UTF8", fieldAttr{convertedType: "UTF8"}, ""},
		"length-good":          {"length", "123", fieldAttr{Length: 123}, ""},
		"length-bad":           {"length", "abc", fieldAttr{}, "parse length value"},
		"scale-good":           {"scale", "123", fieldAttr{Scale: 123}, ""},
		"scale-bad":            {"scale", "abc", fieldAttr{}, "parse scale value"},
		"precision-good":       {"precision", "123", fieldAttr{Precision: 123}, ""},
		"precision-bad":        {"precision", "abc", fieldAttr{}, "parse precision value"},
		"fieldid-good":         {"fieldid", "123", fieldAttr{fieldID: 123}, ""},
		"fieldid-bad":          {"fieldid", "abc", fieldAttr{}, "parse fieldid value"},
		"isadjustedtoutc-good": {"isadjustedtoutc", "true", fieldAttr{isAdjustedToUTC: true}, ""},
		"isadjustedtoutc-bad":  {"isadjustedtoutc", "abc", fieldAttr{}, "parse isadjustedtoutc value"},
		"omitstats-good":       {"omitstats", "true", fieldAttr{OmitStats: true}, ""},
		"omitstats-bad":        {"omitstats", "abc", fieldAttr{}, "parse omitstats value"},
		"repetitiontype-good":  {"repetitiontype", "repeated", fieldAttr{RepetitionType: parquet.FieldRepetitionType_REPEATED}, ""},
		"repetitiontype-bad":   {"repetitiontype", "foobar", fieldAttr{}, "parse repetitiontype:"},
		"encoding-good":        {"encoding", "plain", fieldAttr{Encoding: parquet.Encoding_PLAIN}, ""},
		"encoding-bad":         {"encoding", "foobar", fieldAttr{}, "parse encoding:"},
		"logicaltype":          {"logicaltype.foo", "bar", fieldAttr{logicalTypeFields: map[string]string{"logicaltype.foo": "bar"}}, ""},
		"unknown-tag":          {"unknown-tag.foo", "foobar", fieldAttr{}, "unrecognized tag"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := fieldAttr{}
			err := actual.update(tc.key, tc.val)
			if tc.errMsg == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, actual)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func TestGetKeyTagMap(t *testing.T) {
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

func TestGetValueTagMap(t *testing.T) {
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

func TestIsChildPath(t *testing.T) {
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

func TestNewSchemaElementFromTagMap(t *testing.T) {
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
			"create logicaltype from field map",
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
		"rle-byte-array-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "BYTE_ARRAY",
					Encoding: parquet.Encoding_RLE,
				},
			},
			parquet.SchemaElement{},
			"RLE encoding is not supported for BYTE_ARRAY",
		},
		"rle-fixed-len-byte-array-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "FIXED_LEN_BYTE_ARRAY",
					Encoding: parquet.Encoding_RLE,
				},
			},
			parquet.SchemaElement{},
			"RLE encoding is not supported for FIXED_LEN_BYTE_ARRAY",
		},
		"rle-boolean-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "BOOLEAN",
					Encoding: parquet.Encoding_RLE,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_BOOLEAN),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := NewSchemaElementFromTagMap(&tc.tag)
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

func TestPathStrIndex(t *testing.T) {
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

func TestPathToStr(t *testing.T) {
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

func TestReformPathStr(t *testing.T) {
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

func TestStrToPath(t *testing.T) {
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

func TestStringToTag(t *testing.T) {
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
		"tag-bad":          {"foo=bar", Tag{}, "parse tag"},
		"key-good":         {"keytype=INT32,KeyConvertedtype=TIME", Tag{Key: fieldAttr{Type: "INT32", convertedType: "TIME"}}, ""},
		"key-bad":          {"keyfoo=bar", Tag{}, "parse tag"},
		"value-good":       {"valuetype=INT32, valuerepetitiontype=REPEATED", Tag{Value: fieldAttr{Type: "INT32", RepetitionType: parquet.FieldRepetitionType_REPEATED}}, ""},
		"value-bad":        {"valuefoo=bar", Tag{}, "parse tag"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := StringToTag(tc.tag)
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

func TestStringToVariableName(t *testing.T) {
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

func TestToPtr(t *testing.T) {
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

func TestHeadToUpper(t *testing.T) {
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
