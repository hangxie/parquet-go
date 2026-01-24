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
		"type":                     {"type", "BOOLEAN", fieldAttr{Type: "BOOLEAN"}, ""},
		"convertedtype":            {"convertedtype", "UTF8", fieldAttr{convertedType: "UTF8"}, ""},
		"length-good":              {"length", "123", fieldAttr{Length: 123}, ""},
		"length-bad":               {"length", "abc", fieldAttr{}, "parse length value"},
		"scale-good":               {"scale", "123", fieldAttr{Scale: 123}, ""},
		"scale-bad":                {"scale", "abc", fieldAttr{}, "parse scale value"},
		"precision-good":           {"precision", "123", fieldAttr{Precision: 123}, ""},
		"precision-bad":            {"precision", "abc", fieldAttr{}, "parse precision value"},
		"fieldid-good":             {"fieldid", "123", fieldAttr{fieldID: 123}, ""},
		"fieldid-bad":              {"fieldid", "abc", fieldAttr{}, "parse fieldid value"},
		"isadjustedtoutc-good":     {"isadjustedtoutc", "true", fieldAttr{isAdjustedToUTC: true}, ""},
		"isadjustedtoutc-bad":      {"isadjustedtoutc", "abc", fieldAttr{}, "parse isadjustedtoutc value"},
		"omitstats-good":           {"omitstats", "true", fieldAttr{OmitStats: true}, ""},
		"omitstats-bad":            {"omitstats", "abc", fieldAttr{}, "parse omitstats value"},
		"repetitiontype-good":      {"repetitiontype", "repeated", fieldAttr{RepetitionType: parquet.FieldRepetitionType_REPEATED}, ""},
		"repetitiontype-bad":       {"repetitiontype", "foobar", fieldAttr{}, "parse repetitiontype:"},
		"encoding-good":            {"encoding", "plain", fieldAttr{Encoding: parquet.Encoding_PLAIN}, ""},
		"encoding-bad":             {"encoding", "foobar", fieldAttr{}, "parse encoding:"},
		"compression-snappy":       {"compression", "snappy", fieldAttr{CompressionType: ToPtr(parquet.CompressionCodec_SNAPPY)}, ""},
		"compression-gzip":         {"compression", "GZIP", fieldAttr{CompressionType: ToPtr(parquet.CompressionCodec_GZIP)}, ""},
		"compression-zstd":         {"compression", "zstd", fieldAttr{CompressionType: ToPtr(parquet.CompressionCodec_ZSTD)}, ""},
		"compression-lz4":          {"compression", "LZ4_RAW", fieldAttr{CompressionType: ToPtr(parquet.CompressionCodec_LZ4_RAW)}, ""},
		"compression-uncompressed": {"compression", "UNCOMPRESSED", fieldAttr{CompressionType: ToPtr(parquet.CompressionCodec_UNCOMPRESSED)}, ""},
		"compression-bad":          {"compression", "foobar", fieldAttr{}, "parse compression:"},
		"logicaltype":              {"logicaltype.foo", "bar", fieldAttr{logicalTypeFields: map[string]string{"logicaltype.foo": "bar"}}, ""},
		"unknown-tag":              {"unknown-tag.foo", "foobar", fieldAttr{}, "unrecognized tag"},
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
					Type:              "UNT32",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
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
					Type:              "UNT32",
					logicalTypeFields: map[string]string{"logicaltype.foo": "bar"},
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
			"RLE encoding is only supported for BOOLEAN",
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
			"RLE encoding is only supported for BOOLEAN",
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
		"rle-int32-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "INT32",
					Encoding: parquet.Encoding_RLE,
				},
			},
			parquet.SchemaElement{},
			"RLE encoding is only supported for BOOLEAN",
		},
		"rle-int64-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "INT64",
					Encoding: parquet.Encoding_RLE,
				},
			},
			parquet.SchemaElement{},
			"RLE encoding is only supported for BOOLEAN",
		},
		"rle-float-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "FLOAT",
					Encoding: parquet.Encoding_RLE,
				},
			},
			parquet.SchemaElement{},
			"RLE encoding is only supported for BOOLEAN",
		},
		"rle-double-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "DOUBLE",
					Encoding: parquet.Encoding_RLE,
				},
			},
			parquet.SchemaElement{},
			"RLE encoding is only supported for BOOLEAN",
		},
		"bit-packed-boolean-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "BOOLEAN",
					Encoding: parquet.Encoding_BIT_PACKED,
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
		"bit-packed-int32-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "INT32",
					Encoding: parquet.Encoding_BIT_PACKED,
				},
			},
			parquet.SchemaElement{},
			"BIT_PACKED encoding is deprecated and only supported for BOOLEAN",
		},
		"delta-binary-packed-int32-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "INT32",
					Encoding: parquet.Encoding_DELTA_BINARY_PACKED,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT32),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
		"delta-binary-packed-int64-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "INT64",
					Encoding: parquet.Encoding_DELTA_BINARY_PACKED,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT64),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
		"delta-binary-packed-byte-array-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "BYTE_ARRAY",
					Encoding: parquet.Encoding_DELTA_BINARY_PACKED,
				},
			},
			parquet.SchemaElement{},
			"DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64",
		},
		"delta-binary-packed-float-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "FLOAT",
					Encoding: parquet.Encoding_DELTA_BINARY_PACKED,
				},
			},
			parquet.SchemaElement{},
			"DELTA_BINARY_PACKED encoding is only supported for INT32 and INT64",
		},
		"delta-byte-array-byte-array-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "BYTE_ARRAY",
					Encoding: parquet.Encoding_DELTA_BYTE_ARRAY,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
		"delta-byte-array-fixed-len-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "FIXED_LEN_BYTE_ARRAY",
					Length:   16,
					Encoding: parquet.Encoding_DELTA_BYTE_ARRAY,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(16)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
		"delta-byte-array-int32-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "INT32",
					Encoding: parquet.Encoding_DELTA_BYTE_ARRAY,
				},
			},
			parquet.SchemaElement{},
			"DELTA_BYTE_ARRAY encoding is only supported for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY",
		},
		"delta-length-byte-array-byte-array-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "BYTE_ARRAY",
					Encoding: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
		"delta-length-byte-array-fixed-len-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "FIXED_LEN_BYTE_ARRAY",
					Length:   8,
					Encoding: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
				},
			},
			parquet.SchemaElement{},
			"DELTA_LENGTH_BYTE_ARRAY encoding is only supported for BYTE_ARRAY",
		},
		"delta-length-byte-array-int64-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "INT64",
					Encoding: parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
				},
			},
			parquet.SchemaElement{},
			"DELTA_LENGTH_BYTE_ARRAY encoding is only supported for BYTE_ARRAY",
		},
		"byte-stream-split-float-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "FLOAT",
					Encoding: parquet.Encoding_BYTE_STREAM_SPLIT,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_FLOAT),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
		"byte-stream-split-double-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "DOUBLE",
					Encoding: parquet.Encoding_BYTE_STREAM_SPLIT,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_DOUBLE),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
		"byte-stream-split-int32-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "INT32",
					Encoding: parquet.Encoding_BYTE_STREAM_SPLIT,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT32),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
		"byte-stream-split-fixed-len-valid": {
			Tag{
				fieldAttr: fieldAttr{
					Type:     "FIXED_LEN_BYTE_ARRAY",
					Length:   4,
					Encoding: parquet.Encoding_BYTE_STREAM_SPLIT,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(4)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
			},
			"",
		},
		"byte-stream-split-byte-array-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "BYTE_ARRAY",
					Encoding: parquet.Encoding_BYTE_STREAM_SPLIT,
				},
			},
			parquet.SchemaElement{},
			"BYTE_STREAM_SPLIT encoding is only supported for FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY",
		},
		"byte-stream-split-boolean-invalid": {
			Tag{
				InName: "TestField",
				fieldAttr: fieldAttr{
					Type:     "BOOLEAN",
					Encoding: parquet.Encoding_BYTE_STREAM_SPLIT,
				},
			},
			parquet.SchemaElement{},
			"BYTE_STREAM_SPLIT encoding is only supported for FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY",
		},
		"decimal-logicaltype-sets-schema-scale-precision": {
			Tag{
				fieldAttr: fieldAttr{
					Type: "INT32",
					logicalTypeFields: map[string]string{
						"logicaltype":           "DECIMAL",
						"logicaltype.precision": "9",
						"logicaltype.scale":     "2",
					},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT32),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(2)),
				Precision:      ToPtr(int32(9)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  ToPtr(parquet.ConvertedType_DECIMAL),
				LogicalType:    ToPtr(parquet.LogicalType{DECIMAL: &parquet.DecimalType{Scale: 2, Precision: 9}}),
			},
			"",
		},
		"decimal-convertedtype-sets-schema-scale-precision": {
			Tag{
				fieldAttr: fieldAttr{
					Type:          "INT64",
					convertedType: "DECIMAL",
					Scale:         4,
					Precision:     18,
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT64),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(4)),
				Precision:      ToPtr(int32(18)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  ToPtr(parquet.ConvertedType_DECIMAL),
				LogicalType:    ToPtr(parquet.LogicalType{DECIMAL: &parquet.DecimalType{Scale: 4, Precision: 18}}),
			},
			"",
		},
		"string-logicaltype-sets-convertedtype": {
			Tag{
				fieldAttr: fieldAttr{
					Type:              "BYTE_ARRAY",
					logicalTypeFields: map[string]string{"logicaltype": "STRING"},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  ToPtr(parquet.ConvertedType_UTF8),
				LogicalType:    ToPtr(parquet.LogicalType{STRING: &parquet.StringType{}}),
			},
			"",
		},
		"date-logicaltype-sets-convertedtype": {
			Tag{
				fieldAttr: fieldAttr{
					Type:              "INT32",
					logicalTypeFields: map[string]string{"logicaltype": "DATE"},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT32),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  ToPtr(parquet.ConvertedType_DATE),
				LogicalType:    ToPtr(parquet.LogicalType{DATE: &parquet.DateType{}}),
			},
			"",
		},
		"time-millis-logicaltype-sets-convertedtype": {
			Tag{
				fieldAttr: fieldAttr{
					Type: "INT32",
					logicalTypeFields: map[string]string{
						"logicaltype":                 "TIME",
						"logicaltype.isadjustedtoutc": "true",
						"logicaltype.unit":            "MILLIS",
					},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT32),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  ToPtr(parquet.ConvertedType_TIME_MILLIS),
				LogicalType:    ToPtr(parquet.LogicalType{TIME: &parquet.TimeType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}}),
			},
			"",
		},
		"timestamp-micros-logicaltype-sets-convertedtype": {
			Tag{
				fieldAttr: fieldAttr{
					Type: "INT64",
					logicalTypeFields: map[string]string{
						"logicaltype":                 "TIMESTAMP",
						"logicaltype.isadjustedtoutc": "false",
						"logicaltype.unit":            "MICROS",
					},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT64),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  ToPtr(parquet.ConvertedType_TIMESTAMP_MICROS),
				LogicalType:    ToPtr(parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: false, Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}}),
			},
			"",
		},
		"integer-logicaltype-sets-convertedtype": {
			Tag{
				fieldAttr: fieldAttr{
					Type: "INT32",
					logicalTypeFields: map[string]string{
						"logicaltype":          "INTEGER",
						"logicaltype.bitwidth": "16",
						"logicaltype.issigned": "false",
					},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT32),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  ToPtr(parquet.ConvertedType_UINT_16),
				LogicalType:    ToPtr(parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 16, IsSigned: false}}),
			},
			"",
		},
		"json-logicaltype-sets-convertedtype": {
			Tag{
				fieldAttr: fieldAttr{
					Type:              "BYTE_ARRAY",
					logicalTypeFields: map[string]string{"logicaltype": "JSON"},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  ToPtr(parquet.ConvertedType_JSON),
				LogicalType:    ToPtr(parquet.LogicalType{JSON: &parquet.JsonType{}}),
			},
			"",
		},
		"uuid-logicaltype-no-convertedtype": {
			Tag{
				fieldAttr: fieldAttr{
					Type:              "FIXED_LEN_BYTE_ARRAY",
					Length:            16,
					logicalTypeFields: map[string]string{"logicaltype": "UUID"},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(16)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  nil,
				LogicalType:    ToPtr(parquet.LogicalType{UUID: &parquet.UUIDType{}}),
			},
			"",
		},
		"timestamp-nanos-logicaltype-no-convertedtype": {
			Tag{
				fieldAttr: fieldAttr{
					Type: "INT64",
					logicalTypeFields: map[string]string{
						"logicaltype":                 "TIMESTAMP",
						"logicaltype.isadjustedtoutc": "true",
						"logicaltype.unit":            "NANOS",
					},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT64),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  nil,
				LogicalType:    ToPtr(parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{IsAdjustedToUTC: true, Unit: &parquet.TimeUnit{NANOS: parquet.NewNanoSeconds()}}}),
			},
			"",
		},
		"explicit-convertedtype-preserved": {
			Tag{
				fieldAttr: fieldAttr{
					Type:              "BYTE_ARRAY",
					convertedType:     "UTF8",
					logicalTypeFields: map[string]string{"logicaltype": "JSON"},
				},
			},
			parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_BYTE_ARRAY),
				TypeLength:     ToPtr(int32(0)),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				Scale:          ToPtr(int32(0)),
				Precision:      ToPtr(int32(0)),
				FieldID:        ToPtr(int32(0)),
				ConvertedType:  ToPtr(parquet.ConvertedType_UTF8), // User's explicit value preserved
				LogicalType:    ToPtr(parquet.LogicalType{JSON: &parquet.JsonType{}}),
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
		"compression-nil":  {"type=INT32", Tag{fieldAttr: fieldAttr{Type: "INT32", CompressionType: nil}}, ""}, // CompressionType is nil when not specified
		"compression-set":  {"type=INT32,compression=GZIP", Tag{fieldAttr: fieldAttr{Type: "INT32", CompressionType: ToPtr(parquet.CompressionCodec_GZIP)}}, ""},
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

func TestValidateSchemaElement(t *testing.T) {
	testCases := map[string]struct {
		schema parquet.SchemaElement
		errMsg string
	}{
		"nil-type": {
			parquet.SchemaElement{Type: nil},
			"",
		},
		// LogicalType STRING/JSON/BSON/ENUM require BYTE_ARRAY
		"string-logicaltype-byte-array-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{STRING: &parquet.StringType{}},
			},
			"",
		},
		"string-logicaltype-int32-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{STRING: &parquet.StringType{}},
			},
			"LogicalType STRING/JSON/BSON/ENUM can only be used with BYTE_ARRAY",
		},
		"json-logicaltype-int64-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{JSON: &parquet.JsonType{}},
			},
			"LogicalType STRING/JSON/BSON/ENUM can only be used with BYTE_ARRAY",
		},
		"bson-logicaltype-fixed-len-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{BSON: &parquet.BsonType{}},
			},
			"LogicalType STRING/JSON/BSON/ENUM can only be used with BYTE_ARRAY",
		},
		"enum-logicaltype-float-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_FLOAT),
				LogicalType: &parquet.LogicalType{ENUM: &parquet.EnumType{}},
			},
			"LogicalType STRING/JSON/BSON/ENUM can only be used with BYTE_ARRAY",
		},
		// LogicalType UUID requires FIXED_LEN_BYTE_ARRAY
		"uuid-logicaltype-fixed-len-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			},
			"",
		},
		"uuid-logicaltype-byte-array-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{UUID: &parquet.UUIDType{}},
			},
			"LogicalType UUID can only be used with FIXED_LEN_BYTE_ARRAY",
		},
		// LogicalType DATE requires INT32
		"date-logicaltype-int32-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{DATE: &parquet.DateType{}},
			},
			"",
		},
		"date-logicaltype-int64-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{DATE: &parquet.DateType{}},
			},
			"LogicalType DATE can only be used with INT32",
		},
		// LogicalType TIME(MILLIS) requires INT32
		"time-millis-logicaltype-int32-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			},
			"",
		},
		"time-millis-logicaltype-int64-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			},
			"LogicalType TIME(MILLIS) can only be used with INT32",
		},
		// LogicalType TIME(MICROS/NANOS) requires INT64
		"time-micros-logicaltype-int64-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
			},
			"",
		},
		"time-nanos-logicaltype-int64-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{NANOS: parquet.NewNanoSeconds()}}},
			},
			"",
		},
		"time-micros-logicaltype-int32-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
			},
			"LogicalType TIME(MICROS/NANOS) can only be used with INT64",
		},
		// LogicalType TIMESTAMP requires INT64
		"timestamp-logicaltype-int64-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			},
			"",
		},
		"timestamp-logicaltype-int32-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			},
			"LogicalType TIMESTAMP can only be used with INT64",
		},
		// LogicalType INTEGER bitwidth validation
		"integer-8bit-int32-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 8, IsSigned: true}},
			},
			"",
		},
		"integer-16bit-int32-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 16, IsSigned: false}},
			},
			"",
		},
		"integer-32bit-int32-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: true}},
			},
			"",
		},
		"integer-64bit-int64-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: true}},
			},
			"",
		},
		"integer-8bit-int64-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 8, IsSigned: true}},
			},
			"LogicalType INTEGER(bitwidth<=32) can only be used with INT32",
		},
		"integer-64bit-int32-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: true}},
			},
			"LogicalType INTEGER(bitwidth=64) can only be used with INT64",
		},
		// ConvertedType UTF8/JSON/BSON/ENUM require BYTE_ARRAY
		"utf8-convertedtype-byte-array-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: ToPtr(parquet.ConvertedType_UTF8),
			},
			"",
		},
		"utf8-convertedtype-int32-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: ToPtr(parquet.ConvertedType_UTF8),
			},
			"ConvertedType UTF8 can only be used with BYTE_ARRAY",
		},
		"json-convertedtype-float-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_FLOAT),
				ConvertedType: ToPtr(parquet.ConvertedType_JSON),
			},
			"ConvertedType JSON can only be used with BYTE_ARRAY",
		},
		// ConvertedType DATE/TIME_MILLIS/INT_* require INT32
		"date-convertedtype-int32-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: ToPtr(parquet.ConvertedType_DATE),
			},
			"",
		},
		"date-convertedtype-int64-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT64),
				ConvertedType: ToPtr(parquet.ConvertedType_DATE),
			},
			"ConvertedType DATE can only be used with INT32",
		},
		"time-millis-convertedtype-int64-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT64),
				ConvertedType: ToPtr(parquet.ConvertedType_TIME_MILLIS),
			},
			"ConvertedType TIME_MILLIS can only be used with INT32",
		},
		"int8-convertedtype-int64-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT64),
				ConvertedType: ToPtr(parquet.ConvertedType_INT_8),
			},
			"ConvertedType INT_8 can only be used with INT32",
		},
		// ConvertedType INT_64/UINT_64/TIME_MICROS/TIMESTAMP_* require INT64
		"int64-convertedtype-int64-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT64),
				ConvertedType: ToPtr(parquet.ConvertedType_INT_64),
			},
			"",
		},
		"int64-convertedtype-int32-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: ToPtr(parquet.ConvertedType_INT_64),
			},
			"ConvertedType INT_64 can only be used with INT64",
		},
		"time-micros-convertedtype-int32-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: ToPtr(parquet.ConvertedType_TIME_MICROS),
			},
			"ConvertedType TIME_MICROS can only be used with INT64",
		},
		"timestamp-millis-convertedtype-int32-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: ToPtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
			},
			"ConvertedType TIMESTAMP_MILLIS can only be used with INT64",
		},
		// ConvertedType INTERVAL requires FIXED_LEN_BYTE_ARRAY with length 12
		"interval-convertedtype-fixed-len-12-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:    ToPtr(int32(12)),
				ConvertedType: ToPtr(parquet.ConvertedType_INTERVAL),
			},
			"",
		},
		"interval-convertedtype-byte-array-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: ToPtr(parquet.ConvertedType_INTERVAL),
			},
			"ConvertedType INTERVAL can only be used with FIXED_LEN_BYTE_ARRAY",
		},
		"interval-convertedtype-fixed-len-wrong-length-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:    ToPtr(int32(16)),
				ConvertedType: ToPtr(parquet.ConvertedType_INTERVAL),
			},
			"ConvertedType INTERVAL requires FIXED_LEN_BYTE_ARRAY with length 12",
		},
		"interval-convertedtype-fixed-len-nil-length-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				TypeLength:    nil,
				ConvertedType: ToPtr(parquet.ConvertedType_INTERVAL),
			},
			"ConvertedType INTERVAL requires FIXED_LEN_BYTE_ARRAY with length 12",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := ValidateSchemaElement(&tc.schema)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
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
			result := GetLogicalTypeFromTag(tc.tag)
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
