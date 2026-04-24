package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
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
		"bloomfilter-good":         {"bloomfilter", "true", fieldAttr{BloomFilter: true}, ""},
		"bloomfilter-bad":          {"bloomfilter", "abc", fieldAttr{}, "parse bloomfilter value"},
		"bloomfiltersize-good":     {"bloomfiltersize", "2048", fieldAttr{BloomFilterSize: 2048}, ""},
		"bloomfiltersize-bad":      {"bloomfiltersize", "abc", fieldAttr{}, "parse bloomfiltersize value"},
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
		"bloomfilter":      {"type=INT64,bloomfilter=true", Tag{fieldAttr: fieldAttr{Type: "INT64", BloomFilter: true}}, ""},
		"bloomfilter-size": {"type=INT64,bloomfilter=true,bloomfiltersize=2048", Tag{fieldAttr: fieldAttr{Type: "INT64", BloomFilter: true, BloomFilterSize: 2048}}, ""},
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

func TestValidateEncodingForDataPageVersion(t *testing.T) {
	testCases := map[string]struct {
		encoding parquet.Encoding
		version  int32
		errMsg   string
	}{
		// PLAIN_DICTIONARY is v1 only
		"plain-dictionary-v1-valid": {
			parquet.Encoding_PLAIN_DICTIONARY,
			1,
			"",
		},
		"plain-dictionary-v2-invalid": {
			parquet.Encoding_PLAIN_DICTIONARY,
			2,
			"PLAIN_DICTIONARY encoding is deprecated and only valid for data page v1",
		},
		// Delta encodings are v2 only
		"delta-binary-packed-v2-valid": {
			parquet.Encoding_DELTA_BINARY_PACKED,
			2,
			"",
		},
		"delta-binary-packed-v1-invalid": {
			parquet.Encoding_DELTA_BINARY_PACKED,
			1,
			"DELTA_BINARY_PACKED encoding is only supported for data page v2",
		},
		"delta-byte-array-v2-valid": {
			parquet.Encoding_DELTA_BYTE_ARRAY,
			2,
			"",
		},
		"delta-byte-array-v1-invalid": {
			parquet.Encoding_DELTA_BYTE_ARRAY,
			1,
			"DELTA_BYTE_ARRAY encoding is only supported for data page v2",
		},
		"delta-length-byte-array-v2-valid": {
			parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			2,
			"",
		},
		"delta-length-byte-array-v1-invalid": {
			parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			1,
			"DELTA_LENGTH_BYTE_ARRAY encoding is only supported for data page v2",
		},
		// Other encodings work with both versions
		"plain-v1-valid": {
			parquet.Encoding_PLAIN,
			1,
			"",
		},
		"plain-v2-valid": {
			parquet.Encoding_PLAIN,
			2,
			"",
		},
		"rle-dictionary-v1-valid": {
			parquet.Encoding_RLE_DICTIONARY,
			1,
			"",
		},
		"rle-dictionary-v2-valid": {
			parquet.Encoding_RLE_DICTIONARY,
			2,
			"",
		},
		"rle-v1-valid": {
			parquet.Encoding_RLE,
			1,
			"",
		},
		"rle-v2-valid": {
			parquet.Encoding_RLE,
			2,
			"",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := ValidateEncodingForDataPageVersion("TestField", tc.encoding, tc.version)
			if tc.errMsg == "" {
				require.NoError(t, err)
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
