package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func Test_IsSignedSortOrder(t *testing.T) {
	tests := []struct {
		name string
		pT   *parquet.Type
		cT   *parquet.ConvertedType
		logT *parquet.LogicalType
		want bool
	}{
		{name: "all nil", want: false},
		{name: "boolean", pT: ToPtr(parquet.Type_BOOLEAN), want: true},
		{name: "int32", pT: ToPtr(parquet.Type_INT32), want: true},
		{name: "int64", pT: ToPtr(parquet.Type_INT64), want: true},
		{name: "float", pT: ToPtr(parquet.Type_FLOAT), want: true},
		{name: "double", pT: ToPtr(parquet.Type_DOUBLE), want: true},
		{name: "byte_array", pT: ToPtr(parquet.Type_BYTE_ARRAY), want: false},
		{name: "fixed_len_byte_array", pT: ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), want: false},
		{name: "int96", pT: ToPtr(parquet.Type_INT96), want: false},

		// Converted types.
		{name: "utf8", pT: ToPtr(parquet.Type_BYTE_ARRAY), cT: ToPtr(parquet.ConvertedType_UTF8), want: false},
		{name: "enum", pT: ToPtr(parquet.Type_BYTE_ARRAY), cT: ToPtr(parquet.ConvertedType_ENUM), want: false},
		{name: "json", pT: ToPtr(parquet.Type_BYTE_ARRAY), cT: ToPtr(parquet.ConvertedType_JSON), want: false},
		{name: "bson", pT: ToPtr(parquet.Type_BYTE_ARRAY), cT: ToPtr(parquet.ConvertedType_BSON), want: false},
		{name: "interval", pT: ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), cT: ToPtr(parquet.ConvertedType_INTERVAL), want: false},
		{name: "uint_8", pT: ToPtr(parquet.Type_INT32), cT: ToPtr(parquet.ConvertedType_UINT_8), want: false},
		{name: "uint_16", pT: ToPtr(parquet.Type_INT32), cT: ToPtr(parquet.ConvertedType_UINT_16), want: false},
		{name: "uint_32", pT: ToPtr(parquet.Type_INT32), cT: ToPtr(parquet.ConvertedType_UINT_32), want: false},
		{name: "uint_64", pT: ToPtr(parquet.Type_INT64), cT: ToPtr(parquet.ConvertedType_UINT_64), want: false},
		{name: "int_8", pT: ToPtr(parquet.Type_INT32), cT: ToPtr(parquet.ConvertedType_INT_8), want: true},
		{name: "int_64", pT: ToPtr(parquet.Type_INT64), cT: ToPtr(parquet.ConvertedType_INT_64), want: true},
		{name: "decimal converted", pT: ToPtr(parquet.Type_INT32), cT: ToPtr(parquet.ConvertedType_DECIMAL), want: true},
		{name: "date converted", pT: ToPtr(parquet.Type_INT32), cT: ToPtr(parquet.ConvertedType_DATE), want: true},
		{name: "time_millis", pT: ToPtr(parquet.Type_INT32), cT: ToPtr(parquet.ConvertedType_TIME_MILLIS), want: true},
		{name: "timestamp_micros", pT: ToPtr(parquet.Type_INT64), cT: ToPtr(parquet.ConvertedType_TIMESTAMP_MICROS), want: true},

		// Logical types.
		{name: "string logical", pT: ToPtr(parquet.Type_BYTE_ARRAY), logT: &parquet.LogicalType{STRING: parquet.NewStringType()}, want: false},
		{name: "uuid logical", pT: ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), logT: &parquet.LogicalType{UUID: parquet.NewUUIDType()}, want: false},
		{name: "unsigned integer logical", pT: ToPtr(parquet.Type_INT32), logT: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: false}}, want: false},
		{name: "signed integer logical", pT: ToPtr(parquet.Type_INT32), logT: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32, IsSigned: true}}, want: true},
		{name: "decimal logical", pT: ToPtr(parquet.Type_INT32), logT: &parquet.LogicalType{DECIMAL: &parquet.DecimalType{Scale: 2, Precision: 9}}, want: true},
		{name: "date logical", pT: ToPtr(parquet.Type_INT32), logT: &parquet.LogicalType{DATE: parquet.NewDateType()}, want: true},
		{name: "timestamp logical", pT: ToPtr(parquet.Type_INT64), logT: &parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{}}, want: true},
		{name: "float16 logical", pT: ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), logT: &parquet.LogicalType{FLOAT16: parquet.NewFloat16Type()}, want: true},
		{name: "unknown logical", pT: ToPtr(parquet.Type_INT32), logT: &parquet.LogicalType{UNKNOWN: parquet.NewNullType()}, want: false},
		{name: "variant logical", pT: ToPtr(parquet.Type_BYTE_ARRAY), logT: &parquet.LogicalType{VARIANT: &parquet.VariantType{}}, want: false},
		{name: "geometry logical", pT: ToPtr(parquet.Type_BYTE_ARRAY), logT: &parquet.LogicalType{GEOMETRY: parquet.NewGeometryType()}, want: false},
		{name: "geography logical", pT: ToPtr(parquet.Type_BYTE_ARRAY), logT: &parquet.LogicalType{GEOGRAPHY: parquet.NewGeographyType()}, want: false},

		// Converted type takes precedence over logical type, mirroring
		// FindFuncTable, when the two disagree on signedness.
		{
			name: "converted unsigned wins over logical signed",
			pT:   ToPtr(parquet.Type_INT64),
			cT:   ToPtr(parquet.ConvertedType_UINT_64),
			logT: &parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64, IsSigned: true}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsSignedSortOrder(tt.pT, tt.cT, tt.logT))
		})
	}
}
