package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestValidateConvertedType(t *testing.T) {
	testCases := map[string]struct {
		schema parquet.SchemaElement
		errMsg string
	}{
		// BSON requires BYTE_ARRAY
		"bson-byte-array-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: ToPtr(parquet.ConvertedType_BSON),
			},
			"",
		},
		"bson-int32-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: ToPtr(parquet.ConvertedType_BSON),
			},
			"ConvertedType BSON can only be used with BYTE_ARRAY",
		},

		// ENUM requires BYTE_ARRAY
		"enum-byte-array-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: ToPtr(parquet.ConvertedType_ENUM),
			},
			"",
		},
		"enum-float-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_FLOAT),
				ConvertedType: ToPtr(parquet.ConvertedType_ENUM),
			},
			"ConvertedType ENUM can only be used with BYTE_ARRAY",
		},

		// UINT_64 requires INT64
		"uint64-int64-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT64),
				ConvertedType: ToPtr(parquet.ConvertedType_UINT_64),
			},
			"",
		},
		"uint64-int32-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: ToPtr(parquet.ConvertedType_UINT_64),
			},
			"ConvertedType UINT_64 can only be used with INT64",
		},

		// TIMESTAMP_MICROS requires INT64
		"timestamp-micros-int64-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT64),
				ConvertedType: ToPtr(parquet.ConvertedType_TIMESTAMP_MICROS),
			},
			"",
		},
		"timestamp-micros-int32-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: ToPtr(parquet.ConvertedType_TIMESTAMP_MICROS),
			},
			"ConvertedType TIMESTAMP_MICROS can only be used with INT64",
		},

		// nil ConvertedType is a no-op
		"nil-converted-type": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: nil,
			},
			"",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateConvertedType(&tc.schema)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func TestValidateLogicalTime(t *testing.T) {
	testCases := map[string]struct {
		lt     *parquet.LogicalType
		pT     parquet.Type
		errMsg string
	}{
		"nil-time": {
			&parquet.LogicalType{},
			parquet.Type_INT32,
			"",
		},
		"millis-int32-valid": {
			&parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			parquet.Type_INT32,
			"",
		},
		"millis-int64-invalid": {
			&parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{MILLIS: parquet.NewMilliSeconds()}}},
			parquet.Type_INT64,
			"LogicalType TIME(MILLIS) can only be used with INT32",
		},
		"micros-int64-valid": {
			&parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
			parquet.Type_INT64,
			"",
		},
		"nanos-int64-valid": {
			&parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{NANOS: parquet.NewNanoSeconds()}}},
			parquet.Type_INT64,
			"",
		},
		"micros-int32-invalid": {
			&parquet.LogicalType{TIME: &parquet.TimeType{Unit: &parquet.TimeUnit{MICROS: parquet.NewMicroSeconds()}}},
			parquet.Type_INT32,
			"LogicalType TIME(MICROS/NANOS) can only be used with INT64",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateLogicalTime(tc.lt, &tc.pT)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func TestValidateLogicalInteger(t *testing.T) {
	testCases := map[string]struct {
		lt     *parquet.LogicalType
		pT     parquet.Type
		errMsg string
	}{
		"nil-integer": {
			&parquet.LogicalType{},
			parquet.Type_INT32,
			"",
		},
		"bitwidth-32-int32-valid": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32}},
			parquet.Type_INT32,
			"",
		},
		"bitwidth-32-int64-invalid": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 32}},
			parquet.Type_INT64,
			"LogicalType INTEGER(bitwidth<=32) can only be used with INT32",
		},
		"bitwidth-33-int64-valid": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 33}},
			parquet.Type_INT64,
			"",
		},
		"bitwidth-33-int32-invalid": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 33}},
			parquet.Type_INT32,
			"LogicalType INTEGER(bitwidth=64) can only be used with INT64",
		},
		"bitwidth-64-int64-valid": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64}},
			parquet.Type_INT64,
			"",
		},
		"bitwidth-64-int32-invalid": {
			&parquet.LogicalType{INTEGER: &parquet.IntType{BitWidth: 64}},
			parquet.Type_INT32,
			"LogicalType INTEGER(bitwidth=64) can only be used with INT64",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateLogicalInteger(tc.lt, &tc.pT)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}
