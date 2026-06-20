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

		// DECIMAL requires INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY
		"decimal-int32-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT32),
				ConvertedType: ToPtr(parquet.ConvertedType_DECIMAL),
			},
			"",
		},
		"decimal-int64-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_INT64),
				ConvertedType: ToPtr(parquet.ConvertedType_DECIMAL),
			},
			"",
		},
		"decimal-byte-array-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_BYTE_ARRAY),
				ConvertedType: ToPtr(parquet.ConvertedType_DECIMAL),
			},
			"",
		},
		"decimal-fixed-len-byte-array-valid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				ConvertedType: ToPtr(parquet.ConvertedType_DECIMAL),
			},
			"",
		},
		"decimal-float-invalid": {
			parquet.SchemaElement{
				Type:          ToPtr(parquet.Type_FLOAT),
				ConvertedType: ToPtr(parquet.ConvertedType_DECIMAL),
			},
			"ConvertedType DECIMAL can only be used with INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY",
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

func TestValidateLogicalTypePhysicalConstraints(t *testing.T) {
	testCases := map[string]struct {
		schema parquet.SchemaElement
		errMsg string
	}{
		// FLOAT16 requires FIXED_LEN_BYTE_ARRAY
		"float16-fixed-len-byte-array-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			},
			"",
		},
		"float16-byte-array-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}},
			},
			"LogicalType FLOAT16 can only be used with FIXED_LEN_BYTE_ARRAY",
		},

		// DECIMAL requires INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY
		"decimal-int32-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			},
			"",
		},
		"decimal-int64-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			},
			"",
		},
		"decimal-byte-array-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			},
			"",
		},
		"decimal-fixed-len-byte-array-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			},
			"",
		},
		"decimal-float-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_FLOAT),
				LogicalType: &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}},
			},
			"LogicalType DECIMAL can only be used with INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY",
		},

		// GEOMETRY and GEOGRAPHY require BYTE_ARRAY
		"geometry-byte-array-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{GEOMETRY: &parquet.GeometryType{}},
			},
			"",
		},
		"geometry-int32-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{GEOMETRY: &parquet.GeometryType{}},
			},
			"LogicalType GEOMETRY/GEOGRAPHY can only be used with BYTE_ARRAY",
		},
		"geography-byte-array-valid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{GEOGRAPHY: &parquet.GeographyType{}},
			},
			"",
		},
		"geography-fixed-len-byte-array-invalid": {
			parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
				LogicalType: &parquet.LogicalType{GEOGRAPHY: &parquet.GeographyType{}},
			},
			"LogicalType GEOMETRY/GEOGRAPHY can only be used with BYTE_ARRAY",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateLogicalType(&tc.schema)
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

func TestValidateLogicalUnknown(t *testing.T) {
	testCases := map[string]struct {
		schema *parquet.SchemaElement
		errMsg string
	}{
		"int32-optional-valid": {
			&parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT32),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_OPTIONAL),
				LogicalType:    &parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			},
			"",
		},
		"byte_array-invalid": {
			&parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_BYTE_ARRAY),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_OPTIONAL),
				LogicalType:    &parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			},
			"LogicalType UNKNOWN can only be used with INT32",
		},
		"int64-invalid": {
			&parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT64),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_OPTIONAL),
				LogicalType:    &parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			},
			"LogicalType UNKNOWN can only be used with INT32",
		},
		"required-invalid": {
			&parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT32),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REQUIRED),
				LogicalType:    &parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			},
			"LogicalType UNKNOWN requires OPTIONAL repetition type",
		},
		"nil-repetitiontype-invalid": {
			&parquet.SchemaElement{
				Type:        ToPtr(parquet.Type_INT32),
				LogicalType: &parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			},
			"LogicalType UNKNOWN requires OPTIONAL repetition type",
		},
		"nil_type": {
			&parquet.SchemaElement{
				Type:           nil,
				RepetitionType: ToPtr(parquet.FieldRepetitionType_OPTIONAL),
				LogicalType:    &parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			},
			"LogicalType UNKNOWN can only be used with INT32",
		},
		"repeated-invalid": {
			&parquet.SchemaElement{
				Type:           ToPtr(parquet.Type_INT32),
				RepetitionType: ToPtr(parquet.FieldRepetitionType_REPEATED),
				LogicalType:    &parquet.LogicalType{UNKNOWN: &parquet.NullType{}},
			},
			"LogicalType UNKNOWN requires OPTIONAL repetition type",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateLogicalType(tc.schema)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}
