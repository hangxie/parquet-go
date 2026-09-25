package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestFindFuncTable(t *testing.T) {
	testCases := map[string]struct {
		pT       *parquet.Type
		cT       *parquet.ConvertedType
		lT       *parquet.LogicalType
		expected FuncTable
	}{
		"BOOLEAN-nil-nil":                   {ToPtr(parquet.Type_BOOLEAN), nil, nil, boolFuncTable{}},
		"INT32-nil-nil":                     {ToPtr(parquet.Type_INT32), nil, nil, int32FuncTable{}},
		"INT64-nil-nil":                     {ToPtr(parquet.Type_INT64), nil, nil, int64FuncTable{}},
		"INT96-nil-nil":                     {ToPtr(parquet.Type_INT96), nil, nil, int96FuncTable{}},
		"FLOAT-nil-nil":                     {ToPtr(parquet.Type_FLOAT), nil, nil, float32FuncTable{}},
		"DOUBLE-nil-nil":                    {ToPtr(parquet.Type_DOUBLE), nil, nil, float64FuncTable{}},
		"BYTE_ARRAY-nil-nil":                {ToPtr(parquet.Type_BYTE_ARRAY), nil, nil, stringFuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-nil-nil":      {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, nil, stringFuncTable{}},
		"BYTE_ARRAY-UTF8-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_UTF8), nil, stringFuncTable{}},
		"BYTE_ARRAY-BSON-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_BSON), nil, stringFuncTable{}},
		"BYTE_ARRAY-JSON-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_JSON), nil, stringFuncTable{}},
		"BYTE_ARRAY-ENUM-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_ENUM), nil, stringFuncTable{}},
		"INT32-INT_8-nil":                   {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_INT_8), nil, int32FuncTable{}},
		"INT32-INT_16-nil":                  {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_INT_16), nil, int32FuncTable{}},
		"INT32-INT_32-nil":                  {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_INT_32), nil, int32FuncTable{}},
		"INT64-INT_64-nil":                  {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_INT_64), nil, int64FuncTable{}},
		"INT32-UINT_8-nil":                  {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_UINT_8), nil, uint32FuncTable{}},
		"INT32-UINT_16-nil":                 {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_UINT_16), nil, uint32FuncTable{}},
		"INT32-UINT_32-nil":                 {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_UINT_32), nil, uint32FuncTable{}},
		"INT64-UINT_64-nil":                 {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_UINT_64), nil, uint64FuncTable{}},
		"INT32-DATE-nil":                    {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_DATE), nil, int32FuncTable{}},
		"INT64-TIME_MILLIS-nil":             {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_TIME_MILLIS), nil, int32FuncTable{}},
		"INT64-TIME_MICROS-nil":             {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_TIME_MICROS), nil, int64FuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-INTERVAL-nil": {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), ToPtr(parquet.ConvertedType_INTERVAL), nil, intervalFuncTable{}},
		"BYTE_ARRAY-DECIMAL-nil":            {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_DECIMAL), nil, decimalStringFuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-DECIMAL-nil":  {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), ToPtr(parquet.ConvertedType_DECIMAL), nil, decimalStringFuncTable{}},
		"INT32-DECIMAL-nil":                 {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_DECIMAL), nil, int32FuncTable{}},
		"INT64-DECIMAL-nil":                 {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_DECIMAL), nil, int64FuncTable{}},
		"INT32-nil-TIME":                    {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{TIME: &parquet.TimeType{}}, int32FuncTable{}},
		"INT64-nil-TIMESTAMP":               {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{TIME: &parquet.TimeType{}}, int64FuncTable{}},
		"INT64-nil-DATE":                    {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{DATE: &parquet.DateType{}}, int32FuncTable{}},
		"INT32-nil-INTEGER-signed":          {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{IsSigned: true}}, int32FuncTable{}},
		"INT64-nil-INTEGER-signed":          {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{IsSigned: true}}, int64FuncTable{}},
		"INT32-nil-INTEGER-unsigned":        {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{}}, uint32FuncTable{}},
		"INT64-nil-INTEGER-unsigned":        {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{}}, uint64FuncTable{}},
		"BYTE_ARRAY-nil-DECIMAL":            {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, decimalStringFuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-nil-DECIMAL":  {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, decimalStringFuncTable{}},
		"INT32-nil-DECIMAL":                 {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, int32FuncTable{}},
		"INT64-nil-DECIMAL":                 {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, int64FuncTable{}},
		"BYTE_ARRAY-nil-BSON":               {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{BSON: &parquet.BsonType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-JSON":               {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{JSON: &parquet.JsonType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-STRING":             {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{STRING: &parquet.StringType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-UUID":               {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{UUID: &parquet.UUIDType{}}, stringFuncTable{}},
		// FLOAT16 logical type on FIXED_LEN_BYTE_ARRAY should use numeric ordering (float16FuncTable)
		"FIXED_LEN_BYTE_ARRAY-nil-FLOAT16": {
			ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, &parquet.LogicalType{FLOAT16: &parquet.Float16Type{}}, float16FuncTable{},
		},
		// Treat VARIANT/GEOMETRY/GEOGRAPHY as binary
		"BYTE_ARRAY-nil-VARIANT":   {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{VARIANT: &parquet.VariantType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-GEOMETRY":  {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{GEOMETRY: &parquet.GeometryType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-GEOGRAPHY": {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{GEOGRAPHY: &parquet.GeographyType{}}, stringFuncTable{}},
		// UNKNOWN logical type falls back to the physical type's func table
		"INT32-nil-UNKNOWN": {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{UNKNOWN: parquet.NewNullType()}, int32FuncTable{}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := FindFuncTable(tc.pT, tc.cT, tc.lT)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}

	t.Run("bad", func(t *testing.T) {
		_, err := FindFuncTable(nil, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "all types are nil")
		_, err = FindFuncTable(nil, nil, &parquet.LogicalType{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "find func table for given types")
		// TIME/TIMESTAMP with nil physical type must propagate error, not silently return nil table
		_, err = FindFuncTable(nil, nil, &parquet.LogicalType{TIME: &parquet.TimeType{}})
		require.Error(t, err)
		_, err = FindFuncTable(nil, nil, &parquet.LogicalType{TIMESTAMP: &parquet.TimestampType{}})
		require.Error(t, err)
		// INTEGER(signed) with nil physical type must propagate error
		_, err = FindFuncTable(nil, nil, &parquet.LogicalType{INTEGER: &parquet.IntType{IsSigned: true}})
		require.Error(t, err)
	})
}
