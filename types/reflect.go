package types

import (
	"reflect"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func ParquetTypeToGoReflectType(pT *parquet.Type, rT *parquet.FieldRepetitionType) reflect.Type {
	if pT == nil {
		return nil
	}

	if rT == nil || *rT != parquet.FieldRepetitionType_OPTIONAL {
		switch *pT {
		case parquet.Type_BOOLEAN:
			return reflect.TypeOf(true)
		case parquet.Type_INT32:
			return reflect.TypeOf(int32(0))
		case parquet.Type_INT64:
			return reflect.TypeOf(int64(0))
		case parquet.Type_INT96:
			return reflect.TypeOf("")
		case parquet.Type_FLOAT:
			return reflect.TypeOf(float32(0))
		case parquet.Type_DOUBLE:
			return reflect.TypeOf(float64(0))
		case parquet.Type_BYTE_ARRAY:
			return reflect.TypeOf("")
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			return reflect.TypeOf("")
		default:
			return nil
		}
	}

	switch *pT {
	case parquet.Type_BOOLEAN:
		return reflect.TypeOf(common.ToPtr(true))
	case parquet.Type_INT32:
		return reflect.TypeOf(common.ToPtr(int32(0)))
	case parquet.Type_INT64:
		return reflect.TypeOf(common.ToPtr(int64(0)))
	case parquet.Type_INT96:
		return reflect.TypeOf(common.ToPtr(""))
	case parquet.Type_FLOAT:
		return reflect.TypeOf(common.ToPtr(float32(0)))
	case parquet.Type_DOUBLE:
		return reflect.TypeOf(common.ToPtr(float64(0)))
	case parquet.Type_BYTE_ARRAY:
		return reflect.TypeOf(common.ToPtr(""))
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return reflect.TypeOf(common.ToPtr(""))
	default:
		return nil
	}
}
