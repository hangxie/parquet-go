package types

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func convertToBool(src any) (any, error) {
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || rv.Kind() != reflect.Bool {
		return nil, fmt.Errorf("convert %T to bool", src)
	}
	return rv.Bool(), nil
}

func convertToInt32(src any) (any, error) {
	rv := reflect.ValueOf(src)
	if !rv.IsValid() {
		return nil, fmt.Errorf("convert %T to int32", src)
	}
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int32(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return int32(rv.Uint()), nil
	default:
		return nil, fmt.Errorf("convert %T to int32", src)
	}
}

func convertToInt64(src any) (any, error) {
	rv := reflect.ValueOf(src)
	if !rv.IsValid() {
		return nil, fmt.Errorf("convert %T to int64", src)
	}
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return int64(rv.Uint()), nil
	default:
		return nil, fmt.Errorf("convert %T to int64", src)
	}
}

func convertToFloat32(src any) (any, error) {
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || (rv.Kind() != reflect.Float32 && rv.Kind() != reflect.Float64) {
		return nil, fmt.Errorf("convert %T to float32", src)
	}
	return float32(rv.Float()), nil
}

func convertToFloat64(src any) (any, error) {
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || (rv.Kind() != reflect.Float32 && rv.Kind() != reflect.Float64) {
		return nil, fmt.Errorf("convert %T to float64", src)
	}
	return rv.Float(), nil
}

func convertToString(src any) (any, error) {
	if b, ok := src.([]byte); ok {
		return string(b), nil
	}
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || rv.Kind() != reflect.String {
		return nil, fmt.Errorf("convert %T to string", src)
	}
	return rv.String(), nil
}

func InterfaceToParquetType(src any, pT *parquet.Type) (any, error) {
	if src == nil || pT == nil {
		return src, nil
	}

	switch *pT {
	case parquet.Type_BOOLEAN:
		return convertToBool(src)
	case parquet.Type_INT32:
		return convertToInt32(src)
	case parquet.Type_INT64:
		return convertToInt64(src)
	case parquet.Type_FLOAT:
		return convertToFloat32(src)
	case parquet.Type_DOUBLE:
		return convertToFloat64(src)
	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return convertToString(src)
	default:
		return src, nil
	}
}
