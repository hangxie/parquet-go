package common

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v2/parquet"
)

type FuncTable interface {
	LessThan(a, b any) bool
	MinMaxSize(minVal, maxVal, val any) (any, any, int32)
}

var parquetTypeFuncTable = map[parquet.Type]FuncTable{
	parquet.Type_BOOLEAN:              boolFuncTable{},
	parquet.Type_INT32:                int32FuncTable{},
	parquet.Type_INT64:                int64FuncTable{},
	parquet.Type_INT96:                int96FuncTable{},
	parquet.Type_FLOAT:                float32FuncTable{},
	parquet.Type_DOUBLE:               float64FuncTable{},
	parquet.Type_BYTE_ARRAY:           stringFuncTable{},
	parquet.Type_FIXED_LEN_BYTE_ARRAY: stringFuncTable{},
}

var convertedTypeFuncTable = map[parquet.ConvertedType]FuncTable{
	parquet.ConvertedType_UTF8:             stringFuncTable{},
	parquet.ConvertedType_BSON:             stringFuncTable{},
	parquet.ConvertedType_JSON:             stringFuncTable{},
	parquet.ConvertedType_ENUM:             stringFuncTable{},
	parquet.ConvertedType_INT_8:            int32FuncTable{},
	parquet.ConvertedType_INT_16:           int32FuncTable{},
	parquet.ConvertedType_INT_32:           int32FuncTable{},
	parquet.ConvertedType_INT_64:           int64FuncTable{},
	parquet.ConvertedType_UINT_8:           uint32FuncTable{},
	parquet.ConvertedType_UINT_16:          uint32FuncTable{},
	parquet.ConvertedType_UINT_32:          uint32FuncTable{},
	parquet.ConvertedType_UINT_64:          uint64FuncTable{},
	parquet.ConvertedType_INTERVAL:         intervalFuncTable{},
	parquet.ConvertedType_DATE:             int32FuncTable{},
	parquet.ConvertedType_TIME_MICROS:      int64FuncTable{},
	parquet.ConvertedType_TIME_MILLIS:      int32FuncTable{},
	parquet.ConvertedType_TIMESTAMP_MICROS: int64FuncTable{},
	parquet.ConvertedType_TIMESTAMP_MILLIS: int64FuncTable{},
}

func FindFuncTable(pT *parquet.Type, cT *parquet.ConvertedType, logT *parquet.LogicalType) (FuncTable, error) {
	if pT == nil && cT == nil && logT == nil {
		return nil, fmt.Errorf("all types are nil")
	}

	if cT == nil && logT == nil {
		if table, ok := parquetTypeFuncTable[*pT]; ok {
			return table, nil
		}
	}

	if cT != nil {
		if table, ok := convertedTypeFuncTable[*cT]; ok {
			return table, nil
		} else if *cT == parquet.ConvertedType_DECIMAL {
			switch *pT {
			case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
				return decimalStringFuncTable{}, nil
			case parquet.Type_INT32:
				return int32FuncTable{}, nil
			case parquet.Type_INT64:
				return int64FuncTable{}, nil
			}
		}
	}

	if logT != nil {
		if logT.TIME != nil || logT.TIMESTAMP != nil {
			return FindFuncTable(pT, nil, nil)
		} else if logT.DATE != nil {
			return int32FuncTable{}, nil
		} else if logT.INTEGER != nil {
			if logT.INTEGER.IsSigned {
				return FindFuncTable(pT, nil, nil)
			} else {
				switch *pT {
				case parquet.Type_INT32:
					return uint32FuncTable{}, nil
				case parquet.Type_INT64:
					return uint64FuncTable{}, nil
				}
			}
		} else if logT.DECIMAL != nil {
			switch *pT {
			case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
				return decimalStringFuncTable{}, nil
			case parquet.Type_INT32:
				return int32FuncTable{}, nil
			case parquet.Type_INT64:
				return int64FuncTable{}, nil
			}
		} else if logT.BSON != nil || logT.JSON != nil || logT.STRING != nil || logT.UUID != nil {
			return stringFuncTable{}, nil
		}
	}

	return nil, fmt.Errorf("cannot find func table for given types: %v, %v, %v", pT, cT, logT)
}

func Min(table FuncTable, a, b any) any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return a
	} else {
		return b
	}
}

func Max(table FuncTable, a, b any) any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return b
	} else {
		return a
	}
}

type boolFuncTable struct{}

func (boolFuncTable) LessThan(a, b any) bool {
	return !a.(bool) && b.(bool)
}

func (table boolFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 1
}

type int32FuncTable struct{}

func (int32FuncTable) LessThan(a, b any) bool {
	return a.(int32) < b.(int32)
}

func (table int32FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type uint32FuncTable struct{}

func (uint32FuncTable) LessThan(a, b any) bool {
	return uint32(a.(int32)) < uint32(b.(int32))
}

func (table uint32FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type int64FuncTable struct{}

func (int64FuncTable) LessThan(a, b any) bool {
	return a.(int64) < b.(int64)
}

func (table int64FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type uint64FuncTable struct{}

func (uint64FuncTable) LessThan(a, b any) bool {
	return uint64(a.(int64)) < uint64(b.(int64))
}

func (table uint64FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type int96FuncTable struct{}

func (int96FuncTable) LessThan(ai, bi any) bool {
	aStr, aOk := ai.(string)
	bStr, bOk := bi.(string)
	if !aOk || !bOk {
		return false
	}

	a, b := []byte(aStr), []byte(bStr)
	if len(a) < 12 || len(b) < 12 {
		return false
	}

	fa, fb := a[11]>>7, b[11]>>7
	if fa > fb {
		return true
	} else if fa < fb {
		return false
	}
	for i := 11; i >= 0; i-- {
		if a[i] < b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}
	return false
}

func (table int96FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type float32FuncTable struct{}

func (float32FuncTable) LessThan(a, b any) bool {
	return a.(float32) < b.(float32)
}

func (table float32FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 4
}

type float64FuncTable struct{}

func (float64FuncTable) LessThan(a, b any) bool {
	return a.(float64) < b.(float64)
}

func (table float64FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), 8
}

type stringFuncTable struct{}

func (stringFuncTable) LessThan(a, b any) bool {
	return a.(string) < b.(string)
}

func (table stringFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type intervalFuncTable struct{}

func (intervalFuncTable) LessThan(ai, bi any) bool {
	aStr, aOk := ai.(string)
	bStr, bOk := bi.(string)
	if !aOk || !bOk {
		return false
	}

	a, b := []byte(aStr), []byte(bStr)
	if len(a) < 12 || len(b) < 12 {
		return false
	}

	for i := 11; i >= 0; i-- {
		if a[i] > b[i] {
			return false
		} else if a[i] < b[i] {
			return true
		}
	}
	return false
}

func (table intervalFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

type decimalStringFuncTable struct{}

func (decimalStringFuncTable) LessThan(a, b any) bool {
	return cmpIntBinary(a.(string), b.(string), "BigEndian", true)
}

func (table decimalStringFuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	return Min(table, minVal, val), Max(table, maxVal, val), int32(len(val.(string)))
}

// Get the size of a parquet value
func SizeOf(val reflect.Value) int64 {
	if !val.IsValid() {
		return 0
	}
	var size int64
	switch val.Type().Kind() {
	case reflect.Ptr:
		if val.IsNil() {
			return 0
		}
		return SizeOf(val.Elem())
	case reflect.Slice:
		for i := range val.Len() {
			size += SizeOf(val.Index(i))
		}
		return size
	case reflect.Struct:
		for i := range val.Type().NumField() {
			size += SizeOf(val.Field(i))
		}
		return size
	case reflect.Map:
		keys := val.MapKeys()
		for i := range keys {
			size += SizeOf(keys[i])
			size += SizeOf(val.MapIndex(keys[i]))
		}
		return size
	case reflect.Bool:
		return 1
	case reflect.Int32:
		return 4
	case reflect.Int64:
		return 8
	case reflect.String:
		return int64(val.Len())
	case reflect.Float32:
		return 4
	case reflect.Float64:
		return 8
	}
	return 4
}

func cmpIntBinary(as, bs, order string, signed bool) bool {
	abs := []byte(as)
	bbs := []byte(bs)
	la, lb := len(abs), len(bbs)

	// convert to big endian to simplify logic below
	if order == "LittleEndian" {
		for i, j := 0, len(abs)-1; i < j; i, j = i+1, j-1 {
			abs[i], abs[j] = abs[j], abs[i]
		}
		for i, j := 0, len(bbs)-1; i < j; i, j = i+1, j-1 {
			bbs[i], bbs[j] = bbs[j], bbs[i]
		}
	}

	if !signed {
		if la < lb {
			abs = append(make([]byte, lb-la), abs...)
		} else if lb < la {
			bbs = append(make([]byte, la-lb), bbs...)
		}
	} else {
		if la < lb {
			sb := (abs[0] >> 7) & 1
			pre := make([]byte, lb-la)
			if sb == 1 {
				for i := range lb - la {
					pre[i] = byte(0xFF)
				}
			}
			abs = append(pre, abs...)

		} else if la > lb {
			sb := (bbs[0] >> 7) & 1
			pre := make([]byte, la-lb)
			if sb == 1 {
				for i := range la - lb {
					pre[i] = byte(0xFF)
				}
			}
			bbs = append(pre, bbs...)
		}

		asb, bsb := (abs[0]>>7)&1, (bbs[0]>>7)&1

		if asb < bsb {
			return false
		} else if asb > bsb {
			return true
		}

	}

	for i := range abs {
		if abs[i] < bbs[i] {
			return true
		} else if abs[i] > bbs[i] {
			return false
		}
	}
	return false
}
