package common

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
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

func findFuncTableByConvertedType(pT *parquet.Type, cT *parquet.ConvertedType) (FuncTable, bool) {
	if cT == nil {
		return nil, false
	}
	if table, ok := convertedTypeFuncTable[*cT]; ok {
		return table, true
	} else if *cT == parquet.ConvertedType_DECIMAL && pT != nil {
		switch *pT {
		case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			return decimalStringFuncTable{}, true
		case parquet.Type_INT32:
			return int32FuncTable{}, true
		case parquet.Type_INT64:
			return int64FuncTable{}, true
		}
	}
	return nil, false
}

func findFuncTableByLogicalType(pT *parquet.Type, logT *parquet.LogicalType) (FuncTable, bool) {
	if logT == nil {
		return nil, false
	}
	if logT.UNKNOWN != nil {
		return nil, false
	}
	if logT.TIME != nil || logT.TIMESTAMP != nil {
		table, err := FindFuncTable(pT, nil, nil)
		if err != nil {
			return nil, false
		}
		return table, true
	} else if logT.DATE != nil {
		return int32FuncTable{}, true
	} else if logT.INTEGER != nil {
		if logT.INTEGER.IsSigned {
			table, err := FindFuncTable(pT, nil, nil)
			if err != nil {
				return nil, false
			}
			return table, true
		} else if pT != nil {
			switch *pT {
			case parquet.Type_INT32:
				return uint32FuncTable{}, true
			case parquet.Type_INT64:
				return uint64FuncTable{}, true
			}
		}
	} else if logT.DECIMAL != nil && pT != nil {
		switch *pT {
		case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			return decimalStringFuncTable{}, true
		case parquet.Type_INT32:
			return int32FuncTable{}, true
		case parquet.Type_INT64:
			return int64FuncTable{}, true
		}
	} else if logT.BSON != nil || logT.JSON != nil || logT.STRING != nil || logT.UUID != nil {
		return stringFuncTable{}, true
	} else if logT.FLOAT16 != nil {
		// FLOAT16 stored in FIXED[2]; use numeric ordering
		return float16FuncTable{}, true
	} else if logT.VARIANT != nil || logT.GEOMETRY != nil || logT.GEOGRAPHY != nil {
		// Treat as binary for sizing/min/max
		return stringFuncTable{}, true
	}
	return nil, false
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

	if table, ok := findFuncTableByConvertedType(pT, cT); ok {
		return table, nil
	}

	if table, ok := findFuncTableByLogicalType(pT, logT); ok {
		return table, nil
	}

	// If logT has UNKNOWN set, fall back to physical type
	if logT != nil && logT.UNKNOWN != nil && pT != nil {
		if table, ok := parquetTypeFuncTable[*pT]; ok {
			return table, nil
		}
	}

	return nil, fmt.Errorf("find func table for given types: %v, %v, %v", pT, cT, logT)
}
