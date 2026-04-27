package types

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// Scan a string to parquet value; length and scale just for decimal
func StrToParquetType(s string, pT *parquet.Type, cT *parquet.ConvertedType, length, scale int) (any, error) {
	if cT == nil {
		switch *pT {
		case parquet.Type_BOOLEAN:
			var v bool
			_, err := fmt.Sscanf(s, "%t", &v)
			return v, err
		case parquet.Type_INT32:
			var v int32
			_, err := fmt.Sscanf(s, "%d", &v)
			return v, err
		case parquet.Type_INT64:
			var v int64
			_, err := fmt.Sscanf(s, "%d", &v)
			return v, err
		case parquet.Type_INT96:
			if res, err := ParseINT96String(s); err == nil {
				return res, nil
			}
			res := StrIntToBinary(s, "LittleEndian", 12, true)
			return res, nil
		case parquet.Type_FLOAT:
			var v float32
			_, err := fmt.Sscanf(s, "%f", &v)
			return v, err
		case parquet.Type_DOUBLE:
			var v float64
			_, err := fmt.Sscanf(s, "%f", &v)
			return v, err
		case parquet.Type_BYTE_ARRAY:
			if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
				return string(decoded), nil
			}
			return s, nil
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
				return string(decoded), nil
			}
			return s, nil
		default:
			return nil, nil
		}
	}

	switch *cT {
	case parquet.ConvertedType_UTF8:
		return s, nil
	case parquet.ConvertedType_INT_8:
		var v int8
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err
	case parquet.ConvertedType_INT_16:
		var v int16
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err
	case parquet.ConvertedType_INT_32:
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err
	case parquet.ConvertedType_UINT_8:
		var v uint8
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err
	case parquet.ConvertedType_UINT_16:
		var v uint16
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err
	case parquet.ConvertedType_UINT_32:
		var v uint32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err
	case parquet.ConvertedType_DATE:
		if v, err := ParseDateString(s); err == nil {
			return v, nil
		}
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err
	case parquet.ConvertedType_TIME_MILLIS:
		if nanos, err := ParseTimeString(s); err == nil {
			return int32(nanos / int64(time.Millisecond)), nil
		}
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err
	case parquet.ConvertedType_UINT_64:
		var vt uint64
		_, err := fmt.Sscanf(s, "%d", &vt)
		return int64(vt), err
	case parquet.ConvertedType_INT_64:
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	case parquet.ConvertedType_TIME_MICROS:
		if nanos, err := ParseTimeString(s); err == nil {
			return nanos / int64(time.Microsecond), nil
		}
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixNano() / int64(time.Millisecond), nil
		}
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	case parquet.ConvertedType_TIMESTAMP_MICROS:
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixNano() / int64(time.Microsecond), nil
		}
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	case parquet.ConvertedType_INTERVAL:
		if res, err := ParseIntervalString(s); err == nil {
			return res, nil
		}
		res := StrIntToBinary(s, "LittleEndian", 12, false)
		return res, nil
	case parquet.ConvertedType_DECIMAL:
		numSca := big.NewFloat(1.0)
		for range scale {
			numSca.Mul(numSca, big.NewFloat(10))
		}
		num := new(big.Float)
		num.SetString(s)
		num.Mul(num, numSca)

		switch *pT {
		case parquet.Type_INT32:
			tmp, _ := num.Float64()
			return int32(tmp), nil
		case parquet.Type_INT64:
			tmp, _ := num.Float64()
			return int64(tmp), nil
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			s = num.Text('f', 0)
			res := StrIntToBinary(s, "BigEndian", length, true)
			return res, nil
		default:
			s = num.Text('f', 0)
			res := StrIntToBinary(s, "BigEndian", 0, true)
			return res, nil
		}
	case parquet.ConvertedType_BSON, parquet.ConvertedType_JSON, parquet.ConvertedType_ENUM:
		// These are BYTE_ARRAY types that should preserve the string value as-is
		return s, nil
	default:
		return nil, nil
	}
}

func strToLogicalType(s string, lT *parquet.LogicalType, pT *parquet.Type, length int) (any, bool, error) {
	if lT.IsSetFLOAT16() {
		v, err := ParseFloat16String(s)
		return v, true, err
	}
	if lT.IsSetUUID() {
		if u, err := uuid.Parse(s); err == nil {
			return string(u[:]), true, nil
		}
		return s, true, nil
	}
	if lT.IsSetTIMESTAMP() {
		v, err := strToTimestampLogical(s, lT.GetTIMESTAMP())
		return v, err == nil, err
	}
	if lT.IsSetTIME() {
		v, err := strToTimeLogical(s, lT.GetTIME())
		return v, err == nil, err
	}
	if lT.IsSetDATE() {
		if v, err := ParseDateString(s); err == nil {
			return v, true, nil
		}
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), true, err
	}
	if lT.IsSetDECIMAL() {
		v, err := strToDecimalLogical(s, lT.GetDECIMAL(), pT, length)
		return v, true, err
	}
	return nil, false, nil
}

func StrToParquetTypeWithLogical(s string, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length, scale int) (any, error) {
	if lT != nil {
		if v, handled, err := strToLogicalType(s, lT, pT, length); handled {
			return v, err
		}
	}

	return StrToParquetType(s, pT, cT, length, scale)
}
