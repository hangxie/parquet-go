package types

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/google/uuid"

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

// StrToParquetTypeWithLogical converts a string to a parquet value using LogicalType for newer types
// This function extends StrToParquetType to handle FLOAT16, UUID, GEOMETRY, GEOGRAPHY, and TIME/TIMESTAMP with nanos
func strToTimestampLogical(s string, ts *parquet.TimestampType) (any, error) {
	if ts.Unit != nil {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			switch {
			case ts.Unit.IsSetNANOS():
				return t.UnixNano(), nil
			case ts.Unit.IsSetMICROS():
				return t.UnixNano() / int64(time.Microsecond), nil
			case ts.Unit.IsSetMILLIS():
				return t.UnixNano() / int64(time.Millisecond), nil
			}
		}
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	}
	return nil, fmt.Errorf("timestamp unit not set")
}

func strToTimeLogical(s string, t *parquet.TimeType) (any, error) {
	if t.Unit == nil {
		return nil, fmt.Errorf("time unit not set")
	}
	if nanos, err := ParseTimeString(s); err == nil {
		switch {
		case t.Unit.IsSetNANOS():
			return nanos, nil
		case t.Unit.IsSetMICROS():
			return nanos / int64(time.Microsecond), nil
		case t.Unit.IsSetMILLIS():
			return int32(nanos / int64(time.Millisecond)), nil
		}
	}
	if t.Unit.IsSetMILLIS() {
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	}
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}

func strToDecimalLogical(s string, dec *parquet.DecimalType, pT *parquet.Type, length int) (any, error) {
	decScale := int(dec.GetScale())
	numSca := big.NewFloat(1.0)
	for range decScale {
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
		return StrIntToBinary(num.Text('f', 0), "BigEndian", length, true), nil
	default:
		return StrIntToBinary(num.Text('f', 0), "BigEndian", 0, true), nil
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

func convertToBool(src any) (any, error) {
	if v, ok := src.(bool); ok {
		return v, nil
	}
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || rv.Kind() != reflect.Bool {
		return nil, fmt.Errorf("convert %T to bool", src)
	}
	return rv.Bool(), nil
}

func convertToInt32(src any) (any, error) {
	if v, ok := src.(int32); ok {
		return v, nil
	}
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || rv.Kind() < reflect.Int || rv.Kind() > reflect.Uintptr {
		return nil, fmt.Errorf("convert %T to int32", src)
	}
	return int32(rv.Int()), nil
}

func convertToInt64(src any) (any, error) {
	if v, ok := src.(int64); ok {
		return v, nil
	}
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || rv.Kind() < reflect.Int || rv.Kind() > reflect.Uintptr {
		return nil, fmt.Errorf("convert %T to int64", src)
	}
	return rv.Int(), nil
}

func convertToFloat32(src any) (any, error) {
	if v, ok := src.(float32); ok {
		return v, nil
	}
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || (rv.Kind() != reflect.Float32 && rv.Kind() != reflect.Float64) {
		return nil, fmt.Errorf("convert %T to float32", src)
	}
	return float32(rv.Float()), nil
}

func convertToFloat64(src any) (any, error) {
	if v, ok := src.(float64); ok {
		return v, nil
	}
	rv := reflect.ValueOf(src)
	if !rv.IsValid() || (rv.Kind() != reflect.Float32 && rv.Kind() != reflect.Float64) {
		return nil, fmt.Errorf("convert %T to float64", src)
	}
	return rv.Float(), nil
}

func convertToString(src any) (any, error) {
	if v, ok := src.(string); ok {
		return v, nil
	}
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

// order=LittleEndian or BigEndian; length is byte num
func StrIntToBinary(num, order string, length int, signed bool) string {
	bigNum := new(big.Int)
	bigNum.SetString(num, 10)
	if !signed {
		res := bigNum.Bytes()
		if len(res) < length {
			res = append(make([]byte, length-len(res)), res...)
		}
		if order == "LittleEndian" {
			for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
				res[i], res[j] = res[j], res[i]
			}
		}
		if length > 0 {
			res = res[len(res)-length:]
		}
		return string(res)
	}

	flag := bigNum.Cmp(big.NewInt(0))
	if flag == 0 {
		if length <= 0 {
			length = 1
		}
		return string(make([]byte, length))
	}

	bigNum = bigNum.SetBytes(bigNum.Bytes())
	bs := bigNum.Bytes()

	if len(bs) < length {
		bs = append(make([]byte, length-len(bs)), bs...)
	}

	upperBs := make([]byte, len(bs))
	upperBs[0] = byte(0x80)
	upper := new(big.Int)
	upper.SetBytes(upperBs)
	if flag > 0 {
		upper = upper.Sub(upper, big.NewInt(1))
	}

	if bigNum.Cmp(upper) > 0 {
		bs = append(make([]byte, 1), bs...)
	}

	if flag < 0 {
		modBs := make([]byte, len(bs)+1)
		modBs[0] = byte(0x01)
		mod := new(big.Int)
		mod.SetBytes(modBs)
		bs = mod.Sub(mod, bigNum).Bytes()
	}
	if length > 0 {
		bs = bs[len(bs)-length:]
	}
	if order == "LittleEndian" {
		for i, j := 0, len(bs)-1; i < j; i, j = i+1, j-1 {
			bs[i], bs[j] = bs[j], bs[i]
		}
	}
	return string(bs)
}
