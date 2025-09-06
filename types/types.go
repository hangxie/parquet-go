package types

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
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
			return s, nil
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
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
	case parquet.ConvertedType_DATE, parquet.ConvertedType_TIME_MILLIS:
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), err
	case parquet.ConvertedType_UINT_64:
		var vt uint64
		_, err := fmt.Sscanf(s, "%d", &vt)
		return int64(vt), err
	case parquet.ConvertedType_INT_64,
		parquet.ConvertedType_TIME_MICROS,
		parquet.ConvertedType_TIMESTAMP_MICROS,
		parquet.ConvertedType_TIMESTAMP_MILLIS:
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, err
	case parquet.ConvertedType_INTERVAL:
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
	default:
		return nil, nil
	}
}

func InterfaceToParquetType(src any, pT *parquet.Type) (any, error) {
	if src == nil {
		return src, nil
	}

	if pT == nil {
		return src, nil
	}

	switch *pT {
	case parquet.Type_BOOLEAN:
		if _, ok := src.(bool); ok {
			return src, nil
		}
		rv := reflect.ValueOf(src)
		if !rv.IsValid() || rv.Kind() != reflect.Bool {
			return nil, fmt.Errorf("cannot convert %T to bool", src)
		}
		return rv.Bool(), nil

	case parquet.Type_INT32:
		if _, ok := src.(int32); ok {
			return src, nil
		}
		rv := reflect.ValueOf(src)
		if !rv.IsValid() || (rv.Kind() < reflect.Int || rv.Kind() > reflect.Uintptr) {
			return nil, fmt.Errorf("cannot convert %T to int32", src)
		}
		return int32(rv.Int()), nil

	case parquet.Type_INT64:
		if _, ok := src.(int64); ok {
			return src, nil
		}
		rv := reflect.ValueOf(src)
		if !rv.IsValid() || (rv.Kind() < reflect.Int || rv.Kind() > reflect.Uintptr) {
			return nil, fmt.Errorf("cannot convert %T to int64", src)
		}
		return rv.Int(), nil

	case parquet.Type_FLOAT:
		if _, ok := src.(float32); ok {
			return src, nil
		}
		rv := reflect.ValueOf(src)
		if !rv.IsValid() || (rv.Kind() != reflect.Float32 && rv.Kind() != reflect.Float64) {
			return nil, fmt.Errorf("cannot convert %T to float32", src)
		}
		return float32(rv.Float()), nil

	case parquet.Type_DOUBLE:
		if _, ok := src.(float64); ok {
			return src, nil
		}
		rv := reflect.ValueOf(src)
		if !rv.IsValid() || (rv.Kind() != reflect.Float32 && rv.Kind() != reflect.Float64) {
			return nil, fmt.Errorf("cannot convert %T to float64", src)
		}
		return rv.Float(), nil

	case parquet.Type_INT96, parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if _, ok := src.(string); ok {
			return src, nil
		}
		rv := reflect.ValueOf(src)
		if !rv.IsValid() || rv.Kind() != reflect.String {
			return nil, fmt.Errorf("cannot convert %T to string", src)
		}
		return rv.String(), nil

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

func JSONTypeToParquetType(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, length, scale int) (any, error) {
	if val.Type().Kind() == reflect.Interface && val.IsNil() {
		return nil, nil
	}

	// Handle decimal types specially to preserve precision from JSON numbers
	if cT != nil && *cT == parquet.ConvertedType_DECIMAL {
		switch val.Kind() {
		case reflect.Float32, reflect.Float64:
			// For JSON numbers coming as floats, format with appropriate precision
			s := fmt.Sprintf("%."+fmt.Sprintf("%d", scale)+"f", val.Float())
			return StrToParquetType(s, pT, cT, length, scale)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			// For JSON numbers coming as integers
			s := fmt.Sprintf("%d", val.Int())
			return StrToParquetType(s, pT, cT, length, scale)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// For JSON numbers coming as unsigned integers
			s := fmt.Sprintf("%d", val.Uint())
			return StrToParquetType(s, pT, cT, length, scale)
		case reflect.String:
			// For JSON numbers coming as strings (from json.Number when UseNumber is used)
			return StrToParquetType(val.String(), pT, cT, length, scale)
		}
	}

	s := fmt.Sprintf("%v", val)
	return StrToParquetType(s, pT, cT, length, scale)
}

// ParquetTypeToJSONType converts a parquet physical value back to its logical JSON representation
func ParquetTypeToJSONType(val any, pT *parquet.Type, cT *parquet.ConvertedType, precision, scale int) any {
	if val == nil {
		return nil
	}

	// Handle INT96 timestamp conversion (before checking ConvertedType)
	if pT != nil && *pT == parquet.Type_INT96 {
		return convertINT96Value(val)
	}

	// If no converted type, check for binary types that need base64 encoding
	if cT == nil {
		if pT != nil && (*pT == parquet.Type_BYTE_ARRAY || *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY) {
			return convertBinaryValue(val)
		}
		return val
	}

	switch *cT {
	case parquet.ConvertedType_DECIMAL:
		// Convert decimal to numeric value for JSON output
		switch *pT {
		case parquet.Type_INT32:
			if v, ok := val.(int32); ok {
				return decimalIntToFloat(int64(v), scale)
			}
		case parquet.Type_INT64:
			if v, ok := val.(int64); ok {
				return decimalIntToFloat(v, scale)
			}
		case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
			if v, ok := val.(string); ok {
				return decimalByteArrayToFloat([]byte(v), precision, scale)
			}
		}
		return val

	case parquet.ConvertedType_UTF8:
		// UTF8 strings should remain as strings
		return val

	case parquet.ConvertedType_DATE:
		// DATE is stored as int32 days since epoch, could convert to readable format
		// For now, keeping as int32 for JSON compatibility
		return val

	case parquet.ConvertedType_TIME_MILLIS:
		// TIME_MILLIS is stored as int32 milliseconds, convert to readable time format
		if v, ok := val.(int32); ok {
			return TIME_MILLISToTimeFormat(v)
		}
		return val

	case parquet.ConvertedType_TIME_MICROS:
		// TIME_MICROS is stored as int64 microseconds, convert to readable time format
		if v, ok := val.(int64); ok {
			return TIME_MICROSToTimeFormat(v)
		}
		return val

	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		// TIMESTAMP_MILLIS is stored as int64 milliseconds since epoch
		// Convert to ISO8601 format
		return ConvertTimestampValue(val, parquet.ConvertedType_TIMESTAMP_MILLIS)

	case parquet.ConvertedType_TIMESTAMP_MICROS:
		// TIMESTAMP_MICROS is stored as int64 microseconds since epoch
		// Convert to ISO8601 format
		return ConvertTimestampValue(val, parquet.ConvertedType_TIMESTAMP_MICROS)

	case parquet.ConvertedType_INT_8:
		// Convert back to int8 representation for JSON
		if v, ok := val.(int32); ok {
			return int8(v)
		}
		return val

	case parquet.ConvertedType_INT_16:
		// Convert back to int16 representation for JSON
		if v, ok := val.(int32); ok {
			return int16(v)
		}
		return val

	case parquet.ConvertedType_INT_32:
		// Already int32
		return val

	case parquet.ConvertedType_INT_64:
		// Already int64
		return val

	case parquet.ConvertedType_UINT_8:
		// Convert back to uint8 representation for JSON
		if v, ok := val.(int32); ok {
			return uint8(v)
		}
		return val

	case parquet.ConvertedType_UINT_16:
		// Convert back to uint16 representation for JSON
		if v, ok := val.(int32); ok {
			return uint16(v)
		}
		return val

	case parquet.ConvertedType_UINT_32:
		// Convert back to uint32 representation for JSON
		if v, ok := val.(int32); ok {
			return uint32(v)
		}
		return val

	case parquet.ConvertedType_UINT_64:
		// Convert back to uint64 representation for JSON
		if v, ok := val.(int64); ok {
			return uint64(v)
		}
		return val

	case parquet.ConvertedType_INTERVAL:
		// Convert INTERVAL to Go duration string
		return convertIntervalValue(val)

	default:
		// For other converted types, return as-is
		return val
	}
}

// ParquetTypeToJSONTypeWithLogical converts a parquet physical value back to its logical JSON representation
// supporting both ConvertedType and LogicalType
func ParquetTypeToJSONTypeWithLogical(val any, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, precision, scale int) any {
	if val == nil {
		return nil
	}

	// Handle INT96 timestamp conversion (before checking logical types)
	if pT != nil && *pT == parquet.Type_INT96 {
		return convertINT96Value(val)
	}

	// Check for LogicalType first (newer standard)
	if lT != nil {
		if lT.IsSetDECIMAL() {
			decimal := lT.GetDECIMAL()
			actualPrecision := int(decimal.GetPrecision())
			actualScale := int(decimal.GetScale())
			return ConvertDecimalValue(val, pT, actualPrecision, actualScale)
		}
		// Newer logical types handling
		if lT.IsSetFLOAT16() {
			return ConvertFloat16LogicalValue(val)
		}
		if lT.IsSetTIMESTAMP() {
			return convertTimestampLogicalValue(val, lT.GetTIMESTAMP())
		}
		if lT.IsSetTIME() {
			return ConvertTimeLogicalValue(val, lT.GetTIME())
		}
		if lT.IsSetDATE() {
			return ConvertDateLogicalValue(val)
		}
		if lT.IsSetSTRING() {
			// STRING logical type should return the string value as-is
			return val
		}
		if lT.IsSetINTEGER() {
			return ConvertIntegerLogicalValue(val, pT, lT.GetINTEGER())
		}
		if lT.IsSetUUID() {
			return ConvertUUIDValue(val)
		}
		if lT.IsSetGEOMETRY() {
			return ConvertGeometryLogicalValue(val, lT.GetGEOMETRY())
		}
		if lT.IsSetGEOGRAPHY() {
			return ConvertGeographyLogicalValue(val, lT.GetGEOGRAPHY())
		}
		// For other logical types, don't apply base64 encoding - return as-is
		return val
	}

	// Fall back to ConvertedType (legacy)
	if cT != nil && *cT == parquet.ConvertedType_DECIMAL {
		return ConvertDecimalValue(val, pT, precision, scale)
	}

	// If no logical type and no converted type, check for binary types
	if lT == nil && cT == nil {
		if pT != nil && (*pT == parquet.Type_BYTE_ARRAY || *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY) {
			return convertBinaryValue(val)
		}
	}

	// For other types, use the existing function
	return ParquetTypeToJSONType(val, pT, cT, precision, scale)
}

// ConvertDecimalValue handles decimal conversion for both logical and converted types
func ConvertDecimalValue(val any, pT *parquet.Type, precision, scale int) any {
	switch *pT {
	case parquet.Type_INT32:
		if v, ok := val.(int32); ok {
			return decimalIntToFloat(int64(v), scale)
		}
	case parquet.Type_INT64:
		if v, ok := val.(int64); ok {
			return decimalIntToFloat(v, scale)
		}
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if v, ok := val.(string); ok {
			return decimalByteArrayToFloat([]byte(v), precision, scale)
		}
	}
	return val
}

// convertINT96Value handles INT96 to datetime string conversion
func convertINT96Value(val any) any {
	if v, ok := val.(string); ok {
		// Convert INT96 binary data to time.Time, then format as ISO 8601 string
		t := INT96ToTime(v)
		return t.Format("2006-01-02T15:04:05.000000000Z")
	}
	return val
}

// convertIntervalValue handles INTERVAL to formatted string conversion
func convertIntervalValue(val any) any {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []byte:
		if len(v) == 12 {
			return IntervalToString(v)
		}
	case string:
		if len(v) == 12 {
			return IntervalToString([]byte(v))
		}
	}

	return val
}

// ConvertTimestampValue handles timestamp conversion to ISO8601 format
func ConvertTimestampValue(val any, convertedType parquet.ConvertedType) any {
	if val == nil {
		return nil
	}

	switch convertedType {
	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		if v, ok := val.(int64); ok {
			return TIMESTAMP_MILLISToISO8601(v, true) // Assume UTC adjusted
		}
	case parquet.ConvertedType_TIMESTAMP_MICROS:
		if v, ok := val.(int64); ok {
			return TIMESTAMP_MICROSToISO8601(v, true) // Assume UTC adjusted
		}
	}

	return val
}

// convertTimestampLogicalValue handles timestamp LogicalType conversion to ISO8601 format
func convertTimestampLogicalValue(val any, timestamp *parquet.TimestampType) any {
	if val == nil || timestamp == nil {
		return nil
	}

	v, ok := val.(int64)
	if !ok {
		return val
	}

	// Determine if timestamp is UTC adjusted
	adjustedToUTC := timestamp.IsAdjustedToUTC

	// Handle different timestamp units
	if timestamp.Unit != nil {
		if timestamp.Unit.IsSetMILLIS() {
			return TIMESTAMP_MILLISToISO8601(v, adjustedToUTC)
		}
		if timestamp.Unit.IsSetMICROS() {
			return TIMESTAMP_MICROSToISO8601(v, adjustedToUTC)
		}
		if timestamp.Unit.IsSetNANOS() {
			return TIMESTAMP_NANOSToISO8601(v, adjustedToUTC)
		}
	}

	// Default to milliseconds if unit is not specified
	return TIMESTAMP_MILLISToISO8601(v, adjustedToUTC)
}

// ConvertTimeLogicalValue handles time LogicalType conversion to time format
func ConvertTimeLogicalValue(val any, timeType *parquet.TimeType) any {
	if val == nil || timeType == nil {
		return val
	}

	// Handle different time units
	if timeType.Unit != nil {
		if timeType.Unit.IsSetMILLIS() {
			if v, ok := val.(int32); ok {
				return TIME_MILLISToTimeFormat(v)
			}
		}
		if timeType.Unit.IsSetMICROS() {
			if v, ok := val.(int64); ok {
				return TIME_MICROSToTimeFormat(v)
			}
		}
		if timeType.Unit.IsSetNANOS() {
			// NANOS would be stored as int64, but we can treat it similarly to micros
			if v, ok := val.(int64); ok {
				// Convert nanos to the same format but preserve nanosecond precision
				totalNanos := v
				hours := totalNanos / int64(time.Hour)
				totalNanos %= int64(time.Hour)
				minutes := totalNanos / int64(time.Minute)
				totalNanos %= int64(time.Minute)
				seconds := totalNanos / int64(time.Second)
				totalNanos %= int64(time.Second)
				nanos := totalNanos

				return fmt.Sprintf("%02d:%02d:%02d.%09d", hours, minutes, seconds, nanos)
			}
		}
	}

	return val
}

// ConvertDateLogicalValue handles DATE LogicalType conversion to date format "2006-01-02"
func ConvertDateLogicalValue(val any) any {
	if val == nil {
		return val
	}

	// DATE is stored as int32 days since Unix epoch (1970-01-01)
	if v, ok := val.(int32); ok {
		// Convert days since epoch to time.Time
		epochDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		resultDate := epochDate.AddDate(0, 0, int(v))
		return resultDate.Format("2006-01-02")
	}

	return val
}

// convertBinaryValue handles base64 encoding for binary data without logical/converted types
func convertBinaryValue(val any) any {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	case string:
		return base64.StdEncoding.EncodeToString([]byte(v))
	}

	return val
}

// ConvertFloat16LogicalValue converts FIXED[2] IEEE 754 half-precision to float32
func ConvertFloat16LogicalValue(val any) any {
	if val == nil {
		return nil
	}

	var b []byte
	switch v := val.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return val
	}

	if len(b) != 2 {
		return val
	}

	// Assume big-endian order as commonly used in Parquet fixed byte arrays
	u := uint16(b[0])<<8 | uint16(b[1])
	sign := ((u >> 15) & 0x1) != 0
	exp := (u >> 10) & 0x1F
	frac := u & 0x3FF

	var f float32
	if exp == 0x1F { // Inf/NaN
		if frac != 0 {
			// NaN: keep raw
			return val
		}
		if sign {
			f = float32(math.Inf(-1))
		} else {
			f = float32(math.Inf(1))
		}
	} else if exp == 0 { // subnormal or zero
		mant := float32(frac) / 1024.0
		f = mant / float32(1<<14)
		if sign {
			f = -f
		}
	} else { // normalized
		mant := 1.0 + float32(frac)/1024.0
		exponent := int(exp) - 15
		// compute mant * 2^exponent
		if exponent >= 0 {
			p := 1 << exponent
			f = mant * float32(p)
		} else {
			p := 1 << (-exponent)
			f = mant / float32(p)
		}
		if sign {
			f = -f
		}
	}

	return f
}

// ConvertIntegerLogicalValue converts value based on INTEGER logical type width/sign
func ConvertIntegerLogicalValue(val any, pT *parquet.Type, intType *parquet.IntType) any {
	if val == nil || intType == nil {
		return val
	}

	bitWidth := intType.GetBitWidth()
	signed := intType.GetIsSigned()

	// Helper closures to cast from int32 and int64
	cast32 := func(v int32) any {
		if signed {
			switch bitWidth {
			case 8:
				return int8(v)
			case 16:
				return int16(v)
			case 32:
				return v
			default:
				return v
			}
		} else {
			switch bitWidth {
			case 8:
				return uint8(v)
			case 16:
				return uint16(v)
			case 32:
				return uint32(v)
			default:
				return uint32(v)
			}
		}
	}

	cast64 := func(v int64) any {
		if signed {
			if bitWidth == 64 {
				return v
			}
			// For 32-bit integer stored in int64, downcast safely
			return int32(v)
		} else {
			if bitWidth == 64 {
				return uint64(v)
			}
			return uint32(v)
		}
	}

	switch v := val.(type) {
	case int32:
		return cast32(v)
	case int64:
		return cast64(v)
	default:
		return val
	}
}

// ConvertUUIDValue handles UUID conversion from binary data to standard UUID string format
func ConvertUUIDValue(val any) any {
	if val == nil {
		return nil
	}

	var bytes []byte
	switch v := val.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return val
	}

	// UUID should be exactly 16 bytes
	if len(bytes) != 16 {
		return val
	}

	// Format as standard UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uint32(bytes[0])<<24|uint32(bytes[1])<<16|uint32(bytes[2])<<8|uint32(bytes[3]),
		uint16(bytes[4])<<8|uint16(bytes[5]),
		uint16(bytes[6])<<8|uint16(bytes[7]),
		uint16(bytes[8])<<8|uint16(bytes[9]),
		uint64(bytes[10])<<40|uint64(bytes[11])<<32|uint64(bytes[12])<<24|uint64(bytes[13])<<16|uint64(bytes[14])<<8|uint64(bytes[15]))
}

// ConvertGeometryLogicalValue converts WKB bytes to a JSON-friendly wrapper with hex and CRS
func ConvertGeometryLogicalValue(val any, geom *parquet.GeometryType) any {
	if val == nil {
		return nil
	}
	var b []byte
	switch v := val.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return val
	}
	crs := "OGC:CRS84"
	if geom != nil && geom.CRS != nil && *geom.CRS != "" {
		crs = *geom.CRS
	}
	switch geometryJSONMode {
	case GeospatialModeGeoJSON:
		if gj, ok := wkbToGeoJSON(b); ok {
			if crs != "OGC:CRS84" && geospatialReprojector != nil {
				if rj, ok2 := geospatialReprojector(crs, gj); ok2 {
					gj = rj
				}
			}
			if geospatialGeoJSONAsFeature {
				return makeGeoJSONFeature(gj, map[string]any{"crs": crs})
			}
			return gj
		}
		// fallback
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs}
	case GeospatialModeBase64:
		return map[string]any{"wkb_b64": base64.StdEncoding.EncodeToString(b), "crs": crs}
	case GeospatialModeHybrid:
		if gj, ok := wkbToGeoJSON(b); ok {
			m := wrapGeoJSONHybrid(gj, b, geospatialHybridUseBase64, true)
			m["crs"] = crs
			return m
		}
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs}
	default: // hex
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs}
	}
}

// ConvertGeographyLogicalValue converts WKB bytes to a JSON-friendly wrapper with hex, CRS and algorithm
func ConvertGeographyLogicalValue(val any, geo *parquet.GeographyType) any {
	if val == nil {
		return nil
	}
	var b []byte
	switch v := val.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return val
	}
	crs := "OGC:CRS84"
	if geo != nil && geo.CRS != nil && *geo.CRS != "" {
		crs = *geo.CRS
	}
	algo := "SPHERICAL"
	if geo != nil && geo.Algorithm != nil {
		algo = geo.Algorithm.String()
	}
	switch geographyJSONMode {
	case GeospatialModeGeoJSON:
		if gj, ok := wkbToGeoJSON(b); ok {
			if crs != "OGC:CRS84" && geospatialReprojector != nil {
				if rj, ok2 := geospatialReprojector(crs, gj); ok2 {
					gj = rj
				}
			}
			if geospatialGeoJSONAsFeature {
				return makeGeoJSONFeature(gj, map[string]any{"crs": crs, "algorithm": algo})
			}
			return gj
		}
		// fallback
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs, "algorithm": algo}
	case GeospatialModeBase64:
		return map[string]any{"wkb_b64": base64.StdEncoding.EncodeToString(b), "crs": crs, "algorithm": algo}
	case GeospatialModeHybrid:
		if gj, ok := wkbToGeoJSON(b); ok {
			if crs != "OGC:CRS84" && geospatialReprojector != nil {
				if rj, ok2 := geospatialReprojector(crs, gj); ok2 {
					gj = rj
				}
			}
			m := wrapGeoJSONHybrid(gj, b, geospatialHybridUseBase64, true)
			m["crs"], m["algorithm"] = crs, algo
			return m
		}
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs, "algorithm": algo}
	default: // hex
		return map[string]any{"wkb_hex": hex.EncodeToString(b), "crs": crs, "algorithm": algo}
	}
}

// decimalIntToFloat converts a decimal integer value to float64 for JSON output
func decimalIntToFloat(dec int64, scale int) float64 {
	if scale <= 0 {
		return float64(dec)
	}

	// Calculate the divisor: 10^scale
	divisor := 1.0
	for i := 0; i < scale; i++ {
		divisor *= 10.0
	}

	return float64(dec) / divisor
}

// decimalByteArrayToFloat converts a decimal byte array value to float64 for JSON output
func decimalByteArrayToFloat(data []byte, precision, scale int) any {
	// Convert the string representation to float64
	strValue := DECIMAL_BYTE_ARRAY_ToString(data, precision, scale)

	// Parse the string to float64
	if floatVal, err := strconv.ParseFloat(strValue, 64); err == nil {
		return floatVal
	}

	// If parsing fails, fallback to string (should not happen normally)
	return strValue
}
