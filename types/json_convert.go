package types

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ConvertToJSONType converts a parquet value to its JSON-friendly representation using the
// schema element's type information (physical, converted, logical).
// Options (e.g., WithGeospatialConfig) control type-specific rendering; unset options use defaults.
// This is the canonical conversion entry point; callers only need the SchemaElement.
func ConvertToJSONType(val any, se *parquet.SchemaElement, opts ...JSONTypeOption) any {
	if val == nil || se == nil {
		return val
	}

	var cfg JSONTypeConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	pT, cT, lT := se.Type, se.ConvertedType, se.LogicalType

	// Handle INT96 timestamp conversion (before checking logical/converted types)
	if pT != nil && *pT == parquet.Type_INT96 {
		return convertINT96Value(val)
	}

	// LogicalType takes precedence (newer standard)
	if lT != nil {
		return parquetTypeToJSONTypeWithLogical(val, pT, lT, cfg.Geospatial)
	}

	// Fall back to ConvertedType (legacy)
	return parquetTypeToJSONTypeWithConverted(val, pT, cT, int(se.GetPrecision()), int(se.GetScale()))
}

// parquetTypeToJSONTypeWithLogical converts a value using its LogicalType.
func parquetTypeToJSONTypeWithLogical(val any, pT *parquet.Type, lT *parquet.LogicalType, geoCfg *GeospatialConfig) any {
	if lT.IsSetDECIMAL() {
		decimal := lT.GetDECIMAL()
		return ConvertDecimalValue(val, pT, int(decimal.GetPrecision()), int(decimal.GetScale()))
	}
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
		return val
	}
	if lT.IsSetINTEGER() {
		return ConvertIntegerLogicalValue(val, pT, lT.GetINTEGER())
	}
	if lT.IsSetUUID() {
		return ConvertUUIDValue(val)
	}
	if lT.IsSetGEOMETRY() {
		if geoCfg == nil {
			geoCfg = defaultGeospatialConfig
		}
		return ConvertGeometryLogicalValue(val, lT.GetGEOMETRY(), geoCfg)
	}
	if lT.IsSetGEOGRAPHY() {
		if geoCfg == nil {
			geoCfg = defaultGeospatialConfig
		}
		return ConvertGeographyLogicalValue(val, lT.GetGEOGRAPHY(), geoCfg)
	}
	if lT.IsSetBSON() {
		return ConvertBSONLogicalValue(val)
	}
	return val
}

// parquetTypeToJSONTypeWithConverted converts a value using its ConvertedType (legacy path).
func parquetTypeToJSONTypeWithConverted(val any, pT *parquet.Type, cT *parquet.ConvertedType, precision, scale int) any {
	if cT == nil {
		if pT != nil && (*pT == parquet.Type_BYTE_ARRAY || *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY) {
			return convertBinaryValue(val)
		}
		return val
	}

	switch *cT {
	case parquet.ConvertedType_DECIMAL:
		return ConvertDecimalValue(val, pT, precision, scale)
	case parquet.ConvertedType_UTF8, parquet.ConvertedType_DATE,
		parquet.ConvertedType_INT_32, parquet.ConvertedType_INT_64:
		return val
	case parquet.ConvertedType_TIME_MILLIS:
		if v, ok := val.(int32); ok {
			return TIME_MILLISToTimeFormat(v)
		}
		return val
	case parquet.ConvertedType_TIME_MICROS:
		if v, ok := val.(int64); ok {
			return TIME_MICROSToTimeFormat(v)
		}
		return val
	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		return ConvertTimestampValue(val, parquet.ConvertedType_TIMESTAMP_MILLIS)
	case parquet.ConvertedType_TIMESTAMP_MICROS:
		return ConvertTimestampValue(val, parquet.ConvertedType_TIMESTAMP_MICROS)
	case parquet.ConvertedType_INT_8:
		if v, ok := val.(int32); ok {
			return int8(v)
		}
		return val
	case parquet.ConvertedType_INT_16:
		if v, ok := val.(int32); ok {
			return int16(v)
		}
		return val
	case parquet.ConvertedType_UINT_8:
		if v, ok := val.(int32); ok {
			return uint8(v)
		}
		return val
	case parquet.ConvertedType_UINT_16:
		if v, ok := val.(int32); ok {
			return uint16(v)
		}
		return val
	case parquet.ConvertedType_UINT_32:
		if v, ok := val.(int32); ok {
			return uint32(v)
		}
		return val
	case parquet.ConvertedType_UINT_64:
		if v, ok := val.(int64); ok {
			return uint64(v)
		}
		return val
	case parquet.ConvertedType_INTERVAL:
		return convertIntervalValue(val)
	case parquet.ConvertedType_BSON:
		return ConvertBSONLogicalValue(val)
	default:
		return val
	}
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
		if v, ok := val.([]byte); ok {
			return decimalByteArrayToFloat(v, precision, scale)
		}
	}
	return val
}

// convertINT96Value handles INT96 to datetime string conversion
func convertINT96Value(val any) any {
	if v, ok := val.(string); ok {
		// Convert INT96 binary data to time.Time, then format as ISO 8601 string
		t, err := INT96ToTimeWithError(v)
		if err != nil {
			return val // Return original value if conversion fails
		}
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

// ConvertBSONLogicalValue handles BSON decoding to a map for JSON compatibility
func ConvertBSONLogicalValue(val any) any {
	if val == nil {
		return nil
	}

	var bsonBytes []byte
	switch v := val.(type) {
	case []byte:
		bsonBytes = v
	case string:
		bsonBytes = []byte(v)
	default:
		// If not bytes or string, return as-is
		return val
	}

	// If empty BSON data, return empty map
	if len(bsonBytes) == 0 {
		return map[string]any{}
	}

	// Decode BSON to a map
	var result map[string]any
	err := bson.Unmarshal(bsonBytes, &result)
	if err != nil {
		// If decoding fails, return base64 as fallback
		return base64.StdEncoding.EncodeToString(bsonBytes)
	}

	return result
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

	// Parquet uses little-endian for FLOAT16
	u := binary.LittleEndian.Uint16(b)
	sign := ((u >> 15) & 0x1) != 0
	exp := (u >> 10) & 0x1F
	frac := u & 0x3FF

	var f float32
	switch exp {
	case 0x1F: // Inf/NaN
		if frac != 0 {
			f = float32(math.NaN())
		} else if sign {
			f = float32(math.Inf(-1))
		} else {
			f = float32(math.Inf(1))
		}
	case 0: // subnormal or zero
		mant := float32(frac) / 1024.0
		f = mant / float32(1<<14)
		if sign {
			f = -f
		}
	default: // normalized
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
