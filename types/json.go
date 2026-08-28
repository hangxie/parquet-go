package types

import (
	"encoding/base64"
	"fmt"
	"math"
	"reflect"
	"strconv"

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
	var converted any
	if lT != nil {
		converted = parquetTypeToJSONTypeWithLogical(val, pT, lT, cfg.Geospatial)
	} else {
		// Fall back to ConvertedType (legacy)
		converted = parquetTypeToJSONTypeWithConverted(val, pT, cT, int(se.GetPrecision()), int(se.GetScale()))
	}

	// NaN/Inf have no JSON number representation; quote them the same way
	// JSONWriter already accepts them on input, so output stays round-trippable.
	return nonFiniteFloatToJSONString(converted)
}

// nonFiniteFloatToJSONString rewrites non-finite floating-point values as their canonical
// quoted string form. Finite floats and all other types pass through unchanged.
func nonFiniteFloatToJSONString(val any) any {
	var f float64
	switch v := val.(type) {
	case float64:
		f = v
	case float32:
		f = float64(v)
	default:
		// user structs can declare named float types, which only reflection sees through
		rv := reflect.ValueOf(val)
		if !rv.IsValid() || (rv.Kind() != reflect.Float32 && rv.Kind() != reflect.Float64) {
			return val
		}
		f = rv.Float()
	}

	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		// "Infinity" rather than Go's "+Inf": both round trip here, but "+Inf" is not
		// consistently accepted across ecosystems. Java and Jackson reject it, and
		// JavaScript's Number() silently yields NaN, turning an infinity into a
		// different value without an error.
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	default:
		return val
	}
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

func JSONTypeToParquetType(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, length, scale int) (any, error) {
	return JSONTypeToParquetTypeWithLogical(val, pT, cT, nil, length, scale)
}

func JSONTypeToParquetTypeWithLogical(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length, scale int) (any, error) {
	if val.Type().Kind() == reflect.Interface && val.IsNil() {
		return nil, nil
	}

	// Handle decimal types specially to preserve precision from JSON numbers
	isDecimal := (cT != nil && *cT == parquet.ConvertedType_DECIMAL) || (lT != nil && lT.IsSetDECIMAL())
	if isDecimal {
		// Get scale from LogicalType if available
		if lT != nil && lT.IsSetDECIMAL() {
			scale = int(lT.GetDECIMAL().GetScale())
		}
		switch val.Kind() {
		case reflect.Float32, reflect.Float64:
			// For JSON numbers coming as floats, format with appropriate precision
			s := fmt.Sprintf("%."+fmt.Sprintf("%d", scale)+"f", val.Float())
			return StrToParquetTypeWithLogical(s, pT, cT, lT, length, scale)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			// For JSON numbers coming as integers
			s := strconv.FormatInt(val.Int(), 10)
			return StrToParquetTypeWithLogical(s, pT, cT, lT, length, scale)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// For JSON numbers coming as unsigned integers
			s := strconv.FormatUint(val.Uint(), 10)
			return StrToParquetTypeWithLogical(s, pT, cT, lT, length, scale)
		case reflect.String:
			// For JSON numbers coming as strings (from json.Number when UseNumber is used)
			return StrToParquetTypeWithLogical(val.String(), pT, cT, lT, length, scale)
		}
	}

	// Try direct type conversion for non-decimal types (avoids fmt.Sprintf/Sscanf round-trip)
	if result, ok := jsonValueToParquetDirect(val, pT, cT, lT, length); ok {
		return result, nil
	}

	// Fallback to string-based conversion for complex/unusual types
	s := fmt.Sprintf("%v", val)
	return StrToParquetTypeWithLogical(s, pT, cT, lT, length, scale)
}

// jsonConvertedTypeDirect handles direct conversion for converted types.
func jsonConvertedTypeDirect(val reflect.Value, cT parquet.ConvertedType) (any, bool) {
	switch cT {
	case parquet.ConvertedType_UTF8:
		if val.Kind() == reflect.String {
			return val.String(), true
		}
	case parquet.ConvertedType_INT_8:
		if v, ok := getNumericValue[int64](val); ok {
			return int32(int8(v)), true
		}
	case parquet.ConvertedType_INT_16:
		if v, ok := getNumericValue[int64](val); ok {
			return int32(int16(v)), true
		}
	case parquet.ConvertedType_INT_32:
		if v, ok := getNumericValue[int64](val); ok {
			return int32(v), true
		}
	case parquet.ConvertedType_INT_64:
		if v, ok := getNumericValue[int64](val); ok {
			return v, true
		}
	case parquet.ConvertedType_UINT_8:
		if v, ok := getNumericValue[uint64](val); ok {
			return int32(uint8(v)), true
		}
	case parquet.ConvertedType_UINT_16:
		if v, ok := getNumericValue[uint64](val); ok {
			return int32(uint16(v)), true
		}
	case parquet.ConvertedType_UINT_32:
		if v, ok := getNumericValue[uint64](val); ok {
			return int32(uint32(v)), true
		}
	case parquet.ConvertedType_UINT_64:
		if v, ok := getNumericValue[uint64](val); ok {
			return int64(v), true
		}
	}
	return nil, false
}

// jsonPhysicalTypeDirect handles direct conversion for basic parquet physical types.
func jsonPhysicalTypeDirect(val reflect.Value, pT parquet.Type) (any, bool) {
	switch pT {
	case parquet.Type_BOOLEAN:
		if val.Kind() == reflect.Bool {
			return val.Bool(), true
		}
	case parquet.Type_INT32:
		if v, ok := getNumericValue[int64](val); ok {
			return int32(v), true
		}
	case parquet.Type_INT64:
		if v, ok := getNumericValue[int64](val); ok {
			return v, true
		}
	case parquet.Type_FLOAT:
		if v, ok := getNumericValue[float64](val); ok {
			return float32(v), true
		}
	case parquet.Type_DOUBLE:
		if v, ok := getNumericValue[float64](val); ok {
			return v, true
		}
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if val.Kind() == reflect.String {
			s := val.String()
			if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
				return string(decoded), true
			}
			return s, true
		}
	}
	return nil, false
}

// jsonValueToParquetDirect attempts direct type conversion without string round-trip.
// Returns (result, true) on success, or (nil, false) if fallback is needed.
func jsonValueToParquetDirect(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, _ int) (any, bool) {
	if pT == nil {
		return nil, false
	}

	// Handle converted types that need special treatment (skip time/date types that need string parsing)
	if cT != nil {
		switch *cT {
		case parquet.ConvertedType_DATE, parquet.ConvertedType_TIME_MILLIS, parquet.ConvertedType_TIME_MICROS,
			parquet.ConvertedType_TIMESTAMP_MILLIS, parquet.ConvertedType_TIMESTAMP_MICROS:
			return nil, false
		default:
			if result, ok := jsonConvertedTypeDirect(val, *cT); ok {
				return result, true
			}
		}
	}

	// FLOAT16 and UUID logical types encode string values (e.g. "9.5", "-Inf", a dashed
	// UUID) that need strToLogicalType's parsing, not the raw byte-array treatment below.
	if lT != nil && (lT.IsSetFLOAT16() || lT.IsSetUUID()) && val.Kind() == reflect.String {
		return nil, false
	}

	return jsonPhysicalTypeDirect(val, *pT)
}

// numericType is a constraint for numeric types that can be extracted from reflect.Value.
type numericType interface {
	~int64 | ~uint64 | ~float64
}

// getNumericValue extracts a numeric value from a reflect.Value.
func getNumericValue[T numericType](val reflect.Value) (T, bool) {
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return T(val.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return T(val.Uint()), true
	case reflect.Float32, reflect.Float64:
		return T(val.Float()), true
	}
	return 0, false
}

// JSONTypeConfig holds configuration for ConvertToJSONType.
type JSONTypeConfig struct {
	Geospatial *GeospatialConfig
}

// JSONTypeOption configures ConvertToJSONType behavior.
type JSONTypeOption func(*JSONTypeConfig)

// WithGeospatialConfig sets a custom GeospatialConfig for GEOMETRY/GEOGRAPHY rendering.
func WithGeospatialConfig(cfg *GeospatialConfig) JSONTypeOption {
	return func(c *JSONTypeConfig) { c.Geospatial = cfg }
}
