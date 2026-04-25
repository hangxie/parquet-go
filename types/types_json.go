package types

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"

	"github.com/hangxie/parquet-go/v3/parquet"
)

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
func jsonValueToParquetDirect(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, _ *parquet.LogicalType, _ int) (any, bool) {
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
