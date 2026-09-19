package types

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// jsonNumberType is what encoding/json decodes numbers into under UseNumber.
var jsonNumberType = reflect.TypeOf(json.Number(""))

// isJSONString reports whether val is a JSON string, not a json.Number reading as one.
func isJSONString(val reflect.Value) bool {
	return val.Kind() == reflect.String && val.Type() != jsonNumberType
}

// carriesTextVerbatim reports whether the column's text is taken at face value: base64
// for a byte-backed column with nothing to interpret, the string itself for a text one.
// Rendering a number into that text reads "true" as base64, storing three bytes.
func carriesTextVerbatim(pT parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, mode ValueMode) bool {
	if isTextAnnotated(cT, lT) {
		return true
	}
	switch pT {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
	case parquet.Type_INT96:
		// Interpreted mode reads INT96 as a timestamp or a bare 96-bit integer, so a
		// number there is a value the column holds.
		return mode == ValueModeRaw
	default:
		return false
	}
	// Raw carries every byte-backed column as base64; interpreted only unannotated ones.
	return mode == ValueModeRaw || (cT == nil && lT == nil)
}

// interpretedWriteUnsupported names an annotation with no interpreted write form, or "".
// Falling through to raw would store "POINT (1 2)" as the eleven bytes of its own text.
func interpretedWriteUnsupported(cT *parquet.ConvertedType, lT *parquet.LogicalType) string {
	switch {
	case lT != nil && lT.IsSetGEOMETRY():
		return "GEOMETRY"
	case lT != nil && lT.IsSetGEOGRAPHY():
		return "GEOGRAPHY"
	}
	return ""
}

// errInterpretedWrite reports an annotation that only raw mode can write today.
func errInterpretedWrite(typeName string) error {
	return fmt.Errorf("writing %s in interpreted mode is not supported yet, select raw mode (writer.WithValueMode for a writer, types.WithValueMode for the conversion helpers) and supply base64", typeName)
}

// base64ToBytes decodes a byte-backed value; a length above zero is the width it must
// decode to.
func base64ToBytes(s, typeName string, length int) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", fmt.Errorf("%s %q is not valid base64: %w", typeName, s, err)
	}
	if length > 0 && len(decoded) != length {
		// Reported here so the message names the string the caller supplied.
		return "", fmt.Errorf("%s %q decodes to %d bytes, column length is %d", typeName, s, len(decoded), length)
	}
	return string(decoded), nil
}

// physicalStrToParquetType scans a value with no logical type applied.
func physicalStrToParquetType(s string, pT parquet.Type, length int) (any, error) {
	switch pT {
	case parquet.Type_INT96:
		return base64ToBytes(s, "INT96", int96ByteLength)
	case parquet.Type_BYTE_ARRAY:
		return base64ToBytes(s, "BYTE_ARRAY", 0)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return base64ToBytes(s, "FIXED_LEN_BYTE_ARRAY", length)
	default:
		return scalarStrToParquetType(s, pT)
	}
}

// scalarStrToParquetType scans a boolean or numeric physical value over the whole field.
func scalarStrToParquetType(s string, pT parquet.Type) (any, error) {
	// Whitespace is ordinary in a CSV field; anything else must fail.
	text := strings.TrimSpace(s)
	switch pT {
	case parquet.Type_BOOLEAN:
		v, err := strconv.ParseBool(text)
		return v, wrapScanErr("BOOLEAN", s, err)
	case parquet.Type_INT32:
		v, err := strconv.ParseInt(text, 10, 32)
		return int32(v), wrapScanErr("INT32", s, err)
	case parquet.Type_INT64:
		v, err := strconv.ParseInt(text, 10, 64)
		return v, wrapScanErr("INT64", s, err)
	case parquet.Type_FLOAT:
		v, err := strconv.ParseFloat(text, 32)
		return float32(v), wrapScanErr("FLOAT", s, err)
	case parquet.Type_DOUBLE:
		v, err := strconv.ParseFloat(text, 64)
		return v, wrapScanErr("DOUBLE", s, err)
	default:
		return nil, nil
	}
}

// rawStrToParquetType scans the raw form: text columns verbatim, the rest as physical.
func rawStrToParquetType(s string, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length int) (any, error) {
	if isTextAnnotated(cT, lT) {
		return s, nil
	}
	val, err := physicalStrToParquetType(s, *pT, length)
	if err != nil {
		return nil, err
	}
	if it := narrowIntegerType(cT, lT); it != nil {
		if err := checkNarrowInteger(val, it); err != nil {
			return nil, err
		}
	}
	return val, nil
}

// checkJSONStringColumn requires a JSON string where the column's text is face value,
// and for BSON, whose Extended JSON arrives as one but is parsed rather than stored.
func checkJSONStringColumn(val reflect.Value, pT parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, mode ValueMode) error {
	isBSON := isBSONAnnotated(cT, lT)
	if !carriesTextVerbatim(pT, cT, lT, mode) && !isBSON {
		return nil
	}
	if isJSONString(val) {
		return nil
	}
	kind := val.Kind().String()
	if val.Type() == jsonNumberType {
		kind = "number"
	}
	if isBSON && mode == ValueModeInterpreted {
		// Naming the grammar, since a JSON object is the shape a caller reaches for first.
		return fmt.Errorf("BSON column takes Extended JSON in a JSON string, got %s", kind)
	}
	return fmt.Errorf("%v column takes a JSON string, got %s", pT, kind)
}

// jsonValueText renders a value for the string scanners. Floats go out in plain decimal:
// %v uses exponent notation above seven digits, which no numeric parser here accepts.
// The other kinds skip fmt because this runs once per column value.
func jsonValueText(val reflect.Value) string {
	switch val.Kind() {
	case reflect.String:
		// json.Number lands here: the common case under UseNumber.
		return val.String()
	case reflect.Bool:
		return strconv.FormatBool(val.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(val.Uint(), 10)
	case reflect.Float32:
		return strconv.FormatFloat(val.Float(), 'f', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'f', -1, 64)
	}
	return fmt.Sprintf("%v", val)
}

// jsonRawValueToParquetType converts a decoded JSON value holding its column's raw form.
// The caller has already established that the column has a physical type.
func jsonRawValueToParquetType(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length int) (any, error) {
	if isTextAnnotated(cT, lT) {
		return val.String(), nil
	}
	result, ok, err := jsonPhysicalTypeDirect(val, *pT)
	if !ok {
		// json.Number arrives as a string kind, so numbers reach the scanner too.
		result, err = physicalStrToParquetType(jsonValueText(val), *pT, length)
	}
	if err != nil {
		return nil, err
	}
	if it := narrowIntegerType(cT, lT); it != nil {
		if err := checkNarrowInteger(result, it); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// jsonPhysicalTypeDirect converts a JSON boolean or number to the column's physical type,
// checking it against the column rather than casting through it. Byte-backed columns are
// absent: their JSON form is base64 text, which the string path decodes.
func jsonPhysicalTypeDirect(val reflect.Value, pT parquet.Type) (any, bool, error) {
	switch pT {
	case parquet.Type_BOOLEAN:
		if val.Kind() == reflect.Bool {
			return val.Bool(), true, nil
		}
	case parquet.Type_INT32:
		v, ok, err := jsonIntegerValue(val, "INT32", 32)
		return int32(v), ok, err
	case parquet.Type_INT64:
		return jsonIntegerValue(val, "INT64", 64)
	case parquet.Type_FLOAT:
		v, ok, err := jsonFloatValue(val, "FLOAT", 32)
		return float32(v), ok, err
	case parquet.Type_DOUBLE:
		return jsonFloatValue(val, "DOUBLE", 64)
	}
	return nil, false, nil
}

// jsonIntegerValue reads a whole number that fits in bitSize bits from a JSON number of
// any kind. It claims the value even when it fails, so a number the column cannot hold is
// reported rather than handed to the string path, which would render and re-scan it.
func jsonIntegerValue(val reflect.Value, typeName string, bitSize int) (int64, bool, error) {
	min, max := int64(math.MinInt64), int64(math.MaxInt64)
	if bitSize < 64 {
		max = int64(1)<<(bitSize-1) - 1
		min = -max - 1
	}
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v := val.Int()
		if v < min || v > max {
			return 0, true, fmt.Errorf("%s value %d is out of range", typeName, v)
		}
		return v, true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v := val.Uint()
		if v > uint64(max) {
			return 0, true, fmt.Errorf("%s value %d is out of range", typeName, v)
		}
		return int64(v), true, nil
	case reflect.Float32, reflect.Float64:
		f := val.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) {
			return 0, true, fmt.Errorf("%s value %v is not a whole number", typeName, f)
		}
		// float64(max) rounds up to max+1 at 64 bits, so the upper bound is exclusive;
		// both bounds are exact powers of two, and comparing before the conversion
		// avoids the undefined result of converting an oversized float.
		if f < float64(min) || f >= float64(max)+1 {
			return 0, true, fmt.Errorf("%s value %v is out of range", typeName, f)
		}
		return int64(f), true, nil
	}
	return 0, false, nil
}

// jsonFloatValue reads a JSON number of any kind as a float the column can hold.
func jsonFloatValue(val reflect.Value, typeName string, bitSize int) (float64, bool, error) {
	f, ok := getNumericValue[float64](val)
	if !ok {
		return 0, false, nil
	}
	if bitSize == 32 && !math.IsInf(f, 0) && math.Abs(f) > math.MaxFloat32 {
		return 0, true, fmt.Errorf("%s value %v is out of range", typeName, f)
	}
	// Only the upper bound is checked. A magnitude below the smallest float32 rounds to
	// zero rather than overflowing, which is what every float conversion does and what
	// the string path's ParseFloat does too, so the two agree.
	return f, true, nil
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
