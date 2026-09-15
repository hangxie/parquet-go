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

// errNoPhysicalType reports a schema element with no physical type to scan into.
func errNoPhysicalType(s string) error {
	return fmt.Errorf("cannot scan %q without a physical type", s)
}

// convertedIntegerType maps a legacy integer annotation to the INTEGER type it stands
// for, so both spellings reach one strict reader. nil for any other converted type.
func convertedIntegerType(cT parquet.ConvertedType) *parquet.IntType {
	switch cT {
	case parquet.ConvertedType_INT_8:
		return &intType8
	case parquet.ConvertedType_INT_16:
		return &intType16
	case parquet.ConvertedType_INT_32:
		return &intType32
	case parquet.ConvertedType_INT_64:
		return &intType64
	case parquet.ConvertedType_UINT_8:
		return &uintType8
	case parquet.ConvertedType_UINT_16:
		return &uintType16
	case parquet.ConvertedType_UINT_32:
		return &uintType32
	case parquet.ConvertedType_UINT_64:
		return &uintType64
	}
	return nil
}

// Shared, read-only: the write path resolves one per value and must not allocate.
var (
	intType8   = parquet.IntType{BitWidth: 8, IsSigned: true}
	intType16  = parquet.IntType{BitWidth: 16, IsSigned: true}
	intType32  = parquet.IntType{BitWidth: 32, IsSigned: true}
	intType64  = parquet.IntType{BitWidth: 64, IsSigned: true}
	uintType8  = parquet.IntType{BitWidth: 8}
	uintType16 = parquet.IntType{BitWidth: 16}
	uintType32 = parquet.IntType{BitWidth: 32}
	uintType64 = parquet.IntType{BitWidth: 64}
)

// isAnnotatedInteger reports whether either annotation fixes the width and signedness.
func isAnnotatedInteger(cT *parquet.ConvertedType, lT *parquet.LogicalType) bool {
	if lT != nil && lT.IsSetINTEGER() {
		return true
	}
	return cT != nil && convertedIntegerType(*cT) != nil
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

// jsonNumberType is what encoding/json decodes numbers into under UseNumber.
var jsonNumberType = reflect.TypeOf(json.Number(""))

// isJSONString reports whether val is a JSON string, not a json.Number reading as one.
func isJSONString(val reflect.Value) bool {
	return val.Kind() == reflect.String && val.Type() != jsonNumberType
}

// carriesTextVerbatim reports whether the column's value travels as text that is taken at
// face value: base64 for a byte-backed column with nothing to interpret, and the string
// itself for one annotated as text. Rendering a number or boolean into that text is the
// same guess this file removes one level down, and it is not harmless: "true" is valid
// base64, so a JSON true was stored as the three bytes b6 bb 9e.
//
// INT96 is excluded. It reads its own timestamp form and falls back to the bare 96-bit
// integer the column stores, so a number there is a value the column holds.
func carriesTextVerbatim(pT parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType) bool {
	if isTextAnnotated(cT, lT) {
		return true
	}
	switch pT {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
	default:
		return false
	}
	return cT == nil && lT == nil
}

// checkJSONStringColumn requires a JSON string where the column's text is face value.
func checkJSONStringColumn(val reflect.Value, pT parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType) error {
	if !carriesTextVerbatim(pT, cT, lT) || isJSONString(val) {
		return nil
	}
	kind := val.Kind().String()
	if val.Type() == jsonNumberType {
		kind = "number"
	}
	return fmt.Errorf("%v column takes a JSON string, got %s", pT, kind)
}

// isTextAnnotated reports whether either annotation guarantees the column holds text,
// which is verbatim in both modes; base64-decoding it is the corruption in #420.
func isTextAnnotated(cT *parquet.ConvertedType, lT *parquet.LogicalType) bool {
	if lT != nil && (lT.IsSetSTRING() || lT.IsSetENUM() || lT.IsSetJSON()) {
		return true
	}
	if cT == nil {
		return false
	}
	switch *cT {
	case parquet.ConvertedType_UTF8, parquet.ConvertedType_ENUM, parquet.ConvertedType_JSON:
		return true
	}
	return false
}

// interpretedWriteUnsupported names an annotation with no interpreted write form, or "".
// Falling through to raw would store "POINT (1 2)" as the eleven bytes of its own text.
func interpretedWriteUnsupported(cT *parquet.ConvertedType, lT *parquet.LogicalType) string {
	switch {
	case lT != nil && lT.IsSetGEOMETRY():
		return "GEOMETRY"
	case lT != nil && lT.IsSetGEOGRAPHY():
		return "GEOGRAPHY"
	case lT != nil && lT.IsSetBSON():
		return "BSON"
	case cT != nil && *cT == parquet.ConvertedType_BSON:
		return "BSON"
	}
	return ""
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

// errInterpretedWrite reports an annotation whose text form the write path cannot parse.
func errInterpretedWrite(typeName string) error {
	return fmt.Errorf("writing %s from its text form is not supported yet, supply base64", typeName)
}
