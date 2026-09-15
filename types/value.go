package types

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ValueMode selects how a logical value is represented outside the parquet file.
type ValueMode int

const (
	// ValueModeInterpreted carries the canonical text of the logical type.
	ValueModeInterpreted ValueMode = iota
	// ValueModeRaw carries the physical value: base64 for byte-backed columns, the
	// underlying number otherwise. Text-annotated columns stay text in both modes.
	ValueModeRaw
)

// IsValid reports whether the mode is one this package defines.
func (m ValueMode) IsValid() bool {
	return m == ValueModeInterpreted || m == ValueModeRaw
}

// String returns the mode's name.
func (m ValueMode) String() string {
	switch m {
	case ValueModeInterpreted:
		return "interpreted"
	case ValueModeRaw:
		return "raw"
	default:
		return fmt.Sprintf("ValueMode(%d)", int(m))
	}
}

// ValueConfig holds the settings the value conversion paths share.
type ValueConfig struct {
	// Mode selects the interpreted (default) or raw representation.
	Mode ValueMode
	// Geospatial configures GEOMETRY/GEOGRAPHY rendering; nil selects the defaults.
	Geospatial *GeospatialConfig
}

// ValueOption configures value conversion.
type ValueOption func(*ValueConfig)

// JSONTypeConfig is the former name of ValueConfig.
//
// Deprecated: use ValueConfig.
type JSONTypeConfig = ValueConfig

// JSONTypeOption is the former name of ValueOption.
//
// Deprecated: use ValueOption.
type JSONTypeOption = ValueOption

// NewValueConfig builds a ValueConfig with default values, modified by opts.
func NewValueConfig(opts ...ValueOption) *ValueConfig {
	cfg := resolveValueConfig(opts)
	return &cfg
}

// resolveValueConfig applies opts to a zero config, by value.
func resolveValueConfig(opts []ValueOption) ValueConfig {
	// An option is an opaque closure over a pointer, so building a config through one
	// heap-allocates it. This runs once per value; the usual case of none must not.
	if len(opts) == 0 {
		return ValueConfig{}
	}
	cfg := new(ValueConfig)
	for _, opt := range opts {
		opt(cfg)
	}
	return *cfg
}

// errNoPhysicalType reports a schema element with no physical type to scan into.
func errNoPhysicalType(s string) error {
	return fmt.Errorf("cannot scan %q without a physical type", s)
}

// ErrUnsupportedValueMode reports a mode outside the two this package defines. The
// conversion helpers and the writer option both wrap it, so errors.Is matches either.
var ErrUnsupportedValueMode = errors.New("unsupported value mode")

// WithValueMode selects the interpreted or raw representation of logical values.
func WithValueMode(m ValueMode) ValueOption {
	return func(c *ValueConfig) { c.Mode = m }
}

// WithGeospatialConfig sets a custom GeospatialConfig for GEOMETRY/GEOGRAPHY rendering.
func WithGeospatialConfig(cfg *GeospatialConfig) ValueOption {
	return func(c *ValueConfig) { c.Geospatial = cfg }
}

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

// narrowIntegerType returns the annotation when it pins the column to a range narrower
// than its physical type, which raw mode can check too. nil otherwise.
func narrowIntegerType(cT *parquet.ConvertedType, lT *parquet.LogicalType) *parquet.IntType {
	var it *parquet.IntType
	switch {
	case lT != nil && lT.IsSetINTEGER():
		it = lT.GetINTEGER()
	case cT != nil:
		it = convertedIntegerType(*cT)
	}
	// Only 8 and 16 are narrower than the INT32 they sit on; 32- and 64-bit unsigned
	// carry their upper half as a negative value. Undefined widths, such as the 0 and 12
	// a hand-built schema can hold, go to the physical scan as they do interpreted.
	if it == nil || (it.GetBitWidth() != 8 && it.GetBitWidth() != 16) {
		return nil
	}
	return it
}

// checkNarrowInteger reports a value outside the range its annotation declares. 256 is
// not a UINT_8 in either mode, since every value one holds fits the physical INT32.
func checkNarrowInteger(val any, it *parquet.IntType) error {
	v, ok := val.(int32)
	if !ok {
		return nil
	}
	width := int(it.GetBitWidth())
	var minValue, maxValue int64
	if it.GetIsSigned() {
		maxValue = int64(1)<<(width-1) - 1
		minValue = -maxValue - 1
	} else {
		maxValue = int64(1)<<width - 1
	}
	if int64(v) < minValue || int64(v) > maxValue {
		return fmt.Errorf("%s value %d is out of range", integerLabel(it), v)
	}
	return nil
}

// isAnnotatedInteger reports whether either annotation fixes the width and signedness.
func isAnnotatedInteger(cT *parquet.ConvertedType, lT *parquet.LogicalType) bool {
	if lT != nil && lT.IsSetINTEGER() {
		return true
	}
	return cT != nil && convertedIntegerType(*cT) != nil
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

// checkJSONStringColumn requires a JSON string where the column's text is face value.
func checkJSONStringColumn(val reflect.Value, pT parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, mode ValueMode) error {
	if !carriesTextVerbatim(pT, cT, lT, mode) || isJSONString(val) {
		return nil
	}
	kind := val.Kind().String()
	if val.Type() == jsonNumberType {
		kind = "number"
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
