package types

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ConvertValue renders a parquet value for output from the schema element's type
// information, taking the same options as the write path.
//
// A value the column cannot render is reported rather than replaced, the error wrapping
// ErrUnrenderable, ErrInvalidSchemaElement or ErrUnsupportedValueMode, and the value
// returned with it is still a rendering, so a caller can carry on with what
// ConvertToJSONType would have produced. A nil value renders as nil.
//
// Raw mode reports one case interpreted mode does not, since it promises the value writes
// back as it was read: a FIXED_LEN_BYTE_ARRAY whose width is not the schema element's
// type_length, which a column annotated as text is the exception to, being carried verbatim
// and measured in neither mode. The Reading Values section of the README has the rest.
func ConvertValue(val any, se *parquet.SchemaElement, opts ...ValueOption) (any, error) {
	return convertValue(val, se, resolveValueConfig(opts))
}

// ConvertToJSONType converts a parquet value to its JSON-friendly representation.
//
// Deprecated: use ConvertValue. This keeps the substitutions ConvertValue reports, so a
// value the column cannot render is indistinguishable from one that did. It renders the
// interpreted form whatever mode it is given; ConvertValue is where the mode is read.
func ConvertToJSONType(val any, se *parquet.SchemaElement, opts ...ValueOption) any {
	cfg := resolveValueConfig(opts)
	cfg.Mode = ValueModeInterpreted
	rendered, _ := convertValue(val, se, cfg)
	return rendered
}

// convertValue renders one value, returning both the substitution and the reason so each
// entry point can keep the half it wants.
func convertValue(val any, se *parquet.SchemaElement, cfg ValueConfig) (any, error) {
	// The mode is checked first: it describes the call, not the value, so a bad one is
	// wrong even where there is nothing to render.
	if !cfg.Mode.IsValid() {
		return val, fmt.Errorf("%w %d", ErrUnsupportedValueMode, int(cfg.Mode))
	}
	if val == nil {
		return nil, nil
	}
	if se == nil {
		return val, errNoSchemaElement()
	}

	pT, cT, lT := se.Type, se.ConvertedType, se.LogicalType
	if cfg.Mode == ValueModeRaw {
		if pT == nil {
			// Raw rendering is a reading of the physical type and nothing else.
			return val, errNoPhysicalType(fmt.Sprintf("%v", val))
		}
		rendered, err := rawRenderValue(val, *pT, cT, lT, int(se.GetTypeLength()))
		// A raw FLOAT or DOUBLE is the Go float itself, and NaN and the infinities have
		// no JSON number form, so they are quoted here as the interpreted path quotes
		// them. scalarStrToParquetType reads all three back.
		return nonFiniteFloatToJSONString(rendered), err
	}

	// Handle INT96 timestamp conversion (before checking logical/converted types)
	if pT != nil && *pT == parquet.Type_INT96 {
		return convertINT96Value(val)
	}

	// LogicalType takes precedence (newer standard)
	var converted any
	var err error
	if lT != nil {
		converted, err = parquetTypeToJSONTypeWithLogical(val, pT, lT, cfg.Geospatial)
	} else {
		// Fall back to ConvertedType (legacy)
		converted, err = parquetTypeToJSONTypeWithConverted(val, pT, cT, int(se.GetPrecision()), int(se.GetScale()))
	}

	// NaN/Inf have no JSON number representation; quote them the same way
	// JSONWriter already accepts them on input, so output stays round-trippable.
	return nonFiniteFloatToJSONString(converted), err
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
func parquetTypeToJSONTypeWithLogical(val any, pT *parquet.Type, lT *parquet.LogicalType, geoCfg *GeospatialConfig) (any, error) {
	if lT.IsSetDECIMAL() {
		if pT == nil {
			// The only interpreted converter that reads the physical type, and it takes
			// it by pointer: without one this panicked rather than reporting anything.
			return val, errNoPhysicalType(fmt.Sprintf("%v", val))
		}
		decimal := lT.GetDECIMAL()
		return ConvertDecimalValue(val, pT, int(decimal.GetPrecision()), int(decimal.GetScale())), nil
	}
	if lT.IsSetFLOAT16() {
		return convertFloat16Value(val)
	}
	if lT.IsSetTIMESTAMP() {
		return convertTimestampLogicalValue(val, lT.GetTIMESTAMP()), nil
	}
	if lT.IsSetTIME() {
		return ConvertTimeLogicalValue(val, lT.GetTIME()), nil
	}
	if lT.IsSetDATE() {
		return ConvertDateLogicalValue(val), nil
	}
	if lT.IsSetSTRING() {
		return val, nil
	}
	if lT.IsSetINTEGER() {
		return convertIntegerLogicalValue(val, pT, lT.GetINTEGER())
	}
	if lT.IsSetUUID() {
		return convertUUIDValue(val)
	}
	if lT.IsSetGEOMETRY() {
		if geoCfg == nil {
			geoCfg = defaultGeospatialConfig
		}
		return convertGeometryValue(val, lT.GetGEOMETRY(), geoCfg)
	}
	if lT.IsSetGEOGRAPHY() {
		if geoCfg == nil {
			geoCfg = defaultGeospatialConfig
		}
		return convertGeographyValue(val, lT.GetGEOGRAPHY(), geoCfg)
	}
	if lT.IsSetBSON() {
		return convertBSONValue(val)
	}
	return val, nil
}

// parquetTypeToJSONTypeWithConverted converts a value using its ConvertedType (legacy path).
func parquetTypeToJSONTypeWithConverted(val any, pT *parquet.Type, cT *parquet.ConvertedType, precision, scale int) (any, error) {
	if cT == nil {
		if pT != nil && (*pT == parquet.Type_BYTE_ARRAY || *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY) {
			return convertBinaryValue(val), nil
		}
		return val, nil
	}

	switch *cT {
	case parquet.ConvertedType_DECIMAL:
		if pT == nil {
			return val, errNoPhysicalType(fmt.Sprintf("%v", val))
		}
		return ConvertDecimalValue(val, pT, precision, scale), nil
	case parquet.ConvertedType_UTF8, parquet.ConvertedType_DATE,
		parquet.ConvertedType_INT_32, parquet.ConvertedType_INT_64:
		return val, nil
	case parquet.ConvertedType_TIME_MILLIS:
		if v, ok := val.(int32); ok {
			return TIME_MILLISToTimeFormat(v), nil
		}
		return val, nil
	case parquet.ConvertedType_TIME_MICROS:
		if v, ok := val.(int64); ok {
			return TIME_MICROSToTimeFormat(v), nil
		}
		return val, nil
	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		return ConvertTimestampValue(val, parquet.ConvertedType_TIMESTAMP_MILLIS), nil
	case parquet.ConvertedType_TIMESTAMP_MICROS:
		return ConvertTimestampValue(val, parquet.ConvertedType_TIMESTAMP_MICROS), nil
	case parquet.ConvertedType_INT_8:
		if v, ok := val.(int32); ok {
			return int8(v), errNarrowInteger(v, &intType8)
		}
		return val, nil
	case parquet.ConvertedType_INT_16:
		if v, ok := val.(int32); ok {
			return int16(v), errNarrowInteger(v, &intType16)
		}
		return val, nil
	case parquet.ConvertedType_UINT_8:
		if v, ok := val.(int32); ok {
			return uint8(v), errNarrowInteger(v, &uintType8)
		}
		return val, nil
	case parquet.ConvertedType_UINT_16:
		if v, ok := val.(int32); ok {
			return uint16(v), errNarrowInteger(v, &uintType16)
		}
		return val, nil
	case parquet.ConvertedType_UINT_32:
		if v, ok := val.(int32); ok {
			return uint32(v), nil
		}
		return val, nil
	case parquet.ConvertedType_UINT_64:
		if v, ok := val.(int64); ok {
			return uint64(v), nil
		}
		return val, nil
	case parquet.ConvertedType_INTERVAL:
		return convertIntervalValue(val)
	case parquet.ConvertedType_BSON:
		return convertBSONValue(val)
	default:
		return val, nil
	}
}

func JSONTypeToParquetType(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, length, scale int) (any, error) {
	return JSONTypeToParquetTypeWithLogical(val, pT, cT, nil, length, scale)
}

// JSONTypeToParquetTypeWithLogical converts a decoded JSON value to its column's physical
// value, under the same value mode grammar as StrToParquetTypeWithLogical.
func JSONTypeToParquetTypeWithLogical(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length, scale int, opts ...ValueOption) (any, error) {
	if val.Type().Kind() == reflect.Interface && val.IsNil() {
		return nil, nil
	}
	if pT == nil {
		return nil, errNoPhysicalType(jsonValueText(val))
	}

	mode := resolveValueConfig(opts).Mode
	if !mode.IsValid() {
		return nil, fmt.Errorf("%w %d", ErrUnsupportedValueMode, int(mode))
	}
	if mode == ValueModeRaw {
		if err := checkJSONStringColumn(val, *pT, cT, lT, ValueModeRaw); err != nil {
			return nil, err
		}
		return jsonRawValueToParquetType(val, pT, cT, lT, length)
	}

	if typeName := interpretedWriteUnsupported(cT, lT); typeName != "" {
		return nil, errInterpretedWrite(typeName)
	}
	if err := checkJSONStringColumn(val, *pT, cT, lT, ValueModeInterpreted); err != nil {
		return nil, err
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
	if result, ok, err := jsonValueToParquetDirect(val, pT, cT, lT); ok {
		return result, err
	}

	// Fallback to string-based conversion for complex/unusual types
	return StrToParquetTypeWithLogical(jsonValueText(val), pT, cT, lT, length, scale)
}

// isTimeColumn reports whether either annotation marks the column as TIME.
func isTimeColumn(cT *parquet.ConvertedType, lT *parquet.LogicalType) bool {
	if lT != nil && lT.IsSetTIME() {
		return true
	}
	return cT != nil && (*cT == parquet.ConvertedType_TIME_MILLIS || *cT == parquet.ConvertedType_TIME_MICROS)
}

// jsonTimeUnit reports the tick size and spelling of a TIME column described by either
// annotation. Schema building backfills a matching converted type for a MILLIS or MICROS
// logical type, so this cannot rely on the logical type alone.
func jsonTimeUnit(cT *parquet.ConvertedType, lT *parquet.LogicalType) (time.Duration, string, bool) {
	if lT != nil && lT.IsSetTIME() {
		if unit, typeName, ok := timeUnitOf(lT.GetTIME()); ok {
			return unit, typeName, true
		}
	}
	if cT != nil {
		switch *cT {
		case parquet.ConvertedType_TIME_MILLIS:
			return time.Millisecond, "TIME_MILLIS", true
		case parquet.ConvertedType_TIME_MICROS:
			return time.Microsecond, "TIME_MICROS", true
		}
	}
	return 0, "", false
}

// jsonTimeTicks reads a TIME tick count from a JSON number of any kind, keeping the check
// ahead of the conversion to int64: truncating first would accept 1.9 as 1 and -0.5 as 0,
// and converting a non-finite or oversized float is undefined in Go.
func jsonTimeTicks(val reflect.Value, typeName string, ticksPerDay int64) (int64, bool, error) {
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return val.Int(), true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if u := val.Uint(); u >= uint64(ticksPerDay) {
			return 0, true, fmt.Errorf("%s value %d is outside [0, 24h)", typeName, u)
		}
		return int64(val.Uint()), true, nil
	case reflect.Float32, reflect.Float64:
		f := val.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) {
			return 0, true, fmt.Errorf("%s value %v is not a whole number of ticks", typeName, f)
		}
		if f < 0 || f >= float64(ticksPerDay) {
			return 0, true, fmt.Errorf("%s value %v is outside [0, 24h)", typeName, f)
		}
		return int64(f), true, nil
	}
	return 0, false, nil
}

// jsonTimeDirect range-checks a TIME carried as a JSON number, which reaches neither
// ParseTimeString nor the [0, 24h) check the string form goes through.
func jsonTimeDirect(val reflect.Value, unit time.Duration, typeName string) (any, bool, error) {
	ticksPerDay := timeTicksPerDay(unit)
	ticks, ok, err := jsonTimeTicks(val, typeName, ticksPerDay)
	if !ok || err != nil {
		return nil, ok, err
	}
	if ticks < 0 || ticks >= ticksPerDay {
		return nil, true, fmt.Errorf("%s value %d is outside [0, 24h)", typeName, ticks)
	}
	if unit == time.Millisecond {
		return int32(ticks), true, nil
	}
	return ticks, true, nil
}

// jsonValueToParquetDirect attempts direct type conversion without string round-trip.
// Returns (result, true, nil) on success, (nil, false, nil) if fallback is needed, or
// (nil, true, err) when the value is invalid for the column.
func jsonValueToParquetDirect(val reflect.Value, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType) (any, bool, error) {
	// TIME comes first: the converted types below hand their values to the string path, which
	// renders a float64 in %g and reads "4.5296789e+07" as a failed parse rather than a time.
	if isTimeColumn(cT, lT) {
		unit, typeName, unitOK := jsonTimeUnit(cT, lT)
		if !unitOK {
			// No usable unit: let the string path report the broken schema rather than
			// storing the number as a plain integer.
			return nil, false, nil
		}
		if result, ok, err := jsonTimeDirect(val, unit, typeName); ok {
			return result, true, err
		}
		return nil, false, nil
	}

	// Text on the wire, under either spelling of the annotation.
	if isTextAnnotated(cT, lT) && val.Kind() == reflect.String {
		return val.String(), true, nil
	}

	// An annotated integer needs its declared width and signedness, which a cast cannot
	// express: UINT_8 must refuse 256 and -1, not wrap them. strToIntegerLogical already
	// reads the text that way, so the number takes the same route.
	if isAnnotatedInteger(cT, lT) {
		return nil, false, nil
	}

	if cT != nil {
		switch *cT {
		case parquet.ConvertedType_DATE, parquet.ConvertedType_TIMESTAMP_MILLIS,
			parquet.ConvertedType_TIMESTAMP_MICROS, parquet.ConvertedType_INTERVAL,
			parquet.ConvertedType_DECIMAL:
			// Text forms the string path parses more carefully than a cast can.
			return nil, false, nil
		}
	}

	if lT != nil && val.Kind() == reflect.String {
		// FLOAT16 and UUID hold text ("9.5", a dashed UUID) that needs strToLogicalType.
		if lT.IsSetFLOAT16() || lT.IsSetUUID() {
			return nil, false, nil
		}
	}

	return jsonPhysicalTypeDirect(val, *pT)
}
