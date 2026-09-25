package types

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/hangxie/parquet-go/v3/parquet"
)

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

	cfg := resolveValueConfig(opts)
	mode := cfg.Mode
	if !mode.IsValid() {
		return nil, fmt.Errorf("%w %d", ErrUnsupportedValueMode, int(mode))
	}
	if cfg.EnforceUTF8 && isJSONString(val) {
		if err := cfg.validateTextUTF8(val.String(), cT, lT); err != nil {
			return nil, err
		}
	}
	if mode == ValueModeRaw {
		if err := checkJSONStringColumn(val, *pT, cT, lT, ValueModeRaw); err != nil {
			return nil, err
		}
		return jsonRawValueToParquetType(val, pT, cT, lT, length)
	}

	// Geospatial takes the object its mode renders, which arrives decoded rather than
	// as text; marshal/json.go lets the node walker hand one to a primitive column.
	if typeName := geospatialAnnotation(lT); typeName != "" {
		if err := checkGeospatialColumn(typeName, *pT); err != nil {
			return nil, err
		}
		return geospatialFromValue(val.Interface(), typeName, cfg.Geospatial)
	}
	if err := checkJSONStringColumn(val, *pT, cT, lT, ValueModeInterpreted); err != nil {
		return nil, err
	}
	if err := checkJSONScalarColumn(val, *pT, cT, lT); err != nil {
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
			return StrToParquetTypeWithLogical(s, pT, cT, lT, length, scale, opts...)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			// For JSON numbers coming as integers
			s := strconv.FormatInt(val.Int(), 10)
			return StrToParquetTypeWithLogical(s, pT, cT, lT, length, scale, opts...)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// For JSON numbers coming as unsigned integers
			s := strconv.FormatUint(val.Uint(), 10)
			return StrToParquetTypeWithLogical(s, pT, cT, lT, length, scale, opts...)
		case reflect.String:
			// For JSON numbers coming as strings (from json.Number when UseNumber is used)
			return StrToParquetTypeWithLogical(val.String(), pT, cT, lT, length, scale, opts...)
		}
	}

	// Try direct type conversion for non-decimal types (avoids fmt.Sprintf/Sscanf round-trip)
	if result, ok, err := jsonValueToParquetDirect(val, pT, cT, lT); ok {
		return result, err
	}

	// Fallback to string-based conversion for complex/unusual types
	return StrToParquetTypeWithLogical(jsonValueText(val), pT, cT, lT, length, scale, opts...)
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
