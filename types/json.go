package types

import (
	"fmt"
	"math"
	"reflect"

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
	cfg := resolveValueConfig(opts)
	rendered, err := convertValue(val, se, cfg)
	if err == nil && cfg.EnforceUTF8 && val != nil {
		err = cfg.validateTextUTF8(val, se.ConvertedType, se.LogicalType)
	}
	return rendered, err
}

// ConvertToJSONType converts a parquet value to its JSON-friendly representation.
//
// Deprecated: use ConvertValue. This keeps the substitutions ConvertValue reports, so a
// value the column cannot render is indistinguishable from one that did. It renders the
// interpreted form whatever mode it is given; ConvertValue is where the mode is read.
// WithEnforceUTF8 is ignored because this entry point cannot report validation errors.
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

	// A text column's bytes are its text in both modes, so the rendering below does not
	// depend on which Go type the caller holds them in.
	if isTextAnnotated(cT, lT) {
		val = textValueString(val)
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
