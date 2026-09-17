package types

import (
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// errNoPhysicalType reports a schema element with no physical type to scan into.
func errNoPhysicalType(s string) error {
	return fmt.Errorf("cannot scan %q without a physical type", s)
}

// ErrUnrenderable reports bytes a column cannot produce its rendering from: BSON that does
// not parse, bytes the geospatial parser cannot read as WKB, a UUID or FLOAT16 or INTERVAL
// or INT96 of the wrong width, and in raw mode a FIXED_LEN_BYTE_ARRAY of any annotation
// that is not the schema element's type_length. Every such error wraps it, so a caller can
// tell a column holding bad data from a call made with a bad mode or schema element.
//
// The WKB reading is 2D, so an ISO geometry carrying Z or M coordinates is bytes this
// parser does not understand rather than bytes that are not WKB.
var ErrUnrenderable = errors.New("value cannot be rendered")

// errUnrenderable wraps ErrUnrenderable with what the column could not render. Every
// rendering failure reads TYPE: value cannot be rendered: reason.
func errUnrenderable(typeName, format string, args ...any) error {
	return fmt.Errorf("%s: %w: %s", typeName, ErrUnrenderable, fmt.Sprintf(format, args...))
}

// errUnrenderableCause is errUnrenderable for a reason another package produced, keeping
// that error reachable by errors.Is and errors.As.
func errUnrenderableCause(typeName string, cause error) error {
	return fmt.Errorf("%s: %w: %w", typeName, ErrUnrenderable, cause)
}

// errNotWKB reports bytes the GeoJSON conversion cannot read.
func errNotWKB(typeName string) error {
	return errUnrenderable(typeName, "bytes are not WKB this parser understands")
}

// The convert*Value functions throughout this package return the substitution the deprecated
// ConvertToJSONType keeps as their value, and the reason as their error. Both halves are
// always meant: ConvertToJSONType drops the error, and ConvertValue hands the substitution
// back alongside it so a caller can carry on with the old rendering.

// valueBytes reads the bytes a byte-backed column carries, in either form the reader emits.
func valueBytes(val any) ([]byte, bool) {
	switch v := val.(type) {
	case []byte:
		return v, true
	case string:
		return []byte(v), true
	}
	return nil, false
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

// rawRenderValue renders the physical value a column stores, which is what raw mode carries
// out of the file: base64 for a byte-backed column, the value itself otherwise.
// rawStrToParquetType reads back exactly what this writes. The caller has already
// returned for a nil value.
func rawRenderValue(val any, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length int) (any, error) {
	if pT == nil {
		return val, errNoPhysicalType(fmt.Sprintf("%v", val))
	}
	if isTextAnnotated(cT, lT) {
		return val, nil
	}
	switch *pT {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY, parquet.Type_INT96:
		b, ok := valueBytes(val)
		if !ok {
			return val, errUnrenderable(pT.String(), "value is %T, not bytes", val)
		}
		// A column whose width the format or the schema fixes is checked against it, as
		// the interpreted path checks UUID, FLOAT16 and INTERVAL.
		switch {
		case *pT == parquet.Type_INT96 && len(b) != int96ByteLength:
			return val, errUnrenderable("INT96", "is %d bytes, must be %d", len(b), int96ByteLength)
		case *pT == parquet.Type_FIXED_LEN_BYTE_ARRAY && length > 0 && len(b) != length:
			return val, errUnrenderable(pT.String(), "is %d bytes, must be %d", len(b), length)
		}
		return base64.StdEncoding.EncodeToString(b), nil
	default:
		return val, nil
	}
}
