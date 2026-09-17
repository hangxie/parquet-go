package types

import (
	"errors"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// errNoPhysicalType reports a schema element with no physical type to scan into.
func errNoPhysicalType(s string) error {
	return fmt.Errorf("cannot scan %q without a physical type", s)
}

// ErrUnrenderable reports bytes a column cannot produce its rendering from: BSON that does
// not parse, bytes that are not WKB, a UUID or FLOAT16 or INTERVAL or INT96 of the wrong
// width. Every such error wraps it, so a caller can tell a column holding bad data from a
// call made with a bad mode or schema element.
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
