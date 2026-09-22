package types

import (
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"unicode/utf8"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// ErrInvalidSchemaElement reports a schema element that says nothing about how to render or
// scan a value: a nil element, or one with no physical type where the rendering needs one.
var ErrInvalidSchemaElement = errors.New("invalid schema element")

// errNoSchemaElement reports a value handed over with no schema element to render against.
func errNoSchemaElement() error {
	return fmt.Errorf("%w: cannot render a value without a schema element", ErrInvalidSchemaElement)
}

// errNoPhysicalType reports a schema element with no physical type to convert through.
func errNoPhysicalType(s string) error {
	return fmt.Errorf("%w: cannot convert %q without a physical type", ErrInvalidSchemaElement, s)
}

// ErrUnrenderable reports a value a column cannot produce its rendering from: unparsable
// BSON, bytes the geospatial parser cannot read, a fixed-width value of the wrong width, an
// integer outside its annotation's range. It is distinct from a bad mode
// (ErrUnsupportedValueMode) and a bad schema element (ErrInvalidSchemaElement).
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
func rawRenderValue(val any, pT parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length int) (any, error) {
	if isTextAnnotated(cT, lT) {
		// The bytes a text column holds are its text. Handing them back as []byte would
		// render as base64 through a JSON encoder, which the raw write path would then
		// store verbatim, so the value would not survive the round trip this mode promises.
		if b, ok := val.([]byte); ok {
			return string(b), nil
		}
		return val, nil
	}
	switch pT {
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY, parquet.Type_INT96:
		b, ok := valueBytes(val)
		if !ok {
			return val, errUnrenderable(pT.String(), "value is %T, not bytes", val)
		}
		// A column whose width the format or the schema fixes is checked against it, as the
		// interpreted path checks UUID, FLOAT16 and INTERVAL. The error carries the base64
		// all the same, so a caller that logs and carries on reads one shape.
		encoded := base64.StdEncoding.EncodeToString(b)
		switch {
		case pT == parquet.Type_INT96 && len(b) != int96ByteLength:
			return encoded, errUnrenderable("INT96", "is %d bytes, must be %d", len(b), int96ByteLength)
		case pT == parquet.Type_FIXED_LEN_BYTE_ARRAY && length > 0 && len(b) != length:
			return encoded, errUnrenderable(pT.String(), "is %d bytes, must be %d", len(b), length)
		}
		return encoded, nil
	default:
		// The annotation narrows what the column can hold, and the raw scan rejects a
		// value outside it, so rendering one out unreported would hand the caller a
		// value it could not write back.
		if it := narrowIntegerType(cT, lT); it != nil {
			if reason := narrowIntegerReason(val, it); reason != "" {
				return val, errUnrenderable(integerLabel(it), "%s", reason)
			}
		}
		return val, nil
	}
}

// validateTextUTF8 checks text-annotated values when UTF-8 enforcement is enabled.
func (c ValueConfig) validateTextUTF8(val any, cT *parquet.ConvertedType, lT *parquet.LogicalType) error {
	if !c.EnforceUTF8 || !isTextAnnotated(cT, lT) {
		return nil
	}
	var valid bool
	switch v := val.(type) {
	case string:
		valid = utf8.ValidString(v)
	case []byte:
		valid = utf8.Valid(v)
	default:
		rv := reflect.ValueOf(val)
		switch {
		case rv.Kind() == reflect.String:
			valid = utf8.ValidString(rv.String())
		case rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8:
			valid = utf8.Valid(rv.Bytes())
		default:
			return errUnrenderable("text", "value is %T, not a string or bytes", val)
		}
	}
	if !valid {
		b := textValueBytes(val)
		offset := firstInvalidUTF8(b)
		const maxInvalidUTF8Context = 8
		end := min(offset+maxInvalidUTF8Context, len(b))
		return errUnrenderable("text", "invalid UTF-8 at byte %d near %x", offset, b[offset:end])
	}
	return nil
}

// textValueBytes returns bytes from a value validateTextUTF8 has established is text.
func textValueBytes(val any) []byte {
	switch v := val.(type) {
	case string:
		return []byte(v)
	case []byte:
		return v
	}
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.String {
		return []byte(rv.String())
	}
	return rv.Bytes()
}

// firstInvalidUTF8 returns the byte offset at which UTF-8 decoding first fails.
func firstInvalidUTF8(b []byte) int {
	for offset := 0; offset < len(b); {
		_, size := utf8.DecodeRune(b[offset:])
		if size == 1 && b[offset] >= utf8.RuneSelf {
			return offset
		}
		offset += size
	}
	return len(b)
}
