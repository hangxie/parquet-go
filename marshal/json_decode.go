package marshal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// jsonRowDecoder reuses input readers and strict decoding state within one batch.
type jsonRowDecoder struct {
	enforceUTF8 bool
	strict      *strictJSONDecoder
	text        strings.Reader
	data        bytes.Reader
}

// decode reads one row and leaves previously returned values unchanged.
func (d *jsonRowDecoder) decode(row any) (any, error) {
	var input io.Reader
	switch v := row.(type) {
	case string:
		d.text.Reset(v)
		input = &d.text
	case []byte:
		d.data.Reset(v)
		input = &d.data
	default:
		return nil, fmt.Errorf("JSON row must be string or []byte, got %T", row)
	}
	if d.enforceUTF8 {
		if d.strict == nil {
			d.strict = new(strictJSONDecoder)
		}
		return d.strict.decode(input)
	}
	var value any
	dec := json.NewDecoder(input)
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// decodeStructKey reads the entire property name using the same number and UTF-8 rules as its row.
func (d *jsonRowDecoder) decodeStructKey(key string) (map[string]any, error) {
	// decode stops after the first value, so Valid is what rejects trailing input.
	if !json.Valid([]byte(key)) {
		return nil, fmt.Errorf("expected a single JSON object")
	}
	value, err := d.decode(key)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected a JSON object, got %T", value)
	}
	return object, nil
}
