//go:build !go1.27

package marshal

import (
	"encoding/json"
	"io"

	jsonv2 "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

// exactJSONNumbers preserves the number text used by the column conversion routines.
var exactJSONNumbers = jsonv2.UnmarshalFromFunc(func(dec *jsontext.Decoder, out *any) error {
	if dec.PeekKind() != '0' {
		return jsonv2.SkipFunc
	}
	raw, err := dec.ReadValue()
	if err != nil {
		return err
	}
	*out = json.Number(raw)
	return nil
})

// strictJSONDecoder retains jsonv2 buffers and its string cache between rows.
type strictJSONDecoder struct {
	decoder jsontext.Decoder
}

// decode resets row state while retaining the decoder's reusable storage.
func (d *strictJSONDecoder) decode(input io.Reader) (any, error) {
	// Preserve the existing duplicate-name and first-value semantics.
	d.decoder.Reset(input, jsontext.AllowDuplicateNames(true))
	var value any
	if err := jsonv2.UnmarshalDecode(&d.decoder, &value, jsonv2.WithUnmarshalers(exactJSONNumbers)); err != nil {
		return nil, err
	}
	return value, nil
}
