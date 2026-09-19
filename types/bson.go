package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// emptyBSONExtJSON renders a document with no fields.
const emptyBSONExtJSON = "{}"

// ConvertBSONLogicalValue renders a BSON document as canonical Extended JSON text.
func ConvertBSONLogicalValue(val any) any {
	rendered, _ := convertBSONValue(val)
	return rendered
}

// isBSONAnnotated reports whether either annotation marks the column as BSON.
func isBSONAnnotated(cT *parquet.ConvertedType, lT *parquet.LogicalType) bool {
	if lT != nil && lT.IsSetBSON() {
		return true
	}
	return cT != nil && *cT == parquet.ConvertedType_BSON
}

// convertBSONValue renders a BSON document as canonical Extended JSON.
func convertBSONValue(val any) (any, error) {
	if val == nil {
		return nil, nil
	}

	bsonBytes, ok := valueBytes(val)
	if !ok {
		return val, errUnrenderable("BSON", "value is %T, not bytes", val)
	}

	if len(bsonBytes) == 0 {
		// Reported, but the empty document stays as the long-standing substitution.
		return emptyBSONExtJSON, errUnrenderable("BSON", "document is empty")
	}

	// bson.Raw rather than a decoded bson.D: same bytes, 10-25% faster. Validated first
	// because MarshalExtJSON trusts the length prefix and panics on one that lies.
	doc := bson.Raw(bsonBytes)
	if err := doc.Validate(); err != nil {
		// base64 is the substitution the deprecated path keeps.
		return base64.StdEncoding.EncodeToString(bsonBytes), errUnrenderableCause("BSON", err)
	}
	if declared := int(binary.LittleEndian.Uint32(bsonBytes[:4])); declared != len(bsonBytes) {
		// Validate stops at the first document; the renderer drops the rest silently.
		return base64.StdEncoding.EncodeToString(bsonBytes),
			errUnrenderable("BSON", "document declares %d bytes, value carries %d", declared, len(bsonBytes))
	}
	// Canonical, not relaxed: the wrappers are what tell an int32 from an int64.
	ext, err := bson.MarshalExtJSON(doc, true, false)
	if err != nil {
		return base64.StdEncoding.EncodeToString(bsonBytes), errUnrenderableCause("BSON", err)
	}
	return string(ext), nil
}

// strToBSON scans Extended JSON, canonical or relaxed, into the document's bytes.
func strToBSON(s string, pT *parquet.Type) (any, error) {
	if pT == nil {
		return nil, errNoPhysicalType(s)
	}
	if *pT != parquet.Type_BYTE_ARRAY {
		// Unreachable through either writer, which validate the schema first.
		return nil, fmt.Errorf("BSON requires a BYTE_ARRAY column, not %v", *pT)
	}
	text := []byte(s)
	if !json.Valid(text) {
		// UnmarshalExtJSON keeps the first value and drops the rest, storing
		// `{"a":1} oops` as `{"a":1}`.
		return nil, wrapScanErr("BSON", s, errors.New("not a single JSON value"))
	}
	raw, err := bsonFromExtJSON(text)
	if err != nil {
		return nil, wrapScanErr("BSON", s, err)
	}
	return string(raw), nil
}

// bsonFromExtJSON scans Extended JSON into the document's bytes, unwrapped.
func bsonFromExtJSON(ext []byte) ([]byte, error) {
	var doc bson.D
	if err := bson.UnmarshalExtJSON(ext, false, &doc); err != nil {
		return nil, err
	}
	return bson.Marshal(doc)
}
