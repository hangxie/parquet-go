package types

import (
	"encoding/base64"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ConvertBSONLogicalValue handles BSON decoding to a map for JSON compatibility.
func ConvertBSONLogicalValue(val any) any {
	rendered, _ := convertBSONValue(val)
	return rendered
}

// convertBSONValue renders a BSON document, reporting one it cannot parse.
func convertBSONValue(val any) (any, error) {
	if val == nil {
		return nil, nil
	}

	bsonBytes, ok := valueBytes(val)
	if !ok {
		return val, errUnrenderable("BSON", "value is %T, not bytes", val)
	}

	if len(bsonBytes) == 0 {
		// bson.Unmarshal rejects this as EOF; the empty map is the long-standing
		// substitution, so it stays as the value while the reason is reported.
		return map[string]any{}, errUnrenderable("BSON", "document is empty")
	}

	var result map[string]any
	if err := bson.Unmarshal(bsonBytes, &result); err != nil {
		// base64 is the substitution the deprecated path keeps.
		return base64.StdEncoding.EncodeToString(bsonBytes), errUnrenderableCause("BSON", err)
	}

	return result, nil
}
