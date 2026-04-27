package types

import (
	"encoding/base64"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ConvertBSONLogicalValue handles BSON decoding to a map for JSON compatibility.
func ConvertBSONLogicalValue(val any) any {
	if val == nil {
		return nil
	}

	var bsonBytes []byte
	switch v := val.(type) {
	case []byte:
		bsonBytes = v
	case string:
		bsonBytes = []byte(v)
	default:
		return val
	}

	if len(bsonBytes) == 0 {
		return map[string]any{}
	}

	var result map[string]any
	err := bson.Unmarshal(bsonBytes, &result)
	if err != nil {
		return base64.StdEncoding.EncodeToString(bsonBytes)
	}

	return result
}
