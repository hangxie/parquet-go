package types

import (
	"github.com/google/uuid"

	"github.com/hangxie/parquet-go/v3/common"
)

// ConvertUUIDValue handles UUID conversion from binary data to standard UUID string format.
func ConvertUUIDValue(val any) any {
	if val == nil {
		return nil
	}

	var bytes []byte
	switch v := val.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return val
	}

	if len(bytes) != common.UUIDByteLen {
		return val
	}

	return uuid.UUID(bytes).String()
}
