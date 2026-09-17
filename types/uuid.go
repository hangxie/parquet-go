package types

import (
	"github.com/google/uuid"

	"github.com/hangxie/parquet-go/v3/common"
)

// ConvertUUIDValue handles UUID conversion from binary data to standard UUID string format.
func ConvertUUIDValue(val any) any {
	rendered, _ := convertUUIDValue(val)
	return rendered
}

// convertUUIDValue renders a UUID, reporting bytes that are not one.
func convertUUIDValue(val any) (any, error) {
	if val == nil {
		return nil, nil
	}

	bytes, ok := valueBytes(val)
	if !ok {
		return val, errUnrenderable("UUID", "value is %T, not bytes", val)
	}
	if len(bytes) != common.UUIDByteLen {
		return val, errUnrenderable("UUID", "is %d bytes, must be %d", len(bytes), common.UUIDByteLen)
	}

	return uuid.UUID(bytes).String(), nil
}
