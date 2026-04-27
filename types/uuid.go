package types

import "fmt"

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

	if len(bytes) != 16 {
		return val
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uint32(bytes[0])<<24|uint32(bytes[1])<<16|uint32(bytes[2])<<8|uint32(bytes[3]),
		uint16(bytes[4])<<8|uint16(bytes[5]),
		uint16(bytes[6])<<8|uint16(bytes[7]),
		uint16(bytes[8])<<8|uint16(bytes[9]),
		uint64(bytes[10])<<40|uint64(bytes[11])<<32|uint64(bytes[12])<<24|uint64(bytes[13])<<16|uint64(bytes[14])<<8|uint64(bytes[15]))
}
