package types

import "github.com/hangxie/parquet-go/v3/parquet"

// ConvertIntegerLogicalValue converts value based on INTEGER logical type width/sign.
func ConvertIntegerLogicalValue(val any, pT *parquet.Type, intType *parquet.IntType) any {
	if val == nil || intType == nil {
		return val
	}

	bitWidth := intType.GetBitWidth()
	signed := intType.GetIsSigned()

	cast32 := func(v int32) any {
		if signed {
			switch bitWidth {
			case 8:
				return int8(v)
			case 16:
				return int16(v)
			case 32:
				return v
			default:
				return v
			}
		}

		switch bitWidth {
		case 8:
			return uint8(v)
		case 16:
			return uint16(v)
		case 32:
			return uint32(v)
		default:
			return uint32(v)
		}
	}

	cast64 := func(v int64) any {
		if signed {
			if bitWidth == 64 {
				return v
			}
			return int32(v)
		}

		if bitWidth == 64 {
			return uint64(v)
		}
		return uint32(v)
	}

	switch v := val.(type) {
	case int32:
		return cast32(v)
	case int64:
		return cast64(v)
	default:
		return val
	}
}
