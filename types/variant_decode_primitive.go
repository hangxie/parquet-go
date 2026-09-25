package types

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strings"
)

// formatDecimal formats an int64 unscaled value with the given scale as a decimal string
func formatDecimal(unscaled int64, scale int) string {
	return formatDecimal128(big.NewInt(unscaled), scale)
}

// formatDecimal128 formats a big.Int unscaled value with the given scale as a decimal string
func formatDecimal128(unscaled *big.Int, scale int) string {
	if scale == 0 {
		return unscaled.String()
	}

	r := new(big.Rat).SetInt(unscaled)
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	r.Quo(r, new(big.Rat).SetInt(divisor))

	result := r.FloatString(scale)

	// Remove trailing zeros after decimal point
	result = strings.TrimRight(result, "0")
	result = strings.TrimRight(result, ".")

	return result
}

// decodePrimitiveValue decodes a primitive variant value
func decodePrimitiveTemporal(data []byte, offset int, primitiveType uint8) (int, any, error) {
	switch primitiveType {
	case variantPrimitiveDate:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for date")
		}
		days := int32(binary.LittleEndian.Uint32(data[offset:]))
		return 4, ConvertDateLogicalValue(days), nil

	case variantPrimitiveTimestampMicro, variantPrimitiveTimestampNTZMicro:
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for timestamp")
		}
		micros := int64(binary.LittleEndian.Uint64(data[offset:]))
		return 8, TIMESTAMP_MICROSToISO8601(micros, primitiveType == variantPrimitiveTimestampMicro), nil

	case variantPrimitiveTimeNTZ:
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for time")
		}
		micros := int64(binary.LittleEndian.Uint64(data[offset:]))
		hours := micros / 3600000000
		micros %= 3600000000
		minutes := micros / 60000000
		micros %= 60000000
		seconds := micros / 1000000
		microsFrac := micros % 1000000
		return 8, fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, microsFrac), nil

	default: // variantPrimitiveTimestampNano, variantPrimitiveTimestampNTZNano
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for timestamp")
		}
		nanos := int64(binary.LittleEndian.Uint64(data[offset:]))
		return 8, TIMESTAMP_NANOSToISO8601(nanos, primitiveType == variantPrimitiveTimestampNano), nil
	}
}

func decodePrimitiveVarLen(data []byte, offset int, primitiveType uint8) (int, any, error) {
	switch primitiveType {
	case variantPrimitiveString:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for string length")
		}
		length := int(binary.LittleEndian.Uint32(data[offset:]))
		if offset+4+length > len(data) {
			return 0, nil, fmt.Errorf("string length exceeds data")
		}
		return 4 + length, string(data[offset+4 : offset+4+length]), nil

	case variantPrimitiveBinary:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for binary length")
		}
		length := int(binary.LittleEndian.Uint32(data[offset:]))
		if offset+4+length > len(data) {
			return 0, nil, fmt.Errorf("binary length exceeds data")
		}
		return 4 + length, base64.StdEncoding.EncodeToString(data[offset+4 : offset+4+length]), nil

	case variantPrimitiveDecimal4:
		if offset+5 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for decimal4")
		}
		scale := int(data[offset])
		unscaled := int64(int32(binary.LittleEndian.Uint32(data[offset+1:])))
		return 5, formatDecimal(unscaled, scale), nil

	case variantPrimitiveDecimal8:
		if offset+9 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for decimal8")
		}
		scale := int(data[offset])
		unscaled := int64(binary.LittleEndian.Uint64(data[offset+1:]))
		return 9, formatDecimal(unscaled, scale), nil

	case variantPrimitiveDecimal16:
		if offset+17 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for decimal16")
		}
		scale := int(data[offset])
		unscaled := readLittleEndianInt128(data[offset+1 : offset+17])
		return 17, formatDecimal128(unscaled, scale), nil

	default: // variantPrimitiveUUID
		if offset+16 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for UUID")
		}
		return 16, ConvertUUIDValue(data[offset : offset+16]), nil
	}
}

func decodePrimitiveValue(data []byte, offset int, primitiveType uint8) (int, any, error) {
	switch primitiveType {
	case variantPrimitiveNull:
		return 0, nil, nil

	case variantPrimitiveTrue:
		return 0, true, nil

	case variantPrimitiveFalse:
		return 0, false, nil

	case variantPrimitiveInt8:
		if offset >= len(data) {
			return 0, nil, fmt.Errorf("not enough data for int8")
		}
		return 1, int8(data[offset]), nil

	case variantPrimitiveInt16:
		if offset+2 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for int16")
		}
		return 2, int16(binary.LittleEndian.Uint16(data[offset:])), nil

	case variantPrimitiveInt32:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for int32")
		}
		return 4, int32(binary.LittleEndian.Uint32(data[offset:])), nil

	case variantPrimitiveInt64:
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for int64")
		}
		return 8, int64(binary.LittleEndian.Uint64(data[offset:])), nil

	case variantPrimitiveDouble:
		if offset+8 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for double")
		}
		bits := binary.LittleEndian.Uint64(data[offset:])
		return 8, math.Float64frombits(bits), nil

	case variantPrimitiveFloat:
		if offset+4 > len(data) {
			return 0, nil, fmt.Errorf("not enough data for float")
		}
		bits := binary.LittleEndian.Uint32(data[offset:])
		return 4, math.Float32frombits(bits), nil

	case variantPrimitiveDate, variantPrimitiveTimestampMicro, variantPrimitiveTimestampNTZMicro,
		variantPrimitiveTimeNTZ, variantPrimitiveTimestampNano, variantPrimitiveTimestampNTZNano:
		return decodePrimitiveTemporal(data, offset, primitiveType)

	case variantPrimitiveString, variantPrimitiveBinary, variantPrimitiveUUID,
		variantPrimitiveDecimal4, variantPrimitiveDecimal8, variantPrimitiveDecimal16:
		return decodePrimitiveVarLen(data, offset, primitiveType)

	default:
		return 0, nil, fmt.Errorf("unknown variant primitive type: %d", primitiveType)
	}
}
