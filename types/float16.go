package types

import (
	"encoding/binary"
	"fmt"
	"math"
)

// ParseFloat16String parses a float string and returns 2-byte IEEE 754 half-precision binary
func ParseFloat16String(s string) (string, error) {
	var f32 float32
	if _, err := fmt.Sscanf(s, "%f", &f32); err != nil {
		return "", fmt.Errorf("invalid float16 value: %s", s)
	}
	return Float32ToFloat16(f32), nil
}

func Float32ToFloat16(f32 float32) string {
	// Convert float32 to float16 (IEEE 754 half-precision)
	bits := math.Float32bits(f32)

	// Extract components from float32
	sign := (bits >> 31) & 1
	exp := int((bits >> 23) & 0xFF)
	frac := bits & 0x7FFFFF

	var f16 uint16

	switch exp {
	case 0xFF:
		// Inf or NaN
		if frac != 0 {
			// NaN
			f16 = uint16((sign << 15) | 0x7C00 | (frac >> 13))
		} else {
			// Inf
			f16 = uint16((sign << 15) | 0x7C00)
		}
	case 0:
		// Zero or subnormal
		f16 = uint16(sign << 15)
	default:
		// Normalized number
		newExp := exp - 127 + 15 // Rebias from float32 to float16

		if newExp >= 31 {
			// Overflow to infinity
			f16 = uint16((sign << 15) | 0x7C00)
		} else if newExp <= 0 {
			// Underflow to zero or subnormal
			if newExp < -10 {
				f16 = uint16(sign << 15)
			} else {
				// Subnormal
				frac = (frac | 0x800000) >> uint(1-newExp)
				f16 = uint16((sign << 15) | (frac >> 13))
			}
		} else {
			// Normal number
			f16 = uint16((sign << 15) | (uint32(newExp) << 10) | (frac >> 13))
		}
	}

	// Return as little-endian 2-byte string (Parquet uses little-endian for FLOAT16)
	result := make([]byte, 2)
	binary.LittleEndian.PutUint16(result, f16)
	return string(result)
}

// ConvertFloat16LogicalValue converts FIXED[2] IEEE 754 half-precision to float32.
func ConvertFloat16LogicalValue(val any) any {
	if val == nil {
		return nil
	}

	var b []byte
	switch v := val.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return val
	}

	if len(b) != 2 {
		return val
	}

	u := binary.LittleEndian.Uint16(b)
	sign := ((u >> 15) & 0x1) != 0
	exp := (u >> 10) & 0x1F
	frac := u & 0x3FF

	var f float32
	switch exp {
	case 0x1F:
		if frac != 0 {
			f = float32(math.NaN())
		} else if sign {
			f = float32(math.Inf(-1))
		} else {
			f = float32(math.Inf(1))
		}
	case 0:
		mant := float32(frac) / 1024.0
		f = mant / float32(1<<14)
		if sign {
			f = -f
		}
	default:
		mant := 1.0 + float32(frac)/1024.0
		exponent := int(exp) - 15
		if exponent >= 0 {
			p := 1 << exponent
			f = mant * float32(p)
		} else {
			p := 1 << (-exponent)
			f = mant / float32(p)
		}
		if sign {
			f = -f
		}
	}

	return f
}
