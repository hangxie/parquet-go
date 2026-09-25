package common

import (
	"encoding/binary"
	"math"
)

// float16FuncTable provides numeric ordering for FLOAT16 logical type stored in FIXED[2]
type float16FuncTable struct{}

// isFloat16NaN reports whether v holds an IEEE 754 binary16 NaN: all-ones exponent with a
// non-zero significand. halfToFloat32 cannot answer this, since it rejects NaN outright.
func isFloat16NaN(v any) bool {
	b := toBytes(v)
	if len(b) != 2 {
		return false
	}
	bits := binary.LittleEndian.Uint16(b)
	return bits&0x7C00 == 0x7C00 && bits&0x03FF != 0
}

func toBytes(v any) []byte {
	switch t := v.(type) {
	case string:
		b := []byte(t)
		if len(b) > 0 {
			return b
		}
		return []byte{}
	case []byte:
		return t
	default:
		return nil
	}
}

// float16OrderKey maps IEEE 754 binary16 bits to the Parquet FLOAT16 total order.
func float16OrderKey(v any) (uint16, bool) {
	b := toBytes(v)
	if b == nil || len(b) != 2 {
		return 0, false
	}
	bits := binary.LittleEndian.Uint16(b)
	if bits&0x8000 != 0 {
		return ^bits, true
	}
	return bits | 0x8000, true
}

// halfToFloat32 decodes little-endian IEEE 754 binary16 to float32
func halfToFloat32(v any) (float32, bool) {
	b := toBytes(v)
	if b == nil || len(b) != 2 {
		return 0, false
	}
	u := uint16(b[0]) | uint16(b[1])<<8
	sign := (u>>15)&0x1 != 0
	exp := (u >> 10) & 0x1F
	frac := u & 0x03FF

	var f float32
	switch exp {
	case 0x1F: // Inf/NaN
		if frac != 0 {
			return 0, false
		}
		if sign {
			f = float32(math.Inf(-1))
		} else {
			f = float32(math.Inf(1))
		}
	case 0: // subnormal/zero
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
	return f, true
}
