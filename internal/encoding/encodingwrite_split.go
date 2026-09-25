package encoding

import (
	"math"
)

func WriteByteStreamSplit(nums []any) []byte {
	ln := len(nums)
	if ln <= 0 {
		return []byte{}
	}

	switch nums[0].(type) {
	case float32:
		return WriteByteStreamSplitFloat32(nums)
	case float64:
		return WriteByteStreamSplitFloat64(nums)
	case int32:
		return WriteByteStreamSplitINT32(nums)
	case int64:
		return WriteByteStreamSplitINT64(nums)
	case string, []byte:
		return WriteByteStreamSplitFixedLenByteArray(nums)
	default:
		return []byte{}
	}
}

func WriteByteStreamSplitFloat32(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	buf := make([]byte, ln*4)
	for i, n := range vals {
		v := math.Float32bits(n.(float32))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
	}
	return buf
}

func WriteByteStreamSplitFloat64(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}

	buf := make([]byte, ln*8)
	for i, n := range vals {
		v := math.Float64bits(n.(float64))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
		buf[ln*4+i] = byte(v >> 32)
		buf[ln*5+i] = byte(v >> 40)
		buf[ln*6+i] = byte(v >> 48)
		buf[ln*7+i] = byte(v >> 56)
	}
	return buf
}

func WriteByteStreamSplitINT32(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	buf := make([]byte, ln*4)
	for i, n := range vals {
		v := uint32(n.(int32))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
	}
	return buf
}

func WriteByteStreamSplitINT64(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	buf := make([]byte, ln*8)
	for i, n := range vals {
		v := uint64(n.(int64))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
		buf[ln*4+i] = byte(v >> 32)
		buf[ln*5+i] = byte(v >> 40)
		buf[ln*6+i] = byte(v >> 48)
		buf[ln*7+i] = byte(v >> 56)
	}
	return buf
}

func WriteByteStreamSplitFixedLenByteArray(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	// Get element size from first value
	first, ok := plainByteValue(vals[0])
	if !ok {
		return []byte{}
	}
	elemSize := len(first)
	if elemSize <= 0 {
		return []byte{}
	}
	buf := make([]byte, ln*elemSize)
	for i, n := range vals {
		s, ok := plainByteValue(n)
		if !ok || len(s) != elemSize {
			return []byte{}
		}
		for j := 0; j < elemSize; j++ {
			buf[ln*j+i] = s[j]
		}
	}
	return buf
}
