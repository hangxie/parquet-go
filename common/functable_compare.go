package common

import (
	"bytes"
	"reflect"
)

func (float16FuncTable) LessThan(a, b any) bool {
	fa, oka := float16OrderKey(a)
	fb, okb := float16OrderKey(b)
	if oka && okb {
		return fa < fb
	}
	// Fallback: lexicographic compare on raw bytes
	ab := toBytes(a)
	bb := toBytes(b)
	if ab == nil || bb == nil {
		return false
	}
	return bytes.Compare(ab, bb) < 0
}

func (table float16FuncTable) MinMaxSize(minVal, maxVal, val any) (any, any, int32) {
	if isFloat16NaN(minVal) {
		minVal = nil
	}
	if isFloat16NaN(maxVal) {
		maxVal = nil
	}
	if isFloat16NaN(val) {
		return minVal, maxVal, 2
	}
	return Min(table, minVal, val), Max(table, maxVal, val), 2
}

func Min(table FuncTable, a, b any) any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return a
	} else {
		return b
	}
}

func Max(table FuncTable, a, b any) any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if table.LessThan(a, b) {
		return b
	} else {
		return a
	}
}

// Get the size of a parquet value
func SizeOf(val reflect.Value) int64 {
	if !val.IsValid() {
		return 0
	}
	var size int64
	switch val.Type().Kind() {
	case reflect.Pointer:
		if val.IsNil() {
			return 0
		}
		return SizeOf(val.Elem())
	case reflect.Slice:
		for i := range val.Len() {
			size += SizeOf(val.Index(i))
		}
		return size
	case reflect.Struct:
		for i := range val.Type().NumField() {
			size += SizeOf(val.Field(i))
		}
		return size
	case reflect.Map:
		keys := val.MapKeys()
		for i := range keys {
			size += SizeOf(keys[i])
			size += SizeOf(val.MapIndex(keys[i]))
		}
		return size
	case reflect.Bool:
		return 1
	case reflect.Int32:
		return 4
	case reflect.Int64:
		return 8
	case reflect.String:
		return int64(val.Len())
	case reflect.Float32:
		return 4
	case reflect.Float64:
		return 8
	}
	return 4
}

func handleEmptyBinary(la, lb int, abs, bbs []byte, signed bool) (bool, bool) {
	if la == 0 && lb == 0 {
		return false, true
	}
	if la == 0 {
		// a is zero, b is non-zero
		// for signed: zero < positive, zero > negative
		// for unsigned: zero < any non-zero
		if signed {
			return (bbs[0]>>7)&1 == 0, true // true if b is positive
		}
		return true, true
	}
	if lb == 0 {
		// b is zero, a is non-zero
		if signed {
			return (abs[0]>>7)&1 == 1, true // true if a is negative
		}
		return false, true
	}
	return false, false
}

func padBinary(abs, bbs []byte, signed bool) ([]byte, []byte) {
	la, lb := len(abs), len(bbs)
	if !signed {
		if la < lb {
			abs = append(make([]byte, lb-la), abs...)
		} else if lb < la {
			bbs = append(make([]byte, la-lb), bbs...)
		}
	} else {
		if la < lb {
			sb := (abs[0] >> 7) & 1
			pre := make([]byte, lb-la)
			if sb == 1 {
				for i := range lb - la {
					pre[i] = byte(0xFF)
				}
			}
			abs = append(pre, abs...)
		} else if la > lb {
			sb := (bbs[0] >> 7) & 1
			pre := make([]byte, la-lb)
			if sb == 1 {
				for i := range la - lb {
					pre[i] = byte(0xFF)
				}
			}
			bbs = append(pre, bbs...)
		}
	}
	return abs, bbs
}

func reverseBytes(s []byte) []byte {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

func cmpIntBinary(as, bs, order string, signed bool) bool {
	abs := []byte(as)
	bbs := []byte(bs)
	la, lb := len(abs), len(bbs)

	if res, ok := handleEmptyBinary(la, lb, abs, bbs, signed); ok {
		return res
	}

	// convert to big endian to simplify logic below
	if order == "LittleEndian" {
		abs = reverseBytes(abs)
		bbs = reverseBytes(bbs)
	}

	abs, bbs = padBinary(abs, bbs, signed)

	if signed {
		asb, bsb := (abs[0]>>7)&1, (bbs[0]>>7)&1
		if asb < bsb {
			return false
		} else if asb > bsb {
			return true
		}
	}

	for i := range abs {
		if abs[i] < bbs[i] {
			return true
		} else if abs[i] > bbs[i] {
			return false
		}
	}
	return false
}
