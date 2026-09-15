package types

import (
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

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

// integerLabel names an integer annotation as the schema spells it, declared width and
// all. Built only in the error branches: this runs once per value.
func integerLabel(it *parquet.IntType) string {
	if it.GetIsSigned() {
		return "INT_" + strconv.Itoa(int(it.GetBitWidth()))
	}
	return "UINT_" + strconv.Itoa(int(it.GetBitWidth()))
}

// strToIntegerLogical scans an INTEGER logical value using the annotation's width and
// signedness, inverting ConvertIntegerLogicalValue. Without it an unsigned column falls
// through to the physical INT32/INT64 scan, which rejects every value above the signed
// maximum: exactly the values the read path renders for the upper half of the range.
func strToIntegerLogical(s string, it *parquet.IntType, pT *parquet.Type) (any, bool, error) {
	if pT == nil || (*pT != parquet.Type_INT32 && *pT != parquet.Type_INT64) {
		return nil, false, nil
	}
	physWidth := 32
	if *pT == parquet.Type_INT64 {
		physWidth = 64
	}

	width := int(it.GetBitWidth())
	switch width {
	case 8, 16, 32:
		// The format pins these widths to INT32. On any other column the annotation does
		// not describe the range the column holds, and the read path renders the wider
		// value as it stands, so the physical scan takes it back rather than this
		// rejecting what the reader just emitted.
		if physWidth != 32 {
			return nil, false, nil
		}
	case 64:
		if physWidth != 64 {
			return nil, false, nil
		}
	default:
		// Not a width the format defines; the column decides the range instead.
		width = physWidth
	}

	zero := any(int32(0))
	if physWidth == 64 {
		zero = int64(0)
	}

	// Surrounding whitespace is ordinary in a CSV field, and strToDecimal already drops it
	// before its own strict scan.
	text := strings.TrimSpace(s)

	if it.GetIsSigned() {
		v, err := strconv.ParseInt(text, 10, width)
		if err != nil {
			return zero, true, wrapScanErr(integerLabel(it), s, err)
		}
		if physWidth == 32 {
			return int32(v), true, nil
		}
		return v, true, nil
	}

	v, err := strconv.ParseUint(text, 10, width)
	if err != nil {
		return zero, true, wrapScanErr(integerLabel(it), s, err)
	}
	if physWidth == 32 {
		return int32(uint32(v)), true, nil
	}
	return int64(v), true, nil
}
