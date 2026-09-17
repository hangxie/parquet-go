package types

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func DECIMAL_INT_ToString(dec int64, precision, scale int) string {
	ans := strconv.FormatInt(dec, 10)
	sign := ""
	if dec < 0 {
		sign = "-"
		ans = ans[1:]
	}
	if scale > 0 {
		// One digit past the scale, so the radix point never lands at position zero:
		// ".92" is not a number json.Marshal accepts, "0.92" is.
		if scale >= len(ans) {
			ans = strings.Repeat("0", scale-len(ans)+1) + ans
		}
		radixLoc := len(ans) - scale
		ans = ans[:radixLoc] + "." + ans[radixLoc:]
	}
	return sign + ans
}

func DECIMAL_BYTE_ARRAY_ToString(dec []byte, precision, scale int) string {
	if len(dec) == 0 {
		dec = []byte{0}
	}
	sign := ""
	if dec[0] > 0x7f {
		sign = "-"
		// Clone the slice to avoid mutating the caller's data
		dec = slices.Clone(dec)
		for i := range dec {
			dec[i] = dec[i] ^ 0xff
		}
	}
	a := new(big.Int)
	a.SetBytes(dec)
	if sign == "-" {
		a = a.Add(a, big.NewInt(1))
	}
	sa := a.Text(10)

	if scale > 0 {
		ln := len(sa)
		if ln < scale+1 {
			sa = strings.Repeat("0", scale+1-ln) + sa
			ln = scale + 1
		}
		sa = sa[:ln-scale] + "." + sa[ln-scale:]
	}
	return sign + sa
}

// ConvertDecimalValue handles decimal conversion for both logical and converted types.
// The result is a json.Number carrying the exact decimal text: float64 cannot hold a
// DECIMAL wider than ~15.9 significant digits, and DECIMAL exists to avoid binary floats.
func ConvertDecimalValue(val any, pT *parquet.Type, precision, scale int) any {
	switch *pT {
	case parquet.Type_INT32:
		if v, ok := val.(int32); ok {
			return json.Number(DECIMAL_INT_ToString(int64(v), precision, scale))
		}
	case parquet.Type_INT64:
		if v, ok := val.(int64); ok {
			return json.Number(DECIMAL_INT_ToString(v, precision, scale))
		}
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if v, ok := val.(string); ok {
			return json.Number(DECIMAL_BYTE_ARRAY_ToString([]byte(v), precision, scale))
		}
		if v, ok := val.([]byte); ok {
			return json.Number(DECIMAL_BYTE_ARRAY_ToString(v, precision, scale))
		}
	}
	return val
}

// decimalStrToUnscaled scans a decimal string into its unscaled value at the given scale.
// big.Rat keeps every digit, where the big.Float this replaced carried a 64-bit mantissa
// and rounded a 38-digit DECIMAL away before it was ever stored.
func decimalStrToUnscaled(s string, scale int) (*big.Int, error) {
	text := strings.TrimSpace(s)
	if !isDecimalText(text) {
		return nil, fmt.Errorf("parse DECIMAL %q: invalid syntax", s)
	}
	// isDecimalText checks the syntax; SetString still rejects an exponent too large for
	// it to scale, and returns nil rather than a value when it does.
	num, ok := new(big.Rat).SetString(text)
	if !ok {
		return nil, fmt.Errorf("parse DECIMAL %q: exponent out of range", s)
	}
	if scale > 0 {
		num.Mul(num, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)))
	}

	// Digits below the scale are rounded to nearest, ties away from zero, the rule SQL
	// engines apply when casting to a narrower DECIMAL.
	denom := num.Denom()
	quo, rem := new(big.Int).QuoRem(num.Num(), denom, new(big.Int))
	if rem.Abs(rem).Lsh(rem, 1).Cmp(denom) >= 0 {
		quo.Add(quo, big.NewInt(int64(num.Sign())))
	}
	return quo, nil
}

// isDecimalText reports whether s is a decimal number in plain or exponent form.
// big.Rat.SetString also takes fractions ("1/2"), base prefixes ("0x10") and digit
// separators ("1_000"), none of which are decimal text a writer should accept.
func isDecimalText(s string) bool {
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	mantissa := scanDigits(s, &i)
	if i < len(s) && s[i] == '.' {
		i++
		mantissa += scanDigits(s, &i)
	}
	if mantissa == 0 {
		return false
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		if scanDigits(s, &i) == 0 {
			return false
		}
	}
	return i == len(s)
}

// scanDigits advances i past ASCII digits and returns how many it consumed.
func scanDigits(s string, i *int) int {
	start := *i
	for *i < len(s) && s[*i] >= '0' && s[*i] <= '9' {
		*i++
	}
	return *i - start
}

// strToDecimal scans a decimal string into the physical value backing the column.
// A precision of 0 leaves the declared digit count unchecked, which is all the
// ConvertedType path can do: unlike DecimalType, it does not carry a precision.
func strToDecimal(s string, pT *parquet.Type, precision, length, scale int) (any, error) {
	unscaled, err := decimalStrToUnscaled(s, scale)
	if err != nil {
		return nil, err
	}

	// Precision is the number of digits the schema says the unscaled value has, so a
	// wider value would contradict the metadata written alongside it.
	if precision > 0 && len(new(big.Int).Abs(unscaled).String()) > precision {
		return nil, fmt.Errorf("DECIMAL %q exceeds precision %d", s, precision)
	}

	switch *pT {
	case parquet.Type_INT32:
		if !unscaled.IsInt64() || unscaled.Int64() < math.MinInt32 || unscaled.Int64() > math.MaxInt32 {
			return nil, fmt.Errorf("DECIMAL %q does not fit in INT32", s)
		}
		return int32(unscaled.Int64()), nil
	case parquet.Type_INT64:
		if !unscaled.IsInt64() {
			return nil, fmt.Errorf("DECIMAL %q does not fit in INT64", s)
		}
		return unscaled.Int64(), nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		// StrIntToBinary keeps the low length bytes, so an oversized value would be
		// stored as a silently different number.
		if length > 0 && !fitsInTwosComplement(unscaled, length) {
			return nil, fmt.Errorf("DECIMAL %q does not fit in %d bytes", s, length)
		}
		return StrIntToBinary(unscaled.String(), "BigEndian", length, true), nil
	default:
		return StrIntToBinary(unscaled.String(), "BigEndian", 0, true), nil
	}
}

// fitsInTwosComplement reports whether v is representable in length bytes of big-endian
// two's complement, the encoding StrIntToBinary produces for DECIMAL.
func fitsInTwosComplement(v *big.Int, length int) bool {
	max := new(big.Int).Lsh(big.NewInt(1), uint(length*8-1))
	min := new(big.Int).Neg(max)
	return v.Cmp(min) >= 0 && v.Cmp(max.Sub(max, big.NewInt(1))) <= 0
}
