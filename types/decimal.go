package types

import (
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
		if scale > len(ans) {
			ans = strings.Repeat("0", scale-(len(ans))+1) + ans
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
func ConvertDecimalValue(val any, pT *parquet.Type, precision, scale int) any {
	switch *pT {
	case parquet.Type_INT32:
		if v, ok := val.(int32); ok {
			return decimalIntToFloat(int64(v), scale)
		}
	case parquet.Type_INT64:
		if v, ok := val.(int64); ok {
			return decimalIntToFloat(v, scale)
		}
	case parquet.Type_BYTE_ARRAY, parquet.Type_FIXED_LEN_BYTE_ARRAY:
		if v, ok := val.(string); ok {
			return decimalByteArrayToFloat([]byte(v), precision, scale)
		}
		if v, ok := val.([]byte); ok {
			return decimalByteArrayToFloat(v, precision, scale)
		}
	}
	return val
}

func strToDecimalLogical(s string, dec *parquet.DecimalType, pT *parquet.Type, length int) (any, error) {
	decScale := int(dec.GetScale())
	numSca := big.NewFloat(1.0)
	for range decScale {
		numSca.Mul(numSca, big.NewFloat(10))
	}
	num := new(big.Float)
	num.SetString(s)
	num.Mul(num, numSca)

	switch *pT {
	case parquet.Type_INT32:
		tmp, _ := num.Float64()
		return int32(tmp), nil
	case parquet.Type_INT64:
		tmp, _ := num.Float64()
		return int64(tmp), nil
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return StrIntToBinary(num.Text('f', 0), "BigEndian", length, true), nil
	default:
		return StrIntToBinary(num.Text('f', 0), "BigEndian", 0, true), nil
	}
}

// decimalIntToFloat converts a decimal integer value to float64 for JSON output.
func decimalIntToFloat(dec int64, scale int) float64 {
	if scale <= 0 {
		return float64(dec)
	}

	divisor := 1.0
	for i := 0; i < scale; i++ {
		divisor *= 10.0
	}

	return float64(dec) / divisor
}

// decimalByteArrayToFloat converts a decimal byte array value to float64 for JSON output.
func decimalByteArrayToFloat(data []byte, precision, scale int) any {
	strValue := DECIMAL_BYTE_ARRAY_ToString(data, precision, scale)
	if floatVal, err := strconv.ParseFloat(strValue, 64); err == nil {
		return floatVal
	}
	return strValue
}
