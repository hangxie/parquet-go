package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzFloat16MinMaxSize(f *testing.F) {
	seed := func(values ...uint16) []byte {
		b := make([]byte, 0, len(values)*2)
		for _, v := range values {
			b = append(b, byte(v), byte(v>>8))
		}
		return b
	}
	const (
		nan     = uint16(0x7C01)
		posInf  = uint16(0x7C00)
		negInf  = uint16(0xFC00)
		one     = uint16(0x3C00)
		two     = uint16(0x4000)
		negZero = uint16(0x8000)
	)
	f.Add(seed())
	f.Add(seed(nan))
	f.Add(seed(nan, nan))
	f.Add(seed(one, two, nan))
	f.Add(seed(nan, one, two))
	f.Add(seed(negInf, nan, posInf))
	f.Add(seed(0, negZero))

	f.Fuzz(func(t *testing.T, b []byte) {
		table := float16FuncTable{}
		var values []string
		var minVal, maxVal any
		for i := 0; i+2 <= len(b); i += 2 {
			v := string(b[i : i+2])
			values = append(values, v)

			var size int32
			minVal, maxVal, size = table.MinMaxSize(minVal, maxVal, v)
			require.Equal(t, int32(2), size)
		}

		var ordered []string
		for _, v := range values {
			if !isFloat16NaN(v) {
				ordered = append(ordered, v)
			}
		}

		if len(ordered) == 0 {
			require.Nil(t, minVal)
			require.Nil(t, maxVal)
			return
		}

		require.NotNil(t, minVal)
		require.NotNil(t, maxVal)
		require.False(t, isFloat16NaN(minVal), "min bound is NaN")
		require.False(t, isFloat16NaN(maxVal), "max bound is NaN")
		require.Contains(t, ordered, minVal, "min bound is not one of the values")
		require.Contains(t, ordered, maxVal, "max bound is not one of the values")
		for _, v := range ordered {
			require.False(t, table.LessThan(v, minVal), "value %x sorts below the min bound", v)
			require.False(t, table.LessThan(maxVal, v), "value %x sorts above the max bound", v)
		}
	})
}
