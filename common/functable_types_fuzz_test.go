package common

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// requireFloatBounds asserts the Parquet rule for floating point min/max: bounds never hold NaN,
// are values that were actually seen, and cover every non-NaN value; a page whose values are all
// NaN, or that has no values at all, carries no bounds.
func requireFloatBounds(t *testing.T, values []float64, minVal, maxVal *float64) {
	t.Helper()

	var ordered []float64
	for _, v := range values {
		if !math.IsNaN(v) {
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
	require.False(t, math.IsNaN(*minVal), "min bound is NaN")
	require.False(t, math.IsNaN(*maxVal), "max bound is NaN")
	require.Contains(t, ordered, *minVal, "min bound is not one of the values")
	require.Contains(t, ordered, *maxVal, "max bound is not one of the values")
	for _, v := range ordered {
		require.LessOrEqual(t, *minVal, v)
		require.GreaterOrEqual(t, *maxVal, v)
	}
}

func float32Bound(bound any) *float64 {
	if bound == nil {
		return nil
	}
	f := float64(bound.(float32))
	return &f
}

func float64Bound(bound any) *float64 {
	if bound == nil {
		return nil
	}
	f := bound.(float64)
	return &f
}

func FuzzFloat32MinMaxSize(f *testing.F) {
	seed := func(values ...float32) []byte {
		b := make([]byte, 0, len(values)*4)
		for _, v := range values {
			b = binary.LittleEndian.AppendUint32(b, math.Float32bits(v))
		}
		return b
	}
	nan := float32(math.NaN())
	f.Add(seed())
	f.Add(seed(nan))
	f.Add(seed(nan, nan))
	f.Add(seed(1, 2, nan, 3))
	f.Add(seed(nan, 1, 2, 3))
	f.Add(seed(float32(math.Inf(-1)), nan, float32(math.Inf(1))))
	f.Add(seed(0, -0))

	f.Fuzz(func(t *testing.T, b []byte) {
		table := float32FuncTable{}
		var values []float64
		var minVal, maxVal any
		for i := 0; i+4 <= len(b); i += 4 {
			v := math.Float32frombits(binary.LittleEndian.Uint32(b[i:]))
			values = append(values, float64(v))

			var size int32
			minVal, maxVal, size = table.MinMaxSize(minVal, maxVal, v)
			require.Equal(t, int32(4), size)
		}
		requireFloatBounds(t, values, float32Bound(minVal), float32Bound(maxVal))
	})
}

func FuzzFloat64MinMaxSize(f *testing.F) {
	seed := func(values ...float64) []byte {
		b := make([]byte, 0, len(values)*8)
		for _, v := range values {
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(v))
		}
		return b
	}
	nan := math.NaN()
	f.Add(seed())
	f.Add(seed(nan))
	f.Add(seed(nan, nan))
	f.Add(seed(1, 2, nan, 3))
	f.Add(seed(nan, 1, 2, 3))
	f.Add(seed(math.Inf(-1), nan, math.Inf(1)))
	f.Add(seed(0, math.Copysign(0, -1)))

	f.Fuzz(func(t *testing.T, b []byte) {
		table := float64FuncTable{}
		var values []float64
		var minVal, maxVal any
		for i := 0; i+8 <= len(b); i += 8 {
			v := math.Float64frombits(binary.LittleEndian.Uint64(b[i:]))
			values = append(values, v)

			var size int32
			minVal, maxVal, size = table.MinMaxSize(minVal, maxVal, v)
			require.Equal(t, int32(8), size)
		}
		requireFloatBounds(t, values, float64Bound(minVal), float64Bound(maxVal))
	})
}
