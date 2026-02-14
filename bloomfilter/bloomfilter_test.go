package bloomfilter

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	testCases := map[string]struct {
		numBytes     int
		expectedSize int
	}{
		"minimum-clamped":       {1, 32},
		"exact-32":              {32, 32},
		"round-up-to-64":        {33, 64},
		"exact-64":              {64, 64},
		"round-up-to-128":       {100, 128},
		"exact-1024":            {1024, 1024},
		"round-up-to-2048":      {1025, 2048},
		"maximum-clamped":       {256 * 1024 * 1024, 128 * 1024 * 1024},
		"zero-gets-minimum":     {0, 32},
		"negative-gets-minimum": {-1, 32},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			f := New(tc.numBytes)
			require.NotNil(t, f)
			require.Equal(t, int32(tc.expectedSize), f.NumBytes())
			require.Equal(t, tc.expectedSize, len(f.Bitset()))
		})
	}
}

func TestInsertAndCheck(t *testing.T) {
	f := New(1024)

	// Check should return false for values not inserted
	require.False(t, f.Check(12345))
	require.False(t, f.Check(67890))

	// Insert values
	f.Insert(12345)
	f.Insert(67890)

	// Check should return true for inserted values
	require.True(t, f.Check(12345))
	require.True(t, f.Check(67890))
}

func TestCheckNeverFalseNegative(t *testing.T) {
	f := New(4096)

	// Insert 1000 values
	for i := range 1000 {
		f.Insert(uint64(i * 7919)) // use prime multiplier for spread
	}

	// All inserted values must be found (no false negatives)
	for i := range 1000 {
		require.True(t, f.Check(uint64(i*7919)))
	}
}

func TestFalsePositiveRate(t *testing.T) {
	numDistinct := 10000
	fpp := 0.01
	numBytes := optimalNumBytes(numDistinct, fpp)
	f := New(numBytes)

	// Use xxhash to produce well-distributed hashes, matching real usage
	hashFunc := func(i int) uint64 {
		// Simulate real usage: values get hashed by xxhash before Insert/Check
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(i))
		return xxhash.Sum64(buf)
	}

	// Insert numDistinct values
	for i := range numDistinct {
		f.Insert(hashFunc(i))
	}

	// Test false positive rate with values that were NOT inserted
	falsePositives := 0
	numTests := 100000
	for i := numDistinct; i < numDistinct+numTests; i++ {
		if f.Check(hashFunc(i)) {
			falsePositives++
		}
	}

	actualFPP := float64(falsePositives) / float64(numTests)
	// Allow 3x the expected false positive rate as tolerance
	require.Less(t, actualFPP, fpp*3)
}

func Test_optimalNumBytes(t *testing.T) {
	testCases := map[string]struct {
		numDistinct int
		fpp         float64
		minExpected int
		maxExpected int
	}{
		"small-set":   {100, 0.01, 64, 256},
		"medium-set":  {10000, 0.01, 8192, 32768},
		"low-fpp":     {1000, 0.001, 1024, 4096},
		"high-fpp":    {1000, 0.1, 256, 1024},
		"zero-values": {0, 0.01, 0, 32},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			numBytes := optimalNumBytes(tc.numDistinct, tc.fpp)
			require.GreaterOrEqual(t, numBytes, tc.minExpected)
			require.LessOrEqual(t, numBytes, tc.maxExpected)
		})
	}
}

func TestFromBitset(t *testing.T) {
	t.Run("valid-bitset", func(t *testing.T) {
		original := New(1024)
		original.Insert(42)
		original.Insert(100)

		restored, err := FromBitset(original.Bitset())
		require.NoError(t, err)
		require.True(t, restored.Check(42))
		require.True(t, restored.Check(100))
		require.Equal(t, original.NumBytes(), restored.NumBytes())
	})

	t.Run("empty-bitset", func(t *testing.T) {
		_, err := FromBitset(nil)
		require.Error(t, err)
	})

	t.Run("too-small-bitset", func(t *testing.T) {
		_, err := FromBitset(make([]byte, 16))
		require.Error(t, err)
	})

	t.Run("not-multiple-of-block-size", func(t *testing.T) {
		_, err := FromBitset(make([]byte, 48))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a multiple of block size")
	})

	t.Run("not-power-of-2", func(t *testing.T) {
		// 96 = 3*32: valid multiple of block size but not a power of 2
		_, err := FromBitset(make([]byte, 96))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a power of 2")
	})

	t.Run("not-multiple-of-32", func(t *testing.T) {
		_, err := FromBitset(make([]byte, 33))
		require.Error(t, err)
	})
}

func TestHeader(t *testing.T) {
	f := New(1024)
	h := f.Header()

	require.NotNil(t, h)
	require.Equal(t, int32(1024), h.NumBytes)
	require.NotNil(t, h.Algorithm)
	require.NotNil(t, h.Algorithm.BLOCK)
	require.NotNil(t, h.Hash)
	require.NotNil(t, h.Hash.XXHASH)
	require.NotNil(t, h.Compression)
	require.NotNil(t, h.Compression.UNCOMPRESSED)
}

func TestNextPowerOf2(t *testing.T) {
	testCases := map[string]struct {
		input    uint32
		expected uint32
	}{
		"zero":           {0, 1},
		"one":            {1, 1},
		"two":            {2, 2},
		"three":          {3, 4},
		"four":           {4, 4},
		"five":           {5, 8},
		"thirty-one":     {31, 32},
		"thirty-two":     {32, 32},
		"thirty-three":   {33, 64},
		"large-power":    {1024, 1024},
		"just-over-1024": {1025, 2048},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, nextPowerOf2(tc.input))
		})
	}
}

func TestOptimalNumBytesFormula(t *testing.T) {
	// Verify the formula: m = -n*ln(fpp) / (ln2)^2
	n := 10000
	fpp := 0.01
	expected := int(math.Ceil(-float64(n) * math.Log(fpp) / (math.Ln2 * math.Ln2) / 8.0))
	actual := optimalNumBytes(n, fpp)

	// Should be close (within 2x due to power-of-2 rounding)
	require.InDelta(t, expected, actual, float64(expected))
}
