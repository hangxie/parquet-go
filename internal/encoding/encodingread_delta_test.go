package encoding

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadDeltaBinaryPackedINT32_ErrorPaths(t *testing.T) {
	t.Run("empty_reader_blocksize", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(nil), maxAllowedCount)
		require.Error(t, err)
	})

	t.Run("truncated_after_blocksize", func(t *testing.T) {
		// valid blockSize varint (128), then EOF
		_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader([]byte{0x80, 0x01}), maxAllowedCount)
		require.Error(t, err)
	})

	t.Run("truncated_after_numminiblocks", func(t *testing.T) {
		// blockSize=128, numMiniblocks=4, then EOF
		_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader([]byte{0x80, 0x01, 0x04}), maxAllowedCount)
		require.Error(t, err)
	})

	t.Run("truncated_after_numvalues", func(t *testing.T) {
		// blockSize=128, numMiniblocks=4, numValues=2, then EOF
		_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader([]byte{0x80, 0x01, 0x04, 0x02}), maxAllowedCount)
		require.Error(t, err)
	})
}

func TestReadDeltaBinaryPackedINT64_ErrorPaths(t *testing.T) {
	t.Run("empty_reader", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(nil), maxAllowedCount)
		require.Error(t, err)
	})

	t.Run("truncated_after_blocksize", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader([]byte{0x80, 0x01}), maxAllowedCount)
		require.Error(t, err)
	})

	t.Run("truncated_after_numminiblocks", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader([]byte{0x80, 0x01, 0x04}), maxAllowedCount)
		require.Error(t, err)
	})

	t.Run("truncated_after_numvalues", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader([]byte{0x80, 0x01, 0x04, 0x02}), maxAllowedCount)
		require.Error(t, err)
	})
}

func TestReadDeltaBinaryPackedINT32_SingleValue(t *testing.T) {
	data := []any{int32(42)}
	encoded := WriteDeltaINT32(data)
	res, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(encoded), maxAllowedCount)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, int32(42), res[0])
}

func TestReadDeltaBinaryPackedINT64_SingleValue(t *testing.T) {
	data := []any{int64(9999999999)}
	encoded := WriteDeltaINT64(data)
	res, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(encoded), maxAllowedCount)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, int64(9999999999), res[0])
}

func TestReadDeltaLengthByteArray_ZeroLength(t *testing.T) {
	// A single empty string: delta-packed length=0, then 0 bytes of data
	data := []any{""}
	encoded := WriteDeltaLengthByteArray(data)
	res, err := ReadDeltaLengthByteArray(bytes.NewReader(encoded), maxAllowedCount)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, "", res[0])
}

func TestReadDeltaByteArray_SingleEntry(t *testing.T) {
	data := []any{"hello"}
	encoded := WriteDeltaByteArray(data)
	res, err := ReadDeltaByteArray(bytes.NewReader(encoded), maxAllowedCount)
	require.NoError(t, err)
	require.Equal(t, []any{"hello"}, res)
}

func TestReadDeltaByteArray_PrefixSharing(t *testing.T) {
	data := []any{"foo", "foobar", "foobaz"}
	encoded := WriteDeltaByteArray(data)
	res, err := ReadDeltaByteArray(bytes.NewReader(encoded), maxAllowedCount)
	require.NoError(t, err)
	require.Equal(t, data, res)
}

func TestReadDeltaByteArray_TruncatedSuffixes(t *testing.T) {
	// Write valid prefix lengths but truncate suffix data
	prefixData := []any{int64(0), int64(0), int64(0)}
	prefixEncoded := WriteDeltaINT64(prefixData)
	// truncated suffix section (just the prefix count, no suffix data)
	partial := append(prefixEncoded, 0x01) // single broken varint for suffix count
	_, err := ReadDeltaByteArray(bytes.NewReader(partial), maxAllowedCount)
	require.Error(t, err)
}

// TestReadDeltaBinaryPackedINT32_ZeroMiniblocks guards the divide-by-zero
// fuzzing found when numMiniblocksInBlock is 0 (blockSize / numMiniblocks).
func TestReadDeltaBinaryPackedINT32_ZeroMiniblocks(t *testing.T) {
	// blockSize=128, numMiniblocks=0, numValues=1, firstValue=0
	input := []byte{0x80, 0x01, 0x00, 0x01, 0x00}
	_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(input), maxAllowedCount)
	require.ErrorContains(t, err, "numMiniblocksInBlock is zero")
}

func TestReadDeltaBinaryPackedINT64_ZeroMiniblocks(t *testing.T) {
	input := []byte{0x80, 0x01, 0x00, 0x01, 0x00}
	_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(input), maxAllowedCount)
	require.ErrorContains(t, err, "numMiniblocksInBlock is zero")
}

func TestReadDeltaBinaryPackedINT32_TruncatedAfterFirstValue(t *testing.T) {
	// blockSize=128, numMiniblocks=4, numValues=4, firstValue=0, then EOF before minDelta.
	_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader([]byte{0x80, 0x01, 0x04, 0x04, 0x00}), maxAllowedCount)
	require.Error(t, err)
}

func TestReadDeltaBinaryPackedINT32_TruncatedBeforeBitWidths(t *testing.T) {
	// header + firstValue + minDelta(0), then EOF before bit widths.
	_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader([]byte{0x80, 0x01, 0x04, 0x04, 0x00, 0x00}), maxAllowedCount)
	require.Error(t, err)
}

func TestReadDeltaByteArray_InvalidPrefixLength(t *testing.T) {
	// prefix length 100 exceeds the previous value's length, which is invalid.
	prefixEncoded := WriteDeltaINT64([]any{int64(0), int64(100)})
	suffixEncoded := WriteDeltaLengthByteArray([]any{"ab", "cd"})
	_, err := ReadDeltaByteArray(bytes.NewReader(append(prefixEncoded, suffixEncoded...)), maxAllowedCount)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid prefix length")
}

// TestReadDeltaByteArray_PrefixSuffixMismatch guards the index-out-of-range
// fuzzing found when the decoded prefix and suffix counts disagree.
func TestReadDeltaByteArray_PrefixSuffixMismatch(t *testing.T) {
	// Regression input discovered by FuzzReadDeltaByteArray; previously panicked.
	input := []byte("00\x01000\x000")
	_, err := ReadDeltaByteArray(bytes.NewReader(input), maxAllowedCount)
	require.Error(t, err)
}

// deltaStream builds a delta header (firstValue zigzag-encoded) followed by raw block bytes.
func deltaStream(blockSize, numMiniblocks, numValues, firstValue uint64, blocks ...byte) []byte {
	var b []byte
	for _, v := range []uint64{blockSize, numMiniblocks, numValues, firstValue} {
		b = append(b, WriteUnsignedVarInt(v)...)
	}
	return append(b, blocks...)
}

func TestReadDeltaBinaryPacked_AllocationBounds(t *testing.T) {
	readers := map[string]func(*bytes.Reader, uint64) ([]any, error){
		"int32": ReadDeltaBinaryPackedINT32,
		"int64": ReadDeltaBinaryPackedINT64,
	}
	testCases := map[string]struct {
		input     []byte
		maxValues uint64
		want      []int64
		errMsg    string
	}{
		// Regression input found by FuzzReadDeltaBinaryPackedINT32; it used to allocate ~26 GB.
		"value-count-above-bound":   {deltaStream(1<<28, 1, 1<<31-1, 0, 0, 0), 100, nil, "exceeds expected"},
		"huge-zero-width-miniblock": {deltaStream(1<<30, 1, 3, 0, 0, 0), 3, []int64{0, 0, 0}, ""},
		"exact-bound":               {deltaStream(128, 1, 5, 0, 0, 0), 5, []int64{0, 0, 0, 0, 0}, ""},
		"zero-values":               {deltaStream(128, 1, 0, 0), 5, []int64{}, ""},
		// first=10, minDelta=0, width 1, deltas 0 and 1, padded to the full 256-value miniblock.
		"partial-padded-miniblock": {deltaStream(256, 1, 3, 20, append([]byte{0, 1, 0b110}, make([]byte, 31)...)...), 3, []int64{10, 10, 11}, ""},
		// The spec requires the last used miniblock to be fully padded.
		"unpadded-wide-miniblock": {deltaStream(1<<30, 1, 3, 20, 0, 1, 0b110), 3, nil, "remain"},
		"block-size-zero":         {deltaStream(0, 1, 2, 0, 0, 0), 2, nil, "block size 0"},
		"block-size-not-128x":     {deltaStream(100, 1, 2, 0, 0, 0), 2, nil, "block size 100"},
		"tiny-miniblocks":         {deltaStream(8, 4, 2, 0, 0, 0, 0, 0, 0), 2, nil, "block size 8"},
		"miniblock-not-32x":       {deltaStream(128, 8, 2, 0), 2, nil, "values per miniblock"},
		"huge-miniblock-count":    {deltaStream(1<<30, 1<<25, 2, 0, 0), 2, nil, "miniblock count"},
		"bit-width-too-wide":      {deltaStream(128, 1, 2, 0, 0, 65), 2, nil, "bit width 65"},
		"truncated-packed-data":   {deltaStream(128, 1, 3, 0, 0, 8), 3, nil, "remain"},
	}
	for name, read := range readers {
		for tcName, tc := range testCases {
			t.Run(name+"/"+tcName, func(t *testing.T) {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				res, err := read(bytes.NewReader(tc.input), tc.maxValues)
				runtime.ReadMemStats(&after)
				require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
				if tc.errMsg != "" {
					require.ErrorContains(t, err, tc.errMsg)
					return
				}
				require.NoError(t, err)
				got := make([]int64, len(res))
				for i, v := range res {
					switch v := v.(type) {
					case int32:
						got[i] = int64(v)
					case int64:
						got[i] = v
					}
				}
				require.Equal(t, tc.want, got)
			})
		}
	}
}

func TestReadDeltaBinaryPacked_MultiBlockRoundTrip(t *testing.T) {
	// Counts straddle block boundaries so partial last blocks exercise padding skips.
	for _, n := range []int{33, 128, 129, 300} {
		in32 := make([]any, n)
		in64 := make([]any, n)
		for i := range n {
			v := int64(i * i * (i%7 - 3))
			in32[i], in64[i] = int32(v), v<<20
		}
		res32, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(WriteDeltaINT32(in32)), uint64(n))
		require.NoError(t, err)
		require.Equal(t, in32, res32)
		res64, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(WriteDeltaINT64(in64)), uint64(n))
		require.NoError(t, err)
		require.Equal(t, in64, res64)
	}
}

func TestReadDeltaByteArray_PaddedPrefixThenSuffix(t *testing.T) {
	// Prefix lengths 0, 1, 1: width 1, 32-value miniblock padded to 4 bytes.
	prefix := deltaStream(128, 4, 3, 0, 0, 1, 0, 0, 0, 0b001, 0, 0, 0)
	suffix := WriteDeltaLengthByteArray([]any{"a", "b", "c"})
	res, err := ReadDeltaByteArray(bytes.NewReader(append(prefix, suffix...)), 3)
	require.NoError(t, err)
	require.Equal(t, []any{"a", "ab", "ac"}, res)
}

func TestReadDeltaByteArray_ValueCountAboveBound(t *testing.T) {
	encoded := WriteDeltaLengthByteArray([]any{"a", "b", "c"})
	_, err := ReadDeltaLengthByteArray(bytes.NewReader(encoded), 2)
	require.ErrorContains(t, err, "exceeds expected")

	encoded = WriteDeltaByteArray([]any{"a", "ab", "abc"})
	_, err = ReadDeltaByteArray(bytes.NewReader(encoded), 2)
	require.ErrorContains(t, err, "exceeds expected")
}
