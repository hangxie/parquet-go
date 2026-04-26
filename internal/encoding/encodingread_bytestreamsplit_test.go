package encoding

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadByteStreamSplitINT32_RoundTrip(t *testing.T) {
	data := []any{int32(0), int32(1), int32(-1), int32(math.MaxInt32), int32(math.MinInt32)}
	encoded := WriteByteStreamSplit(data)
	res, err := ReadByteStreamSplitINT32(bytes.NewReader(encoded), uint64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, res)
}

func TestReadByteStreamSplitINT64_RoundTrip(t *testing.T) {
	data := []any{int64(0), int64(1), int64(-1), int64(math.MaxInt64), int64(math.MinInt64)}
	encoded := WriteByteStreamSplit(data)
	res, err := ReadByteStreamSplitINT64(bytes.NewReader(encoded), uint64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, res)
}

func TestReadByteStreamSplitINT32_TruncatedData(t *testing.T) {
	// Request 4 int32 values but only provide 3 bytes
	_, err := ReadByteStreamSplitINT32(bytes.NewReader([]byte{0x01, 0x02, 0x03}), 4)
	require.Error(t, err)
}

func TestReadByteStreamSplitINT64_TruncatedData(t *testing.T) {
	// Request 2 int64 values but only provide 4 bytes
	_, err := ReadByteStreamSplitINT64(bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}), 2)
	require.Error(t, err)
}

func TestReadByteStreamSplitINT32_ZeroCount(t *testing.T) {
	res, err := ReadByteStreamSplitINT32(bytes.NewReader(nil), 0)
	require.NoError(t, err)
	require.Empty(t, res)
}

func TestReadByteStreamSplitINT64_ZeroCount(t *testing.T) {
	res, err := ReadByteStreamSplitINT64(bytes.NewReader(nil), 0)
	require.NoError(t, err)
	require.Empty(t, res)
}

func TestReadByteStreamSplitFixedLenByteArray_RoundTrip(t *testing.T) {
	// 3 elements of 4 bytes each
	data := []any{string([]byte{0x01, 0x02, 0x03, 0x04}), string([]byte{0x05, 0x06, 0x07, 0x08}), string([]byte{0x09, 0x0a, 0x0b, 0x0c})}
	encoded := WriteByteStreamSplit(data)
	res, err := ReadByteStreamSplitFixedLenByteArray(bytes.NewReader(encoded), 3, 4)
	require.NoError(t, err)
	require.Equal(t, data, res)
}
