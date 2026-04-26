package encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadDeltaBinaryPackedINT32_ErrorPaths(t *testing.T) {
	t.Run("empty_reader_blocksize", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(nil))
		require.Error(t, err)
	})

	t.Run("truncated_after_blocksize", func(t *testing.T) {
		// valid blockSize varint (128), then EOF
		_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader([]byte{0x80, 0x01}))
		require.Error(t, err)
	})

	t.Run("truncated_after_numminiblocks", func(t *testing.T) {
		// blockSize=128, numMiniblocks=4, then EOF
		_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader([]byte{0x80, 0x01, 0x04}))
		require.Error(t, err)
	})

	t.Run("truncated_after_numvalues", func(t *testing.T) {
		// blockSize=128, numMiniblocks=4, numValues=2, then EOF
		_, err := ReadDeltaBinaryPackedINT32(bytes.NewReader([]byte{0x80, 0x01, 0x04, 0x02}))
		require.Error(t, err)
	})
}

func TestReadDeltaBinaryPackedINT64_ErrorPaths(t *testing.T) {
	t.Run("empty_reader", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(nil))
		require.Error(t, err)
	})

	t.Run("truncated_after_blocksize", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader([]byte{0x80, 0x01}))
		require.Error(t, err)
	})

	t.Run("truncated_after_numminiblocks", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader([]byte{0x80, 0x01, 0x04}))
		require.Error(t, err)
	})

	t.Run("truncated_after_numvalues", func(t *testing.T) {
		_, err := ReadDeltaBinaryPackedINT64(bytes.NewReader([]byte{0x80, 0x01, 0x04, 0x02}))
		require.Error(t, err)
	})
}

func TestReadDeltaBinaryPackedINT32_SingleValue(t *testing.T) {
	data := []any{int32(42)}
	encoded := WriteDeltaINT32(data)
	res, err := ReadDeltaBinaryPackedINT32(bytes.NewReader(encoded))
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, int32(42), res[0])
}

func TestReadDeltaBinaryPackedINT64_SingleValue(t *testing.T) {
	data := []any{int64(9999999999)}
	encoded := WriteDeltaINT64(data)
	res, err := ReadDeltaBinaryPackedINT64(bytes.NewReader(encoded))
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, int64(9999999999), res[0])
}

func TestReadDeltaLengthByteArray_ZeroLength(t *testing.T) {
	// A single empty string: delta-packed length=0, then 0 bytes of data
	data := []any{""}
	encoded := WriteDeltaLengthByteArray(data)
	res, err := ReadDeltaLengthByteArray(bytes.NewReader(encoded))
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, "", res[0])
}

func TestReadDeltaByteArray_SingleEntry(t *testing.T) {
	data := []any{"hello"}
	encoded := WriteDeltaByteArray(data)
	res, err := ReadDeltaByteArray(bytes.NewReader(encoded))
	require.NoError(t, err)
	require.Equal(t, []any{"hello"}, res)
}

func TestReadDeltaByteArray_PrefixSharing(t *testing.T) {
	data := []any{"foo", "foobar", "foobaz"}
	encoded := WriteDeltaByteArray(data)
	res, err := ReadDeltaByteArray(bytes.NewReader(encoded))
	require.NoError(t, err)
	require.Equal(t, data, res)
}

func TestReadDeltaByteArray_TruncatedSuffixes(t *testing.T) {
	// Write valid prefix lengths but truncate suffix data
	prefixData := []any{int64(0), int64(0), int64(0)}
	prefixEncoded := WriteDeltaINT64(prefixData)
	// truncated suffix section (just the prefix count, no suffix data)
	partial := append(prefixEncoded, 0x01) // single broken varint for suffix count
	_, err := ReadDeltaByteArray(bytes.NewReader(partial))
	require.Error(t, err)
}
