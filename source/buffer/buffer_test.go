package buffer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferFile_Bytes_Empty(t *testing.T) {
	bf := &bufferFile{}
	require.Empty(t, bf.Bytes())
}

func TestBufferFile_Bytes_WithData(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	bf := &bufferFile{buff: data}
	require.Equal(t, data, bf.Bytes())
}

func TestBufferFile_Bytes_DoesNotCopy(t *testing.T) {
	data := []byte{0xAA, 0xBB}
	bf := &bufferFile{buff: data}
	result := bf.Bytes()
	// Mutating the original slice is reflected in Bytes() (no copy)
	data[0] = 0xFF
	require.Equal(t, byte(0xFF), result[0])
}
