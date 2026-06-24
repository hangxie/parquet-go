package encoding

import (
	"bytes"
	"testing"
)

// minimal valid delta header: blockSize=128, numMiniblocks=4, numValues=1, firstValue=0
var deltaSeed = []byte{0x80, 0x01, 0x04, 0x01, 0x00}

func FuzzReadDeltaBinaryPackedINT32(f *testing.F) {
	f.Add([]byte{})
	f.Add(deltaSeed)

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadDeltaBinaryPackedINT32(bytes.NewReader(data))
	})
}

func FuzzReadDeltaBinaryPackedINT64(f *testing.F) {
	f.Add([]byte{})
	f.Add(deltaSeed)

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadDeltaBinaryPackedINT64(bytes.NewReader(data))
	})
}

func FuzzReadDeltaLengthByteArray(f *testing.F) {
	f.Add([]byte{})
	f.Add(deltaSeed)

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadDeltaLengthByteArray(bytes.NewReader(data))
	})
}

func FuzzReadDeltaByteArray(f *testing.F) {
	f.Add([]byte{})
	// two concatenated delta blocks: prefix lengths then suffixes
	f.Add(append(append([]byte{}, deltaSeed...), deltaSeed...))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadDeltaByteArray(bytes.NewReader(data))
	})
}
