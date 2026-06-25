package encoding

import (
	"bytes"
	"testing"
)

func FuzzReadRLEBitPackedHybrid(f *testing.F) {
	f.Add([]byte{}, uint(1))
	// RLE run: header=8 (4 repeats), value byte 0x00
	f.Add([]byte{0x08, 0x00}, uint(1))
	// bit-packed run: header=3 (1 group of 8), 1 byte of data
	f.Add([]byte{0x03, 0xFF}, uint(1))

	f.Fuzz(func(t *testing.T, data []byte, bitWidth uint) {
		// bitWidth is bounded to a realistic range; length tracks the input
		// size so the decoder consumes only what the fuzzer provided, and
		// maxCount is bounded by the input so a crafted RLE run cannot amplify
		// a few bytes into a huge allocation.
		_, _ = ReadRLEBitPackedHybrid(bytes.NewReader(data), uint64(bitWidth%65), uint64(len(data)), uint64(len(data))*8+8)
	})
}

func FuzzReadBitPackedCount(f *testing.F) {
	f.Add([]byte{}, uint(0), uint(0))
	f.Add([]byte{0xFF}, uint(8), uint(1))
	f.Add([]byte{0xFF, 0xFF}, uint(8), uint(2))

	f.Fuzz(func(t *testing.T, data []byte, cnt, bitWidth uint) {
		_, _ = ReadBitPackedCount(bytes.NewReader(data), uint64(cnt%4096), uint64(bitWidth%65))
	})
}
