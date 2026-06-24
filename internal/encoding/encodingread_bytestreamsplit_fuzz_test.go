package encoding

import (
	"bytes"
	"testing"
)

func FuzzReadByteStreamSplitFloat32(f *testing.F) {
	f.Add([]byte{}, uint(0))
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}, uint(1))

	f.Fuzz(func(t *testing.T, data []byte, cnt uint) {
		_, _ = ReadByteStreamSplitFloat32(bytes.NewReader(data), uint64(cnt%4096))
	})
}

func FuzzReadByteStreamSplitFloat64(f *testing.F) {
	f.Add([]byte{}, uint(0))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, uint(1))

	f.Fuzz(func(t *testing.T, data []byte, cnt uint) {
		_, _ = ReadByteStreamSplitFloat64(bytes.NewReader(data), uint64(cnt%4096))
	})
}

func FuzzReadByteStreamSplitINT32(f *testing.F) {
	f.Add([]byte{}, uint(0))
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}, uint(1))

	f.Fuzz(func(t *testing.T, data []byte, cnt uint) {
		_, _ = ReadByteStreamSplitINT32(bytes.NewReader(data), uint64(cnt%4096))
	})
}

func FuzzReadByteStreamSplitINT64(f *testing.F) {
	f.Add([]byte{}, uint(0))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, uint(1))

	f.Fuzz(func(t *testing.T, data []byte, cnt uint) {
		_, _ = ReadByteStreamSplitINT64(bytes.NewReader(data), uint64(cnt%4096))
	})
}

func FuzzReadByteStreamSplitFixedLenByteArray(f *testing.F) {
	f.Add([]byte{}, uint(0), uint(1))
	f.Add([]byte{'a', 'b', 'c'}, uint(1), uint(3))

	f.Fuzz(func(t *testing.T, data []byte, cnt, elemSize uint) {
		_, _ = ReadByteStreamSplitFixedLenByteArray(bytes.NewReader(data), uint64(cnt%4096), uint64(elemSize%256))
	})
}
