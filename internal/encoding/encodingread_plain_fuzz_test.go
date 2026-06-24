package encoding

import (
	"bytes"
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func FuzzReadPlain(f *testing.F) {
	f.Add([]byte{0x01, 0x00, 0x00, 0x00}, int32(parquet.Type_INT32), uint(1), uint(0))
	f.Add([]byte{0x01}, int32(parquet.Type_BOOLEAN), uint(1), uint(0))
	f.Add([]byte{0x04, 0x00, 0x00, 0x00, 'a', 'b', 'c', 'd'}, int32(parquet.Type_BYTE_ARRAY), uint(1), uint(0))
	f.Add([]byte{'a', 'b', 'c'}, int32(parquet.Type_FIXED_LEN_BYTE_ARRAY), uint(1), uint(3))

	f.Fuzz(func(t *testing.T, data []byte, typ int32, cnt, bitWidth uint) {
		// counts come from page headers in practice; bound them so the fuzzer
		// explores decoding logic rather than just large allocations.
		_, _ = ReadPlain(bytes.NewReader(data), parquet.Type(typ), uint64(cnt%4096), uint64(bitWidth%256))
	})
}
