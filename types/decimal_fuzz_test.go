package types

import "testing"

func FuzzDECIMAL_BYTE_ARRAY_ToString(f *testing.F) {
	f.Add([]byte{}, 0, 0)
	f.Add([]byte{0x00}, 10, 0)
	f.Add([]byte{0x01}, 10, 2)
	f.Add([]byte{0x80}, 10, 0) // negative (MSB set)
	f.Add([]byte{0x7f, 0xff}, 10, 2)

	f.Fuzz(func(t *testing.T, dec []byte, precision, scale int) {
		if scale < 0 || scale > 20 {
			return
		}
		DECIMAL_BYTE_ARRAY_ToString(dec, precision, scale)
	})
}
