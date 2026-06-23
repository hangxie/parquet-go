package types

import "testing"

func FuzzStrIntToBinary(f *testing.F) {
	f.Add("0", false, 0, false)
	f.Add("1", false, 4, false)
	f.Add("-1", false, 4, true)
	f.Add("127", false, 1, true)
	f.Add("-128", false, 1, true)
	f.Add("255", true, 1, false)
	f.Add("", false, 0, false)

	f.Fuzz(func(t *testing.T, num string, littleEndian bool, length int, signed bool) {
		if length < 0 || length > 64 {
			return
		}
		order := "BigEndian"
		if littleEndian {
			order = "LittleEndian"
		}
		StrIntToBinary(num, order, length, signed)
	})
}
