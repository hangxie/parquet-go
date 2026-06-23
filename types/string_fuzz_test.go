package types

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func FuzzStrToParquetType(f *testing.F) {
	// (s, pTVal, useCT, cTVal, length, scale)
	f.Add("true", int32(parquet.Type_BOOLEAN), false, int32(0), 0, 0)
	f.Add("42", int32(parquet.Type_INT32), false, int32(0), 0, 0)
	f.Add("-1", int32(parquet.Type_INT64), false, int32(0), 0, 0)
	f.Add("hello", int32(parquet.Type_BYTE_ARRAY), true, int32(parquet.ConvertedType_UTF8), 0, 0)
	f.Add("1.5", int32(parquet.Type_DOUBLE), true, int32(parquet.ConvertedType_DECIMAL), 0, 2)
	f.Add("2023-01-01", int32(parquet.Type_INT32), true, int32(parquet.ConvertedType_DATE), 0, 0)
	f.Add("1 mon 2 day", int32(parquet.Type_FIXED_LEN_BYTE_ARRAY), true, int32(parquet.ConvertedType_INTERVAL), 12, 0)
	f.Add("", int32(parquet.Type_INT32), false, int32(0), 0, 0)

	f.Fuzz(func(t *testing.T, s string, pTVal int32, useCT bool, cTVal int32, length, scale int) {
		if length < 0 || length > 64 {
			return
		}
		if scale < 0 || scale > 20 {
			return
		}
		pT := parquet.Type(pTVal)
		var cT *parquet.ConvertedType
		if useCT {
			ct := parquet.ConvertedType(cTVal)
			cT = &ct
		}
		_, _ = StrToParquetType(s, &pT, cT, length, scale)
	})
}
