package layout

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestReadDictionaryValues(t *testing.T) {
	for _, tt := range []struct {
		typ   parquet.Type
		value any
		width uint64
	}{
		{parquet.Type_BOOLEAN, true, 0},
		{parquet.Type_INT32, int32(7), 0},
		{parquet.Type_INT64, int64(7), 0},
		{parquet.Type_FLOAT, float32(7), 0},
		{parquet.Type_DOUBLE, float64(7), 0},
		{parquet.Type_INT96, "123456789012", 0},
		{parquet.Type_BYTE_ARRAY, "value", 0},
		{parquet.Type_FIXED_LEN_BYTE_ARRAY, "abcd", 4},
	} {
		t.Run(tt.typ.String(), func(t *testing.T) {
			data, err := encoding.WritePlain([]any{tt.value}, tt.typ)
			require.NoError(t, err)
			values, err := readDictionaryValues(bytes.NewReader(data), tt.typ, 1, tt.width)
			require.NoError(t, err)
			require.Equal(t, []any{tt.value}, values)
			_, err = readDictionaryValues(bytes.NewReader(data), tt.typ, 1<<31-1, tt.width)
			require.Error(t, err)
		})
	}
	for _, tt := range []struct {
		typ   parquet.Type
		count int32
		width uint64
	}{
		{parquet.Type_INT32, -1, 0},
		{parquet.Type_FIXED_LEN_BYTE_ARRAY, 1, 0},
		{parquet.Type_FIXED_LEN_BYTE_ARRAY, 2, 1 << 63},
		{parquet.Type(-1), 1, 0},
	} {
		_, err := readDictionaryValues(bytes.NewReader(nil), tt.typ, tt.count, tt.width)
		require.Error(t, err)
	}
	values, err := readDictionaryValues(bytes.NewReader(nil), parquet.Type_FIXED_LEN_BYTE_ARRAY, 0, 0)
	require.NoError(t, err)
	require.Empty(t, values)
}
