package layout

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestNumericDecoderBits(t *testing.T) {
	for _, tt := range []struct {
		typ    parquet.Type
		values []any
	}{
		{parquet.Type_INT32, []any{int32(0), int32(-1), int32(math.MinInt32), int32(math.MaxInt32)}},
		{parquet.Type_INT64, []any{int64(0), int64(-1), int64(math.MinInt64), int64(math.MaxInt64)}},
		{parquet.Type_FLOAT, []any{float32(0), float32(math.Copysign(0, -1)), float32(math.Inf(1)), float32(math.Inf(-1)), math.Float32frombits(0x7fc12345), float32(math.SmallestNonzeroFloat32)}},
		{parquet.Type_DOUBLE, []any{float64(0), math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8123456789abc), math.SmallestNonzeroFloat64}},
	} {
		for _, method := range []parquet.Encoding{parquet.Encoding_PLAIN, parquet.Encoding_BYTE_STREAM_SPLIT} {
			t.Run(tt.typ.String()+"/"+method.String(), func(t *testing.T) {
				page := NewDataPage()
				page.Schema = &parquet.SchemaElement{Type: common.ToPtr(tt.typ)}
				page.Info.Encoding = method
				data, err := page.EncodingValues(tt.values)
				require.NoError(t, err)
				d, err := newValueDecoder(bytes.NewReader(data), method, tt.typ, uint64(len(tt.values)), 0)
				require.NoError(t, err)
				for _, want := range tt.values {
					got, err := d.next()
					require.NoError(t, err)
					switch tt.typ {
					case parquet.Type_FLOAT:
						require.Equal(t, math.Float32bits(want.(float32)), math.Float32bits(got.(float32)))
					case parquet.Type_DOUBLE:
						require.Equal(t, math.Float64bits(want.(float64)), math.Float64bits(got.(float64)))
					default:
						require.Equal(t, want, got)
					}
				}
			})
		}
	}
}

var benchmarkNumericValue any

func BenchmarkValueDecoderNumeric(b *testing.B) {
	for _, typ := range []parquet.Type{parquet.Type_INT32, parquet.Type_INT64, parquet.Type_FLOAT, parquet.Type_DOUBLE} {
		for _, method := range []parquet.Encoding{parquet.Encoding_PLAIN, parquet.Encoding_BYTE_STREAM_SPLIT} {
			b.Run(typ.String()+"/"+method.String(), func(b *testing.B) {
				values := make([]any, 1024)
				for i := range values {
					values[i] = numericValue(typ, uint64(i+1000))
				}

				page := NewDataPage()
				page.Schema = &parquet.SchemaElement{Type: common.ToPtr(typ)}
				page.Info.Encoding = method
				data, err := page.EncodingValues(values)
				require.NoError(b, err)
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				b.ResetTimer()
				for range b.N {
					d, err := newValueDecoder(bytes.NewReader(data), method, typ, uint64(len(values)), 0)
					if err != nil {
						b.Fatal(err)
					}
					for range values {
						benchmarkNumericValue, err = d.next()
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
