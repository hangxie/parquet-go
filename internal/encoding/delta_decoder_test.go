package encoding

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeltaDecoderRoundTrip(t *testing.T) {
	for _, int32Values := range []bool{false, true} {
		for _, size := range []int{1, 33, 128, 129, 513, 2051} {
			t.Run(fmt.Sprintf("int32=%t/size=%d", int32Values, size), func(t *testing.T) {
				values := make([]any, size)
				for i := range values {
					v := int64(i*i%79) - int64(i*3)
					if int32Values {
						values[i] = int32(v)
					} else {
						values[i] = v
					}
				}
				var data []byte
				if int32Values {
					data = WriteDeltaINT32(values)
				} else {
					data = WriteDeltaINT64(values)
				}
				reader := bytes.NewReader(append(data, 0x42))
				d, err := NewDeltaDecoder(reader, uint64(size), int32Values)
				require.NoError(t, err)
				end, err := d.End()
				require.NoError(t, err)
				require.Equal(t, int64(len(data)), end)
				skip := size / 3
				require.NoError(t, d.Skip(uint64(skip)))
				for _, want := range values[skip:] {
					got, err := d.Next()
					require.NoError(t, err)
					if int32Values {
						require.Equal(t, int64(want.(int32)), got)
					} else {
						require.Equal(t, want, got)
					}
				}
				_, err = d.Next()
				require.ErrorIs(t, err, io.EOF)
				require.ErrorIs(t, d.Skip(1), io.ErrUnexpectedEOF)
			})
		}
	}
}

func TestDeltaDecoderHugeZeroWidthMiniblock(t *testing.T) {
	// One block and one miniblock representing over a billion identical values.
	const count = uint64(1 << 30)
	var data []byte
	for _, v := range []uint64{count, 1, count, 14, 0} {
		data = append(data, WriteUnsignedVarInt(v)...)
	}
	data = append(data, 0)
	d, err := NewDeltaDecoder(bytes.NewReader(data), count, false)
	require.NoError(t, err)
	end, err := d.End()
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), end)
	require.NoError(t, d.Skip(count-1))
	v, err := d.Next()
	require.NoError(t, err)
	require.Equal(t, int64(7), v)
}

func TestDeltaDecoderMalformed(t *testing.T) {
	valid := WriteDeltaINT64([]any{int64(1), int64(2), int64(5), int64(8)})
	for i := 0; i < len(valid); i++ {
		d, err := NewDeltaDecoder(bytes.NewReader(valid[:i]), 4, false)
		if err != nil {
			continue
		}
		_, err = d.End()
		require.Error(t, err)
		d, err = NewDeltaDecoder(bytes.NewReader(valid[:i]), 4, false)
		require.NoError(t, err)
		require.Error(t, d.Skip(4))
	}
}

func TestDeltaDecoderEmptyAndInvalidWidth(t *testing.T) {
	d, err := NewDeltaDecoder(bytes.NewReader([]byte{0x80, 1, 1, 0, 0}), 0, false)
	require.NoError(t, err)
	_, err = d.Next()
	require.ErrorIs(t, err, io.EOF)
	for _, width := range []byte{65, 255} {
		// 128-value block, one miniblock, two values, first value 0, minimum delta 0.
		data := []byte{0x80, 1, 1, 2, 0, 0, width}
		d, err := NewDeltaDecoder(bytes.NewReader(data), 2, false)
		require.NoError(t, err)
		_, err = d.End()
		require.Error(t, err)
		_, err = d.Next()
		require.NoError(t, err)
		_, err = d.Next()
		require.Error(t, err)
	}
	data := WriteDeltaINT32([]any{int32(4), int32(6), int32(8), int32(10)})
	d, err = NewDeltaDecoder(bytes.NewReader(data), 4, true)
	require.NoError(t, err)
	require.NoError(t, d.Skip(3))
	v, err := d.Next()
	require.NoError(t, err)
	require.Equal(t, int64(10), v)
	for i := 0; i < len(data); i++ {
		d, err := NewDeltaDecoder(bytes.NewReader(data[:i]), 4, true)
		if err != nil {
			continue
		}
		for range 4 {
			_, err = d.Next()
			if err != nil {
				break
			}
		}
		require.Error(t, err)
	}
}
