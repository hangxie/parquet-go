package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntDecoder(t *testing.T) {
	for _, width := range []uint64{0, 1, 8, 32, 64} {
		values := []any{int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0)}
		if width > 0 {
			values[1], values[5] = int64(1), int64(1)
		}
		data := WriteBitPacked(values, int64(width), true)
		d, err := NewIntDecoder(bytes.NewReader(data), width, uint64(len(values)), true, false)
		require.NoError(t, err)
		count, err := d.Count(0)
		require.NoError(t, err)
		want := uint64(8)
		if width > 0 {
			want = 6
		}
		require.Equal(t, want, count)
		require.NoError(t, d.Skip(1))
		for _, want := range values[1:] {
			got, err := d.Next()
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
		_, err = d.Next()
		require.ErrorIs(t, err, io.EOF)
		require.ErrorIs(t, d.Skip(1), io.ErrUnexpectedEOF)
	}
}

func TestIntDecoderHugeRuns(t *testing.T) {
	for _, packed := range []bool{false, true} {
		header := uint64(1<<31-1) << 1
		if packed {
			header = uint64(1<<60)<<1 | 1
		}
		d, err := NewIntDecoder(bytes.NewReader(WriteUnsignedVarInt(header)), 0, 1<<31-1, true, false)
		require.NoError(t, err)
		count, err := d.Count(0)
		require.NoError(t, err)
		require.Equal(t, uint64(1<<31-1), count)
		require.NoError(t, d.Skip(1<<31-2))
		value, err := d.Next()
		require.NoError(t, err)
		require.Zero(t, value)
	}
	d := NewConstantDecoder(7, 1<<31-1)
	n, err := d.Count(7)
	require.NoError(t, err)
	require.Equal(t, uint64(1<<31-1), n)
	require.NoError(t, d.Skip(1<<31-1))
}

func TestIntDecoderErrors(t *testing.T) {
	tests := []struct {
		name           string
		data           []byte
		width, count   uint64
		hybrid, prefix bool
	}{
		{name: "width", width: 65, count: 1},
		{name: "short prefix", data: []byte{1}, width: 1, count: 1, hybrid: true, prefix: true},
		{name: "long section", data: []byte{10, 0, 0, 0, 2}, width: 1, count: 1, hybrid: true, prefix: true},
		{name: "short packed", width: 1, count: 8},
		{name: "missing packed padding", data: append([]byte{5}, make([]byte, 9)...), width: 8, count: 9, hybrid: true},
		{name: "packed size overflow", data: WriteUnsignedVarInt(uint64(1<<60)<<1 | 1), width: 64, count: 1, hybrid: true},
		{name: "empty run", data: []byte{0}, count: 1, hybrid: true},
		{name: "short repeated", data: []byte{2}, width: 8, count: 1, hybrid: true},
		{name: "short header", data: []byte{0x80}, width: 8, count: 1, hybrid: true},
		{name: "overflow header", data: bytes.Repeat([]byte{0xff}, 12), width: 8, count: 1, hybrid: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := NewIntDecoder(bytes.NewReader(tt.data), tt.width, tt.count, tt.hybrid, tt.prefix)
			if err == nil {
				_, err = d.Next()
				require.Error(t, err)
			} else {
				require.Error(t, err)
			}
			d, err = NewIntDecoder(bytes.NewReader(tt.data), tt.width, tt.count, tt.hybrid, tt.prefix)
			if err == nil {
				_, err = d.Count(0)
				require.Error(t, err)
			}
			d, err = NewIntDecoder(bytes.NewReader(tt.data), tt.width, tt.count, tt.hybrid, tt.prefix)
			if err == nil {
				require.Error(t, d.Skip(1))
			}
		})
	}
}

func TestIntDecoderPrefixedAndMixedRuns(t *testing.T) {
	// Three repeated ones, then eight bit-packed zeros.
	data := []byte{6, 1, 3, 0}
	var prefix [4]byte
	binary.LittleEndian.PutUint32(prefix[:], uint32(len(data)))
	d, err := NewIntDecoder(bytes.NewReader(append(prefix[:], data...)), 1, 11, true, true)
	require.NoError(t, err)
	n, err := d.Count(1)
	require.NoError(t, err)
	require.Equal(t, uint64(3), n)
	require.NoError(t, d.Skip(4))
	v, err := d.Next()
	require.NoError(t, err)
	require.Zero(t, v)
}

func TestIntDecoderCountPrefix(t *testing.T) {
	d, err := NewIntDecoder(bytes.NewReader([]byte{6, 1, 3, 0}), 1, 11, true, false)
	require.NoError(t, err)
	count, err := d.CountPrefix(1, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
	require.NoError(t, d.Skip(2))
	count, err = d.CountPrefix(1, 4)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	_, err = d.CountPrefix(1, 12)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	_, err = NewIntDecoder(bytes.NewReader(nil), 1, 1<<32, true, false)
	require.Error(t, err)
}

func TestIntDecoderConsumesPaddedRun(t *testing.T) {
	for _, width := range []uint64{0, 1, 3, 7, 8, 64} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("width=%d/skip=%t", width, skip), func(t *testing.T) {
				values := make([]any, 16)
				mask := ^uint64(0) >> (64 - width)
				for i := range values {
					values[i] = int64(uint64(i) & mask)
				}
				encoded := WriteBitPacked(values, int64(width), true)
				// The following section is a one-value RLE run at width 8.
				reader := bytes.NewReader(append(encoded, 2, 123))
				d, err := NewIntDecoder(reader, width, 9, true, false)
				require.NoError(t, err)
				before := reader.Len()
				_, err = d.CountPrefix(0, 9)
				require.NoError(t, err)
				require.Equal(t, before, reader.Len())
				if skip {
					require.NoError(t, d.Skip(9))
				} else {
					for _, want := range values[:9] {
						got, err := d.Next()
						require.NoError(t, err)
						require.Equal(t, want, got)
					}
				}
				require.Equal(t, 2, reader.Len(), "padding must be consumed before the following section")
				_, err = d.Next()
				require.ErrorIs(t, err, io.EOF)
				next, err := NewIntDecoder(reader, 8, 1, true, false)
				require.NoError(t, err)
				got, err := next.Next()
				require.NoError(t, err)
				require.Equal(t, int64(123), got)
				require.Zero(t, reader.Len())
			})
		}
	}
}
