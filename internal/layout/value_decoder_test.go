package layout

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestValueDecoderRoundTrip(t *testing.T) {
	cases := []struct {
		method parquet.Encoding
		typ    parquet.Type
		value  func(int) any
		width  uint64
	}{
		{parquet.Encoding_PLAIN, parquet.Type_BOOLEAN, func(i int) any { return i%3 == 0 }, 0},
		{parquet.Encoding_PLAIN, parquet.Type_INT32, func(i int) any { return int32(i) }, 0},
		{parquet.Encoding_PLAIN, parquet.Type_INT64, func(i int) any { return int64(i) }, 0},
		{parquet.Encoding_PLAIN, parquet.Type_INT96, func(i int) any { return "123456789012" }, 0},
		{parquet.Encoding_PLAIN, parquet.Type_BYTE_ARRAY, func(i int) any { return fmt.Sprint(i) }, 0},
		{parquet.Encoding_PLAIN, parquet.Type_FIXED_LEN_BYTE_ARRAY, func(i int) any { return "abcd" }, 4},
		{parquet.Encoding_RLE, parquet.Type_BOOLEAN, func(i int) any { return i%3 == 0 }, 0},
		{parquet.Encoding_RLE, parquet.Type_INT32, func(i int) any { return int32(i / 10) }, 0},
		{parquet.Encoding_RLE, parquet.Type_INT64, func(i int) any { return int64(i / 10) }, 0},
		{parquet.Encoding_DELTA_BINARY_PACKED, parquet.Type_INT32, func(i int) any { return int32(i * i) }, 0},
		{parquet.Encoding_DELTA_BINARY_PACKED, parquet.Type_INT64, func(i int) any { return int64(i * i) }, 0},
		{parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Type_BYTE_ARRAY, func(i int) any { return fmt.Sprint(i) }, 0},
		{parquet.Encoding_DELTA_BYTE_ARRAY, parquet.Type_BYTE_ARRAY, func(i int) any { return fmt.Sprint(i) }, 0},
		{parquet.Encoding_BYTE_STREAM_SPLIT, parquet.Type_FLOAT, func(i int) any { return float32(i) / 7 }, 0},
		{parquet.Encoding_BYTE_STREAM_SPLIT, parquet.Type_DOUBLE, func(i int) any { return float64(i) / 7 }, 0},
		{parquet.Encoding_BYTE_STREAM_SPLIT, parquet.Type_INT32, func(i int) any { return int32(i) }, 0},
		{parquet.Encoding_BYTE_STREAM_SPLIT, parquet.Type_INT64, func(i int) any { return int64(i) }, 0},
		{parquet.Encoding_BYTE_STREAM_SPLIT, parquet.Type_FIXED_LEN_BYTE_ARRAY, func(i int) any { return "abcd" }, 4},
	}
	for _, tt := range cases {
		t.Run(tt.method.String()+"/"+tt.typ.String(), func(t *testing.T) {
			values := make([]any, 2051)
			for i := range values {
				values[i] = tt.value(i)
			}
			page := NewDataPage()
			page.Schema = &parquet.SchemaElement{Type: common.ToPtr(tt.typ), TypeLength: common.ToPtr(int32(tt.width))}
			page.Info.Encoding = tt.method
			data, err := page.EncodingValues(values)
			require.NoError(t, err)
			d, err := newValueDecoder(bytes.NewReader(data), tt.method, tt.typ, uint64(len(values)), tt.width)
			require.NoError(t, err)
			require.NoError(t, d.skip(7))
			for _, want := range values[7:] {
				got, err := d.next()
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
			_, err = d.next()
			require.ErrorIs(t, err, io.EOF)
			require.ErrorIs(t, d.skip(1), io.ErrUnexpectedEOF)
		})
	}
}

func TestValueDecoderDictionaryAndBitPacked(t *testing.T) {
	for _, method := range []parquet.Encoding{parquet.Encoding_RLE_DICTIONARY, parquet.Encoding_PLAIN_DICTIONARY, parquet.Encoding_BIT_PACKED} {
		var data []byte
		if method == parquet.Encoding_BIT_PACKED {
			data = encoding.WriteBitPacked([]any{int64(1), int64(2)}, 32, false)
		} else {
			run, err := encoding.WriteRLE([]any{int64(1), int64(2)}, 2, parquet.Type_INT64)
			require.NoError(t, err)
			data = append([]byte{2}, run...)
		}
		d, err := newValueDecoder(bytes.NewReader(data), method, parquet.Type_INT32, 2, 0)
		require.NoError(t, err)
		require.NoError(t, d.skip(1))
		got, err := d.next()
		require.NoError(t, err)
		if method == parquet.Encoding_BIT_PACKED {
			require.Equal(t, int32(2), got)
		} else {
			require.Equal(t, int64(2), got)
		}
	}
}

func TestValueDecoderInvalid(t *testing.T) {
	for _, tt := range []struct {
		method parquet.Encoding
		typ    parquet.Type
		data   []byte
		count  uint64
	}{
		{parquet.Encoding(-1), parquet.Type_INT32, nil, 1},
		{parquet.Encoding_DELTA_BINARY_PACKED, parquet.Type_BOOLEAN, nil, 1},
		{parquet.Encoding_DELTA_BINARY_PACKED, parquet.Type_INT64, nil, 1},
		{parquet.Encoding_DELTA_BYTE_ARRAY, parquet.Type_BYTE_ARRAY, nil, 1},
		{parquet.Encoding_RLE_DICTIONARY, parquet.Type_INT32, nil, 1},
		{parquet.Encoding_RLE_DICTIONARY, parquet.Type_INT32, []byte{65}, 1},
		{parquet.Encoding_BYTE_STREAM_SPLIT, parquet.Type_BOOLEAN, nil, 1},
		{parquet.Encoding_BYTE_STREAM_SPLIT, parquet.Type_FLOAT, nil, 1},
	} {
		_, err := newValueDecoder(bytes.NewReader(tt.data), tt.method, tt.typ, tt.count, 0)
		require.Error(t, err)
	}
	d, err := newValueDecoder(bytes.NewReader(nil), parquet.Encoding_PLAIN, parquet.Type_INT32, 1, 0)
	require.NoError(t, err)
	_, err = d.next()
	require.Error(t, err)
	d, err = newValueDecoder(bytes.NewReader(nil), parquet.Encoding_PLAIN, parquet.Type_INT32, 0, 0)
	require.NoError(t, err)
	_, err = d.next()
	require.ErrorIs(t, err, io.EOF)
}

func TestValueDecoderMalformedStreams(t *testing.T) {
	oneLength := func(n int64) []byte { return encoding.WriteDeltaINT64([]any{n}) }
	cases := []struct {
		name   string
		method parquet.Encoding
		typ    parquet.Type
		data   []byte
		count  uint64
	}{
		{"truncated hybrid value", parquet.Encoding_RLE, parquet.Type_BOOLEAN, []byte{1, 0, 0, 0, 2}, 1},
		{"truncated delta block", parquet.Encoding_DELTA_BINARY_PACKED, parquet.Type_INT64, []byte{0x80, 1, 1, 2, 0}, 2},
		{"negative length", parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Type_BYTE_ARRAY, oneLength(-1), 1},
		{"length past input", parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Type_BYTE_ARRAY, oneLength(5), 1},
		{"missing lengths", parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY, parquet.Type_BYTE_ARRAY, oneLength(0), 2},
		{"invalid prefix", parquet.Encoding_DELTA_BYTE_ARRAY, parquet.Type_BYTE_ARRAY, append(oneLength(1), oneLength(0)...), 1},
		{"missing prefixes", parquet.Encoding_DELTA_BYTE_ARRAY, parquet.Type_BYTE_ARRAY, append(oneLength(0), encoding.WriteDeltaINT64([]any{int64(0), int64(0)})...), 2},
		{"truncated plain", parquet.Encoding_PLAIN, parquet.Type_INT32, nil, 1},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			d, err := newValueDecoder(bytes.NewReader(tt.data), tt.method, tt.typ, tt.count, 0)
			require.NoError(t, err)
			for range tt.count {
				_, err = d.next()
				if err != nil {
					break
				}
			}
			require.Error(t, err)
			d, err = newValueDecoder(bytes.NewReader(tt.data), tt.method, tt.typ, tt.count, 0)
			require.NoError(t, err)
			require.Error(t, d.skip(tt.count))
		})
	}
	_, err := openDeltaSection(bytes.NewReader([]byte{0x80, 1, 1, 2, 0}), 2)
	require.Error(t, err)
}

func TestDefaultBitWidth(t *testing.T) {
	testCases := []struct {
		name     string
		dataType parquet.Type
		expected uint64
	}{
		{
			name:     "boolean",
			dataType: parquet.Type_BOOLEAN,
			expected: 1,
		},
		{
			name:     "int32",
			dataType: parquet.Type_INT32,
			expected: 32,
		},
		{
			name:     "int64",
			dataType: parquet.Type_INT64,
			expected: 64,
		},
		{
			name:     "float",
			dataType: parquet.Type_FLOAT,
			expected: 0,
		},
		{
			name:     "double",
			dataType: parquet.Type_DOUBLE,
			expected: 0,
		},
		{
			name:     "byte_array",
			dataType: parquet.Type_BYTE_ARRAY,
			expected: 0,
		},
		{
			name:     "fixed_len_byte_array",
			dataType: parquet.Type_FIXED_LEN_BYTE_ARRAY,
			expected: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := defaultBitWidth(tc.dataType)
			require.Equal(t, tc.expected, result)
		})
	}
}
