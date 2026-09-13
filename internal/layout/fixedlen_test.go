package layout

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func flbaSchema(length int32) *parquet.SchemaElement {
	return &parquet.SchemaElement{
		Type:       common.ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
		TypeLength: &length,
		Name:       "flba_col",
	}
}

func TestFixedLenByteArrayWidth(t *testing.T) {
	testCases := map[string]struct {
		schema   *parquet.SchemaElement
		expected int
		ok       bool
	}{
		"nil-schema":     {nil, 0, false},
		"nil-type":       {&parquet.SchemaElement{}, 0, false},
		"not-fixed-len":  {&parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY)}, 0, false},
		"fixed-len":      {flbaSchema(12), 12, true},
		"fixed-len-zero": {&parquet.SchemaElement{Type: common.ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)}, 0, true},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			width, ok := fixedLenByteArrayWidth(tc.schema)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.expected, width)
		})
	}
}

func TestCheckFixedLenByteArrayWidth(t *testing.T) {
	testCases := map[string]struct {
		schema *parquet.SchemaElement
		val    any
		errMsg string
	}{
		"non-fixed-len-column": {
			&parquet.SchemaElement{Type: common.ToPtr(parquet.Type_BYTE_ARRAY)}, "short", "",
		},
		"nil-schema": {
			nil, "short", "",
		},
		"exact-string": {
			flbaSchema(4), "abcd", "",
		},
		"exact-bytes": {
			flbaSchema(4), []byte("abcd"), "",
		},
		"nil-value": {
			flbaSchema(4), nil, "",
		},
		"non-byte-value": {
			flbaSchema(4), int32(42), "",
		},
		"too-short": {
			flbaSchema(16), "abc",
			"FIXED_LEN_BYTE_ARRAY value of length 3 does not match column length 16",
		},
		"too-long": {
			flbaSchema(16), []byte("0123456789abcdefGHIJ"),
			"FIXED_LEN_BYTE_ARRAY value of length 20 does not match column length 16",
		},
		"zero-width-column": {
			flbaSchema(0), "a",
			"FIXED_LEN_BYTE_ARRAY value of length 1 does not match column length 0",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := checkFixedLenByteArrayWidth(tc.schema, []string{"root", "flba_col"}, tc.val)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
			require.Contains(t, err.Error(), "column [root.flba_col]")
		})
	}
}

func TestCheckFixedLenByteArrayWidths(t *testing.T) {
	testCases := map[string]struct {
		schema *parquet.SchemaElement
		values []any
		errMsg string
	}{
		"non-fixed-len-column": {
			&parquet.SchemaElement{Type: common.ToPtr(parquet.Type_INT32)}, []any{int32(1)}, "",
		},
		"no-values": {
			flbaSchema(2), nil, "",
		},
		"all-exact": {
			flbaSchema(2), []any{"ab", []byte("cd"), "ef"}, "",
		},
		"bad-value-last": {
			flbaSchema(2),
			[]any{"ab", "cd", "e"},
			"value of length 1 does not match column length 2",
		},
		"bad-value-middle": {
			flbaSchema(2),
			[]any{"ab", "cde", "fg"},
			"value of length 3 does not match column length 2",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := checkFixedLenByteArrayWidths(tc.schema, []string{"root", "flba_col"}, tc.values)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestEncodingValues_FixedLenByteArrayWidth(t *testing.T) {
	testCases := map[string]struct {
		encoding parquet.Encoding
		values   []any
		errMsg   string
	}{
		"plain-exact":              {parquet.Encoding_PLAIN, []any{"0123456789abcdef"}, ""},
		"plain-too-short":          {parquet.Encoding_PLAIN, []any{"abc"}, "value of length 3"},
		"plain-too-long":           {parquet.Encoding_PLAIN, []any{"0123456789abcdefGHIJ"}, "value of length 20"},
		"byte-stream-split-short":  {parquet.Encoding_BYTE_STREAM_SPLIT, []any{"abc"}, "value of length 3"},
		"delta-byte-array-short":   {parquet.Encoding_DELTA_BYTE_ARRAY, []any{"abc"}, "value of length 3"},
		"byte-stream-split-exact":  {parquet.Encoding_BYTE_STREAM_SPLIT, []any{"0123456789abcdef"}, ""},
		"plain-second-value-short": {parquet.Encoding_PLAIN, []any{"0123456789abcdef", "abc"}, "value of length 3"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			page := newPageWithEncoding(parquet.Type_FIXED_LEN_BYTE_ARRAY, tc.encoding)
			page.Schema.TypeLength = common.ToPtr(int32(16))
			page.Path = []string{"root", "flba_col"}
			_, err := page.EncodingValues(tc.values)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func BenchmarkCheckFixedLenByteArrayWidths(b *testing.B) {
	for _, count := range []int{64, 4096} {
		values := make([]any, count)
		for i := range values {
			values[i] = fmt.Sprintf("%016d", i)
		}
		path := []string{"root", "flba_col"}
		schema := flbaSchema(16)
		b.Run(fmt.Sprintf("values-%d", count), func(b *testing.B) {
			for b.Loop() {
				if err := checkFixedLenByteArrayWidths(schema, path, values); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
