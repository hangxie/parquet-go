package writer

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

func TestCSVWriter(t *testing.T) {
	t.Run("new_csv_writer", func(t *testing.T) {
		testCases := map[string]struct {
			schema []string
			errMsg string
		}{
			"bad":   {[]string{"abc"}, "create schema from metadata"},
			"empty": {[]string{}, ""},
			"good":  {[]string{"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN"}, ""},
		}
		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				var buf bytes.Buffer
				bw := bufio.NewWriter(&buf)
				wf := writerfile.NewWriterFile(bw)
				cw, err := NewCSVWriter(tc.schema, wf)
				if tc.errMsg == "" {
					require.NoError(t, err)
					require.Equal(t, cw.np, int64(4))
					require.Equal(t, cw.pageSize, int64(8*1024))
					require.Equal(t, cw.rowGroupSize, int64(128*1024*1024))
					require.Equal(t, cw.compressionType, parquet.CompressionCodec_SNAPPY)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errMsg)
				}
			})
		}
	})

	t.Run("new_csv_writer_from_writer", func(t *testing.T) {
		schema := []string{
			"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
			"Name=Last, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, err := NewCSVWriterFromWriter(schema, bw)
		require.NoError(t, err)
		require.Equal(t, cw.np, int64(4))
		require.Equal(t, cw.pageSize, int64(8*1024))
		require.Equal(t, cw.rowGroupSize, int64(128*1024*1024))
		require.Equal(t, cw.compressionType, parquet.CompressionCodec_SNAPPY)
	})

	t.Run("new_csv_writer_with_options", func(t *testing.T) {
		schema := []string{
			"Name=First, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, err := NewCSVWriterFromWriter(schema, bw,
			WithNP(2),
			WithPageSize(4096),
			WithCompressionType(parquet.CompressionCodec_GZIP),
		)
		require.NoError(t, err)
		require.Equal(t, int64(2), cw.np)
		require.Equal(t, int64(4096), cw.pageSize)
		require.Equal(t, parquet.CompressionCodec_GZIP, cw.compressionType)
	})

	t.Run("write_csv", func(t *testing.T) {
		testCases := map[string]struct {
			data   []*string
			errMsg string
		}{
			"empty": {[]*string{nil, nil}, ""},
			"good":  {[]*string{common.ToPtr("name"), common.ToPtr("123")}, ""},
			"bad":   {[]*string{common.ToPtr("name"), common.ToPtr("abc")}, "expected integer"},
		}
		schema := []string{
			"Name=Name, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
			"Name=id, Type=INT32",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, _ := NewCSVWriterFromWriter(schema, bw)

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				err := cw.WriteString(tc.data)
				if tc.errMsg == "" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errMsg)
				}
			})
		}
	})

	t.Run("write_string_wrong_type", func(t *testing.T) {
		testCases := map[string]struct {
			data   any
			errMsg string
		}{
			"string_slice":  {[]string{"name", "123"}, "WriteString: expected []*string, got []string"},
			"int_slice":     {[]int{1, 2}, "WriteString: expected []*string, got []int"},
			"nil":           {nil, "WriteString: expected []*string, got <nil>"},
			"single_string": {"name", "WriteString: expected []*string, got string"},
			"any_slice":     {[]any{"name", "123"}, "WriteString: expected []*string, got []interface {}"},
		}
		schema := []string{
			"Name=Name, Type=BYTE_ARRAY, ConvertedType=UTF8, Encoding=PLAIN",
			"Name=id, Type=INT32",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, err := NewCSVWriterFromWriter(schema, bw)
		require.NoError(t, err)

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				require.NotPanics(t, func() {
					err := cw.WriteString(tc.data)
					require.Error(t, err)
					require.Equal(t, tc.errMsg, err.Error())
				})
			})
		}
	})
}
