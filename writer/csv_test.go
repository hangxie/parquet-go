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
		cw, err := NewCSVWriterFromWriter(
			schema, bw,
			WithNP(2),
			WithPageSize(4096),
			WithCompressionCodec(parquet.CompressionCodec_GZIP),
		)
		require.NoError(t, err)
		require.Equal(t, int64(2), cw.np)
		require.Equal(t, int64(4096), cw.pageSize)
		require.Equal(t, parquet.CompressionCodec_GZIP, cw.compressionType)
	})

	t.Run("new_csv_writer_invalid_option", func(t *testing.T) {
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		_, err := NewCSVWriterFromWriter(nil, bw, WithNP(0))
		require.Error(t, err)
		require.Contains(t, err.Error(), "init CSV writer base")
	})

	t.Run("new_csv_writer_invalid_column_key_path", func(t *testing.T) {
		schema := []string{
			"Name=id, Type=INT32",
			"Name=name, Type=BYTE_ARRAY, ConvertedType=UTF8",
		}
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		_, err := NewCSVWriterFromWriter(
			schema,
			bw,
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("invalid_name", ColumnKey([]byte("abcdef0123456789"))),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "validate encryption column keys")
		require.Contains(t, err.Error(), "invalid_name")
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

	t.Run("write_csv_uuid", func(t *testing.T) {
		testCases := map[string]struct {
			value  string
			errMsg string
		}{
			"dashed":         {"550e8400-e29b-41d4-a716-446655440000", ""},
			"undashed":       {"550e8400e29b41d4a716446655440000", ""},
			"braced":         {"{550e8400-e29b-41d4-a716-446655440000}", ""},
			"urn":            {"urn:uuid:550e8400-e29b-41d4-a716-446655440000", ""},
			"not_a_uuid":     {"not-a-uuid", "parse UUID"},
			"truncated":      {"550e8400-e29b-41d4-a716-44665544", "parse UUID"},
			"square_bracket": {"[550e8400-e29b-41d4-a716-446655440000]", "parse UUID"},
			"raw_binary":     {"\x55\x0e\x84\x00\xe2\x9b\x41\xd4\xa7\x16\x44\x66\x55\x44\x00\x00", "parse UUID"},
		}
		schema := []string{"Name=Id, Type=FIXED_LEN_BYTE_ARRAY, Length=16, LogicalType=UUID"}

		for name, tc := range testCases {
			t.Run(name, func(t *testing.T) {
				var buf bytes.Buffer
				bw := bufio.NewWriter(&buf)
				cw, err := NewCSVWriterFromWriter(schema, bw)
				require.NoError(t, err)

				err = cw.WriteString([]*string{common.ToPtr(tc.value)})
				if tc.errMsg == "" {
					require.NoError(t, err)
					require.NoError(t, cw.WriteStop())
				} else {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errMsg)
				}
			})
		}
	})

	t.Run("uuid_column_length_omitted", func(t *testing.T) {
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		cw, err := NewCSVWriterFromWriter([]string{"Name=Id, Type=FIXED_LEN_BYTE_ARRAY, LogicalType=UUID"}, bw)
		require.NoError(t, err)
		require.Equal(t, int32(16), cw.SchemaHandler.SchemaElements[1].GetTypeLength())

		require.NoError(t, cw.WriteString([]*string{common.ToPtr("550e8400-e29b-41d4-a716-446655440000")}))
		require.NoError(t, cw.WriteStop())
	})

	t.Run("uuid_column_wrong_length", func(t *testing.T) {
		var buf bytes.Buffer
		bw := bufio.NewWriter(&buf)
		_, err := NewCSVWriterFromWriter([]string{"Name=Id, Type=FIXED_LEN_BYTE_ARRAY, Length=8, LogicalType=UUID"}, bw)
		require.Error(t, err)
		require.Contains(t, err.Error(), "LogicalType UUID requires FIXED_LEN_BYTE_ARRAY with length 16")
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

func TestCSVWriterFixedLenByteArrayWidth(t *testing.T) {
	testCases := map[string]struct {
		value  string
		errMsg string
	}{
		"raw-width-match": {
			// Valid base64, but 16 raw characters is what the column takes.
			"0123456789abcdef", "",
		},
		"base64-width-match": {
			"YWJjZGVmZ2hpamtsbW5vcA==", "",
		},
		"neither-width-matches": {
			"abc", `FIXED_LEN_BYTE_ARRAY "abc" is 3 bytes, column length is 16`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			cw, err := NewCSVWriter(
				[]string{"name=V, type=FIXED_LEN_BYTE_ARRAY, length=16"},
				writerfile.NewWriterFile(&buf), WithNP(1),
			)
			require.NoError(t, err)

			// A width the converter cannot satisfy is reported here, not once the
			// page is built from a value the caller never supplied.
			err = cw.WriteString([]*string{common.ToPtr(tc.value)})
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.NoError(t, cw.WriteStop())
		})
	}
}
