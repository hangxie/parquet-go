package layout

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestNewDataPage(t *testing.T) {
	page := NewDataPage()
	require.NotNil(t, page)
	require.NotNil(t, page.Header)
	require.Equal(t, parquet.PageType_DATA_PAGE, page.Header.Type)
	require.NotNil(t, page.Info)
}

func TestNewDictPage(t *testing.T) {
	page := NewDictPage()
	require.NotNil(t, page)
	require.NotNil(t, page.Header)
	require.NotNil(t, page.Header.DictionaryPageHeader)
	require.NotNil(t, page.Info)
}

func TestNewPage(t *testing.T) {
	page := NewPage()
	require.NotNil(t, page)
	require.NotNil(t, page.Header)
	require.NotNil(t, page.Info)
	require.Nil(t, page.DataTable)
}

func TestResolveCompressor(t *testing.T) {
	require.NotNil(t, resolveCompressor(nil))
	c, _ := compress.NewCompressor()
	require.Equal(t, c, resolveCompressor(c))
}

func TestResolveSizeLimit(t *testing.T) {
	for _, tt := range []struct {
		name  string
		limit int64
		want  int64
	}{
		{"unset", 0, 7},
		{"legacy-negative", -2, 7},
		{"unlimited", NoSizeLimit, math.MaxInt64},
		{"explicit", 3, 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ResolveSizeLimit(tt.limit, 7))
		})
	}
}

func TestPageReadOptionsValidate(t *testing.T) {
	require.NoError(t, (*PageReadOptions)(nil).Validate())
	for _, opts := range []PageReadOptions{{}, {MaxPageSize: NoSizeLimit, MaxDecodedBatchBytes: NoSizeLimit}, {MaxPageSize: 1, MaxDecodedBatchBytes: 1}} {
		require.NoError(t, opts.Validate())
	}
	require.ErrorContains(t, (&PageReadOptions{MaxPageSize: -2}).Validate(), "MaxPageSize: value must be positive, 0 or -1, got -2")
	require.ErrorContains(t, (&PageReadOptions{MaxDecodedBatchBytes: -2}).Validate(), "MaxDecodedBatchBytes: value must be positive, 0 or -1, got -2")
}

func TestReadPageModule(t *testing.T) {
	encoded, err := encryption.EncodeModule([]byte("hello"))
	require.NoError(t, err)
	module, err := ReadPageModule(bytes.NewReader(encoded), 5)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), module)
	_, err = ReadPageModule(bytes.NewReader(encoded), 4)
	require.ErrorContains(t, err, "page size 5 exceeds limit 4 (see reader.WithMaxPageSize)")
	_, err = ReadPageModule(bytes.NewReader(nil), 4)
	require.ErrorContains(t, err, "read encrypted module length")
}
