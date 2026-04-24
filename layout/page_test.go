package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

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
