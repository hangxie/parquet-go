package reader_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/reader"
)

func TestReaderOptionIsOpaque(t *testing.T) {
	t.Parallel()

	optionType := reflect.TypeOf((*reader.ReaderOption)(nil)).Elem()
	require.NotEqual(t, reflect.Func, optionType.Kind())
}
