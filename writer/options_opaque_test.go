package writer_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/writer"
)

func TestWriterOptionIsOpaque(t *testing.T) {
	t.Parallel()

	optionType := reflect.TypeOf((*writer.WriterOption)(nil)).Elem()
	require.NotEqual(t, reflect.Func, optionType.Kind())
}
