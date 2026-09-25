package writer

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriterOptionIsOpaque(t *testing.T) {
	t.Parallel()

	optionType := reflect.TypeOf((*WriterOption)(nil)).Elem()
	require.NotEqual(t, reflect.Func, optionType.Kind())
}
