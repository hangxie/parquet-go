package types

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestValueModeString(t *testing.T) {
	require.Equal(t, "interpreted", ValueModeInterpreted.String())
	require.Equal(t, "raw", ValueModeRaw.String())
	require.Equal(t, "ValueMode(7)", ValueMode(7).String())
}

func TestNewValueConfig(t *testing.T) {
	t.Run("defaults to interpreted with no geospatial override", func(t *testing.T) {
		cfg := NewValueConfig()
		require.Equal(t, ValueModeInterpreted, cfg.Mode)
		require.Nil(t, cfg.Geospatial)
	})

	t.Run("options apply in order", func(t *testing.T) {
		geo := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeBase64))
		cfg := NewValueConfig(WithValueMode(ValueModeRaw), WithGeospatialConfig(geo))
		require.Equal(t, ValueModeRaw, cfg.Mode)
		require.Same(t, geo, cfg.Geospatial)
	})

	t.Run("deprecated aliases stay assignable", func(t *testing.T) {
		opts := []JSONTypeOption{WithValueMode(ValueModeRaw)}
		cfg := JSONTypeConfig{}
		opts[0](&cfg)
		require.Equal(t, ValueModeRaw, cfg.Mode)
	})
}

// TestValueModeUnsupported pins that a mode outside the two defined ones is reported
// rather than quietly taking the interpreted grammar.
func TestValueModeUnsupported(t *testing.T) {
	byteArray := parquet.Type_BYTE_ARRAY
	bogus := WithValueMode(ValueMode(7))

	_, err := StrToParquetTypeWithLogical("aGk=", &byteArray, nil, nil, 0, 0, bogus)
	require.ErrorIs(t, err, ErrUnsupportedValueMode)
	require.ErrorContains(t, err, "unsupported value mode 7")

	_, err = JSONTypeToParquetTypeWithLogical(reflect.ValueOf("aGk="), &byteArray, nil, nil, 0, 0, bogus)
	require.ErrorIs(t, err, ErrUnsupportedValueMode)
	require.ErrorContains(t, err, "unsupported value mode 7")
}
