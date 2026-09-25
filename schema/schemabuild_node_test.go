package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestCreateVariantSchema_StructBacked(t *testing.T) {
	t.Run("field_without_parquet_tag_is_skipped", func(t *testing.T) {
		// covers schemabuild.go line 75: skip fields with no parquet tag inside VARIANT struct
		type VariantWithExtra struct {
			Metadata []byte `parquet:"name=metadata, type=BYTE_ARRAY"`
			Value    []byte `parquet:"name=value, type=BYTE_ARRAY"`
			NoTag    string // no parquet tag — must be skipped
		}
		type Outer struct {
			V VariantWithExtra `parquet:"name=v, type=VARIANT"`
		}
		sh, err := NewSchemaHandlerFromStruct(new(Outer))
		require.NoError(t, err)
		// root (0), V group (1), Metadata (2), Value (3) — NoTag must not appear
		require.Equal(t, 4, len(sh.SchemaElements))
	})

	t.Run("pointer_field_becomes_optional", func(t *testing.T) {
		// covers schemabuild.go line 88: pointer field in VARIANT struct → OPTIONAL repetition
		type VariantWithPtr struct {
			Metadata []byte  `parquet:"name=metadata, type=BYTE_ARRAY"`
			Value    *[]byte `parquet:"name=value, type=BYTE_ARRAY"`
		}
		type Outer struct {
			V VariantWithPtr `parquet:"name=v, type=VARIANT"`
		}
		sh, err := NewSchemaHandlerFromStruct(new(Outer))
		require.NoError(t, err)
		require.Equal(t, 4, len(sh.SchemaElements))
		// Value field (index 3) must be OPTIONAL because it's a pointer
		require.Equal(t, parquet.FieldRepetitionType_OPTIONAL, sh.Infos[3].RepetitionType)
	})

	t.Run("invalid_tag_in_variant_struct_returns_error", func(t *testing.T) {
		// covers schemabuild.go lines 83–85 and 264–266: parse error for a field
		// inside a struct-backed VARIANT propagates out of NewSchemaHandlerFromStruct.
		type VariantWithBadTag struct {
			Metadata []byte `parquet:"name=metadata, type=BYTE_ARRAY"`
			Bad      int32  `parquet:"name=bad, type=INT32, encoding=INVALID_ENCODING"`
		}
		type Outer struct {
			V VariantWithBadTag `parquet:"name=v, type=VARIANT"`
		}
		_, err := NewSchemaHandlerFromStruct(new(Outer))
		require.Error(t, err)
		require.Contains(t, err.Error(), "parse tag")
	})
}
