package writer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

// flbaSchemaList builds a one-column schema list whose FLBA column declares length.
func flbaSchemaList(length *int32) []*parquet.SchemaElement {
	return []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "V",
			Type:           common.ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			TypeLength:     length,
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	}
}

func TestValidateSchemaForWrite(t *testing.T) {
	type withLength struct {
		V string `parquet:"name=V, type=FIXED_LEN_BYTE_ARRAY, length=4"`
	}
	type withoutLength struct {
		V string `parquet:"name=V, type=FIXED_LEN_BYTE_ARRAY"`
	}

	fromObj := func(obj any) func() error {
		return func() error {
			_, _, err := createTestParquetWriter(obj, WithNP(1))
			return err
		}
	}
	fromSchemaList := func(length *int32) func() error {
		return fromObj(flbaSchemaList(length))
	}
	fromSchemaHandler := func(length *int32) func() error {
		return fromObj(schema.NewSchemaHandlerFromSchemaList(flbaSchemaList(length)))
	}
	fromJSONSchema := func(tag string) func() error {
		return fromObj(`{"Tag": "name=parquet-go-root", "Fields": [{"Tag": "` + tag + `"}]}`)
	}
	fromJSONWriter := func(tag string) func() error {
		return func() error {
			var buf bytes.Buffer
			_, err := NewJSONWriter(
				`{"Tag": "name=parquet-go-root", "Fields": [{"Tag": "`+tag+`"}]}`,
				writerfile.NewWriterFile(&buf), WithNP(1),
			)
			return err
		}
	}
	fromCSVWriter := func(md string) func() error {
		return func() error {
			var buf bytes.Buffer
			_, err := NewCSVWriter([]string{md}, writerfile.NewWriterFile(&buf), WithNP(1))
			return err
		}
	}

	testCases := map[string]struct {
		newWriter func() error
		errMsg    string
	}{
		"struct-tag-length-given": {
			fromObj(new(withLength)), "",
		},
		"struct-tag-length-omitted": {
			fromObj(new(withoutLength)), "FIXED_LEN_BYTE_ARRAY requires a positive length, got 0",
		},
		"schema-list-length-given": {
			fromSchemaList(common.ToPtr(int32(4))), "",
		},
		"schema-list-length-nil": {
			fromSchemaList(nil), "column [Parquet_go_root.V]: FIXED_LEN_BYTE_ARRAY requires a positive length, got 0",
		},
		"schema-list-length-zero": {
			fromSchemaList(common.ToPtr(int32(0))), "column [Parquet_go_root.V]: FIXED_LEN_BYTE_ARRAY requires a positive length, got 0",
		},
		"schema-list-length-negative": {
			fromSchemaList(common.ToPtr(int32(-1))), "column [Parquet_go_root.V]: FIXED_LEN_BYTE_ARRAY requires a positive length, got -1",
		},
		"schema-handler-length-given": {
			fromSchemaHandler(common.ToPtr(int32(4))), "",
		},
		"schema-handler-length-nil": {
			fromSchemaHandler(nil), "column [Parquet_go_root.V]: FIXED_LEN_BYTE_ARRAY requires a positive length, got 0",
		},
		"schema-handler-length-zero": {
			fromSchemaHandler(common.ToPtr(int32(0))), "column [Parquet_go_root.V]: FIXED_LEN_BYTE_ARRAY requires a positive length, got 0",
		},
		"schema-handler-length-negative": {
			fromSchemaHandler(common.ToPtr(int32(-1))), "column [Parquet_go_root.V]: FIXED_LEN_BYTE_ARRAY requires a positive length, got -1",
		},
		"json-schema-length-given": {
			fromJSONSchema("name=V, type=FIXED_LEN_BYTE_ARRAY, length=4"), "",
		},
		"json-schema-length-omitted": {
			fromJSONSchema("name=V, type=FIXED_LEN_BYTE_ARRAY"), "FIXED_LEN_BYTE_ARRAY requires a positive length, got 0",
		},
		"json-writer-length-given": {
			fromJSONWriter("name=V, type=FIXED_LEN_BYTE_ARRAY, length=4"), "",
		},
		"json-writer-length-omitted": {
			fromJSONWriter("name=V, type=FIXED_LEN_BYTE_ARRAY"), "FIXED_LEN_BYTE_ARRAY requires a positive length, got 0",
		},
		"csv-writer-length-given": {
			fromCSVWriter("name=V, type=FIXED_LEN_BYTE_ARRAY, length=4"), "",
		},
		"csv-writer-length-omitted": {
			fromCSVWriter("name=V, type=FIXED_LEN_BYTE_ARRAY"), "FIXED_LEN_BYTE_ARRAY requires a positive length, got 0",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := tc.newWriter()
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestValidateSchemaForWrite_SetSchemaHandlerFromJSON(t *testing.T) {
	testCases := map[string]struct {
		tag    string
		errMsg string
	}{
		"length-given": {
			"name=V, type=FIXED_LEN_BYTE_ARRAY, length=4", "",
		},
		"length-omitted": {
			"name=V, type=FIXED_LEN_BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY requires a positive length, got 0",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pw, _, err := createTestParquetWriter(nil, WithNP(1))
			require.NoError(t, err)

			err = pw.SetSchemaHandlerFromJSON(`{"Tag": "name=parquet-go-root", "Fields": [{"Tag": "` + tc.tag + `"}]}`)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestValidateSchemaForWrite_NoSchemaHandler(t *testing.T) {
	pw := new(ParquetWriter)
	require.NoError(t, pw.validateSchemaForWrite())
}

func TestValidateSchemaForWrite_ColumnPathFallback(t *testing.T) {
	// A schema handler carrying no index map still names the column in the error.
	pw := &ParquetWriter{
		SchemaHandler: &schema.SchemaHandler{
			SchemaElements: flbaSchemaList(common.ToPtr(int32(0))),
		},
	}
	err := pw.validateSchemaForWrite()
	require.Error(t, err)
	require.Contains(t, err.Error(), "column [V]: FIXED_LEN_BYTE_ARRAY requires a positive length, got 0")
}
