package writer

import (
	"bytes"
	"io"
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

func TestValidateDictionaryEncodings(t *testing.T) {
	type boolDict struct {
		Value bool `parquet:"name=value, type=BOOLEAN, encoding=RLE_DICTIONARY"`
	}
	type int32Dict struct {
		Value int32 `parquet:"name=value, type=INT32, encoding=RLE_DICTIONARY"`
	}
	jsonSchema := func(tag string) string {
		return `{"Tag": "name=parquet_go_root", "Fields": [{"Tag": "` + tag + `"}]}`
	}

	testCases := map[string]struct {
		newWriter func(w io.Writer) error
		errMsg    string
	}{
		"struct-rle-dictionary": {
			newWriter: func(w io.Writer) error {
				_, err := NewParquetWriterFromWriter(w, new(boolDict), WithNP(1))
				return err
			},
			errMsg: "column [Parquet_go_root.Value]: dictionary encoding is not supported for BOOLEAN",
		},
		"struct-int32-dictionary-allowed": {
			newWriter: func(w io.Writer) error {
				_, err := NewParquetWriterFromWriter(w, new(int32Dict), WithNP(1))
				return err
			},
		},
		"json-schema-string-plain-dictionary": {
			newWriter: func(w io.Writer) error {
				_, err := NewParquetWriterFromWriter(w, jsonSchema("name=value, type=BOOLEAN, encoding=PLAIN_DICTIONARY"), WithNP(1))
				return err
			},
			errMsg: "dictionary encoding is not supported for BOOLEAN",
		},
		"json-writer": {
			newWriter: func(w io.Writer) error {
				_, err := NewJSONWriterFromWriter(jsonSchema("name=value, type=BOOLEAN, encoding=RLE_DICTIONARY"), w, WithNP(1))
				return err
			},
			errMsg: "dictionary encoding is not supported for BOOLEAN",
		},
		"csv-writer": {
			newWriter: func(w io.Writer) error {
				_, err := NewCSVWriterFromWriter([]string{"name=value, type=BOOLEAN, encoding=RLE_DICTIONARY"}, w, WithNP(1))
				return err
			},
			errMsg: "dictionary encoding is not supported for BOOLEAN",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := tc.newWriter(&buf)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
			require.Zero(t, buf.Len())
		})
	}
}

func TestValidateDictionaryEncodings_SetSchemaHandlerFromJSON(t *testing.T) {
	pw := &ParquetWriter{Footer: parquet.NewFileMetaData()}
	err := pw.SetSchemaHandlerFromJSON(`{"Tag": "name=parquet_go_root", "Fields": [{"Tag": "name=value, type=BOOLEAN, encoding=RLE_DICTIONARY"}]}`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dictionary encoding is not supported for BOOLEAN")
}

func TestValidateDictionaryEncodings_MissingInfos(t *testing.T) {
	elements := []*parquet.SchemaElement{
		{Name: "parquet_go_root", NumChildren: common.ToPtr(int32(2))},
		{Name: "a", Type: common.ToPtr(parquet.Type_BOOLEAN)},
		{Name: "b", Type: common.ToPtr(parquet.Type_BOOLEAN)},
	}
	pw := &ParquetWriter{SchemaHandler: &schema.SchemaHandler{
		SchemaElements: elements,
		Infos:          []*common.Tag{{}, nil},
	}}
	require.NoError(t, pw.validateDictionaryEncodings())
}

func TestValidateSchemaForWrite_DuplicateColumnNames(t *testing.T) {
	type row struct {
		A int64  `parquet:"name=value, type=INT64"`
		B string `parquet:"name=value, type=BYTE_ARRAY"`
	}
	schemaList := []*parquet.SchemaElement{
		{Name: "parquet_go_root", NumChildren: common.ToPtr(int32(2))},
		{Name: "value", Type: common.ToPtr(parquet.Type_INT32), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
		{Name: "value", Type: common.ToPtr(parquet.Type_INT64), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
	}

	testCases := map[string]any{
		"struct":         new(row),
		"schema-list":    schemaList,
		"schema-handler": schema.NewSchemaHandlerFromSchemaList(schemaList),
	}
	for name, obj := range testCases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := NewParquetWriter(writerfile.NewWriterFile(&buf), obj, WithNP(1))
			require.Error(t, err)
			require.Contains(t, err.Error(), `duplicate column name "value"`)
			require.Zero(t, buf.Len())
		})
	}
}
