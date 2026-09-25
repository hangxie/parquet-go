package writer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

func TestNewParquetWriter_SchemaVariants(t *testing.T) {
	tests := map[string]struct {
		obj     any
		wantErr bool
	}{
		"invalid_json_schema_string": {
			obj:     `{"invalid": json}`,
			wantErr: true,
		},
		"valid_json_schema_string": {
			obj: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
					{"Tag": "name=age, type=INT32"}
				]
			}`,
			wantErr: false,
		},
		"nil_object": {
			obj:     nil,
			wantErr: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			fw := writerfile.NewWriterFile(&buf)
			pw, err := NewParquetWriter(fw, tt.obj, WithNP(1))
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "unmarshal json schema")
			} else {
				require.NoError(t, err)
				require.NotNil(t, pw)
			}
		})
	}
}

func TestNewParquetWriterFromWriter(t *testing.T) {
	type TestStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age  int32  `parquet:"name=age, type=INT32"`
	}

	t.Run("successful_creation", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriterFromWriter(&buf, new(TestStruct), WithNP(1))
		require.NoError(t, err)
		require.NotNil(t, pw)

		data := TestStruct{Name: "Alice", Age: 30}
		err = pw.Write(data)
		require.NoError(t, err)

		err = pw.WriteStop()
		require.NoError(t, err)

		require.Greater(t, buf.Len(), 0)
	})

	t.Run("invalid_object", func(t *testing.T) {
		var buf bytes.Buffer
		pw, err := NewParquetWriterFromWriter(&buf, nil, WithNP(1))
		if err != nil {
			require.Error(t, err)
			require.Nil(t, pw)
		} else {
			require.NotNil(t, pw)
		}
	})
}

func TestNewParquetWriter_SchemaHandlerInput(t *testing.T) {
	type S struct {
		ID   int32  `parquet:"name=id, type=INT32"`
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(S))
	require.NoError(t, err)

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, sh, WithNP(1))
	require.NoError(t, err)
	require.NotNil(t, pw)
	require.NoError(t, pw.WriteStop())
}

// TestNewParquetWriter_SchemaElementsInput covers the []*parquet.SchemaElement branch.
func TestNewParquetWriter_SchemaElementsInput(t *testing.T) {
	type S struct {
		ID int32 `parquet:"name=id, type=INT32"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(S))
	require.NoError(t, err)

	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := NewParquetWriter(fw, sh.SchemaElements, WithNP(1))
	require.NoError(t, err)
	require.NotNil(t, pw)
	require.NoError(t, pw.WriteStop())
}

// TestNewParquetWriter_InvalidStructInput covers the NewSchemaHandlerFromStruct error branch.
func TestNewParquetWriter_InvalidStructInput(t *testing.T) {
	type BadStruct struct {
		ID int32 `parquet:"name=id, type=INVALID_TYPE"`
	}
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	_, err := NewParquetWriter(fw, new(BadStruct), WithNP(1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "build schema handler")
}
