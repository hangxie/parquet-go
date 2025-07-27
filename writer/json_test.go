package writer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/buffer"
	"github.com/hangxie/parquet-go/v2/source/writerfile"
)

func Test_NewJSONWriter(t *testing.T) {
	t.Run("valid_schema", func(t *testing.T) {
		jsonSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
				{"Tag": "name=age, type=INT32"}
			]
		}`

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter(jsonSchema, fw, 1)

		require.NoError(t, err)
		require.NotNil(t, jw)
		require.NotNil(t, jw.SchemaHandler)
		require.Equal(t, int64(1), jw.NP)

		// Clean up
		err = jw.WriteStop()
		require.NoError(t, err)
	})

	t.Run("invalid_json_schema", func(t *testing.T) {
		invalidSchema := `{"invalid": json syntax}`

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter(invalidSchema, fw, 1)

		require.Error(t, err)
		require.NotNil(t, jw) // Function returns struct even on error
	})

	t.Run("empty_schema", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter("", fw, 1)

		require.Error(t, err)
		require.NotNil(t, jw) // Function returns struct even on error
	})

	t.Run("malformed_schema", func(t *testing.T) {
		malformedSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=INVALID_TYPE"}
			]
		}`

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter(malformedSchema, fw, 1)

		require.Error(t, err)
		require.NotNil(t, jw) // Function returns struct even on error
	})
}

func Test_NewJSONWriterFromWriter_Comprehensive(t *testing.T) {
	t.Run("successful_creation", func(t *testing.T) {
		jsonSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
				{"Tag": "name=age, type=INT32"}
			]
		}`

		var buf bytes.Buffer
		jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, 2)

		require.NoError(t, err)
		require.NotNil(t, jw)
		require.Equal(t, int64(2), jw.NP)

		// Clean up
		err = jw.WriteStop()
		require.NoError(t, err)
	})

	t.Run("invalid_schema", func(t *testing.T) {
		invalidSchema := `not valid json`

		var buf bytes.Buffer
		jw, err := NewJSONWriterFromWriter(invalidSchema, &buf, 1)

		require.Error(t, err)
		require.NotNil(t, jw) // Function returns struct even on error
	})
}

func Test_JSONWriter(t *testing.T) {
	tests := []struct {
		name           string
		jsonSchema     string
		writeData      []any // Can be string or []byte
		expectWriteErr bool
		expectStopErr  bool
		expectRows     *int64                                                // nil means don't check rows
		minBufSize     int                                                   // minimum buffer size after write
		customTest     func(t *testing.T, jw *JSONWriter, buf *bytes.Buffer) // For special test cases
	}{
		{
			name: "simple_data",
			jsonSchema: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
					{"Tag": "name=age, type=INT32"},
					{"Tag": "name=active, type=BOOLEAN"}
				]
			}`,
			writeData: []any{
				`{"name": "Alice", "age": 25, "active": true}`,
				`{"name": "Bob", "age": 30, "active": false}`,
				`{"name": "Charlie", "age": 35, "active": true}`,
			},
			expectWriteErr: false,
			expectStopErr:  false,
			expectRows:     func() *int64 { r := int64(3); return &r }(),
			minBufSize:     4,
		},
		{
			name: "byte_slice_input",
			jsonSchema: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=id, type=INT32"}
				]
			}`,
			writeData: []any{
				[]byte(`{"id": 42}`),
			},
			expectWriteErr: false,
			expectStopErr:  false,
			minBufSize:     4,
		},
		{
			name: "invalid_json_data",
			jsonSchema: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}
				]
			}`,
			writeData: []any{
				`{"name": "Alice"`, // Missing closing brace
			},
			expectWriteErr: false, // Write doesn't validate immediately
			expectStopErr:  true,  // Error occurs during marshaling
			minBufSize:     4,
		},
		{
			name: "empty_data",
			jsonSchema: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}
				]
			}`,
			writeData:      []any{}, // No data
			expectWriteErr: false,
			expectStopErr:  false,
			minBufSize:     4,
		},
		{
			name: "nested_structure",
			jsonSchema: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=user_id, type=INT64"},
					{"Tag": "name=metadata, type=BYTE_ARRAY, convertedtype=UTF8"}
				]
			}`,
			writeData: []any{
				`{"user_id": 123, "metadata": "{\"country\": \"US\", \"city\": \"NYC\"}"}`,
			},
			expectWriteErr: false,
			expectStopErr:  false,
			minBufSize:     4,
		},
		{
			name: "null_values",
			jsonSchema: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
					{"Tag": "name=age, type=INT32, repetitiontype=OPTIONAL"}
				]
			}`,
			writeData: []any{
				`{"name": "Alice", "age": 25}`,
				`{"name": "Bob", "age": null}`,
				`{"name": "Charlie"}`, // Missing age field
			},
			expectWriteErr: false,
			expectStopErr:  false,
			minBufSize:     4,
		},
		{
			name: "write_after_stop",
			jsonSchema: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=id, type=INT32"}
				]
			}`,
			writeData:      []any{},
			expectWriteErr: false,
			expectStopErr:  false,
			minBufSize:     4,
			customTest: func(t *testing.T, jw *JSONWriter, buf *bytes.Buffer) {
				// Stop the writer first
				err := jw.WriteStop()
				require.NoError(t, err)

				// Try to write after stop - should handle gracefully
				_ = jw.Write(`{"id": 1}`)
				// The behavior depends on implementation, just verify no panic
			},
		},
		{
			name: "multiple_stops",
			jsonSchema: `{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=id, type=INT32"}
				]
			}`,
			writeData:      []any{},
			expectWriteErr: false,
			expectStopErr:  false,
			minBufSize:     4,
			customTest: func(t *testing.T, jw *JSONWriter, buf *bytes.Buffer) {
				// Call WriteStop multiple times
				err1 := jw.WriteStop()
				_ = jw.WriteStop() // Second call may error, but shouldn't panic

				// At least the first call should succeed
				require.NoError(t, err1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			jw, err := NewJSONWriterFromWriter(tt.jsonSchema, &buf, 1)
			require.NoError(t, err)

			// Handle custom test cases
			if tt.customTest != nil {
				tt.customTest(t, jw, &buf)
				return
			}

			// Write test data
			var writeErr error
			for _, data := range tt.writeData {
				writeErr = jw.Write(data)
				if tt.expectWriteErr {
					break
				}
				require.NoError(t, writeErr)
			}

			if tt.expectWriteErr {
				require.Error(t, writeErr)
				return
			}

			// Stop the writer
			stopErr := jw.WriteStop()
			if tt.expectStopErr {
				require.Error(t, stopErr)
				return
			}
			require.NoError(t, stopErr)

			// Verify buffer size
			require.Greater(t, buf.Len(), tt.minBufSize)

			// Verify row count if specified
			if tt.expectRows != nil {
				pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
				pr, err := reader.NewParquetReader(pf, nil, 1)
				require.NoError(t, err)

				numRows := pr.GetNumRows()
				require.Equal(t, *tt.expectRows, numRows)

				pr.ReadStop()
				_ = pf.Close()
			}
		})
	}
}
