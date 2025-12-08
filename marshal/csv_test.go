package marshal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/schema"
)

func TestMarshalCSV(t *testing.T) {
	// Create a simple schema for CSV data
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
			{"Tag": "name=age, type=INT32", "Type": "int32"},
			{"Tag": "name=score, type=FLOAT", "Type": "float32"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Test with empty records
	t.Run("empty-records", func(t *testing.T) {
		emptyRecords := []any{}
		result, err := MarshalCSV(emptyRecords, sch)
		require.NoError(t, err)
		require.Len(t, *result, 0)
	})

	// Test with inconsistent number of fields in records
	t.Run("field-count", func(t *testing.T) {
		records := []any{
			[]any{"foo", 12, 34.56},
			[]any{"bar"},
		}
		_, err := MarshalCSV(records, sch)
		require.Error(t, err)
		require.Contains(t, err.Error(), "row 1 has less than 3 fields")
	})

	// Test with actual data
	t.Run("all-good", func(t *testing.T) {
		records := []any{
			[]any{"Alice", int32(25), float32(95.5)},
			[]any{"Bob", int32(30), float32(87.2)},
			[]any{"Charlie", int32(35), float32(92.1)},
		}

		result, err := MarshalCSV(records, sch)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Check that we have the expected number of columns
		expectedColumns := 3
		require.Len(t, *result, expectedColumns)

		// Check specific column data
		nameColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Name"]
		require.NotNil(t, nameColumn)
		require.Len(t, nameColumn.Values, 3)
		require.Equal(t, "Alice", nameColumn.Values[0])
	})
}
