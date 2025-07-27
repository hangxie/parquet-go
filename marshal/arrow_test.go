package marshal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/schema"
)

func Test_MarshalArrow(t *testing.T) {
	// Create a simple schema for Arrow data
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=id, type=INT64", "Type": "int64"},
			{"Tag": "name=value, type=FLOAT", "Type": "float32"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Test with empty records
	emptyRecords := []any{}
	result, err := MarshalArrow(emptyRecords, sch)
	require.NoError(t, err)
	require.Len(t, *result, 0)

	// Test with actual data - Arrow format has rows as []any
	records := []any{
		[]any{int64(1), float32(10.5)},
		[]any{int64(2), float32(20.3)},
		[]any{int64(3), float32(30.7)},
	}

	result, err = MarshalArrow(records, sch)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Check that we have the expected number of columns
	expectedColumns := 2
	require.Len(t, *result, expectedColumns)

	// Check specific column data
	idColumn := (*result)[sch.GetRootInName()+common.PAR_GO_PATH_DELIMITER+"Id"]
	require.NotNil(t, idColumn)
	if idColumn != nil {
		require.Len(t, idColumn.Values, 3)
		require.Equal(t, int64(1), idColumn.Values[0])
	}
}
