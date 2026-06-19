package marshal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/schema"
)

func TestMarshalArrowUnknown(t *testing.T) {
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=null_col, type=INT32, logicaltype=UNKNOWN, repetitiontype=OPTIONAL", "Type": "int32"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	t.Run("nil_value_accepted", func(t *testing.T) {
		_, err := MarshalArrow([]any{[]any{nil}}, sch)
		require.NoError(t, err)
	})

	t.Run("non_nil_value_rejected", func(t *testing.T) {
		_, err := MarshalArrow([]any{[]any{int32(42)}}, sch)
		require.Error(t, err)
		require.Contains(t, err.Error(), "UNKNOWN column")
		require.Contains(t, err.Error(), "nil value")
	})
}

func TestMarshalArrow(t *testing.T) {
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
	idColumn := (*result)[sch.GetRootInName()+common.ParGoPathDelimiter+"Id"]
	require.NotNil(t, idColumn)
	if idColumn != nil {
		require.Len(t, idColumn.Values, 3)
		require.Equal(t, int64(1), idColumn.Values[0])
	}
}
