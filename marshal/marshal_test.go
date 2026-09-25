package marshal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/schema"
)

type marshalCases struct {
	nullPtr    *int //lint:ignore U1000 this is a placeholder for testing
	integerPtr *int
}

func TestMarshalNilPathMapReturnsError(t *testing.T) {
	// Create a simple schema
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	// Manually create a SchemaHandler with nil PathMap to simulate the bug condition
	// This tests the defensive nil check added to Marshal
	schNilPathMap := &schema.SchemaHandler{
		SchemaElements: sch.SchemaElements,
		MapIndex:       sch.MapIndex,
		IndexMap:       sch.IndexMap,
		Infos:          sch.Infos,
		PathMap:        nil, // Intentionally nil to test the defensive check
	}

	// Try to marshal with nil PathMap - should return error, not panic
	data := []any{map[string]any{"name": "test"}}
	_, err = Marshal(data, schNilPathMap)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil PathMap")
}

func TestMarshalMissingNestedGroupKey(t *testing.T) {
	// A record omitting a nested group key must still get nils across that
	// subtree, otherwise its columns fall short and later rows misalign.
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=id, type=INT64", "Type": "int64"},
			{"Tag": "name=info, repetitiontype=OPTIONAL", "Fields": [
				{"Tag": "name=city, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
				{"Tag": "name=zip, type=INT32", "Type": "int32"}
			]}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	data := []any{
		map[string]any{"Id": int64(1), "Info": map[string]any{"City": "NYC", "Zip": int32(10001)}},
		map[string]any{"Id": int64(2)}, // omits the whole "Info" group
		map[string]any{"Id": int64(3), "Info": map[string]any{"City": "LA", "Zip": int32(90001)}},
	}

	res, err := Marshal(data, sch)
	require.NoError(t, err)

	root := common.ParGoRootInName + common.ParGoPathDelimiter
	idTable := (*res)[root+"Id"]
	cityTable := (*res)[root+"Info"+common.ParGoPathDelimiter+"City"]
	zipTable := (*res)[root+"Info"+common.ParGoPathDelimiter+"Zip"]
	require.NotNil(t, idTable)
	require.NotNil(t, cityTable)
	require.NotNil(t, zipTable)

	// Every column holds one entry per record; the missing group is
	// back-filled with nils at the parent definition level.
	require.Equal(t, []any{int64(1), int64(2), int64(3)}, idTable.Values)
	require.Equal(t, []any{"NYC", nil, "LA"}, cityTable.Values)
	require.Equal(t, []any{int32(10001), nil, int32(90001)}, zipTable.Values)
	require.Equal(t, []int32{1, 0, 1}, cityTable.DefinitionLevels)
	require.Equal(t, []int32{1, 0, 1}, zipTable.DefinitionLevels)
}

func TestMarshalNonGroupForGroupKeyErrors(t *testing.T) {
	// A non-group value where a group is expected must error, not drop data
	// silently or panic on a leaf-table lookup for the group path.
	schemaString := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=id, type=INT64", "Type": "int64"},
			{"Tag": "name=info, repetitiontype=OPTIONAL", "Fields": [
				{"Tag": "name=city, type=BYTE_ARRAY, convertedtype=UTF8", "Type": "string"},
				{"Tag": "name=zip, type=INT32", "Type": "int32"}
			]}
		]
	}`

	sch, err := schema.NewSchemaHandlerFromJSON(schemaString)
	require.NoError(t, err)

	tests := []struct {
		name string
		val  any
	}{
		{"string scalar", "invalid"},
		{"int scalar", int32(42)},
		{"byte slice", []byte("invalid")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []any{map[string]any{"Id": int64(1), "Info": tt.val}}
			_, err := Marshal(data, sch)
			require.Error(t, err)
			require.Contains(t, err.Error(), "expected a group")
		})
	}
}

func TestMarshalByteArrayField(t *testing.T) {
	type ByteStruct struct {
		Data []byte `parquet:"name=data, type=BYTE_ARRAY"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(ByteStruct))
	require.NoError(t, err)

	data := []any{
		ByteStruct{Data: []byte{0x01, 0x02, 0x03}},
		ByteStruct{Data: []byte{0xAA, 0xBB}},
	}

	res, err := Marshal(data, sh)
	require.NoError(t, err)
	require.NotNil(t, res)

	table := (*res)[common.ParGoRootInName+common.ParGoPathDelimiter+"Data"]
	require.NotNil(t, table)
	require.Equal(t, 2, len(table.Values))
	// Values should be non-nil strings (BYTE_ARRAY stored as string in parquet)
	require.NotNil(t, table.Values[0])
	require.NotNil(t, table.Values[1])
}

func TestMarshalEmptyContainer(t *testing.T) {
	type SliceStruct struct {
		Items []string `parquet:"name=items, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8, repetitiontype=OPTIONAL"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(SliceStruct))
	require.NoError(t, err)

	data := []any{
		SliceStruct{Items: []string{}}, // empty slice
	}

	res, err := Marshal(data, sh)
	require.NoError(t, err)
	require.NotNil(t, res)

	// With an empty slice, child tables should get nil appended
	for key, table := range *res {
		if key != "" {
			require.Equal(t, 1, len(table.Values))
			require.Nil(t, table.Values[0])
		}
	}
}
