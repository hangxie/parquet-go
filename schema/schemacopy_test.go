package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestNewSchemaHandlerFromSchemaHandlerWithError(t *testing.T) {
	tests := []struct {
		name    string
		handler *SchemaHandler
		errMsg  string
	}{
		{name: "nil", errMsg: "schema handler is nil"},
		{name: "empty", handler: &SchemaHandler{}, errMsg: "empty schema"},
		{name: "nil-element", handler: &SchemaHandler{SchemaElements: []*parquet.SchemaElement{nil}}, errMsg: "element 0 is nil"},
		{name: "missing-child", handler: &SchemaHandler{SchemaElements: []*parquet.SchemaElement{{Name: "root", NumChildren: common.ToPtr(int32(1))}}}, errMsg: "unfulfilled child"},
		{name: "trailing-element", handler: &SchemaHandler{SchemaElements: []*parquet.SchemaElement{{Name: "root"}, {Name: "extra"}}}, errMsg: "outside the root subtree"},
		{name: "missing-infos", handler: &SchemaHandler{SchemaElements: []*parquet.SchemaElement{{Name: "root"}}}, errMsg: "schema info count"},
		{name: "nil-info", handler: &SchemaHandler{SchemaElements: []*parquet.SchemaElement{{Name: "root"}}, Infos: []*common.Tag{nil}}, errMsg: "schema info 0 is nil"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sh, err := NewSchemaHandlerFromSchemaHandlerWithError(tt.handler)
			require.ErrorContains(t, err, tt.errMsg)
			require.Nil(t, sh)
		})
	}
	original, err := NewSchemaHandlerFromStruct(new(struct {
		Value int32 `parquet:"name=value, type=INT32"`
	}))
	require.NoError(t, err)
	copied, err := NewSchemaHandlerFromSchemaHandlerWithError(original)
	require.NoError(t, err)
	require.Equal(t, NewSchemaHandlerFromSchemaHandler(original), copied)
	require.NotSame(t, original.Infos[1], copied.Infos[1])
}

func TestNewSchemaHandlerFromSchemaHandler(t *testing.T) {
	// Create an original schema using NewSchemaHandlerFromStruct for simplicity
	originalSchema, err := NewSchemaHandlerFromStruct(new(struct {
		Field1 int32  `parquet:"name=field1, type=INT32"`
		Field2 string `parquet:"name=field2, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	}))
	require.NoError(t, err)

	// Test NewSchemaHandlerFromSchemaHandler
	schema2 := NewSchemaHandlerFromSchemaHandler(originalSchema)

	// Verify that the schema was copied correctly
	require.Equal(t, len(originalSchema.SchemaElements), len(schema2.SchemaElements))
	require.Equal(t, common.ParGoRootInName, schema2.SchemaElements[0].Name)
	require.Equal(t, "Field1", schema2.SchemaElements[1].Name)
	require.Equal(t, "Field2", schema2.SchemaElements[2].Name)

	// Verify the schema elements were deep copied
	require.Equal(t, "REQUIRED", schema2.SchemaElements[1].RepetitionType.String())
	require.Equal(t, "INT32", schema2.SchemaElements[1].Type.String())

	require.Equal(t, "OPTIONAL", schema2.SchemaElements[2].RepetitionType.String())
	require.Equal(t, "BYTE_ARRAY", schema2.SchemaElements[2].Type.String())
	require.Equal(t, "UTF8", schema2.SchemaElements[2].ConvertedType.String())

	// Verify it's a separate copy, not the same reference
	require.NotSame(t, originalSchema, schema2)
	require.NotSame(t, &originalSchema.SchemaElements, &schema2.SchemaElements)
}
