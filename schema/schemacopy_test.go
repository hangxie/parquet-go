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

func TestNewSchemaHandlerFromSchemaHandlerWithErrorNames(t *testing.T) {
	tests := []struct {
		name   string
		infos  []*common.Tag
		errMsg string
	}{
		{name: "empty-column-names", infos: []*common.Tag{{InName: "Root", ExName: "root"}, {}, {InName: "Other", ExName: "other"}}, errMsg: "schema info 1 has empty InName"},
		{name: "empty-column-internal", infos: []*common.Tag{{InName: "Root", ExName: "root"}, {ExName: "value"}, {InName: "Other", ExName: "other"}}, errMsg: "schema info 1 has empty InName"},
		{name: "empty-column-external", infos: []*common.Tag{{InName: "Root", ExName: "root"}, {InName: "Value"}, {InName: "Other", ExName: "other"}}, errMsg: "schema info 1 has empty ExName"},
		{name: "empty-root-internal", infos: []*common.Tag{{ExName: "root"}, {InName: "Value", ExName: "value"}, {InName: "Other", ExName: "other"}}, errMsg: "schema info 0 has empty InName"},
		{name: "empty-root-external", infos: []*common.Tag{{InName: "Root"}, {InName: "Value", ExName: "value"}, {InName: "Other", ExName: "other"}}, errMsg: "schema info 0 has empty ExName"},
		{name: "duplicate-internal", infos: []*common.Tag{{InName: "Root", ExName: "root"}, {InName: "Value", ExName: "value"}, {InName: "Value", ExName: "other"}}, errMsg: "duplicate column name"},
		{name: "duplicate-external", infos: []*common.Tag{{InName: "Root", ExName: "root"}, {InName: "Value", ExName: "value"}, {InName: "Other", ExName: "value"}}, errMsg: "duplicate column name"},
		{name: "valid", infos: []*common.Tag{{InName: "Root", ExName: "root"}, {InName: "Value", ExName: "value"}, {InName: "Other", ExName: "other"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &SchemaHandler{
				SchemaElements: []*parquet.SchemaElement{
					{Name: "root", NumChildren: common.ToPtr(int32(2))},
					{Name: "value", Type: common.ToPtr(parquet.Type_INT32)},
					{Name: "other", Type: common.ToPtr(parquet.Type_INT32)},
				},
				Infos: tt.infos,
			}
			rebuilt, err := NewSchemaHandlerFromSchemaHandlerWithError(original)
			if tt.errMsg != "" {
				require.ErrorContains(t, err, tt.errMsg)
				require.Nil(t, rebuilt)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int32(1), rebuilt.MapIndex[common.PathToStr([]string{"Root", "Value"})])
			require.Equal(t, int32(2), rebuilt.MapIndex[common.PathToStr([]string{"Root", "Other"})])
		})
	}
}

func TestNewSchemaHandlerFromSchemaHandlerWithErrorNamesAcrossGroups(t *testing.T) {
	original, err := NewSchemaHandlerFromStruct(new(struct {
		Left struct {
			Value int32 `parquet:"name=value, type=INT32"`
		} `parquet:"name=left"`
		Right struct {
			Value int32 `parquet:"name=value, type=INT32"`
		} `parquet:"name=right"`
	}))
	require.NoError(t, err)
	rebuilt, err := NewSchemaHandlerFromSchemaHandlerWithError(original)
	require.NoError(t, err)
	require.Len(t, rebuilt.ValueColumns, 2)
	require.NotEqual(t, rebuilt.ValueColumns[0], rebuilt.ValueColumns[1])
}
