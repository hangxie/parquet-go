package schema

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

func TestSchemaHandler_GetColumnNum(t *testing.T) {
	tests := []struct {
		name         string
		setupHandler func() *SchemaHandler
		expected     int64
	}{
		{
			name: "empty_value_columns",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					ValueColumns: []string{},
				}
			},
			expected: int64(0),
		},
		{
			name: "multiple_value_columns",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					ValueColumns: []string{"col1", "col2", "col3"},
				}
			},
			expected: int64(3),
		},
		{
			name: "single_value_column",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					ValueColumns: []string{"single_col"},
				}
			},
			expected: int64(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sh := tt.setupHandler()

			result := sh.GetColumnNum()

			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSchemaHandler_GetRepetitionType(t *testing.T) {
	tests := []struct {
		name         string
		setupHandler func() *SchemaHandler
		path         []string
		expected     parquet.FieldRepetitionType
		expectError  bool
	}{
		{
			name: "valid_path_required_field",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "test_field",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
					},
					MapIndex: map[string]int32{
						"test_field": 0,
					},
					InPathToExPath: map[string]string{
						"test_field": "test_field",
					},
					ExPathToInPath: map[string]string{
						"test_field": "test_field",
					},
				}
			},
			path:        []string{"test_field"},
			expected:    parquet.FieldRepetitionType_REQUIRED,
			expectError: false,
		},
		{
			name: "valid_path_optional_field",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "test_field",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
						},
					},
					MapIndex: map[string]int32{
						"test_field": 0,
					},
					InPathToExPath: map[string]string{
						"test_field": "test_field",
					},
					ExPathToInPath: map[string]string{
						"test_field": "test_field",
					},
				}
			},
			path:        []string{"test_field"},
			expected:    parquet.FieldRepetitionType_OPTIONAL,
			expectError: false,
		},
		{
			name: "path_not_found",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					MapIndex:       map[string]int32{},
					InPathToExPath: map[string]string{},
					ExPathToInPath: map[string]string{},
				}
			},
			path:        []string{"nonexistent_field"},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sh := tt.setupHandler()

			result, err := sh.GetRepetitionType(tt.path)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSchemaHandler_GetType(t *testing.T) {
	tests := []struct {
		name          string
		setupHandler  func() *SchemaHandler
		path          string
		expectError   bool
		expectedError string
		validateType  func(t *testing.T, resultType reflect.Type)
	}{
		{
			name: "path_not_found",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{},
					MapIndex:       map[string]int32{},
					InPathToExPath: map[string]string{},
					ExPathToInPath: map[string]string{},
				}
			},
			path:        "nonexistent_field",
			expectError: true,
		},
		{
			name: "empty_path",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{},
					MapIndex:       map[string]int32{},
					InPathToExPath: map[string]string{},
					ExPathToInPath: map[string]string{},
				}
			},
			path:        "",
			expectError: true,
		},
		{
			name: "successful_type_retrieval",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "test_field",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Test_field", ExName: "test_field"},
					},
					MapIndex: map[string]int32{
						"test_field": 1,
					},
					InPathToExPath: map[string]string{
						"test_field": "test_field",
					},
					ExPathToInPath: map[string]string{
						"test_field": "test_field",
					},
				}
			},
			path:        "test_field",
			expectError: false,
			validateType: func(t *testing.T, resultType reflect.Type) {
				require.Equal(t, reflect.Int32, resultType.Kind())
			},
		},
		{
			name: "path_conversion_error",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{},
					MapIndex:       map[string]int32{},
					InPathToExPath: map[string]string{},
					ExPathToInPath: map[string]string{},
				}
			},
			path:        "invalid..path..format",
			expectError: true,
		},
		{
			name: "path_not_found_in_map_index",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(0)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
					},
					MapIndex: map[string]int32{
						"existing_field": 0,
					},
					InPathToExPath: map[string]string{
						"existing_field": "existing_field",
					},
					ExPathToInPath: map[string]string{
						"existing_field": "existing_field",
					},
				}
			},
			path:          "nonexistent_field",
			expectError:   true,
			expectedError: "path not found: nonexistent_field",
		},
		{
			name: "complex_nested_type_retrieval",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "list_field",
							ConvertedType:  common.ToPtr(parquet.ConvertedType_LIST),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "List",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "Element",
							Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "List_field", ExName: "list_field"},
						{InName: "List", ExName: "list"},
						{InName: "Element", ExName: "element"},
					},
					MapIndex: map[string]int32{
						"list_field": 1,
					},
					InPathToExPath: map[string]string{
						"list_field": "list_field",
					},
					ExPathToInPath: map[string]string{
						"list_field": "list_field",
					},
				}
			},
			path:        "list_field",
			expectError: false,
			validateType: func(t *testing.T, resultType reflect.Type) {
				// Should be *[]string
				require.Equal(t, reflect.Ptr, resultType.Kind())
				require.Equal(t, reflect.Slice, resultType.Elem().Kind())
				require.Equal(t, reflect.String, resultType.Elem().Elem().Kind())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sh := tt.setupHandler()

			resultType, err := sh.GetType(tt.path)

			if tt.expectError {
				require.Error(t, err)
				if tt.expectedError != "" {
					require.Equal(t, tt.expectedError, err.Error())
				}
			} else {
				require.NoError(t, err)
				if tt.validateType != nil {
					tt.validateType(t, resultType)
				}
			}
		})
	}
}

// Before the fix in schema/gettype.go, this scenario would panic inside
// reflect.StructOf because a child field had a nil reflect.Type (leaf with
// missing/unknown physical type). Now it should return an error instead of
// panicking or silently accepting corrupt schema.
// merged scenario: malformed leaf type should not panic GetTypes and should
// fall back to interface{} for the leaf reflect type

func TestSchemaHandler_GetTypes(t *testing.T) {
	tests := []struct {
		name          string
		setupHandler  func() *SchemaHandler
		expectedCount int
		validateTypes func(t *testing.T, types []reflect.Type)
		expectPanic   bool
	}{
		{
			name: "malformed_leaf_falls_back_to_interface",
			setupHandler: func() *SchemaHandler {
				elements := []*parquet.SchemaElement{
					{
						Name:           "root",
						NumChildren:    common.ToPtr(int32(1)),
						RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
					},
					{
						Name:           "bad_group",
						NumChildren:    common.ToPtr(int32(1)),
						RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
					},
					{
						Name: "bad_leaf",
						// Physical type intentionally nil to simulate corrupt schema
						NumChildren:    common.ToPtr(int32(0)),
						RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
					},
				}
				return NewSchemaHandlerFromSchemaList(elements)
			},
			expectedCount: 3,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Index 0: root struct, 1: bad_group struct, 2: bad_leaf -> interface{}
				require.Equal(t, reflect.Struct, types[0].Kind())
				require.Equal(t, reflect.Struct, types[1].Kind())
				require.Equal(t, reflect.Interface, types[2].Kind())
			},
		},
		{
			name: "empty_schema",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{},
				}
			},
			expectedCount: 0,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				require.NotNil(t, types)
			},
		},
		{
			name: "nil_schema_elements",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: nil,
				}
			},
			expectedCount: 0,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				require.NotNil(t, types)
			},
		},
		{
			name: "simple_primitive_types",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(3)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "bool_field",
							Type:           common.ToPtr(parquet.Type_BOOLEAN),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "int32_field",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "string_field",
							Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
							ConvertedType:  common.ToPtr(parquet.ConvertedType_UTF8),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Bool_field", ExName: "bool_field"},
						{InName: "Int32_field", ExName: "int32_field"},
						{InName: "String_field", ExName: "string_field"},
					},
				}
			},
			expectedCount: 4,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Check root struct type
				rootType := types[0]
				require.Equal(t, reflect.Struct, rootType.Kind())
				require.Equal(t, 3, rootType.NumField())

				// Check bool field (required)
				boolType := types[1]
				require.Equal(t, reflect.Bool, boolType.Kind())

				// Check int32 field (optional -> pointer)
				int32Type := types[2]
				require.Equal(t, reflect.Ptr, int32Type.Kind())
				require.Equal(t, reflect.Int32, int32Type.Elem().Kind())

				// Check string field (repeated -> slice)
				stringType := types[3]
				require.Equal(t, reflect.Slice, stringType.Kind())
				require.Equal(t, reflect.String, stringType.Elem().Kind())
			},
		},
		{
			name: "list_type_optional",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "list_field",
							ConvertedType:  common.ToPtr(parquet.ConvertedType_LIST),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "List",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "Element",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "List_field", ExName: "list_field"},
						{InName: "List", ExName: "list"},
						{InName: "Element", ExName: "element"},
					},
				}
			},
			expectedCount: 4,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Check list field type (should be *[]int32)
				listType := types[1]
				require.Equal(t, reflect.Ptr, listType.Kind())
				require.Equal(t, reflect.Slice, listType.Elem().Kind())
				require.Equal(t, reflect.Int32, listType.Elem().Elem().Kind())
			},
		},
		{
			name: "map_type_required",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "map_field",
							ConvertedType:  common.ToPtr(parquet.ConvertedType_MAP),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "Key_value",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(2)),
						},
						{
							Name:           "Key",
							Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "Value",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Map_field", ExName: "map_field"},
						{InName: "Key_value", ExName: "key_value"},
						{InName: "Key", ExName: "key"},
						{InName: "Value", ExName: "value"},
					},
				}
			},
			expectedCount: 5,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Check map field type (should be map[string]int32)
				mapType := types[1]
				require.Equal(t, reflect.Map, mapType.Kind())
				require.Equal(t, reflect.String, mapType.Key().Kind())
				require.Equal(t, reflect.Int32, mapType.Elem().Kind())
			},
		},
		{
			name: "map_type_optional",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "map_field",
							ConvertedType:  common.ToPtr(parquet.ConvertedType_MAP),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "Key_value",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(2)),
						},
						{
							Name:           "Key",
							Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "Value",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Map_field", ExName: "map_field"},
						{InName: "Key_value", ExName: "key_value"},
						{InName: "Key", ExName: "key"},
						{InName: "Value", ExName: "value"},
					},
				}
			},
			expectedCount: 5,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Check map field type (should be *map[string]int32)
				mapType := types[1]
				require.Equal(t, reflect.Ptr, mapType.Kind())
				require.Equal(t, reflect.Map, mapType.Elem().Kind())
				require.Equal(t, reflect.String, mapType.Elem().Key().Kind())
				require.Equal(t, reflect.Int32, mapType.Elem().Elem().Kind())
			},
		},
		{
			name: "nested_struct_required",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "nested_struct",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(2)),
						},
						{
							Name:           "field1",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "field2",
							Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Nested_struct", ExName: "nested_struct"},
						{InName: "Field1", ExName: "field1"},
						{InName: "Field2", ExName: "field2"},
					},
				}
			},
			expectedCount: 4,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Check nested struct type
				structType := types[1]
				require.Equal(t, reflect.Struct, structType.Kind())
				require.Equal(t, 2, structType.NumField())

				// Check struct fields
				field1 := structType.Field(0)
				require.Equal(t, "Field1", field1.Name)
				require.Equal(t, reflect.Int32, field1.Type.Kind())
				require.Equal(t, reflect.StructTag(`json:"field1"`), field1.Tag)
			},
		},
		{
			name: "nested_struct_optional",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "nested_struct",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "field1",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Nested_struct", ExName: "nested_struct"},
						{InName: "Field1", ExName: "field1"},
					},
				}
			},
			expectedCount: 3,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Check nested struct type (should be pointer to struct)
				structType := types[1]
				require.Equal(t, reflect.Ptr, structType.Kind())
				require.Equal(t, reflect.Struct, structType.Elem().Kind())
			},
		},
		{
			name: "nested_struct_repeated",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "nested_struct",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "field1",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Nested_struct", ExName: "nested_struct"},
						{InName: "Field1", ExName: "field1"},
					},
				}
			},
			expectedCount: 3,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Check nested struct type (should be slice of struct)
				structType := types[1]
				require.Equal(t, reflect.Slice, structType.Kind())
				require.Equal(t, reflect.Struct, structType.Elem().Kind())
			},
		},
		{
			name: "single_field",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "single_field",
							Type:           common.ToPtr(parquet.Type_BOOLEAN),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Single_field", ExName: "single_field"},
					},
				}
			},
			expectedCount: 1,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				require.Equal(t, reflect.Bool, types[0].Kind())
			},
		},
		{
			name: "malformed_list_structure",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "fake_list",
							ConvertedType:  common.ToPtr(parquet.ConvertedType_LIST),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "NotList", // Wrong name (should be "List")
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "Element",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Fake_list", ExName: "fake_list"},
						{InName: "NotList", ExName: "notlist"},
						{InName: "Element", ExName: "element"},
					},
				}
			},
			expectedCount: 4,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Should fall back to struct type instead of list
				fakeListType := types[1]
				require.Equal(t, reflect.Struct, fakeListType.Kind())
			},
		},
		{
			name: "malformed_map_structure",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "fake_map",
							ConvertedType:  common.ToPtr(parquet.ConvertedType_MAP),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "NotKeyValue", // Wrong name (should be "Key_value")
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(2)),
						},
						{
							Name:           "Key",
							Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "Value",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Fake_map", ExName: "fake_map"},
						{InName: "NotKeyValue", ExName: "notkeyvalue"},
						{InName: "Key", ExName: "key"},
						{InName: "Value", ExName: "value"},
					},
				}
			},
			expectedCount: 5,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Should fall back to struct type instead of map
				fakeMapType := types[1]
				require.Equal(t, reflect.Struct, fakeMapType.Kind())
			},
		},
		{
			name: "deeply_nested_structure",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "level1",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "level2",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "level3",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Level1", ExName: "level1"},
						{InName: "Level2", ExName: "level2"},
						{InName: "Level3", ExName: "level3"},
					},
				}
			},
			expectedCount: 4,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Check root structure
				rootType := types[0]
				require.Equal(t, reflect.Struct, rootType.Kind())

				// Check nested structures
				level1Type := types[1]
				require.Equal(t, reflect.Struct, level1Type.Kind())

				level2Type := types[2]
				require.Equal(t, reflect.Struct, level2Type.Kind())

				// Check leaf type
				level3Type := types[3]
				require.Equal(t, reflect.Int32, level3Type.Kind())
			},
		},
		{
			name: "list_type_required",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "list_field",
							ConvertedType:  common.ToPtr(parquet.ConvertedType_LIST),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "List",
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(1)),
						},
						{
							Name:           "Element",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "List_field", ExName: "list_field"},
						{InName: "List", ExName: "list"},
						{InName: "Element", ExName: "element"},
					},
				}
			},
			expectedCount: 4,
			validateTypes: func(t *testing.T, types []reflect.Type) {
				// Check list field type (should be []int32)
				listType := types[1]
				require.Equal(t, reflect.Slice, listType.Kind())
				require.Equal(t, reflect.Int32, listType.Elem().Kind())
			},
		},
		{
			name: "nil_repetition_type",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "field1",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: nil, // nil repetition type - this causes panic
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Field1", ExName: "field1"},
					},
				}
			},
			expectedCount: 2, // root and field1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sh := tt.setupHandler()

			types := sh.GetTypes()
			require.Equal(t, tt.expectedCount, len(types))
			if tt.validateTypes != nil {
				tt.validateTypes(t, types)
			}
		})
	}
}

func TestSchemaHandler_Caching(t *testing.T) {
	tests := []struct {
		name         string
		setupHandler func() *SchemaHandler
		validateFunc func(t *testing.T, sh *SchemaHandler)
	}{
		{
			name: "cache_children_map_after_first_GetTypes_call",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(2)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "field1",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "field2",
							Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Field1", ExName: "field1"},
						{InName: "Field2", ExName: "field2"},
					},
				}
			},
			validateFunc: func(t *testing.T, sh *SchemaHandler) {
				// Initially, childrenMap should be nil
				require.Nil(t, sh.childrenMap)

				// First call to GetTypes should build and cache the childrenMap
				types1 := sh.GetTypes()
				require.NotNil(t, sh.childrenMap)
				require.Equal(t, 3, len(types1))

				// Store the cached childrenMap
				cachedMap := sh.childrenMap

				// Second call to GetTypes should reuse the cached childrenMap
				types2 := sh.GetTypes()
				require.Equal(t, cachedMap, sh.childrenMap)
				require.Equal(t, types1, types2)
			},
		},
		{
			name: "cache_element_types_after_first_GetTypes_call",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "field1",
							Type:           common.ToPtr(parquet.Type_INT64),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Field1", ExName: "field1"},
					},
				}
			},
			validateFunc: func(t *testing.T, sh *SchemaHandler) {
				// Initially, elementTypes should be nil
				require.Nil(t, sh.elementTypes)

				// First call to GetTypes should compute and cache elementTypes
				types1 := sh.GetTypes()
				require.NotNil(t, sh.elementTypes)
				require.Equal(t, 2, len(types1))

				// Store the cached elementTypes
				cachedTypes := sh.elementTypes

				// Second call to GetTypes should return the cached elementTypes immediately
				types2 := sh.GetTypes()
				require.Equal(t, cachedTypes, sh.elementTypes)
				require.Equal(t, types1, types2)
			},
		},
		{
			name: "GetType_uses_cached_children_map",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(1)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "test_field",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Test_field", ExName: "test_field"},
					},
					MapIndex: map[string]int32{
						"test_field": 1,
					},
					InPathToExPath: map[string]string{
						"test_field": "test_field",
					},
					ExPathToInPath: map[string]string{
						"test_field": "test_field",
					},
				}
			},
			validateFunc: func(t *testing.T, sh *SchemaHandler) {
				// Call GetTypes first to populate the cache
				_ = sh.GetTypes()
				require.NotNil(t, sh.childrenMap)
				cachedMap := sh.childrenMap

				// Call GetType - it should use the cached childrenMap
				typ, err := sh.GetType("test_field")
				require.NoError(t, err)
				require.NotNil(t, typ)
				require.Equal(t, reflect.Int32, typ.Kind())

				// Verify the cache is still the same
				require.Equal(t, cachedMap, sh.childrenMap)
			},
		},
		{
			name: "buildChildrenMap_creates_correct_parent_child_relationships",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(2)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "parent1",
							NumChildren:    common.ToPtr(int32(2)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "child1",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "child2",
							Type:           common.ToPtr(parquet.Type_INT64),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "parent2",
							Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Parent1", ExName: "parent1"},
						{InName: "Child1", ExName: "child1"},
						{InName: "Child2", ExName: "child2"},
						{InName: "Parent2", ExName: "parent2"},
					},
				}
			},
			validateFunc: func(t *testing.T, sh *SchemaHandler) {
				// Call GetTypes to trigger buildChildrenMap
				_ = sh.GetTypes()

				// Verify the children map structure
				require.NotNil(t, sh.childrenMap)
				require.Equal(t, 5, len(sh.childrenMap))

				// Root (index 0) should have children at indices 1 and 4
				require.ElementsMatch(t, []int32{1, 4}, sh.childrenMap[0])

				// Parent1 (index 1) should have children at indices 2 and 3
				require.ElementsMatch(t, []int32{2, 3}, sh.childrenMap[1])

				// child1 (index 2) should have no children
				require.Empty(t, sh.childrenMap[2])

				// child2 (index 3) should have no children
				require.Empty(t, sh.childrenMap[3])

				// parent2 (index 4) should have no children
				require.Empty(t, sh.childrenMap[4])
			},
		},
		{
			name: "multiple_GetType_calls_reuse_cache",
			setupHandler: func() *SchemaHandler {
				return &SchemaHandler{
					SchemaElements: []*parquet.SchemaElement{
						{
							Name:           "root",
							NumChildren:    common.ToPtr(int32(3)),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
						},
						{
							Name:           "field1",
							Type:           common.ToPtr(parquet.Type_INT32),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "field2",
							Type:           common.ToPtr(parquet.Type_INT64),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
							NumChildren:    common.ToPtr(int32(0)),
						},
						{
							Name:           "field3",
							Type:           common.ToPtr(parquet.Type_BYTE_ARRAY),
							RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED),
							NumChildren:    common.ToPtr(int32(0)),
						},
					},
					Infos: []*common.Tag{
						{InName: "Root", ExName: "root"},
						{InName: "Field1", ExName: "field1"},
						{InName: "Field2", ExName: "field2"},
						{InName: "Field3", ExName: "field3"},
					},
					MapIndex: map[string]int32{
						"field1": 1,
						"field2": 2,
						"field3": 3,
					},
					InPathToExPath: map[string]string{
						"field1": "field1",
						"field2": "field2",
						"field3": "field3",
					},
					ExPathToInPath: map[string]string{
						"field1": "field1",
						"field2": "field2",
						"field3": "field3",
					},
				}
			},
			validateFunc: func(t *testing.T, sh *SchemaHandler) {
				// First GetTypes call
				_ = sh.GetTypes()
				require.NotNil(t, sh.childrenMap)
				cachedMap := sh.childrenMap

				// Multiple GetType calls
				typ1, err := sh.GetType("field1")
				require.NoError(t, err)
				require.Equal(t, reflect.Int32, typ1.Kind())
				require.Equal(t, cachedMap, sh.childrenMap)

				typ2, err := sh.GetType("field2")
				require.NoError(t, err)
				require.Equal(t, reflect.Ptr, typ2.Kind())
				require.Equal(t, reflect.Int64, typ2.Elem().Kind())
				require.Equal(t, cachedMap, sh.childrenMap)

				typ3, err := sh.GetType("field3")
				require.NoError(t, err)
				require.Equal(t, reflect.Slice, typ3.Kind())
				require.Equal(t, cachedMap, sh.childrenMap)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sh := tt.setupHandler()
			tt.validateFunc(t, sh)
		})
	}
}
