package marshal

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/types"
)

type Student struct {
	Name    string               `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age     int32                `parquet:"name=age, type=INT32"`
	Weight  *int32               `parquet:"name=weight, type=INT32"`
	Classes *map[string][]*Class `parquet:"name=classes, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
}

type Class struct {
	Name     string   `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	ID       *int64   `parquet:"name=id, type=INT64"`
	Required []string `parquet:"name=required, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

func (c Class) String() string {
	id := "nil"
	if c.ID != nil {
		id = fmt.Sprintf("%d", *c.ID)
	}
	res := fmt.Sprintf("{Name:%s, ID:%v, Required:%s}", c.Name, id, fmt.Sprint(c.Required))
	return res
}

func (s Student) String() string {
	weight := "nil"
	if s.Weight != nil {
		weight = fmt.Sprintf("%d", *s.Weight)
	}

	cs := "{"
	for key, classes := range *s.Classes {
		s := string(key) + ":["
		for _, class := range classes {
			s += (*class).String() + ","
		}
		s += "]"
		cs += s
	}
	cs += "}"
	res := fmt.Sprintf("{Name:%s, Age:%d, Weight:%s, Classes:%s}", s.Name, s.Age, weight, cs)
	return res
}

func TestMarshalUnmarshal(t *testing.T) {
	schemaHandler, _ := schema.NewSchemaHandlerFromStruct(new(Student))

	math01ID := int64(1)
	math01 := Class{
		Name:     "Math1",
		ID:       &math01ID,
		Required: make([]string, 0),
	}

	math02ID := int64(2)
	math02 := Class{
		Name:     "Math2",
		ID:       &math02ID,
		Required: make([]string, 0),
	}
	math02.Required = append(math02.Required, "Math01")

	physics := Class{
		Name:     "Physics",
		ID:       nil,
		Required: make([]string, 0),
	}
	physics.Required = append(physics.Required, "Math01", "Math02")

	weight01 := int32(60)
	stu01Class := make(map[string][]*Class)
	stu01Class["Science"] = make([]*Class, 0)
	stu01Class["Science"] = append(stu01Class["Science"], &math01, &math02)
	stu01 := Student{
		Name:    "zxt",
		Age:     18,
		Weight:  &weight01,
		Classes: &stu01Class,
	}

	stu02Class := make(map[string][]*Class)
	stu02Class["Science"] = make([]*Class, 0)
	stu02Class["Science"] = append(stu02Class["Science"], &physics)
	stu02 := Student{
		Name:    "tong",
		Age:     29,
		Weight:  nil,
		Classes: &stu02Class,
	}

	stus := make([]any, 0)
	stus = append(stus, stu01, stu02)

	src, _ := Marshal(stus, schemaHandler)

	for range *src {
		// Iteration over marshaled data for testing
	}

	dst := make([]Student, 0)
	// return error till we can change function signature
	_ = Unmarshal(src, 0, len(stus), &dst, schemaHandler, "")

	s0 := fmt.Sprint(stus)
	s1 := fmt.Sprint(dst)
	require.Equal(t, s0, s1)
}

func TestUnmarshal_PanicZeroValue(t *testing.T) {
	type TestStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(TestStruct))
	require.NoError(t, err)

	tableMap := map[string]*layout.Table{
		"name": {
			Path:             []string{"name"}, // Start with valid field, but we'll simulate corruption
			Values:           []interface{}{"test_value"},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{0},
		},
	}
	dst := (*any)(nil)
	err = Unmarshal(&tableMap, 0, 1, dst, schemaHandler, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "dstInterface must be a non-nil pointer")
}

func TestConvertToJSONFriendly_Combined(t *testing.T) {
	type DecimalStruct struct {
		Decimal1 int32  `parquet:"name=decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
		Decimal2 int64  `parquet:"name=decimal2, type=INT64, convertedtype=DECIMAL, scale=3, precision=18"`
		Decimal3 string `parquet:"name=decimal3, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=4, precision=20"`
		Decimal4 string `parquet:"name=decimal4, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12"`
		Decimal5 int32  `parquet:"name=decimal5, type=INT32, logicaltype=DECIMAL, logicaltype.precision=9, logicaltype.scale=2"`
		Name     string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Age      int32  `parquet:"name=age, type=INT32"`
	}
	decimalSchemaHandler, err := schema.NewSchemaHandlerFromStruct(new(DecimalStruct))
	require.NoError(t, err)

	type NestedStruct struct {
		Value int32 `parquet:"name=value, type=INT32, convertedtype=DECIMAL, scale=2, precision=5"`
	}
	type ContainerStruct struct {
		Nested   NestedStruct     `parquet:"name=nested"`
		SliceVal []int32          `parquet:"name=slice_val, type=LIST, valuetype=INT32"`
		MapVal   map[string]int32 `parquet:"name=map_val, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
		PtrVal   *int32           `parquet:"name=ptr_val, type=INT32, convertedtype=DECIMAL, scale=1, precision=4"`
	}
	containerSchemaHandler, err := schema.NewSchemaHandlerFromStruct(new(ContainerStruct))
	require.NoError(t, err)

	type SimpleStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	simpleSchemaHandler, err := schema.NewSchemaHandlerFromStruct(new(SimpleStruct))
	require.NoError(t, err)

	type NestedDecimalStruct struct {
		MapWithDecimals  map[string]int32 `parquet:"name=map_with_decimals, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=5"`
		ListWithDecimals []int32          `parquet:"name=list_with_decimals, type=LIST, valuetype=INT32, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=5"`
	}
	nestedDecimalSchemaHandler, err := schema.NewSchemaHandlerFromStruct(new(NestedDecimalStruct))
	require.NoError(t, err)

	ptrValue := int32(1234)

	tests := []struct {
		name          string
		input         any
		schemaHandler *schema.SchemaHandler
		expected      any
	}{
		{
			name: "decimal_struct_with_various_types",
			input: DecimalStruct{
				Decimal1: 12345,
				Decimal2: 98765432,
				Decimal3: types.StrIntToBinary("1000000000000000", "BigEndian", 0, true),
				Decimal4: types.StrIntToBinary("9876543210", "BigEndian", 12, true),
				Decimal5: -4444,
				Name:     "TestUser",
				Age:      25,
			},
			schemaHandler: decimalSchemaHandler,
			expected: map[string]any{
				"Decimal1": float64(123.45),
				"Decimal2": float64(98765.432),
				"Decimal3": float64(100000000000.0000), // BYTE_ARRAY now also returns float64
				"Decimal4": float64(98765432.10),       // FIXED_LEN_BYTE_ARRAY now also returns float64
				"Decimal5": float64(-44.44),
				"Name":     "TestUser",
				"Age":      int32(25),
			},
		},
		{
			name: "nested_structures",
			input: ContainerStruct{
				Nested: NestedStruct{
					Value: 9876,
				},
				SliceVal: []int32{100, 200, 300},
				MapVal: map[string]int32{
					"key1": 111,
					"key2": 222,
				},
				PtrVal: &ptrValue,
			},
			schemaHandler: containerSchemaHandler,
			expected: map[string]any{
				"Nested":   map[string]any{"Value": float64(98.76)},
				"SliceVal": []any{int32(100), int32(200), int32(300)},
				"MapVal":   map[string]any{"key1": int32(111), "key2": int32(222)},
				"PtrVal":   float64(123.4),
			},
		},
		{
			name:          "nil_value",
			input:         nil,
			schemaHandler: simpleSchemaHandler,
			expected:      nil,
		},
		{
			name: "slice_of_structs",
			input: []SimpleStruct{
				{Name: "test1"},
				{Name: "test2"},
			},
			schemaHandler: simpleSchemaHandler,
			expected: []any{
				map[string]any{"Name": "test1"},
				map[string]any{"Name": "test2"},
			},
		},
		{
			name: "map_with_string_keys",
			input: map[string]SimpleStruct{
				"key1": {Name: "value1"},
				"key2": {Name: "value2"},
			},
			schemaHandler: simpleSchemaHandler,
			expected: map[string]any{
				"key1": map[string]any{"Name": "value1"},
				"key2": map[string]any{"Name": "value2"},
			},
		},
		{
			name: "map_with_int_keys",
			input: map[int]SimpleStruct{
				5: {Name: "value5"},
				7: {Name: "value7"},
			},
			schemaHandler: simpleSchemaHandler,
			expected: map[string]any{
				"5": map[string]any{"Name": "value5"},
				"7": map[string]any{"Name": "value7"},
			},
		},
		{
			name:          "primitive_int",
			input:         int32(42),
			schemaHandler: simpleSchemaHandler,
			expected:      int32(42),
		},
		{
			name:          "primitive_string",
			input:         "hello",
			schemaHandler: simpleSchemaHandler,
			expected:      "hello",
		},
		{
			name:          "interface_types",
			input:         any(SimpleStruct{Name: "interface_test"}),
			schemaHandler: simpleSchemaHandler,
			expected:      map[string]any{"Name": "interface_test"},
		},
		{
			name:          "nil_interface",
			input:         any(nil),
			schemaHandler: simpleSchemaHandler,
			expected:      nil,
		},
		{
			name:          "pointer_to_struct_through_interface",
			input:         any(&SimpleStruct{Name: "ptr_interface_test"}),
			schemaHandler: simpleSchemaHandler,
			expected:      map[string]any{"Name": "ptr_interface_test"},
		},
		{
			name: "nested_decimals",
			input: NestedDecimalStruct{
				MapWithDecimals: map[string]int32{
					"price1": 12345,
					"price2": -6789,
				},
				ListWithDecimals: []int32{1111, 2222, -3333},
			},
			schemaHandler: nestedDecimalSchemaHandler,
			expected: map[string]any{
				"MapWithDecimals": map[string]any{
					"price1": float64(123.45),
					"price2": float64(-67.89),
				},
				"ListWithDecimals": []any{float64(11.11), float64(22.22), float64(-33.33)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertToJSONFriendly(tt.input, tt.schemaHandler)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGetFieldNameFromTag(t *testing.T) {
	tests := []struct {
		name     string
		field    reflect.StructField
		expected string
	}{
		{
			name: "json_tag_with_name",
			field: reflect.StructField{
				Name: "FieldName",
				Tag:  `json:"json_name"`,
			},
			expected: "json_name",
		},
		{
			name: "json_tag_with_options",
			field: reflect.StructField{
				Name: "FieldName",
				Tag:  `json:"json_name,omitempty"`,
			},
			expected: "json_name",
		},
		{
			name: "json_tag_with_multiple_options",
			field: reflect.StructField{
				Name: "FieldName",
				Tag:  `json:"json_name,omitempty,string"`,
			},
			expected: "json_name",
		},
		{
			name: "json_tag_dash_means_skip",
			field: reflect.StructField{
				Name: "FieldName",
				Tag:  `json:"-"`,
			},
			expected: "FieldName", // Falls back to struct field name
		},
		{
			name: "json_tag_empty_name",
			field: reflect.StructField{
				Name: "FieldName",
				Tag:  `json:",omitempty"`,
			},
			expected: "FieldName", // Falls back to struct field name
		},
		{
			name: "no_json_tag",
			field: reflect.StructField{
				Name: "FieldName",
				Tag:  `parquet:"name=field_name"`,
			},
			expected: "FieldName", // Falls back to struct field name
		},
		{
			name: "empty_tag",
			field: reflect.StructField{
				Name: "FieldName",
				Tag:  "",
			},
			expected: "FieldName", // Falls back to struct field name
		},
		{
			name: "json_tag_empty",
			field: reflect.StructField{
				Name: "FieldName",
				Tag:  `json:""`,
			},
			expected: "FieldName", // Falls back to struct field name
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFieldNameFromTag(tt.field)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertValueToJSONFriendlyWithContext(t *testing.T) {
	type TestStruct struct {
		Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Value int32  `parquet:"name=value, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
	}

	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(TestStruct))
	require.NoError(t, err)

	ctx := &conversionContext{}

	tests := []struct {
		name          string
		input         any
		pathPrefix    string
		expected      any
		expectError   bool
		useInvalidVal bool
	}{
		{
			name:          "invalid_reflect_value",
			input:         nil, // Will be ignored due to useInvalidVal
			pathPrefix:    "",
			expected:      nil,
			expectError:   false,
			useInvalidVal: true,
		},
		{
			name:        "nil_interface",
			input:       (interface{})(nil),
			pathPrefix:  "",
			expected:    nil,
			expectError: false,
		},
		{
			name:        "non_nil_interface",
			input:       interface{}("test_string"),
			pathPrefix:  "",
			expected:    "test_string",
			expectError: false,
		},
		{
			name:        "nil_pointer",
			input:       (*string)(nil),
			pathPrefix:  "",
			expected:    nil,
			expectError: false,
		},
		{
			name: "non_nil_pointer",
			input: func() *string {
				s := "test_string"
				return &s
			}(),
			pathPrefix:  "",
			expected:    "test_string",
			expectError: false,
		},
		{
			name:        "slice_conversion",
			input:       []string{"a", "b", "c"},
			pathPrefix:  "",
			expected:    []any{"a", "b", "c"},
			expectError: false,
		},
		{
			name: "map_conversion",
			input: map[string]int{
				"key1": 1,
				"key2": 2,
			},
			pathPrefix: "",
			expected: map[string]any{
				"key1": 1,
				"key2": 2,
			},
			expectError: false,
		},
		{
			name: "struct_conversion",
			input: TestStruct{
				Name:  "test",
				Value: 1234,
			},
			pathPrefix: "",
			expected: map[string]any{
				"Name":  "test",
				"Value": float64(12.34), // Converted due to decimal type
			},
			expectError: false,
		},
		{
			name:        "primitive_conversion",
			input:       int32(42),
			pathPrefix:  "",
			expected:    int32(42),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var val reflect.Value
			if tt.useInvalidVal {
				// Use invalid reflect value directly
				val = reflect.Value{}
			} else {
				val = reflect.ValueOf(tt.input)
			}

			result, err := convertValueToJSONFriendlyWithContext(val, schemaHandler, tt.pathPrefix, ctx)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestConvertToJSONFriendly_NonDefaultRootName(t *testing.T) {
	// Test the fix for hardcoded "Parquet_go_root" assumption
	// Create a schema with a custom root name to verify the fix works

	type TestStruct struct {
		Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
		Value int32  `parquet:"name=value, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
	}

	// Create schema with custom root name
	customRootSchema, err := schema.NewSchemaHandlerFromStruct(new(TestStruct))
	require.NoError(t, err)

	// Change the root name to test non-default scenario
	customRootSchema.Infos[0].InName = "Custom_Root"
	customRootSchema.Infos[0].ExName = "custom_root"

	// Update the maps to reflect the new root name
	oldMapIndex := make(map[string]int32)
	for k, v := range customRootSchema.MapIndex {
		oldMapIndex[k] = v
	}

	customRootSchema.MapIndex = make(map[string]int32)
	for k, v := range oldMapIndex {
		newKey := strings.Replace(k, "Parquet_go_root", "Custom_Root", 1)
		customRootSchema.MapIndex[newKey] = v
	}

	for k, v := range customRootSchema.IndexMap {
		newValue := strings.Replace(v, "Parquet_go_root", "Custom_Root", 1)
		customRootSchema.IndexMap[k] = newValue
	}

	require.Equal(t, "Custom_Root", customRootSchema.GetRootInName())

	// Test ConvertToJSONFriendly with custom root name
	testData := TestStruct{
		Name:  "TestUser",
		Value: 12345,
	}

	result, err := ConvertToJSONFriendly(testData, customRootSchema)
	require.NoError(t, err)

	expected := map[string]any{
		"Name":  "TestUser",
		"Value": float64(123.45), // Converted due to decimal type
	}
	require.Equal(t, expected, result)
}

func TestConvertValueToJSONFriendlyWithContext_NilCases(t *testing.T) {
	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(struct{}))
	require.NoError(t, err)

	ctx := &conversionContext{}

	tests := []struct {
		name        string
		setupValue  func() reflect.Value
		expected    any
		expectError bool
	}{
		{
			name: "nil_interface_value",
			setupValue: func() reflect.Value {
				var nilInterface interface{} = nil
				return reflect.ValueOf(&nilInterface).Elem()
			},
			expected:    nil,
			expectError: false,
		},
		{
			name: "nil_pointer_value",
			setupValue: func() reflect.Value {
				var nilPtr *string = nil
				return reflect.ValueOf(nilPtr)
			},
			expected:    nil,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := tt.setupValue()
			result, err := convertValueToJSONFriendlyWithContext(val, schemaHandler, "", ctx)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestUnmarshal_ErrorHandling_ReflectionSafety(t *testing.T) {
	// This test verifies that the new error handling and safety checks work properly
	// by testing the unmarshal function with edge cases that could cause panics

	type SimpleTestStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(SimpleTestStruct))
	require.NoError(t, err)

	t.Run("test_graceful_error_handling", func(t *testing.T) {
		// Test that the function doesn't panic on various edge cases
		// The key improvement is that panics should be converted to errors

		// First, test with a properly structured simple case
		src := []SimpleTestStruct{{Name: "test"}}
		marshaled, err := Marshal([]any{src[0]}, schemaHandler)
		require.NoError(t, err)

		dst := make([]SimpleTestStruct, 0)
		err = Unmarshal(marshaled, 0, 1, &dst, schemaHandler, "")
		require.NoError(t, err)
		require.Len(t, dst, 1)
		require.Equal(t, "test", dst[0].Name)
	})

	t.Run("test_nil_destination_handling", func(t *testing.T) {
		// Test nil destination - this should return a clear error, not panic
		src := []SimpleTestStruct{{Name: "test"}}
		marshaled, err := Marshal([]any{src[0]}, schemaHandler)
		require.NoError(t, err)

		err = Unmarshal(marshaled, 0, 1, nil, schemaHandler, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "dstInterface must be a non-nil pointer")
	})
}

func TestUnmarshal_EdgeCases_InvalidValues(t *testing.T) {
	// This test ensures the improved error handling works with edge cases
	type SimpleStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(SimpleStruct))
	require.NoError(t, err)

	t.Run("empty_values_handling", func(t *testing.T) {
		// Test with empty data - this should not panic with the new safety checks
		marshaledData := make([]any, 0)

		marshaled, err := Marshal(marshaledData, schemaHandler)
		require.NoError(t, err)

		dst := make([]SimpleStruct, 0)
		err = Unmarshal(marshaled, 0, 0, &dst, schemaHandler, "") // Note: end=0 for empty data
		require.NoError(t, err)
		require.Len(t, dst, 0)
	})

	t.Run("safety_improvements_verification", func(t *testing.T) {
		// The main improvement is that the code now has extensive error handling
		// and safety checks to prevent panics. This test verifies basic functionality.
		src := []SimpleStruct{{Name: "test"}}
		marshaled, err := Marshal([]any{src[0]}, schemaHandler)
		require.NoError(t, err)

		dst := make([]SimpleStruct, 0)
		err = Unmarshal(marshaled, 0, 1, &dst, schemaHandler, "")
		require.NoError(t, err)
		require.Len(t, dst, 1)
		require.Equal(t, "test", dst[0].Name)
	})
}

func TestUnmarshal_ReflectionSafety_TypeConversion(t *testing.T) {
	// This test focuses on the new safety features added to handle type conversion and reflection errors
	type ConversionTestStruct struct {
		IntField    int32  `parquet:"name=int_field, type=INT32"`
		StringField string `parquet:"name=string_field, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(ConversionTestStruct))
	require.NoError(t, err)

	// Test that the function properly handles various scenarios without panicking
	t.Run("valid_type_conversion", func(t *testing.T) {
		// Test with valid data that should work
		src := []ConversionTestStruct{{IntField: 42, StringField: "test"}}
		marshaled, err := Marshal([]any{src[0]}, schemaHandler)
		require.NoError(t, err)

		dst := make([]ConversionTestStruct, 0)
		err = Unmarshal(marshaled, 0, 1, &dst, schemaHandler, "")
		require.NoError(t, err)
		require.Len(t, dst, 1)
		require.Equal(t, int32(42), dst[0].IntField)
		require.Equal(t, "test", dst[0].StringField)
	})

	t.Run("test_reflection_safety_improvements", func(t *testing.T) {
		// The key improvement in the new code is that it adds extensive error handling
		// and safety checks to prevent panics in reflection operations.
		// This test verifies that the basic functionality still works correctly.

		src := []ConversionTestStruct{
			{IntField: 123, StringField: "hello"},
			{IntField: 456, StringField: "world"},
		}

		// Marshal the data
		marshaledData := make([]any, len(src))
		for i, s := range src {
			marshaledData[i] = s
		}

		marshaled, err := Marshal(marshaledData, schemaHandler)
		require.NoError(t, err)

		// Unmarshal the data back
		dst := make([]ConversionTestStruct, 0)
		err = Unmarshal(marshaled, 0, len(src), &dst, schemaHandler, "")
		require.NoError(t, err)
		require.Len(t, dst, len(src))

		for i, expected := range src {
			require.Equal(t, expected.IntField, dst[i].IntField)
			require.Equal(t, expected.StringField, dst[i].StringField)
		}
	})
}

// Additional coverage for marshal/unmarshal.go focusing on edge branches
func TestUnmarshal_TypeConversionAndNilHandling(t *testing.T) {
	type Conv struct {
		A int64 `parquet:"name=a, type=INT64"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(Conv))
	require.NoError(t, err)

	// Start with a valid marshal
	src := Conv{A: 123}
	tmap, err := Marshal([]any{src}, sh)
	require.NoError(t, err)

	// Force a type conversion path: replace the value with int32 to trigger reflect.Value.Convert
	// Locate the table whose last path segment is the field name "A"
	var keyForA string
	for k, tbl := range *tmap {
		if len(tbl.Path) > 0 && tbl.Path[len(tbl.Path)-1] == "A" {
			keyForA = k
			break
		}
	}
	require.NotEmpty(t, keyForA)

	tblA := (*tmap)[keyForA]
	require.Len(t, tblA.Values, 1)
	tblA.Values[0] = int32(123) // different type than destination (int64)

	dst := make([]Conv, 0)
	require.NoError(t, Unmarshal(tmap, 0, 1, &dst, sh, ""))
	require.Len(t, dst, 1)
	require.Equal(t, int64(123), dst[0].A)

	// Now exercise the val == nil path in default case; value should be skipped gracefully
	tblA.Values[0] = nil
	dst = make([]Conv, 0)
	require.NoError(t, Unmarshal(tmap, 0, 1, &dst, sh, ""))
	require.Len(t, dst, 1)
	require.Equal(t, int64(0), dst[0].A) // remained zero value
}

func TestUnmarshal_FieldNotFound(t *testing.T) {
	type Pair struct {
		A int32 `parquet:"name=a, type=INT32"`
		B int32 `parquet:"name=b, type=INT32"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(Pair))
	require.NoError(t, err)

	src := Pair{A: 10, B: 20}
	tmap, err := Marshal([]any{src}, sh)
	require.NoError(t, err)

	// Field not found: mutate a table path to a non-existent field name to hit the error branch
	// Find table for A and set its last path segment to a non-existing field
	var keyForA string
	for k, tbl := range *tmap {
		if len(tbl.Path) > 0 && tbl.Path[len(tbl.Path)-1] == "A" {
			keyForA = k
			break
		}
	}
	require.NotEmpty(t, keyForA)

	tblA := (*tmap)[keyForA]
	if len(tblA.Path) > 0 {
		tblA.Path[len(tblA.Path)-1] = "NotExist"
	}

	dst := make([]Pair, 0)
	err = Unmarshal(tmap, 0, 1, &dst, sh, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "field \"NotExist\" not found")
}

func TestUnmarshal_MapAndList_MultipleEntries(t *testing.T) {
	type Combo struct {
		M map[string]int32 `parquet:"name=m, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
		L []int32          `parquet:"name=l, type=LIST, valuetype=INT32"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(Combo))
	require.NoError(t, err)

	src := []Combo{
		{M: map[string]int32{"a": 1, "b": 2}, L: []int32{7, 8}},
		{M: map[string]int32{"x": 9}, L: []int32{10}},
	}
	items := make([]any, len(src))
	for i := range src {
		items[i] = src[i]
	}

	tmap, err := Marshal(items, sh)
	require.NoError(t, err)

	// Unmarshal back and ensure data integrity; this also exercises slice/map stacks flushing
	dst := make([]Combo, 0)
	require.NoError(t, Unmarshal(tmap, 0, len(src), &dst, sh, ""))
	require.Equal(t, src, dst)
}

// Test helper types and functions for old list format testing
type TestTarget [][]int32

type OldListTestStruct struct {
	Array []int32 `json:"array"`
}

// createOldListSchema creates the standard old list schema pattern used across tests
func createOldListSchema() *schema.SchemaHandler {
	schemaElements := []*parquet.SchemaElement{
		{Name: "Parquet_go_root", RepetitionType: nil, NumChildren: common.ToPtr(int32(1))},
		{Name: "array", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED), ConvertedType: common.ToPtr(parquet.ConvertedType_LIST), NumChildren: common.ToPtr(int32(1))},
		{Name: "array", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REPEATED), Type: common.ToPtr(parquet.Type_INT32)},
	}

	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)
	// Set up the critical MapIndex entries for old list detection
	schemaHandler.MapIndex["Parquet_go_root"] = 1
	schemaHandler.MapIndex["Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"array"] = 1
	schemaHandler.MapIndex["Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"array"+common.PAR_GO_PATH_DELIMITER+"array"] = 2

	return schemaHandler
}

// createOldListTable creates a table map for old list testing
func createOldListTable(path []string, values []any, repetitionLevels, definitionLevels []int32) *map[string]*layout.Table {
	pathKey := "Parquet_go_root" + common.PAR_GO_PATH_DELIMITER + strings.Join(path, common.PAR_GO_PATH_DELIMITER)
	return &map[string]*layout.Table{
		pathKey: {
			Path:             path,
			Values:           values,
			RepetitionLevels: repetitionLevels,
			DefinitionLevels: definitionLevels,
		},
	}
}

// createConversionContext creates a standard conversion context for JSON conversion tests
func createConversionContext() *conversionContext {
	return &conversionContext{
		fieldCache: sync.Map{},
	}
}

func TestUnmarshal_MapPathMissingKeyValueComponent(t *testing.T) {
	// Test that a malformed path missing key/value component after map returns an error
	// instead of panicking with index out of bounds
	type MapStruct struct {
		M map[string]int32 `parquet:"name=m, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	}

	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(MapStruct))
	require.NoError(t, err)

	// Create a table with a malformed path that ends at the map without key/value
	// This simulates corrupted data where the path is truncated
	tableMap := &map[string]*layout.Table{
		"Parquet_go_root.M.Key_value": {
			Path:             []string{"Parquet_go_root", "M", "Key_value"}, // Missing "Key" or "Value" after Key_value
			Values:           []any{"test_key"},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{3},
		},
	}

	dst := make([]MapStruct, 0)
	err = Unmarshal(tableMap, 0, 1, &dst, schemaHandler, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing key/value component after map")
}

// Test_OldStyleList targets test old style list
func TestOldStyleList(t *testing.T) {
	t.Run("unmarshal_old_list_multi_inner_segments", func(t *testing.T) {
		schemaHandler := createOldListSchema()

		values := []any{int32(1), int32(2), int32(3), int32(4), int32(5), int32(6), int32(7), int32(8)}
		repetitionLevels := []int32{0, 2, 1, 2, 0, 2, 1, 2, 0, 2, 1, 2}
		definitionLevels := []int32{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
		tableMap := createOldListTable([]string{"Parquet_go_root", "array", "array"}, values, repetitionLevels, definitionLevels)

		result := make([]TestTarget, 0)
		require.NoError(t, Unmarshal(tableMap, 0, 3, &result, schemaHandler, ""))
		require.Len(t, result, 3)
	})

	t.Run("convertStructToJSONFriendly_old_list_format", func(t *testing.T) {
		val := reflect.ValueOf(OldListTestStruct{Array: []int32{1, 2, 3}})
		schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(OldListTestStruct))
		require.NoError(t, err)

		ctx := createConversionContext()

		result, err := convertStructToJSONFriendly(val, schemaHandler, "", ctx)
		require.NoError(t, err)

		// Should return the slice content directly, not wrapped in a map
		sliceResult, ok := result.([]interface{})
		require.True(t, ok)
		require.Len(t, sliceResult, 3)
	})

	t.Run("convertStructToJSONFriendly_with_path_prefix", func(t *testing.T) {
		val := reflect.ValueOf(OldListTestStruct{Array: []int32{42}})
		schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(OldListTestStruct))
		require.NoError(t, err)

		ctx := createConversionContext()

		result, err := convertStructToJSONFriendly(val, schemaHandler, "root.nested", ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
	})

	t.Run("unmarshal_nil_value_handling", func(t *testing.T) {
		type TestStruct struct {
			Value *int32 `parquet:"name=value, type=INT32, repetitiontype=OPTIONAL"`
		}

		schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(TestStruct))
		require.NoError(t, err)

		tableMap := createOldListTable([]string{"value"}, []interface{}{nil}, []int32{0}, []int32{0})
		result := make([]TestStruct, 0)
		err = Unmarshal(tableMap, 0, 1, &result, schemaHandler, "")
		require.NoError(t, err)
	})

	t.Run("convertStructToJSONFriendly_unexported_fields", func(t *testing.T) {
		type MixedFieldStruct struct {
			ExportedField   int32 `json:"exported"`
			unexportedField int32 // This should be skipped
		}

		val := reflect.ValueOf(MixedFieldStruct{
			ExportedField:   42,
			unexportedField: 100,
		})

		schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(MixedFieldStruct))
		require.NoError(t, err)
		ctx := createConversionContext()
		result, err := convertStructToJSONFriendly(val, schemaHandler, "", ctx)
		require.NoError(t, err)

		resultMap, ok := result.(map[string]interface{})
		require.True(t, ok)

		require.Contains(t, resultMap, "exported")
		require.Equal(t, int32(42), resultMap["exported"])
		require.NotContains(t, resultMap, "unexportedField")
	})
}

// Tests for shredded variant reconstruction functions

func TestNewShreddedVariantReconstructor(t *testing.T) {
	// Create schema with variant
	variantInfo := &schema.VariantSchemaInfo{
		MetadataIdx:    2,
		ValueIdx:       3,
		TypedValueIdxs: []int32{4},
		IsShredded:     true,
	}

	sh := &schema.SchemaHandler{
		IndexMap: map[int32]string{
			2: "Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata",
			3: "Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value",
			4: "Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Typed_value",
		},
	}

	metadataTable := &layout.Table{
		Path:             []string{"Root", "Var", "Metadata"},
		Values:           []any{[]byte{0x01, 0x00, 0x00}},
		RepetitionLevels: []int32{0},
		DefinitionLevels: []int32{1},
	}
	valueTable := &layout.Table{
		Path:             []string{"Root", "Var", "Value"},
		Values:           []any{[]byte{0x00}},
		RepetitionLevels: []int32{0},
		DefinitionLevels: []int32{2},
	}
	typedValueTable := &layout.Table{
		Path:             []string{"Root", "Var", "Typed_value"},
		Values:           []any{int32(42)},
		RepetitionLevels: []int32{0},
		DefinitionLevels: []int32{2},
	}

	tableMap := &map[string]*layout.Table{
		"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata":    metadataTable,
		"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value":       valueTable,
		"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Typed_value": typedValueTable,
	}

	t.Run("all_tables_present", func(t *testing.T) {
		r := NewShreddedVariantReconstructor(
			"Root"+common.PAR_GO_PATH_DELIMITER+"Var",
			variantInfo,
			tableMap,
			sh,
		)

		require.NotNil(t, r)
		require.Equal(t, "Root"+common.PAR_GO_PATH_DELIMITER+"Var", r.Path)
		require.NotNil(t, r.MetadataTable)
		require.NotNil(t, r.ValueTable)
		require.Len(t, r.TypedValueTables, 1)
	})

	t.Run("no_value_table", func(t *testing.T) {
		infoNoValue := &schema.VariantSchemaInfo{
			MetadataIdx:    2,
			ValueIdx:       -1, // No value column
			TypedValueIdxs: []int32{4},
			IsShredded:     true,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.PAR_GO_PATH_DELIMITER+"Var",
			infoNoValue,
			tableMap,
			sh,
		)

		require.NotNil(t, r)
		require.NotNil(t, r.MetadataTable)
		require.Nil(t, r.ValueTable)
		require.Len(t, r.TypedValueTables, 1)
	})

	t.Run("missing_tables_in_map", func(t *testing.T) {
		emptyTableMap := &map[string]*layout.Table{}

		r := NewShreddedVariantReconstructor(
			"Root"+common.PAR_GO_PATH_DELIMITER+"Var",
			variantInfo,
			emptyTableMap,
			sh,
		)

		require.NotNil(t, r)
		require.Nil(t, r.MetadataTable)
		require.Nil(t, r.ValueTable)
		require.Empty(t, r.TypedValueTables)
	})
}

func TestShreddedVariantReconstructor_GetValueAtRow(t *testing.T) {
	sh := &schema.SchemaHandler{
		MapIndex: map[string]int32{
			"Root": 0,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var":                                             1,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": 2,
		},
		SchemaElements: []*parquet.SchemaElement{
			{Name: "Root"},
			{Name: "Var", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
			{Name: "Metadata", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
		},
	}

	r := &ShreddedVariantReconstructor{
		SchemaHandler: sh,
	}

	t.Run("nil_table", func(t *testing.T) {
		result := r.getValueAtRow(nil, 0, nil, nil)
		require.Nil(t, result)
	})

	t.Run("missing_table_in_maps", func(t *testing.T) {
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		tableBgn := map[string]int{}
		tableEnd := map[string]int{}

		result := r.getValueAtRow(table, 0, tableBgn, tableEnd)
		require.Nil(t, result)
	})

	t.Run("negative_bgn", func(t *testing.T) {
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		tablePath := common.PathToStr(table.Path)
		tableBgn := map[string]int{tablePath: -1}
		tableEnd := map[string]int{tablePath: 1}

		result := r.getValueAtRow(table, 0, tableBgn, tableEnd)
		require.Nil(t, result)
	})

	t.Run("valid_row_retrieval", func(t *testing.T) {
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}, []byte{0x02, 0x00, 0x00}},
			RepetitionLevels: []int32{0, 0},
			DefinitionLevels: []int32{1, 1},
		}
		tablePath := common.PathToStr(table.Path)
		tableBgn := map[string]int{tablePath: 0}
		tableEnd := map[string]int{tablePath: 2}

		result := r.getValueAtRow(table, 0, tableBgn, tableEnd)
		require.Equal(t, []byte{0x01, 0x00, 0x00}, result)

		result = r.getValueAtRow(table, 1, tableBgn, tableEnd)
		require.Equal(t, []byte{0x02, 0x00, 0x00}, result)
	})

	t.Run("row_not_found", func(t *testing.T) {
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		tablePath := common.PathToStr(table.Path)
		tableBgn := map[string]int{tablePath: 0}
		tableEnd := map[string]int{tablePath: 1}

		result := r.getValueAtRow(table, 5, tableBgn, tableEnd)
		require.Nil(t, result)
	})

	t.Run("null_value_low_definition_level", func(t *testing.T) {
		// When definition level is less than max, value is null
		table := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{nil},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{0}, // Lower than max
		}
		tablePath := common.PathToStr(table.Path)
		tableBgn := map[string]int{tablePath: 0}
		tableEnd := map[string]int{tablePath: 1}

		result := r.getValueAtRow(table, 0, tableBgn, tableEnd)
		require.Nil(t, result)
	})
}

func TestShreddedVariantReconstructor_Reconstruct(t *testing.T) {
	sh := &schema.SchemaHandler{
		MapIndex: map[string]int32{
			"Root": 0,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var":                                                1,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata":    2,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value":       3,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Typed_value": 4,
		},
		IndexMap: map[int32]string{
			0: "Root",
			1: "Root" + common.PAR_GO_PATH_DELIMITER + "Var",
			2: "Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata",
			3: "Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value",
			4: "Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Typed_value",
		},
		SchemaElements: []*parquet.SchemaElement{
			{Name: "Root"},
			{Name: "Var", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
			{Name: "Metadata", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
			{Name: "Value", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
			{Name: "Typed_value", RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL)},
		},
	}

	t.Run("metadata_as_bytes", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		valueTable := &layout.Table{
			Path:             []string{"Root", "Var", "Value"},
			Values:           []any{[]byte{0x00}}, // null variant value
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{2},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": metadataTable,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value":    valueTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.PAR_GO_PATH_DELIMITER+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       3,
				TypedValueIdxs: nil,
				IsShredded:     false,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": 0,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value":    0,
		}
		tableEnd := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": 1,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value":    1,
		}

		variant, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.NotNil(t, variant.Metadata)
		require.Equal(t, []byte{0x01, 0x00, 0x00}, variant.Metadata)
	})

	t.Run("metadata_as_string", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{string([]byte{0x01, 0x00, 0x00})},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": metadataTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.PAR_GO_PATH_DELIMITER+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       -1,
				TypedValueIdxs: nil,
				IsShredded:     false,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": 0,
		}
		tableEnd := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": 1,
		}

		variant, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.NotNil(t, variant.Metadata)
	})

	t.Run("metadata_unexpected_type", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{int32(123)}, // Unexpected type
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": metadataTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.PAR_GO_PATH_DELIMITER+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       -1,
				TypedValueIdxs: nil,
				IsShredded:     false,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": 0,
		}
		tableEnd := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": 1,
		}

		_, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected metadata type")
	})

	t.Run("value_as_string", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		valueTable := &layout.Table{
			Path:             []string{"Root", "Var", "Value"},
			Values:           []any{string([]byte{0x00})},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{2},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": metadataTable,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value":    valueTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.PAR_GO_PATH_DELIMITER+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       3,
				TypedValueIdxs: nil,
				IsShredded:     false,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": 0,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value":    0,
		}
		tableEnd := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata": 1,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Value":    1,
		}

		variant, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.NotNil(t, variant.Value)
	})

	t.Run("with_typed_value", func(t *testing.T) {
		metadataTable := &layout.Table{
			Path:             []string{"Root", "Var", "Metadata"},
			Values:           []any{[]byte{0x01, 0x00, 0x00}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		}
		typedValueTable := &layout.Table{
			Path:             []string{"Root", "Var", "Typed_value"},
			Values:           []any{int32(12345)},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{2},
		}

		tableMap := &map[string]*layout.Table{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata":    metadataTable,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Typed_value": typedValueTable,
		}

		r := NewShreddedVariantReconstructor(
			"Root"+common.PAR_GO_PATH_DELIMITER+"Var",
			&schema.VariantSchemaInfo{
				MetadataIdx:    2,
				ValueIdx:       -1,
				TypedValueIdxs: []int32{4},
				IsShredded:     true,
			},
			tableMap,
			sh,
		)

		tableBgn := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata":    0,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Typed_value": 0,
		}
		tableEnd := map[string]int{
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Metadata":    1,
			"Root" + common.PAR_GO_PATH_DELIMITER + "Var" + common.PAR_GO_PATH_DELIMITER + "Typed_value": 1,
		}

		variant, err := r.Reconstruct(0, tableBgn, tableEnd)
		require.NoError(t, err)
		require.NotNil(t, variant.Value)
	})
}

func TestSetVariantValue(t *testing.T) {
	t.Run("simple_struct_with_variant", func(t *testing.T) {
		type SimpleStruct struct {
			Name string
			Var  types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := SimpleStruct{Name: "test"}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Var", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testStruct.Var)
	})

	t.Run("struct_with_pointer_variant", func(t *testing.T) {
		type StructWithPtrVariant struct {
			Name string
			Var  *types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithPtrVariant{Name: "test"}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Var", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.NotNil(t, testStruct.Var)
		require.Equal(t, variant, *testStruct.Var)
	})

	t.Run("nested_struct_with_variant", func(t *testing.T) {
		type Inner struct {
			Var types.Variant
		}
		type Outer struct {
			Inner Inner
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := Outer{}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Inner"+common.PAR_GO_PATH_DELIMITER+"Var", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testStruct.Inner.Var)
	})

	t.Run("slice_of_structs_with_variant", func(t *testing.T) {
		type ItemWithVariant struct {
			Var types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testSlice := []ItemWithVariant{{}, {}}
		root := reflect.ValueOf(&testSlice).Elem()

		// Create slice record to track slice values
		sliceRecords[root] = &SliceRecord{
			Values: []reflect.Value{
				reflect.ValueOf(&testSlice[0]).Elem(),
				reflect.ValueOf(&testSlice[1]).Elem(),
			},
			Index: 0,
		}

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Var", "", sh, variant, 1, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testSlice[1].Var)
	})

	t.Run("field_not_found", func(t *testing.T) {
		type SimpleStruct struct {
			Name string
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := SimpleStruct{Name: "test"}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"NonExistent", "", sh, variant, 0, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("pointer_initialization", func(t *testing.T) {
		type StructWithPtrNested struct {
			Inner *struct {
				Var types.Variant
			}
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithPtrNested{}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Inner"+common.PAR_GO_PATH_DELIMITER+"Var", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.NotNil(t, testStruct.Inner)
		require.Equal(t, variant, testStruct.Inner.Var)
	})

	t.Run("slice_index_out_of_bounds", func(t *testing.T) {
		type ItemWithVariant struct {
			Var types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testSlice := []ItemWithVariant{{}}
		root := reflect.ValueOf(&testSlice).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		// rowIdx 5 is out of bounds for a slice with 1 element
		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Var", "", sh, variant, 5, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "out of bounds")
	})

	t.Run("byte_slice_error", func(t *testing.T) {
		type StructWithBytes struct {
			Data []byte
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithBytes{Data: []byte{1, 2, 3}}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Data"+common.PAR_GO_PATH_DELIMITER+"Var", "", sh, variant, 0, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "[]byte")
	})

	t.Run("unexpected_kind", func(t *testing.T) {
		type StructWithInt struct {
			Value int
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithInt{Value: 42}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Value"+common.PAR_GO_PATH_DELIMITER+"Var", "", sh, variant, 0, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected kind")
	})

	t.Run("path_end_not_variant_type", func(t *testing.T) {
		type StructWithString struct {
			Name string
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		testStruct := StructWithString{Name: "test"}
		root := reflect.ValueOf(&testStruct).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Name", "", sh, variant, 0, sliceRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not set variant value at path end")
	})

	t.Run("slice_direct_access_without_sliceRecords", func(t *testing.T) {
		type ItemWithVariant struct {
			Var types.Variant
		}

		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord) // Empty - no SliceRecord for this slice

		testSlice := []ItemWithVariant{{}, {}, {}}
		root := reflect.ValueOf(&testSlice).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		// Access row 1 without SliceRecord - should use direct po.Index(rowIdx)
		err := setVariantValue(root, "Parquet_go_root"+common.PAR_GO_PATH_DELIMITER+"Var", "", sh, variant, 1, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testSlice[1].Var)
	})

	t.Run("path_ends_at_variant_type_directly", func(t *testing.T) {
		// Test case where the path ends right at a types.Variant field
		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		// Create a Variant directly
		testVariant := types.Variant{}
		root := reflect.ValueOf(&testVariant).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		// Path is just the root (empty path after prefix)
		err := setVariantValue(root, "Parquet_go_root", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.Equal(t, variant, testVariant)
	})

	t.Run("path_ends_at_pointer_to_variant_directly", func(t *testing.T) {
		// Test case where the path ends at a *types.Variant field
		sh := &schema.SchemaHandler{}
		sliceRecords := make(map[reflect.Value]*SliceRecord)

		var testVariant *types.Variant
		root := reflect.ValueOf(&testVariant).Elem()

		variant := types.Variant{
			Metadata: []byte{0x01, 0x00, 0x00},
			Value:    []byte{0x00},
		}

		// Path is just the root (empty path after prefix)
		err := setVariantValue(root, "Parquet_go_root", "", sh, variant, 0, sliceRecords)
		require.NoError(t, err)
		require.NotNil(t, testVariant)
		require.Equal(t, variant, *testVariant)
	})
}

func TestUnmarshal_ShreddedVariant_NoDuplicateRows(t *testing.T) {
	type VariantRow struct {
		ID      int32         `parquet:"name=id, type=INT32"`
		Variant types.Variant `parquet:"name=variant, type=VARIANT"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(VariantRow))
	require.NoError(t, err)

	// Manually inject VariantSchemas info to simulate a shredded variant.
	// NewSchemaHandlerFromStruct creates "Parquet_go_root", "ID", "Variant", "Variant.Metadata", "Variant.Value"
	variantPath := "Parquet_go_root" + common.PAR_GO_PATH_DELIMITER + "Variant"

	// We verify indices
	var metadataIdx, valueIdx int32
	if idx, ok := sh.MapIndex[variantPath+common.PAR_GO_PATH_DELIMITER+"Metadata"]; ok {
		metadataIdx = idx
	} else {
		t.Fatal("Metadata index not found")
	}
	if idx, ok := sh.MapIndex[variantPath+common.PAR_GO_PATH_DELIMITER+"Value"]; ok {
		valueIdx = idx
	} else {
		t.Fatal("Value index not found")
	}

	sh.VariantSchemas = map[string]*schema.VariantSchemaInfo{
		variantPath: {
			MetadataIdx: metadataIdx,
			ValueIdx:    valueIdx,
			IsShredded:  true, // Mark as shredded to trigger the reconstruction path
		},
	}

	numRows := 10
	idValues := make([]any, numRows)
	// Metadata: version 1, empty dict -> 0x01 0x00 0x00
	metaBytes := []byte{0x01, 0x00, 0x00}
	metaValues := make([]any, numRows)
	// Value: null -> 0x00
	valBytes := []byte{0x00}
	valValues := make([]any, numRows)

	rLs := make([]int32, numRows) // all 0
	dLs := make([]int32, numRows) // all 0 (REQUIRED fields)

	for i := 0; i < numRows; i++ {
		idValues[i] = int32(i)
		metaValues[i] = metaBytes
		valValues[i] = valBytes
		rLs[i] = 0
		dLs[i] = 0
	}

	tableMap := map[string]*layout.Table{
		"Parquet_go_root" + common.PAR_GO_PATH_DELIMITER + "ID": {
			Path:             []string{"Parquet_go_root", "ID"},
			Values:           idValues,
			RepetitionLevels: rLs,
			DefinitionLevels: dLs,
		},
		"Parquet_go_root" + common.PAR_GO_PATH_DELIMITER + "Variant" + common.PAR_GO_PATH_DELIMITER + "Metadata": {
			Path:             []string{"Parquet_go_root", "Variant", "Metadata"},
			Values:           metaValues,
			RepetitionLevels: rLs,
			DefinitionLevels: dLs,
		},
		"Parquet_go_root" + common.PAR_GO_PATH_DELIMITER + "Variant" + common.PAR_GO_PATH_DELIMITER + "Value": {
			Path:             []string{"Parquet_go_root", "Variant", "Value"},
			Values:           valValues,
			RepetitionLevels: rLs,
			DefinitionLevels: dLs,
		},
	}

	dst := make([]VariantRow, 0)
	err = Unmarshal(&tableMap, 0, numRows, &dst, sh, "")
	require.NoError(t, err)
	require.Len(t, dst, numRows, "Row count should match input rows without duplication")

	for i := 0; i < numRows; i++ {
		require.Equal(t, int32(i), dst[i].ID)
		require.Equal(t, metaBytes, dst[i].Variant.Metadata)
		require.Equal(t, valBytes, dst[i].Variant.Value)
	}
}

func TestUnmarshal_ErrorHandling_TypeConversion(t *testing.T) {
	// This test simulates a case where type conversion might fail.
	// We want to ensure that Unmarshal handles this gracefully by returning an error,
	// instead of panicking.

	type DestStruct struct {
		Field int `parquet:"name=field, type=INT64"`
	}

	// Create a schema handler for the destination struct
	sh, err := schema.NewSchemaHandlerFromStruct(new(DestStruct))
	require.NoError(t, err)

	// Create a table map that mimics a valid parquet structure
	// but provides a value that cannot be converted to the destination type
	// in a way that might trigger a panic if not checked.
	type IncompatibleType struct {
		Something string
	}

	tableMap := map[string]*layout.Table{
		"Parquet_go_root.Field": {
			Path:             []string{"Parquet_go_root", "Field"},
			Values:           []interface{}{IncompatibleType{Something: "bad"}},
			RepetitionLevels: []int32{0},
			DefinitionLevels: []int32{1},
		},
	}

	dst := make([]DestStruct, 0)
	err = Unmarshal(&tableMap, 0, 1, &dst, sh, "")

	// We expect an error here, not a panic.
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot convert value of type")
}
