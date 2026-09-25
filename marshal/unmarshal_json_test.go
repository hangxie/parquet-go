package marshal

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func TestConvertToJSONFriendly_OldListFormat(t *testing.T) {
	// Struct with single exported "Array" field of slice type simulates old list format
	type OldList struct {
		Array []int32 `json:"array"`
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}))
	require.NoError(t, err)

	input := OldList{Array: []int32{1, 2, 3}}
	result, err := ConvertToJSONFriendly(input, sh)
	require.NoError(t, err)

	// Should unwrap to a slice, not a map
	slice, ok := result.([]any)
	require.True(t, ok)
	require.Equal(t, 3, len(slice))
	require.Equal(t, int32(1), slice[0])
	require.Equal(t, int32(2), slice[1])
	require.Equal(t, int32(3), slice[2])
}

func TestConvertToJSONFriendly_UnexportedFields(t *testing.T) {
	type MixedStruct struct {
		Public  string `json:"public"`
		private string //nolint:unused // intentionally unexported for test
	}

	sh, err := schema.NewSchemaHandlerFromStruct(new(struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}))
	require.NoError(t, err)

	input := MixedStruct{Public: "visible"}
	result, err := ConvertToJSONFriendly(input, sh)
	require.NoError(t, err)

	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "public")
	require.Equal(t, "visible", m["public"])
	// Unexported field should not be in the result
	require.NotContains(t, m, "private")
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
				"Decimal1": json.Number("123.45"),
				"Decimal2": json.Number("98765.432"),
				"Decimal3": json.Number("100000000000.0000"),
				"Decimal4": json.Number("98765432.10"),
				"Decimal5": json.Number("-44.44"),
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
				"Nested":   map[string]any{"Value": json.Number("98.76")},
				"SliceVal": []any{int32(100), int32(200), int32(300)},
				"MapVal":   map[string]any{"key1": int32(111), "key2": int32(222)},
				"PtrVal":   json.Number("123.4"),
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
					"price1": json.Number("123.45"),
					"price2": json.Number("-67.89"),
				},
				"ListWithDecimals": []any{json.Number("11.11"), json.Number("22.22"), json.Number("-33.33")},
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

func TestConvertToJSONFriendly_NonFiniteFloat(t *testing.T) {
	type NestedFloat struct {
		Value float64 `parquet:"name=value, type=DOUBLE"`
	}
	type NonFiniteStruct struct {
		FloatVal   float32            `parquet:"name=float_val, type=FLOAT"`
		DoubleVal  float64            `parquet:"name=double_val, type=DOUBLE"`
		Float16Val string             `parquet:"name=float16_val, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16"`
		Nested     NestedFloat        `parquet:"name=nested"`
		ListVal    []float64          `parquet:"name=list_val, type=LIST, valuetype=DOUBLE"`
		MapVal     map[string]float64 `parquet:"name=map_val, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=DOUBLE"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(NonFiniteStruct))
	require.NoError(t, err)

	// FLOAT16 NaN, little-endian bit pattern 0x7e00
	float16NaN := string([]byte{0x00, 0x7e})

	input := NonFiniteStruct{
		FloatVal:   float32(math.NaN()),
		DoubleVal:  math.Inf(1),
		Float16Val: float16NaN,
		Nested:     NestedFloat{Value: math.Inf(-1)},
		ListVal:    []float64{1.5, math.NaN(), math.Inf(1)},
		MapVal:     map[string]float64{"a": math.Inf(-1), "b": 2.5},
	}

	result, err := ConvertToJSONFriendly(input, sh)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"FloatVal":   "NaN",
		"DoubleVal":  "Infinity",
		"Float16Val": "NaN",
		"Nested":     map[string]any{"Value": "-Infinity"},
		"ListVal":    []any{1.5, "NaN", "Infinity"},
		"MapVal":     map[string]any{"a": "-Infinity", "b": 2.5},
	}, result)

	marshaled, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(marshaled), `"FloatVal":"NaN"`)
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

	ctx := &jsonConverter{}
	require.NoError(t, ctx.resolveRootPath(schemaHandler))
	root := schemaHandler.GetRootInName()

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
			// A conversion path names the value from the root down.
			pathPrefix: root,
			expected: map[string]any{
				"Name":  "test",
				"Value": json.Number("12.34"), // Converted due to decimal type
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
		newKey := strings.Replace(k, common.ParGoRootInName, "Custom_Root", 1)
		customRootSchema.MapIndex[newKey] = v
	}

	for k, v := range customRootSchema.IndexMap {
		newValue := strings.Replace(v, common.ParGoRootInName, "Custom_Root", 1)
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
		"Value": json.Number("123.45"), // Converted due to decimal type
	}
	require.Equal(t, expected, result)
}

func TestConvertValueToJSONFriendlyWithContext_NilCases(t *testing.T) {
	schemaHandler, err := schema.NewSchemaHandlerFromStruct(new(struct{}))
	require.NoError(t, err)

	ctx := &jsonConverter{}

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

func TestConvertToJSONFriendly_WithGeospatialConfig(t *testing.T) {
	type GeoRow struct {
		Geom string `parquet:"name=geom, type=BYTE_ARRAY, logicaltype=GEOMETRY"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(GeoRow))
	require.NoError(t, err)

	// WKB Point(100, 50) little-endian
	wkbPoint := string([]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x49, 0x40})

	tests := []struct {
		name     string
		opts     []JSONConvertOption
		expected map[string]any
	}{
		{
			name: "default_uses_hex_mode",
			opts: nil,
			expected: map[string]any{
				"wkb_hex": "010100000000000000000059400000000000004940",
				"crs":     "OGC:CRS84",
			},
		},
		{
			name: "custom_geojson_mode",
			opts: []JSONConvertOption{WithGeospatialConfig(types.NewGeospatialConfig(
				types.WithGeometryJSONMode(types.GeospatialModeGeoJSON),
			))},
			expected: map[string]any{
				"type":       "Feature",
				"geometry":   map[string]any{"type": "Point", "coordinates": []float64{100, 50}},
				"properties": map[string]any{"crs": "OGC:CRS84"},
			},
		},
		{
			name: "custom_base64_mode",
			opts: []JSONConvertOption{WithGeospatialConfig(types.NewGeospatialConfig(
				types.WithGeometryJSONMode(types.GeospatialModeBase64),
			))},
			expected: map[string]any{
				"wkb_b64": "AQEAAAAAAAAAAABZQAAAAAAAAElA",
				"crs":     "OGC:CRS84",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := GeoRow{Geom: wkbPoint}
			result, err := ConvertToJSONFriendly(input, sh, tt.opts...)
			require.NoError(t, err)
			resultMap, ok := result.(map[string]any)
			require.True(t, ok)
			require.Equal(t, tt.expected, resultMap["Geom"])
		})
	}
}

func TestConvertToJSONFriendly_LegacyRepeated(t *testing.T) {
	type RepeatedGroup struct {
		Value float64 `parquet:"name=value, type=DOUBLE"`
	}
	type LegacyRepeated struct {
		Scores  []float64       `parquet:"name=scores, type=DOUBLE, repetitiontype=REPEATED"`
		Days    []int32         `parquet:"name=days, type=INT32, convertedtype=DATE, repetitiontype=REPEATED"`
		Amounts []int32         `parquet:"name=amounts, type=INT32, convertedtype=DECIMAL, scale=2, precision=9, repetitiontype=REPEATED"`
		Groups  []RepeatedGroup `parquet:"name=groups, repetitiontype=REPEATED"`
		// A three-level LIST in the same schema must keep using the List/Element path
		Ratios []float64 `parquet:"name=ratios, type=LIST, valuetype=DOUBLE"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(LegacyRepeated))
	require.NoError(t, err)

	input := LegacyRepeated{
		Scores:  []float64{math.NaN(), math.Inf(1), 1.5},
		Days:    []int32{19000},
		Amounts: []int32{12345},
		Groups:  []RepeatedGroup{{Value: math.Inf(-1)}, {Value: 2.5}},
		Ratios:  []float64{math.NaN(), 0.5},
	}

	result, err := ConvertToJSONFriendly(input, sh)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"Scores":  []any{"NaN", "Infinity", 1.5},
		"Days":    []any{"2022-01-08"},
		"Amounts": []any{json.Number("123.45")},
		"Groups":  []any{map[string]any{"Value": "-Infinity"}, map[string]any{"Value": 2.5}},
		"Ratios":  []any{"NaN", 0.5},
	}, result)

	// The whole point of the conversion: the result must be marshalable
	_, err = json.Marshal(result)
	require.NoError(t, err)
}

func TestListElementPath(t *testing.T) {
	type Nested struct {
		Value float64 `parquet:"name=value, type=DOUBLE"`
	}
	type PathStruct struct {
		Scores []float64 `parquet:"name=scores, type=DOUBLE, repetitiontype=REPEATED"`
		Ratios []float64 `parquet:"name=ratios, type=LIST, valuetype=DOUBLE"`
		Nested Nested    `parquet:"name=nested"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(PathStruct))
	require.NoError(t, err)

	converter := &jsonConverter{}
	require.NoError(t, converter.resolveRootPath(sh))
	root := sh.GetRootInName()
	listPath := func(parts ...string) string {
		return strings.Join(parts, common.ParGoPathDelimiter)
	}
	// A conversion path names the value from the root down.
	fromRoot := func(name string) string { return listPath(root, name) }

	tests := []struct {
		name       string
		pathPrefix string
		expected   string
	}{
		{
			name:       "empty prefix stays empty",
			pathPrefix: "",
			expected:   "",
		},
		{
			name:       "three level list uses List/Element",
			pathPrefix: fromRoot("Ratios"),
			expected:   listPath(root, "Ratios", "List", "Element"),
		},
		{
			name:       "legacy repeated column is its own element",
			pathPrefix: fromRoot("Scores"),
			expected:   fromRoot("Scores"),
		},
		{
			name:       "non-repeated path falls back to List/Element",
			pathPrefix: fromRoot("Nested"),
			expected:   listPath(root, "Nested", "List", "Element"),
		},
		{
			name:       "unknown path falls back to List/Element",
			pathPrefix: fromRoot("Missing"),
			expected:   listPath(root, "Missing", "List", "Element"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			elementPath, err := listElementPath(sh, tt.pathPrefix, converter)
			require.NoError(t, err)
			require.Equal(t, tt.expected, elementPath)
		})
	}
}

// renameSchemaRoot rewrites a schema handler's root name in place, as reading a file whose root
// carries a non-default name would.
func renameSchemaRoot(t *testing.T, sh *schema.SchemaHandler, inName, exName string) {
	t.Helper()

	renamed := make(map[string]int32, len(sh.MapIndex))
	for path, index := range sh.MapIndex {
		renamed[strings.Replace(path, common.ParGoRootInName, inName, 1)] = index
	}
	sh.MapIndex = renamed
	for index, path := range sh.IndexMap {
		sh.IndexMap[index] = strings.Replace(path, common.ParGoRootInName, inName, 1)
	}
	sh.Infos[0].InName, sh.Infos[0].ExName = inName, exName
	require.Equal(t, inName, sh.GetRootInName())
}

// A root name that is a lexical prefix of a field name must not be mistaken for a rooted path.
func TestConvertToJSONFriendly_RootNamePrefixOfField(t *testing.T) {
	type Amounted struct {
		Amounts []int32   `parquet:"name=amounts, type=INT32, convertedtype=DECIMAL, scale=2, precision=9, repetitiontype=REPEATED"`
		Ascores []float64 `parquet:"name=ascores, type=DOUBLE, repetitiontype=REPEATED"`
		Age     int32     `parquet:"name=age, type=INT32, convertedtype=DATE"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(Amounted))
	require.NoError(t, err)

	// "A" is a prefix of every field name above
	renameSchemaRoot(t, sh, "A", "a")

	input := Amounted{
		Amounts: []int32{12345},
		Ascores: []float64{math.NaN(), 1.5},
		Age:     19000,
	}

	result, err := ConvertToJSONFriendly(input, sh)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"Amounts": []any{json.Number("123.45")},
		"Ascores": []any{"NaN", 1.5},
		"Age":     "2022-01-08",
	}, result)

	_, err = json.Marshal(result)
	require.NoError(t, err)
}

// A repeated group whose own fields are named List and Element must not look like a three-level LIST.
func TestConvertToJSONFriendly_RepeatedGroupNamedLikeList(t *testing.T) {
	type ListGroup struct {
		Element float64 `parquet:"name=element, type=DOUBLE"`
	}
	type Group struct {
		List ListGroup `parquet:"name=list"`
	}
	type Decoy struct {
		Groups []Group `parquet:"name=groups, repetitiontype=REPEATED"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(Decoy))
	require.NoError(t, err)

	input := Decoy{Groups: []Group{{List: ListGroup{Element: math.NaN()}}}}

	result, err := ConvertToJSONFriendly(input, sh)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"Groups": []any{map[string]any{"List": map[string]any{"Element": "NaN"}}},
	}, result)

	_, err = json.Marshal(result)
	require.NoError(t, err)
}

// A top-level field with the same name as the root must not be mistaken for the root itself.
func TestConvertToJSONFriendly_RootNameEqualsFieldName(t *testing.T) {
	type Inner struct {
		Value  int32     `parquet:"name=value, type=INT32, convertedtype=DATE"`
		Scores []float64 `parquet:"name=scores, type=DOUBLE, repetitiontype=REPEATED"`
	}
	type Outer struct {
		A Inner `parquet:"name=a"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(Outer))
	require.NoError(t, err)
	renameSchemaRoot(t, sh, "A", "a")

	input := Outer{A: Inner{Value: 19000, Scores: []float64{math.NaN()}}}

	result, err := ConvertToJSONFriendly(input, sh)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"A": map[string]any{
			"Value":  "2022-01-08",
			"Scores": []any{"NaN"},
		},
	}, result)

	_, err = json.Marshal(result)
	require.NoError(t, err)
}

// A MapIndex entry pointing outside SchemaElements means the handler has been mutated apart;
// that is a broken handler rather than a path without conversion, so it must not pass silently.
func TestConvertToJSONFriendly_InconsistentSchemaHandler(t *testing.T) {
	type Row struct {
		Day    int32     `parquet:"name=day, type=INT32, convertedtype=DATE"`
		Scores []float64 `parquet:"name=scores, type=DOUBLE, repetitiontype=REPEATED"`
	}

	tests := []struct {
		name  string
		field string
		input Row
	}{
		{
			name:  "primitive lookup",
			field: "Day",
			input: Row{Day: 19000},
		},
		{
			name:  "list element lookup",
			field: "Scores",
			input: Row{Scores: []float64{1.5}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sh, err := schema.NewSchemaHandlerFromStruct(new(Row))
			require.NoError(t, err)

			path := common.ParGoRootInName + common.ParGoPathDelimiter + tt.field
			require.Contains(t, sh.MapIndex, path)
			sh.MapIndex[path] = int32(len(sh.SchemaElements))

			_, err = ConvertToJSONFriendly(tt.input, sh)
			require.ErrorContains(t, err, "schema handler is inconsistent")
			require.ErrorContains(t, err, common.ParGoRootInName+"."+tt.field)
		})
	}
}

func TestLookupSchemaElement_NegativeIndex(t *testing.T) {
	type Row struct {
		Day int32 `parquet:"name=day, type=INT32, convertedtype=DATE"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(Row))
	require.NoError(t, err)
	sh.MapIndex[common.ParGoRootInName+common.ParGoPathDelimiter+"Day"] = -1

	_, err = lookupSchemaElement(sh, common.ParGoRootInName+common.ParGoPathDelimiter+"Day", &jsonConverter{})
	require.ErrorContains(t, err, "schema handler is inconsistent")
}

// TestConvertToJSONFriendly_ReportsUnrenderable covers the read path's half of ConvertValue:
// a value the column cannot render is reported rather than handed back as a substitute.
func TestConvertToJSONFriendly_ReportsUnrenderable(t *testing.T) {
	type GeomRow struct {
		Geom string `parquet:"name=geom, type=BYTE_ARRAY, logicaltype=GEOMETRY"`
	}
	type BSONRow struct {
		Doc string `parquet:"name=doc, type=BYTE_ARRAY, convertedtype=BSON"`
	}
	type UUIDRow struct {
		ID string `parquet:"name=id, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"`
	}
	handler := func(obj any) *schema.SchemaHandler {
		sh, err := schema.NewSchemaHandlerFromStruct(obj)
		require.NoError(t, err)
		return sh
	}
	geoJSON := WithGeospatialConfig(types.NewGeospatialConfig(
		types.WithGeometryJSONMode(types.GeospatialModeGeoJSON),
	))

	tests := []struct {
		name   string
		row    any
		sh     *schema.SchemaHandler
		column string
	}{
		{
			name: "bytes that are not WKB",
			row: struct {
				Geom string `json:"geom"`
			}{Geom: "not-wkb-at-all"},
			sh:     handler(new(GeomRow)),
			column: "Geom",
		},
		{
			name: "a BSON document that does not parse",
			row: struct {
				Doc string `json:"doc"`
			}{Doc: "not-a-bson-document"},
			sh:     handler(new(BSONRow)),
			column: "Doc",
		},
		{
			name: "a UUID of the wrong width",
			row: struct {
				ID string `json:"id"`
			}{ID: "short"},
			sh:     handler(new(UUIDRow)),
			column: "ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := ConvertToJSONFriendly(tt.row, tt.sh, geoJSON)

			require.ErrorIs(t, err, types.ErrUnrenderable)
			require.Nil(t, out, "the conversion abandons its output")
			// The wrapper is what makes a report usable against a file, so pin it whole:
			// the bare name passes on "UUID" for a field called ID.
			require.ErrorContains(t, err, "convert field "+tt.column)
		})
	}

	t.Run("a value the column can render", func(t *testing.T) {
		row := struct {
			Geom string `json:"geom"`
		}{Geom: wkbPoint(10.5, 20.3)}
		out, err := ConvertToJSONFriendly(row, handler(new(GeomRow)), geoJSON)

		require.NoError(t, err)
		require.Equal(t, map[string]any{
			"geom": map[string]any{
				"type":       "Feature",
				"geometry":   map[string]any{"type": "Point", "coordinates": []float64{10.5, 20.3}},
				"properties": map[string]any{"crs": "OGC:CRS84"},
			},
		}, out)
	})

	t.Run("one bad value costs the batch", func(t *testing.T) {
		type row struct {
			Geom string `json:"geom"`
		}
		rows := []row{{Geom: wkbPoint(1, 2)}, {Geom: "not-wkb-at-all"}, {Geom: wkbPoint(3, 4)}}
		out, err := ConvertToJSONFriendly(rows, handler(new(GeomRow)), geoJSON)

		require.ErrorIs(t, err, types.ErrUnrenderable)
		require.Nil(t, out, "the readable rows go with it")
		// And says which row, which is what makes a batch failure actionable.
		require.ErrorContains(t, err, "convert row 1")
	})
}

func TestConvertToJSONFriendly_ReportsVariantErrors(t *testing.T) {
	type row struct {
		Value types.Variant `parquet:"name=value, type=VARIANT"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(row))
	require.NoError(t, err)

	tests := []struct {
		name    string
		variant types.Variant
		errMsg  string
	}{
		{
			name:    "invalid metadata",
			variant: types.Variant{Metadata: []byte{0xff, 0xff}, Value: []byte{0x04}},
			errMsg:  "decode metadata",
		},
		{
			name: "invalid value",
			variant: types.Variant{
				Metadata: types.EncodeVariantMetadata([]string{}),
				Value:    []byte{0x05},
			},
			errMsg: "decode value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := ConvertToJSONFriendly(row{Value: tt.variant}, sh)
			require.ErrorIs(t, err, types.ErrUnrenderable)
			require.ErrorContains(t, err, tt.errMsg)
			require.ErrorContains(t, err, "convert field Value")
			require.Nil(t, out)
		})
	}
}

// wkbPoint spells a little-endian WKB point.
func wkbPoint(x, y float64) string {
	b := binary.LittleEndian.AppendUint32([]byte{1}, 1)
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(x))
	return string(binary.LittleEndian.AppendUint64(b, math.Float64bits(y)))
}

func TestConvertToJSONFriendlyEnforceUTF8(t *testing.T) {
	type MyString string
	type Row struct {
		Text MyString `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(Row))
	require.NoError(t, err)
	for _, text := range []string{"你好", "", "A\xffB"} {
		for _, enabled := range []bool{false, true} {
			got, err := ConvertToJSONFriendly([]Row{{Text: MyString(text)}}, sh, WithEnforceUTF8(enabled))
			if enabled && text == "A\xffB" {
				require.ErrorIs(t, err, types.ErrUnrenderable)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				before, err := ConvertToJSONFriendly([]Row{{Text: MyString(text)}}, sh)
				require.NoError(t, err)
				require.Equal(t, before, got)
			}
		}
	}
	got, err := ConvertToJSONFriendly(Row{Text: MyString("\xff")}, sh, WithEnforceUTF8(true), WithEnforceUTF8(false))
	require.NoError(t, err)
	require.NotNil(t, got)
}

func TestConvertToJSONFriendlyEnforceUTF8ByteSlices(t *testing.T) {
	type MyBytes []byte
	type bytesRow struct {
		Text []byte `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	type namedBytesRow struct {
		Text MyBytes `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	tests := []struct {
		name   string
		schema any
		row    func([]byte) any
	}{
		{
			name:   "byte array bytes",
			schema: new(bytesRow),
			row:    func(text []byte) any { return bytesRow{Text: text} },
		},
		{
			name:   "byte array named bytes",
			schema: new(namedBytesRow),
			row:    func(text []byte) any { return namedBytesRow{Text: MyBytes(text)} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sh, err := schema.NewSchemaHandlerFromStruct(tt.schema)
			require.NoError(t, err)

			for _, tc := range []struct {
				name    string
				text    []byte
				invalid bool
			}{
				{name: "valid", text: []byte("你好")},
				{name: "nil", text: nil},
				{name: "empty", text: []byte{}},
				{name: "invalid", text: []byte{0xff}, invalid: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := ConvertToJSONFriendly(tt.row(tc.text), sh, WithEnforceUTF8(true))
					if tc.invalid {
						require.ErrorIs(t, err, types.ErrUnrenderable)
						require.Nil(t, got)
					} else {
						// A text column's bytes are its text, whether the field is
						// []byte or a type defined from it, so both render as a string.
						require.NoError(t, err)
						require.Equal(t, string(tc.text), got.(map[string]any)["Text"])
					}

					// The rendering is the column's, so enforcement only decides whether
					// invalid UTF-8 is reported, not what a valid value comes back as.
					got, err = ConvertToJSONFriendly(tt.row(tc.text), sh, WithEnforceUTF8(false))
					require.NoError(t, err)
					require.Equal(t, string(tc.text), got.(map[string]any)["Text"])
				})
			}
		})
	}
}

func TestConvertToJSONFriendlyByteSliceList(t *testing.T) {
	type Row struct {
		Values []byte `parquet:"name=values, type=LIST, valuetype=INT32"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(Row))
	require.NoError(t, err)

	got, err := ConvertToJSONFriendly(Row{Values: []byte{1, 2}}, sh, WithEnforceUTF8(true))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"Values": []any{uint8(1), uint8(2)}}, got)
}

// TestConvertToJSONFriendlyBinaryByteSlices pins base64 for an unannotated byte-backed column.
func TestConvertToJSONFriendlyBinaryByteSlices(t *testing.T) {
	type bytesRow struct {
		Value []byte `parquet:"name=value, type=BYTE_ARRAY"`
	}
	type fixedRow struct {
		Value []byte `parquet:"name=value, type=FIXED_LEN_BYTE_ARRAY, length=2"`
	}

	for _, tt := range []struct {
		name   string
		schema any
		row    any
	}{
		{name: "byte array", schema: new(bytesRow), row: bytesRow{Value: []byte{1, 2}}},
		{name: "fixed", schema: new(fixedRow), row: fixedRow{Value: []byte{1, 2}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sh, err := schema.NewSchemaHandlerFromStruct(tt.schema)
			require.NoError(t, err)
			for _, tc := range []struct {
				name string
				opts []JSONConvertOption
			}{
				{name: "default"},
				{name: "UTF-8 enforced", opts: []JSONConvertOption{WithEnforceUTF8(true)}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, err := ConvertToJSONFriendly(tt.row, sh, tc.opts...)
					require.NoError(t, err)
					require.Equal(t, map[string]any{"Value": base64.StdEncoding.EncodeToString([]byte{1, 2})}, got)
				})
			}
		})
	}
}

// TestConvertToJSONFriendlyPartialRead pins that a partial read converts its logical values
// once told which prefix its objects are rooted at, and passes them through without it.
func TestConvertToJSONFriendlyPartialRead(t *testing.T) {
	jsonSchema := `{
		"Tag": "name=parquet-go-root",
		"Fields": [
			{"Tag": "name=nested", "Fields": [
				{"Tag": "name=day, type=INT32, convertedtype=DATE"},
				{"Tag": "name=score, type=DOUBLE"}
			]}
		]
	}`
	sh, err := schema.NewSchemaHandlerFromJSON(jsonSchema)
	require.NoError(t, err)

	// The shape ReadPartialByNumber returns for the "nested" subtree.
	type nested struct {
		Day   int32
		Score float64
	}
	rows := []any{nested{Day: 19000, Score: math.NaN()}}
	prefix := common.PathToStr([]string{"Parquet45go45root", "Nested"})

	converted := []any{map[string]any{"Day": "2022-01-08", "Score": "NaN"}}

	t.Run("without a prefix nothing resolves", func(t *testing.T) {
		// The contract other callers rely on: an unknown path passes the value through.
		got, err := ConvertToJSONFriendly(rows, sh)
		require.NoError(t, err)
		row := got.([]any)[0].(map[string]any)
		require.Equal(t, int32(19000), row["Day"])
		require.True(t, math.IsNaN(row["Score"].(float64)), "the raw NaN json.Marshal refuses")
	})

	t.Run("with the prefix the data is rooted at", func(t *testing.T) {
		got, err := ConvertToJSONFriendly(rows, sh, WithPrefixPath(prefix))
		require.NoError(t, err)
		require.Equal(t, converted, got)
	})

	t.Run("the external names ReadPartial also takes", func(t *testing.T) {
		got, err := ConvertToJSONFriendly(rows, sh, WithPrefixPath(common.PathToStr([]string{"parquet-go-root", "nested"})))
		require.NoError(t, err)
		require.Equal(t, converted, got)
	})

	t.Run("an unknown prefix is reported", func(t *testing.T) {
		_, err := ConvertToJSONFriendly(rows, sh, WithPrefixPath(common.PathToStr([]string{"Parquet45go45root", "Missing"})))
		require.Error(t, err)
	})

	t.Run("a value with no schema to consult is passed through", func(t *testing.T) {
		// A nil handler describes nothing, so there is no root to name paths from.
		got, err := ConvertToJSONFriendly([]any{int32(19000)}, nil)
		require.NoError(t, err)
		require.Equal(t, []any{int32(19000)}, got)
	})

	t.Run("a full read is unaffected", func(t *testing.T) {
		type root struct {
			Nested nested
		}
		got, err := ConvertToJSONFriendly([]any{root{Nested: nested{Day: 19000, Score: math.NaN()}}}, sh)
		require.NoError(t, err)
		require.Equal(t, []any{map[string]any{"Nested": map[string]any{"Day": "2022-01-08", "Score": "NaN"}}}, got)
	})
}

// TestConvertToJSONFriendlyPartialReadRoots covers a prefix naming the value itself rather
// than a group above it, which is what ReadPartialByNumber builds its result from.
func TestConvertToJSONFriendlyPartialReadRoots(t *testing.T) {
	jsonSchema := `{
		"Tag": "name=parquet-go-root",
		"Fields": [
			{"Tag": "name=day, type=INT32, convertedtype=DATE"},
			{"Tag": "name=days, type=LIST", "Fields": [{"Tag": "name=element, type=INT32, convertedtype=DATE"}]},
			{"Tag": "name=scores, type=MAP", "Fields": [
				{"Tag": "name=key, type=BYTE_ARRAY, convertedtype=UTF8"},
				{"Tag": "name=value, type=INT32, convertedtype=DATE"}
			]}
		]
	}`
	sh, err := schema.NewSchemaHandlerFromJSON(jsonSchema)
	require.NoError(t, err)

	testCases := []struct {
		name   string
		prefix []string
		rows   []any
		want   any
	}{
		{
			name:   "leaf",
			prefix: []string{"Parquet45go45root", "Day"},
			rows:   []any{int32(19000)},
			want:   []any{"2022-01-08"},
		},
		{
			name:   "list",
			prefix: []string{"Parquet45go45root", "Days"},
			rows:   []any{[]int32{19000, 19001}},
			want:   []any{[]any{"2022-01-08", "2022-01-09"}},
		},
		{
			name:   "map",
			prefix: []string{"Parquet45go45root", "Scores"},
			rows:   []any{map[string]int32{"a": 19000}},
			want:   []any{map[string]any{"a": "2022-01-08"}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertToJSONFriendly(tc.rows, sh, WithPrefixPath(common.PathToStr(tc.prefix)))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// The function takes one value as readily as a batch of them, and a LIST is the one
	// root where both are slices: the batch holds a list each, the single value its
	// elements.
	t.Run("a list handed over as one value", func(t *testing.T) {
		got, err := ConvertToJSONFriendly([]int32{19000, 19001}, sh,
			WithPrefixPath(common.PathToStr([]string{"Parquet45go45root", "Days"})))
		require.NoError(t, err)
		require.Equal(t, []any{"2022-01-08", "2022-01-09"}, got)
	})

	t.Run("a batch of lists is still a batch", func(t *testing.T) {
		got, err := ConvertToJSONFriendly([][]int32{{19000}, {19001}}, sh,
			WithPrefixPath(common.PathToStr([]string{"Parquet45go45root", "Days"})))
		require.NoError(t, err)
		require.Equal(t, []any{[]any{"2022-01-08"}, []any{"2022-01-09"}}, got)
	})
}

// TestIsListAnnotated covers the three spellings that make a value a list of its own, which
// is what tells a batch of rows from one list handed over directly.
func TestIsListAnnotated(t *testing.T) {
	repeated := parquet.FieldRepetitionType_REPEATED
	optional := parquet.FieldRepetitionType_OPTIONAL
	logical := parquet.NewLogicalType()
	logical.LIST = parquet.NewListType()

	testCases := []struct {
		name    string
		element *parquet.SchemaElement
		want    bool
	}{
		{"nil", nil, false},
		{"legacy repeated", &parquet.SchemaElement{RepetitionType: &repeated}, true},
		{"logical LIST", &parquet.SchemaElement{RepetitionType: &optional, LogicalType: logical}, true},
		{"converted LIST", &parquet.SchemaElement{RepetitionType: &optional, ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST)}, true},
		{"plain group", &parquet.SchemaElement{RepetitionType: &optional}, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isListAnnotated(tc.element))
		})
	}
}

// TestConvertToJSONFriendly_InconsistentRootReported covers the root lookup that decides
// whether the data is a batch: a handler that cannot answer it is reported, not guessed at.
func TestConvertToJSONFriendly_InconsistentRootReported(t *testing.T) {
	type Row struct {
		Day int32 `parquet:"name=day, type=INT32, convertedtype=DATE"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(Row))
	require.NoError(t, err)
	sh.MapIndex[common.ParGoRootInName] = -1

	_, err = ConvertToJSONFriendly([]any{Row{Day: 19000}}, sh)
	require.ErrorContains(t, err, "schema handler is inconsistent")
}

// TestConvertToJSONFriendlyPrimitiveByteSlice pins a []byte at a primitive column as one value.
func TestConvertToJSONFriendlyPrimitiveByteSlice(t *testing.T) {
	type row struct {
		Text   []byte `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8"`
		Binary []byte `parquet:"name=binary, type=BYTE_ARRAY"`
		List   []byte `parquet:"name=list, type=LIST, valuetype=INT32"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(row))
	require.NoError(t, err)

	for _, enforceUTF8 := range []bool{false, true} {
		t.Run(fmt.Sprintf("enforceUTF8=%v", enforceUTF8), func(t *testing.T) {
			got, err := ConvertToJSONFriendly(
				[]any{row{Text: []byte("hi"), Binary: []byte{1, 2}, List: []byte{1, 2}}},
				sh, WithEnforceUTF8(enforceUTF8),
			)
			require.NoError(t, err)
			require.Equal(t, []any{map[string]any{
				"Text":   "hi",
				"Binary": base64.StdEncoding.EncodeToString([]byte{1, 2}),
				// A LIST column keeps its elements, whatever Go type carries them.
				"List": []any{uint8(1), uint8(2)},
			}}, got)
		})
	}
}

// TestConvertToJSONFriendlyRepeatedByteArray covers the legacy repeated primitive, whose
// column and values share one schema path.
func TestConvertToJSONFriendlyRepeatedByteArray(t *testing.T) {
	type row struct {
		Bin  [][]byte `parquet:"name=bin, type=BYTE_ARRAY, repetitiontype=REPEATED"`
		Text [][]byte `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REPEATED"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(row))
	require.NoError(t, err)

	got, err := ConvertToJSONFriendly(row{
		Bin:  [][]byte{{1, 2}, {3, 4}},
		Text: [][]byte{[]byte("hi"), []byte("yo")},
	}, sh)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"Bin": []any{
			base64.StdEncoding.EncodeToString([]byte{1, 2}),
			base64.StdEncoding.EncodeToString([]byte{3, 4}),
		},
		"Text": []any{"hi", "yo"},
	}, got)
}

// TestConvertToJSONFriendlyNamedByteSlice covers a type defined from []byte at the converters.
func TestConvertToJSONFriendlyNamedByteSlice(t *testing.T) {
	type namedBytes []byte
	type row struct {
		Bin  namedBytes `parquet:"name=bin, type=BYTE_ARRAY"`
		Text namedBytes `parquet:"name=text, type=BYTE_ARRAY, convertedtype=UTF8"`
		UUID namedBytes `parquet:"name=uuid, type=FIXED_LEN_BYTE_ARRAY, logicaltype=UUID, length=16"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(row))
	require.NoError(t, err)

	uuid := namedBytes{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	got, err := ConvertToJSONFriendly(row{Bin: namedBytes{1, 2}, Text: namedBytes("hi"), UUID: uuid}, sh)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"Bin":  base64.StdEncoding.EncodeToString([]byte{1, 2}),
		"Text": "hi",
		"UUID": "550e8400-e29b-41d4-a716-446655440000",
	}, got)
}
