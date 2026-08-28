package marshal

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
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
		"Value": float64(123.45), // Converted due to decimal type
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
		"Amounts": []any{float64(123.45)},
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

	listPath := func(parts ...string) string {
		return strings.Join(parts, common.ParGoPathDelimiter)
	}

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
			pathPrefix: "Ratios",
			expected:   listPath("Ratios", "List", "Element"),
		},
		{
			name:       "legacy repeated column is its own element",
			pathPrefix: "Scores",
			expected:   "Scores",
		},
		{
			name:       "non-repeated path falls back to List/Element",
			pathPrefix: "Nested",
			expected:   listPath("Nested", "List", "Element"),
		},
		{
			name:       "unknown path falls back to List/Element",
			pathPrefix: "Missing",
			expected:   listPath("Missing", "List", "Element"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			elementPath, err := listElementPath(sh, tt.pathPrefix, &jsonConverter{})
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
		"Amounts": []any{float64(123.45)},
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

	_, err = lookupSchemaElement(sh, "Day", &jsonConverter{})
	require.ErrorContains(t, err, "schema handler is inconsistent")
}
