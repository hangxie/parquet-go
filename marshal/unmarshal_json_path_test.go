package marshal

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

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

func TestIsPrimitiveByteSlice(t *testing.T) {
	type Row struct {
		Bin   []byte   `parquet:"name=bin, type=BYTE_ARRAY"`
		Fixed []byte   `parquet:"name=fixed, type=FIXED_LEN_BYTE_ARRAY, length=12"`
		Ts    []byte   `parquet:"name=ts, type=INT96"`
		Rep   [][]byte `parquet:"name=rep, type=INT96, repetitiontype=REPEATED"`
		Ints  []byte   `parquet:"name=ints, type=LIST, valuetype=INT32"`
		Num   int32    `parquet:"name=num, type=INT32"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(Row))
	require.NoError(t, err)
	converter := &jsonConverter{}
	require.NoError(t, converter.resolveRootPath(sh))
	path := func(name string) string { return common.PathToStr([]string{sh.GetRootInName(), name}) }
	bytesVal := reflect.ValueOf([]byte{1})

	testCases := []struct {
		name string
		val  reflect.Value
		path string
		want bool
	}{
		{"BYTE_ARRAY", bytesVal, path("Bin"), true},
		{"FIXED_LEN_BYTE_ARRAY", bytesVal, path("Fixed"), true},
		{"INT96", bytesVal, path("Ts"), true},
		{"repeated INT96 shares its values' path", bytesVal, path("Rep"), true},
		{"LIST carried in a []byte", bytesVal, path("Ints"), false},
		{"INT32 leaf", bytesVal, path("Num"), false},
		{"group", bytesVal, sh.GetRootInName(), false},
		{"unknown path", bytesVal, path("Missing"), false},
		{"no path", bytesVal, "", false},
		{"not a byte slice", reflect.ValueOf([]int32{1}), path("Ts"), false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := isPrimitiveByteSlice(tc.val, sh, tc.path, converter)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestIsRowBatch covers telling the rows a read returns from one value at the prefix, where
// a byte-backed value is itself a slice.
func TestIsRowBatch(t *testing.T) {
	type Row struct {
		Ts     []byte   `parquet:"name=ts, type=INT96"`
		Bin    []byte   `parquet:"name=bin, type=BYTE_ARRAY"`
		TsList [][]byte `parquet:"name=ts_list, type=LIST, valuetype=INT96"`
		TsRep  [][]byte `parquet:"name=ts_rep, type=INT96, repetitiontype=REPEATED"`
		Days   []int32  `parquet:"name=days, type=LIST, valuetype=INT32"`
		Num    int32    `parquet:"name=num, type=INT32"`
	}
	sh, err := schema.NewSchemaHandlerFromStruct(new(Row))
	require.NoError(t, err)
	ts := make([]byte, 12)

	testCases := []struct {
		name   string
		prefix string
		val    any
		want   bool
	}{
		{"rows at the file root", "", []any{Row{}}, true},
		{"one row at the file root", "", Row{}, false},
		{"INT96 leaf, one value", "Ts", ts, false},
		{"INT96 leaf, batch", "Ts", [][]byte{ts, ts}, true},
		{"BYTE_ARRAY leaf, one value", "Bin", []byte("ab"), false},
		{"BYTE_ARRAY leaf, batch", "Bin", [][]byte{[]byte("ab")}, true},
		{"LIST of INT96, one list", "TsList", [][]byte{ts}, false},
		{"LIST of INT96, batch", "TsList", [][][]byte{{ts}}, true},
		{"LIST of INT96, batch as []any", "TsList", []any{[][]byte{ts}}, true},
		{"repeated INT96, one value", "TsRep", ts, false},
		{"LIST of INT32, one list", "Days", []int32{1, 2}, false},
		{"LIST of INT32, batch", "Days", [][]int32{{1}, {2}}, true},
		{"INT32 leaf, batch", "Num", []int32{1, 2}, true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			converter := &jsonConverter{}
			if tc.prefix != "" {
				converter.prefixPath = common.PathToStr([]string{"Parquet_go_root", tc.prefix})
			}
			require.NoError(t, converter.resolveRootPath(sh))
			got, err := converter.isRowBatch(reflect.ValueOf(tc.val), sh)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
