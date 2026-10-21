package reader

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/writer"
)

func testPathSchemaHandler() *schema.SchemaHandler {
	inPath := common.PathToStr([]string{common.ParGoRootInName, "ColL1"})
	exPath := common.PathToStr([]string{common.ParGoRootExName, "col_l1"})
	return &schema.SchemaHandler{
		Infos: []*common.Tag{
			{InName: common.ParGoRootInName, ExName: common.ParGoRootExName},
			{InName: "ColL1", ExName: "col_l1"},
		},
		MapIndex:       map[string]int32{inPath: 1},
		ExPathToInPath: map[string]string{exPath: inPath},
	}
}

// unnamedRootSchemaHandler mirrors a parquet-mr file whose root element carries no name.
func unnamedRootSchemaHandler() *schema.SchemaHandler {
	inPath := common.PathToStr([]string{"", "C0"})
	exPath := common.PathToStr([]string{"", "c0"})
	return &schema.SchemaHandler{
		Infos: []*common.Tag{
			{InName: "", ExName: ""},
			{InName: "C0", ExName: "c0"},
		},
		MapIndex:       map[string]int32{inPath: 1},
		ExPathToInPath: map[string]string{exPath: inPath},
	}
}

func TestSchemaRootNames(t *testing.T) {
	tests := []struct {
		name       string
		handler    *schema.SchemaHandler
		wantInName string
		wantExName string
		wantOK     bool
	}{
		{name: "nil_handler"},
		{name: "empty_infos", handler: &schema.SchemaHandler{}},
		{name: "nil_root_info", handler: &schema.SchemaHandler{Infos: []*common.Tag{nil}}},
		{
			name: "root_names",
			handler: &schema.SchemaHandler{
				Infos: []*common.Tag{{InName: "RootIn", ExName: "root_ex"}},
			},
			wantInName: "RootIn",
			wantExName: "root_ex",
			wantOK:     true,
		},
		{
			name:    "empty_root_names_still_present",
			handler: &schema.SchemaHandler{Infos: []*common.Tag{{}}},
			wantOK:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotInName, gotInOK := schemaRootInName(tt.handler)
			require.Equal(t, tt.wantInName, gotInName)
			require.Equal(t, tt.wantOK, gotInOK)

			gotExName, gotExOK := schemaRootExName(tt.handler)
			require.Equal(t, tt.wantExName, gotExName)
			require.Equal(t, tt.wantOK, gotExOK)
		})
	}
}

func TestLookupExternalPath(t *testing.T) {
	handler := testPathSchemaHandler()
	wantInPath := common.PathToStr([]string{common.ParGoRootInName, "ColL1"})
	exPath := common.PathToStr([]string{common.ParGoRootExName, "col_l1"})
	upperExPath := common.PathToStr([]string{common.ParGoRootExName, "COL_L1"})

	tests := []struct {
		name            string
		handler         *schema.SchemaHandler
		path            string
		caseInsensitive bool
		wantPath        string
		wantOK          bool
	}{
		{name: "nil_handler", path: exPath},
		{name: "nil_map", handler: &schema.SchemaHandler{}, path: exPath},
		{name: "exact_match_case_sensitive", handler: handler, path: exPath, wantPath: wantInPath, wantOK: true},
		{name: "missing_case_sensitive", handler: handler, path: upperExPath},
		{name: "case_insensitive_match", handler: handler, path: upperExPath, caseInsensitive: true, wantPath: wantInPath, wantOK: true},
		{name: "case_insensitive_missing", handler: handler, path: common.PathToStr([]string{common.ParGoRootExName, "missing"}), caseInsensitive: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotOK := lookupExternalPath(tt.handler, tt.path, tt.caseInsensitive)
			require.Equal(t, tt.wantOK, gotOK)
			require.Equal(t, tt.wantPath, gotPath)
		})
	}
}

func TestColumnPathToInPath(t *testing.T) {
	handler := testPathSchemaHandler()
	wantInPath := common.PathToStr([]string{common.ParGoRootInName, "ColL1"})

	tests := []struct {
		name            string
		handler         *schema.SchemaHandler
		path            []string
		caseInsensitive bool
		want            string
	}{
		{name: "nil_handler", path: []string{"leaf"}, want: "leaf"},
		{name: "already_internal_path", handler: handler, path: []string{"ColL1"}, want: wantInPath},
		{name: "external_path", handler: handler, path: []string{"col_l1"}, want: wantInPath},
		{name: "case_mismatch_falls_back", handler: handler, path: []string{"COL_L1"}, want: common.PathToStr([]string{common.ParGoRootInName, "COL_L1"})},
		{name: "case_insensitive_external_path", handler: handler, path: []string{"COL_L1"}, caseInsensitive: true, want: wantInPath},
		{
			// An empty external root name still builds a path; this falls back to
			// the internal one because nothing maps it.
			name:    "unmapped_external_path_falls_back",
			handler: &schema.SchemaHandler{Infos: []*common.Tag{{InName: "Root"}}, MapIndex: map[string]int32{}},
			path:    []string{"leaf"},
			want:    common.PathToStr([]string{"Root", "leaf"}),
		},
		{
			// parquet-mr leaves the root unnamed; that empty name must still
			// prefix the path so the column resolves through ExPathToInPath.
			name:    "unnamed_root",
			handler: unnamedRootSchemaHandler(),
			path:    []string{"c0"},
			want:    common.PathToStr([]string{"", "C0"}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, columnPathToInPath(tt.handler, nil, tt.path, tt.caseInsensitive))
		})
	}
}

func TestColumnMetaDataForRead(t *testing.T) {
	require.Nil(t, columnMetaDataForRead(testPathSchemaHandler(), nil, nil, false))

	meta := &parquet.ColumnMetaData{
		PathInSchema: []string{"col_l1"},
		NumValues:    7,
	}
	readMeta := columnMetaDataForRead(testPathSchemaHandler(), nil, meta, false)

	require.NotSame(t, meta, readMeta)
	require.Equal(t, []string{"col_l1"}, meta.PathInSchema)
	require.Equal(t, []string{"ColL1"}, readMeta.PathInSchema)
	require.Equal(t, int64(7), readMeta.NumValues)
}

type listWrapperItem struct {
	X []int32 `parquet:"name=x, type=LIST, valuetype=INT32"`
}

type listWrapperRecord struct {
	V []int32           `parquet:"name=v, type=LIST, valuetype=INT32"`
	N []listWrapperItem `parquet:"name=n, type=LIST"`
}

// writeListWrapperParquet writes rows and applies mutate to the footer.
func writeListWrapperParquet(t *testing.T, rows []listWrapperRecord, mutate func(*parquet.FileMetaData)) []byte {
	t.Helper()

	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(context.Background(), &buf, new(listWrapperRecord), writer.WithNP(1))
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, pw.WriteWithContext(context.Background(), row))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))
	return rewriteFooter(t, buf.Bytes(), mutate)
}

// renameListWrappers renames every LIST wrapper and element the way Athena names them.
func renameListWrappers(footer *parquet.FileMetaData) {
	renameFooterNames(footer, map[string]string{"list": "bag", "element": "array_element"}, true)
}

// renameFooterNames renames footer column paths, and schema elements too when withSchema is set.
func renameFooterNames(footer *parquet.FileMetaData, rename map[string]string, withSchema bool) {
	if withSchema {
		for _, element := range footer.Schema {
			if name, ok := rename[element.Name]; ok {
				element.Name = name
			}
		}
	}
	for _, rowGroup := range footer.RowGroups {
		for _, column := range rowGroup.Columns {
			for i, name := range column.MetaData.PathInSchema {
				if renamed, ok := rename[name]; ok {
					column.MetaData.PathInSchema[i] = renamed
				}
			}
		}
	}
}

// annotateListsByLogicalTypeOnly leaves every LIST group with only its logical type.
func annotateListsByLogicalTypeOnly(footer *parquet.FileMetaData) {
	for _, element := range footer.Schema {
		if element.GetConvertedType() == parquet.ConvertedType_LIST {
			element.ConvertedType = nil
			element.LogicalType = &parquet.LogicalType{LIST: parquet.NewListType()}
		}
	}
}

func TestReadListWithAlternateWrapperNames(t *testing.T) {
	rows := []listWrapperRecord{
		{V: []int32{4, 5, 6}, N: []listWrapperItem{{X: []int32{1, 2}}, {X: []int32{3}}}},
		{V: []int32{}, N: []listWrapperItem{}},
	}

	mixedCase := func(footer *parquet.FileMetaData) {
		renameFooterNames(footer, map[string]string{"list": "Bag", "element": "Array_Element"}, true)
	}
	// A footer whose column paths disagree in case with its own schema.
	inconsistentCase := func(footer *parquet.FileMetaData) {
		renameListWrappers(footer)
		renameFooterNames(footer, map[string]string{"bag": "Bag"}, false)
	}

	tests := []struct {
		name            string
		mutate          func(*parquet.FileMetaData)
		caseInsensitive bool
	}{
		{name: "alternate_names", mutate: renameListWrappers},
		{name: "mixed_case_names", mutate: mixedCase},
		{name: "mixed_case_names_case_insensitive", mutate: mixedCase, caseInsensitive: true},
		{name: "inconsistent_case_case_insensitive", mutate: inconsistentCase, caseInsensitive: true},
		{name: "logical_type_only", mutate: annotateListsByLogicalTypeOnly},
		{
			name: "alternate_names_logical_type_only",
			mutate: func(footer *parquet.FileMetaData) {
				renameListWrappers(footer)
				annotateListsByLogicalTypeOnly(footer)
			},
		},
	}

	for _, tt := range tests {
		data := writeListWrapperParquet(t, rows, tt.mutate)

		t.Run(tt.name+"/typed", func(t *testing.T) {
			pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), new(listWrapperRecord), WithNP(1), WithCaseInsensitive(tt.caseInsensitive))
			require.NoError(t, err)
			defer func() { require.NoError(t, pr.ReadStop()) }()

			got := make([]listWrapperRecord, len(rows))
			require.NoError(t, pr.ReadWithContext(context.Background(), &got))
			require.Equal(t, rows, got)
		})

		t.Run(tt.name+"/inferred", func(t *testing.T) {
			pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), nil, WithNP(1), WithCaseInsensitive(tt.caseInsensitive))
			require.NoError(t, err)
			defer func() { require.NoError(t, pr.ReadStop()) }()

			got, err := pr.ReadByNumberWithContext(context.Background(), len(rows))
			require.NoError(t, err)
			jsonBytes, err := json.Marshal(got)
			require.NoError(t, err)
			require.JSONEq(t, `[{"v":[4,5,6],"n":[{"x":[1,2]},{"x":[3]}]},{"v":[],"n":[]}]`, string(jsonBytes))
		})
	}
}

// legacyArrayList renames v's wrapper to array, which the spec's legacy rules read as a list of {x} structs.
func legacyArrayList(footer *parquet.FileMetaData) {
	footer.Schema[2].Name, footer.Schema[3].Name = "array", "x"
	for _, rowGroup := range footer.RowGroups {
		path := rowGroup.Columns[0].MetaData.PathInSchema
		path[1], path[2] = "array", "x"
	}
}

// footerSchemaHandler returns a schema handler for the footer of data.
func footerSchemaHandler(t *testing.T, data []byte) *schema.SchemaHandler {
	t.Helper()
	pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), nil, WithNP(1))
	require.NoError(t, err)
	require.NoError(t, pr.ReadStop())
	return pr.fileSchemaHandler()
}

// TestReadLegacyListElementGroup covers a legacy layout whose repeated group is the element itself.
func TestReadLegacyListElementGroup(t *testing.T) {
	rows := []listWrapperRecord{{V: []int32{4, 5}, N: []listWrapperItem{}}}
	data := writeListWrapperParquet(t, rows, legacyArrayList)

	type legacyItem struct {
		X int32 `parquet:"name=x, type=INT32"`
	}
	type structList struct {
		V []legacyItem `parquet:"name=v, type=LIST"`
	}
	type mirrored struct {
		V struct {
			Array []legacyItem `parquet:"name=array, repetitiontype=REPEATED"`
		} `parquet:"name=v"`
	}

	t.Run("typed_value_list", func(t *testing.T) {
		_, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), new(listWrapperRecord), WithNP(1))
		require.ErrorContains(t, err, "Column not found")
	})

	t.Run("typed_struct_list", func(t *testing.T) {
		// The struct layer must not be skipped as a wrapper to fit the four-level destination.
		_, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), new(structList), WithNP(1))
		require.ErrorContains(t, err, "Column not found")
	})

	t.Run("mirrored_layout", func(t *testing.T) {
		pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytes(data), new(mirrored), WithNP(1))
		require.NoError(t, err)
		defer func() { require.NoError(t, pr.ReadStop()) }()

		got := make([]mirrored, len(rows))
		require.NoError(t, pr.ReadWithContext(context.Background(), &got))
		require.Equal(t, []legacyItem{{X: 4}, {X: 5}}, got[0].V.Array)
	})
}

func TestListAliasInPath(t *testing.T) {
	handler, err := schema.NewSchemaHandlerFromStruct(new(listWrapperRecord))
	require.NoError(t, err)
	inPath := func(names ...string) string {
		return common.PathToStr(append([]string{common.ParGoRootInName}, names...))
	}
	listType := parquet.ConvertedType_LIST
	// A LIST-annotated group without List and Element children must not consume wrapper names.
	bareList := &schema.SchemaHandler{
		Infos:          []*common.Tag{{InName: "Root", ExName: "root"}},
		SchemaElements: []*parquet.SchemaElement{{}, {ConvertedType: &listType}, {}},
		MapIndex:       map[string]int32{"Root": 0, common.PathToStr([]string{"Root", "V"}): 1, common.PathToStr([]string{"Root", "V", "Bag"}): 2},
		InPathToExPath: map[string]string{common.PathToStr([]string{"Root", "V"}): common.PathToStr([]string{"root", "v"})},
		ExPathToInPath: map[string]string{common.PathToStr([]string{"root", "v"}): common.PathToStr([]string{"Root", "V"})},
	}

	// A destination built from footer elements may carry only the logical LIST annotation.
	one := int32(1)
	required, repeated := parquet.FieldRepetitionType_REQUIRED, parquet.FieldRepetitionType_REPEATED
	int32Type := parquet.Type_INT32
	logicalOnly := schema.NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
		{Name: common.ParGoRootExName, NumChildren: &one},
		{Name: "v", RepetitionType: &required, LogicalType: &parquet.LogicalType{LIST: parquet.NewListType()}, NumChildren: &one},
		{Name: "list", RepetitionType: &repeated, NumChildren: &one},
		{Name: "element", RepetitionType: &required, Type: &int32Type},
	})

	rows := []listWrapperRecord{{V: []int32{1}, N: []listWrapperItem{{X: []int32{2}}}}}
	altFile := footerSchemaHandler(t, writeListWrapperParquet(t, rows, renameListWrappers))
	upperFile := footerSchemaHandler(t, writeListWrapperParquet(t, rows, func(footer *parquet.FileMetaData) {
		renameListWrappers(footer)
		upper := map[string]string{"n": "N", "x": "X"}
		for _, element := range footer.Schema {
			if name, ok := upper[element.Name]; ok {
				element.Name = name
			}
		}
		for _, rowGroup := range footer.RowGroups {
			for _, column := range rowGroup.Columns {
				for i, name := range column.MetaData.PathInSchema {
					if renamed, ok := upper[name]; ok {
						column.MetaData.PathInSchema[i] = renamed
					}
				}
			}
		}
	}))
	legacyFile := footerSchemaHandler(t, writeListWrapperParquet(t, rows, legacyArrayList))

	tests := []struct {
		name            string
		handler         *schema.SchemaHandler
		fileSH          *schema.SchemaHandler
		path            []string
		caseInsensitive bool
		want            string
		wantOK          bool
	}{
		{name: "nil_maps", handler: &schema.SchemaHandler{}, fileSH: altFile, path: []string{"v"}},
		{name: "nil_file_schema", handler: handler, path: []string{"v", "bag", "array_element"}},
		{name: "file_element_group", handler: handler, fileSH: legacyFile, path: []string{"v", "array", "x"}},
		{name: "alternate_names", handler: handler, fileSH: altFile, path: []string{"v", "bag", "array_element"}, want: inPath("V", "List", "Element"), wantOK: true},
		{name: "nested_alternate_names", handler: handler, fileSH: altFile, path: []string{"n", "bag", "array_element", "x", "bag", "array_element"}, want: inPath("N", "List", "Element", "X", "List", "Element"), wantOK: true},
		{name: "case_insensitive", handler: handler, fileSH: upperFile, path: []string{"N", "bag", "array_element", "X", "bag", "array_element"}, caseInsensitive: true, want: inPath("N", "List", "Element", "X", "List", "Element"), wantOK: true},
		{name: "case_sensitive_mismatch", handler: handler, fileSH: upperFile, path: []string{"N", "bag", "array_element", "X", "bag", "array_element"}},
		{name: "missing_element", handler: handler, fileSH: altFile, path: []string{"v", "bag"}},
		{name: "unknown_column", handler: handler, fileSH: altFile, path: []string{"w", "bag", "array_element"}},
		{name: "list_without_wrappers", handler: bareList, fileSH: altFile, path: []string{"v", "bag"}},
		{name: "logical_type_only_destination", handler: logicalOnly, fileSH: altFile, path: []string{"v", "bag", "array_element"}, want: inPath("V", "List", "Element"), wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootInName, _ := schemaRootInName(tt.handler)
			rootExName, _ := schemaRootExName(tt.handler)
			got, ok := listAliasInPath(tt.handler, tt.fileSH, rootInName, rootExName, tt.path, tt.caseInsensitive)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.want, got)
		})
	}
}
