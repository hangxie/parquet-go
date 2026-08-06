package reader

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
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
			require.Equal(t, tt.want, columnPathToInPath(tt.handler, tt.path, tt.caseInsensitive))
		})
	}
}

func TestColumnMetaDataForRead(t *testing.T) {
	require.Nil(t, columnMetaDataForRead(testPathSchemaHandler(), nil, false))

	meta := &parquet.ColumnMetaData{
		PathInSchema: []string{"col_l1"},
		NumValues:    7,
	}
	readMeta := columnMetaDataForRead(testPathSchemaHandler(), meta, false)

	require.NotSame(t, meta, readMeta)
	require.Equal(t, []string{"col_l1"}, meta.PathInSchema)
	require.Equal(t, []string{"ColL1"}, readMeta.PathInSchema)
	require.Equal(t, int64(7), readMeta.NumValues)
}
