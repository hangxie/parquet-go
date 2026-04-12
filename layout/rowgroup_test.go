package layout

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRowGroup(t *testing.T) {
	rowGroup := NewRowGroup()
	require.NotNil(t, rowGroup)
	// Chunks slice is not initialized by NewRowGroup - it's nil initially
	require.NotNil(t, rowGroup.RowGroupHeader)
}

func TestRowGroupToTableMap(t *testing.T) {
	tests := []struct {
		name          string
		setupRowGroup func() *RowGroup
		expectedKeys  []string
		checkResult   func(t *testing.T, tableMap *map[string]*Table)
	}{
		{
			name: "multiple_columns",
			setupRowGroup: func() *RowGroup {
				return &RowGroup{
					Chunks: []*Chunk{
						{
							Pages: []*Page{
								{
									DataTable: &Table{
										Path:             []string{"root", "col1"},
										Values:           []any{int32(1), int32(2)},
										DefinitionLevels: []int32{0, 0},
										RepetitionLevels: []int32{0, 0},
									},
								},
							},
						},
						{
							Pages: []*Page{
								{
									DataTable: &Table{
										Path:             []string{"root", "col2"},
										Values:           []any{"a", "b"},
										DefinitionLevels: []int32{0, 0},
										RepetitionLevels: []int32{0, 0},
									},
								},
							},
						},
					},
				}
			},
			expectedKeys: []string{"root\x01col1", "root\x01col2"},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				require.Equal(t, 2, len(*tableMap), "Expected 2 entries in table map, got %d", len(*tableMap))

				table, exists := (*tableMap)["root\x01col1"]
				require.True(t, exists)
				require.Len(t, table.Values, 2, "Expected 2 values in root\x01col1 table, got %d", len(table.Values))

				table, exists = (*tableMap)["root\x01col2"]
				require.True(t, exists)
				require.Len(t, table.Values, 2, "Expected 2 values in root\x01col2 table, got %d", len(table.Values))
			},
		},
		{
			name: "empty_row_group",
			setupRowGroup: func() *RowGroup {
				return NewRowGroup()
			},
			expectedKeys: []string{},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				require.Empty(t, *tableMap, "Expected empty table map, got %d entries", len(*tableMap))
			},
		},
		{
			name: "multiple_pages_same_column",
			setupRowGroup: func() *RowGroup {
				return &RowGroup{
					Chunks: []*Chunk{
						{
							Pages: []*Page{
								{
									DataTable: &Table{
										Path:             []string{"root", "col1"},
										Values:           []any{int32(1), int32(2)},
										DefinitionLevels: []int32{0, 0},
										RepetitionLevels: []int32{0, 0},
									},
								},
								{
									DataTable: &Table{
										Path:             []string{"root", "col1"},
										Values:           []any{int32(3), int32(4)},
										DefinitionLevels: []int32{0, 0},
										RepetitionLevels: []int32{0, 0},
									},
								},
							},
						},
					},
				}
			},
			expectedKeys: []string{"root\x01col1"},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				table, exists := (*tableMap)["root\x01col1"]
				require.True(t, exists)
				require.Len(t, table.Values, 4, "Expected 4 values in merged table, got %d", len(table.Values))
			},
		},
		{
			name: "empty_chunks",
			setupRowGroup: func() *RowGroup {
				return &RowGroup{
					Chunks: []*Chunk{
						{
							Pages: []*Page{},
						},
					},
				}
			},
			expectedKeys: []string{},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				require.Empty(t, *tableMap, "Expected empty table map for empty chunks, got %d entries", len(*tableMap))
			},
		},
		{
			name: "empty_path",
			setupRowGroup: func() *RowGroup {
				return &RowGroup{
					Chunks: []*Chunk{
						{
							Pages: []*Page{
								{
									DataTable: &Table{
										Path:             []string{}, // empty path
										Values:           []any{int32(1)},
										DefinitionLevels: []int32{0},
										RepetitionLevels: []int32{0},
									},
								},
							},
						},
					},
				}
			},
			expectedKeys: []string{""},
			checkResult: func(t *testing.T, tableMap *map[string]*Table) {
				_, exists := (*tableMap)[""]
				require.True(t, exists)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowGroup := tt.setupRowGroup()
			tableMap := rowGroup.RowGroupToTableMap()

			require.NotNil(t, tableMap)

			if tt.checkResult != nil {
				tt.checkResult(t, tableMap)
			}
		})
	}
}
