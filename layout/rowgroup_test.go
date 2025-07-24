package layout

import (
	"testing"
)

func Test_NewRowGroup(t *testing.T) {
	rowGroup := NewRowGroup()
	if rowGroup == nil {
		t.Fatal("Expected non-nil row group")
	}
	// Chunks slice is not initialized by NewRowGroup - it's nil initially
	if rowGroup.RowGroupHeader == nil {
		t.Error("Expected RowGroupHeader to be initialized")
	}
}

func Test_RowGroupToTableMap(t *testing.T) {
	// Create a row group with test chunks
	rowGroup := &RowGroup{
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

	tableMap := rowGroup.RowGroupToTableMap()
	if tableMap == nil {
		t.Fatal("Expected non-nil table map")
	}

	// Check that the table map contains the expected keys
	if len(*tableMap) != 2 {
		t.Errorf("Expected 2 entries in table map, got %d", len(*tableMap))
	}

	// Check specific columns (using the correct delimiter)
	if table, exists := (*tableMap)["root\x01col1"]; !exists {
		t.Error("Expected 'root\x01col1' key in table map")
	} else if len(table.Values) != 2 {
		t.Errorf("Expected 2 values in root\x01col1 table, got %d", len(table.Values))
	}

	if table, exists := (*tableMap)["root\x01col2"]; !exists {
		t.Error("Expected 'root\x01col2' key in table map")
	} else if len(table.Values) != 2 {
		t.Errorf("Expected 2 values in root\x01col2 table, got %d", len(table.Values))
	}
}

func Test_RowGroupToTableMapWithEmptyRowGroup(t *testing.T) {
	rowGroup := NewRowGroup()

	tableMap := rowGroup.RowGroupToTableMap()
	if tableMap == nil {
		t.Fatal("Expected non-nil table map")
	}
	if len(*tableMap) != 0 {
		t.Errorf("Expected empty table map, got %d entries", len(*tableMap))
	}
}

func Test_RowGroupToTableMapWithMultiplePages(t *testing.T) {
	// Create a row group with multiple pages for the same column
	rowGroup := &RowGroup{
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

	tableMap := rowGroup.RowGroupToTableMap()
	if tableMap == nil {
		t.Fatal("Expected non-nil table map")
	}

	// Check that pages were merged for the same column
	if table, exists := (*tableMap)["root\x01col1"]; !exists {
		t.Error("Expected 'root\x01col1' key in table map")
	} else if len(table.Values) != 4 {
		t.Errorf("Expected 4 values in merged table, got %d", len(table.Values))
	}
}

func Test_RowGroupToTableMapWithEmptyChunks(t *testing.T) {
	// Create a row group with an empty chunk
	rowGroup := &RowGroup{
		Chunks: []*Chunk{
			{
				Pages: []*Page{},
			},
		},
	}

	tableMap := rowGroup.RowGroupToTableMap()
	if tableMap == nil {
		t.Fatal("Expected non-nil table map")
	}

	// Should handle empty chunks gracefully
	if len(*tableMap) != 0 {
		t.Errorf("Expected empty table map for empty chunks, got %d entries", len(*tableMap))
	}
}

func Test_RowGroupToTableMapWithEmptyPath(t *testing.T) {
	// Create a row group with a page that has an empty path
	rowGroup := &RowGroup{
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

	tableMap := rowGroup.RowGroupToTableMap()
	if tableMap == nil {
		t.Fatal("Expected non-nil table map")
	}

	// Should handle empty paths
	if _, exists := (*tableMap)[""]; !exists {
		t.Error("Expected empty string key for empty path")
	}
}
