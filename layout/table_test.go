package layout

import (
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_Table_Merge(t *testing.T) {
	// Create first table with initial data
	sourceTable := &Table{
		Values:           []any{int32(1), int32(2)},
		DefinitionLevels: []int32{0, 0},
		RepetitionLevels: []int32{0, 0},
	}

	// Create second table to merge
	targetTable := &Table{
		Values:           []any{int32(3), int32(4)},
		DefinitionLevels: []int32{0, 0},
		RepetitionLevels: []int32{0, 0},
	}

	// Perform merge operation
	sourceTable.Merge(targetTable)

	// Verify merged results
	expectedValues := []any{int32(1), int32(2), int32(3), int32(4)}
	expectedDefinitionLevels := []int32{0, 0, 0, 0}
	expectedRepetitionLevels := []int32{0, 0, 0, 0}

	if !equalSlices(sourceTable.Values, expectedValues) {
		t.Errorf("Values merge failed: expected %v, got %v", expectedValues, sourceTable.Values)
	}

	if !equalInt32Slices(sourceTable.DefinitionLevels, expectedDefinitionLevels) {
		t.Errorf("DefinitionLevels merge failed: expected %v, got %v", expectedDefinitionLevels, sourceTable.DefinitionLevels)
	}

	if !equalInt32Slices(sourceTable.RepetitionLevels, expectedRepetitionLevels) {
		t.Errorf("RepetitionLevels merge failed: expected %v, got %v", expectedRepetitionLevels, sourceTable.RepetitionLevels)
	}
}

// Helper function to compare any slices
func equalSlices(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Helper function to compare int32 slices
func equalInt32Slices(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func Test_NewTableFromTable(t *testing.T) {
	// Test with nil table
	result := NewTableFromTable(nil)
	if result != nil {
		t.Errorf("Expected nil result for nil input, got %v", result)
	}

	// Test with valid table
	src := &Table{
		Schema: &parquet.SchemaElement{Name: "test"},
		Path:   []string{"root", "test"},
		Info:   common.NewTag(),
	}

	result = NewTableFromTable(src)
	if result == nil {
		t.Fatal("Expected non-nil result for valid input")
	}
	if result.Schema != src.Schema {
		t.Errorf("Expected schema to be copied, got %v", result.Schema)
	}
	if len(result.Path) != len(src.Path) {
		t.Errorf("Expected path length %d, got %d", len(src.Path), len(result.Path))
	}
	if result.MaxDefinitionLevel != 0 || result.MaxRepetitionLevel != 0 {
		t.Errorf("Expected max levels to be 0, got def=%d rep=%d", result.MaxDefinitionLevel, result.MaxRepetitionLevel)
	}
}

func Test_NewEmptyTable(t *testing.T) {
	table := NewEmptyTable()
	if table == nil {
		t.Fatal("Expected non-nil table")
	}
	if table.Info == nil {
		t.Error("Expected Info field to be initialized")
	}
}

func Test_Table_Merge_maxLevels(t *testing.T) {
	table1 := &Table{
		Values:             []any{int32(1), int32(2)},
		DefinitionLevels:   []int32{0, 1},
		RepetitionLevels:   []int32{0, 0},
		MaxDefinitionLevel: 1,
		MaxRepetitionLevel: 0,
	}

	table2 := &Table{
		Values:             []any{int32(3), int32(4)},
		DefinitionLevels:   []int32{2, 1},
		RepetitionLevels:   []int32{1, 1},
		MaxDefinitionLevel: 2,
		MaxRepetitionLevel: 1,
	}

	table1.Merge(table2)

	if table1.MaxDefinitionLevel != 2 {
		t.Errorf("Expected MaxDefinitionLevel=2, got %d", table1.MaxDefinitionLevel)
	}
	if table1.MaxRepetitionLevel != 1 {
		t.Errorf("Expected MaxRepetitionLevel=1, got %d", table1.MaxRepetitionLevel)
	}
	if len(table1.Values) != 4 {
		t.Errorf("Expected 4 values, got %d", len(table1.Values))
	}
}

func Test_Table_Merge_nilTable(t *testing.T) {
	table := &Table{
		Values:           []any{int32(1)},
		DefinitionLevels: []int32{0},
		RepetitionLevels: []int32{0},
	}

	originalLen := len(table.Values)
	table.Merge(nil)

	if len(table.Values) != originalLen {
		t.Errorf("Expected values length unchanged after merging nil, got %d", len(table.Values))
	}
}

func Test_Table_Pop(t *testing.T) {
	table := &Table{
		Values:           []any{int32(1), int32(2), int32(3), int32(4)},
		DefinitionLevels: []int32{0, 1, 0, 1},
		RepetitionLevels: []int32{0, 1, 0, 1},
	}

	result := table.Pop(1)

	if result == nil {
		t.Fatal("Expected non-nil result from Pop")
	}

	// Should return first row (2 values since repetition level 1 means continuation)
	if len(result.Values) != 2 {
		t.Errorf("Expected 2 values in result, got %d", len(result.Values))
	}

	// Original table should have remaining values
	if len(table.Values) != 2 {
		t.Errorf("Expected 2 values remaining in original table, got %d", len(table.Values))
	}
}

func Test_Table_Pop_emptyTable(t *testing.T) {
	table := &Table{
		Values:           []any{},
		DefinitionLevels: []int32{},
		RepetitionLevels: []int32{},
	}

	result := table.Pop(1)

	if result == nil {
		t.Fatal("Expected non-nil result from Pop")
	}

	if len(result.Values) != 0 {
		t.Errorf("Expected 0 values in result, got %d", len(result.Values))
	}
}
