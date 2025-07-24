package layout

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

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

func Test_NewEmptyTable(t *testing.T) {
	table := NewEmptyTable()
	require.NotNil(t, table)
	require.NotNil(t, table.Info)
}

func Test_NewTableFromTable(t *testing.T) {
	// Test with nil table
	result := NewTableFromTable(nil)
	require.Nil(t, result)

	// Test with valid table
	src := &Table{
		Schema: &parquet.SchemaElement{Name: "test"},
		Path:   []string{"root", "test"},
		Info:   common.NewTag(),
	}

	result = NewTableFromTable(src)
	require.NotNil(t, result)
	require.Equal(t, src.Schema, result.Schema)
	require.Len(t, result.Path, len(src.Path))
	require.Equal(t, int32(0), result.MaxDefinitionLevel)
	require.Equal(t, int32(0), result.MaxRepetitionLevel)
}

func Test_Table_Merge(t *testing.T) {
	tests := []struct {
		name                string
		setupSource         func() *Table
		setupTarget         func() *Table
		expectedValues      []any
		expectedDefLevels   []int32
		expectedRepLevels   []int32
		expectedMaxDefLevel int32
		expectedMaxRepLevel int32
		checkMaxLevels      bool
	}{
		{
			name: "basic_merge",
			setupSource: func() *Table {
				return &Table{
					Values:           []any{int32(1), int32(2)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				}
			},
			setupTarget: func() *Table {
				return &Table{
					Values:           []any{int32(3), int32(4)},
					DefinitionLevels: []int32{0, 0},
					RepetitionLevels: []int32{0, 0},
				}
			},
			expectedValues:    []any{int32(1), int32(2), int32(3), int32(4)},
			expectedDefLevels: []int32{0, 0, 0, 0},
			expectedRepLevels: []int32{0, 0, 0, 0},
		},
		{
			name: "max_levels",
			setupSource: func() *Table {
				return &Table{
					Values:             []any{int32(1), int32(2)},
					DefinitionLevels:   []int32{0, 1},
					RepetitionLevels:   []int32{0, 0},
					MaxDefinitionLevel: 1,
					MaxRepetitionLevel: 0,
				}
			},
			setupTarget: func() *Table {
				return &Table{
					Values:             []any{int32(3), int32(4)},
					DefinitionLevels:   []int32{2, 1},
					RepetitionLevels:   []int32{1, 1},
					MaxDefinitionLevel: 2,
					MaxRepetitionLevel: 1,
				}
			},
			expectedValues:      []any{int32(1), int32(2), int32(3), int32(4)},
			expectedDefLevels:   []int32{0, 1, 2, 1},
			expectedRepLevels:   []int32{0, 0, 1, 1},
			expectedMaxDefLevel: 2,
			expectedMaxRepLevel: 1,
			checkMaxLevels:      true,
		},
		{
			name: "nil_table",
			setupSource: func() *Table {
				return &Table{
					Values:           []any{int32(1)},
					DefinitionLevels: []int32{0},
					RepetitionLevels: []int32{0},
				}
			},
			setupTarget: func() *Table {
				return nil
			},
			expectedValues:    []any{int32(1)},
			expectedDefLevels: []int32{0},
			expectedRepLevels: []int32{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceTable := tt.setupSource()
			targetTable := tt.setupTarget()

			sourceTable.Merge(targetTable)
			require.True(t, equalSlices(sourceTable.Values, tt.expectedValues))
			require.True(t, equalInt32Slices(sourceTable.DefinitionLevels, tt.expectedDefLevels))
			require.True(t, equalInt32Slices(sourceTable.RepetitionLevels, tt.expectedRepLevels))

			if tt.checkMaxLevels {
				require.Equal(t, tt.expectedMaxDefLevel, sourceTable.MaxDefinitionLevel)
				require.Equal(t, tt.expectedMaxRepLevel, sourceTable.MaxRepetitionLevel)
			}
		})
	}
}

func Test_Table_Pop(t *testing.T) {
	tests := []struct {
		name           string
		setupTable     func() *Table
		rowCount       int64
		expectedResult int
		expectedRemain int
	}{
		{
			name: "normal_table",
			setupTable: func() *Table {
				return &Table{
					Values:           []any{int32(1), int32(2), int32(3), int32(4)},
					DefinitionLevels: []int32{0, 1, 0, 1},
					RepetitionLevels: []int32{0, 1, 0, 1},
				}
			},
			rowCount:       1,
			expectedResult: 2, // Should return first row (2 values since repetition level 1 means continuation)
			expectedRemain: 2, // Original table should have remaining values
		},
		{
			name: "empty_table",
			setupTable: func() *Table {
				return &Table{
					Values:           []any{},
					DefinitionLevels: []int32{},
					RepetitionLevels: []int32{},
				}
			},
			rowCount:       1,
			expectedResult: 0,
			expectedRemain: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := tt.setupTable()

			result := table.Pop(tt.rowCount)

			require.NotNil(t, result)

			require.Len(t, result.Values, tt.expectedResult)

			require.Len(t, table.Values, tt.expectedRemain)
		})
	}
}
