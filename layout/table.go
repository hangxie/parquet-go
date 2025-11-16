package layout

import (
	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

func NewTableFromTable(src *Table) *Table {
	if src == nil {
		return nil
	}
	table := new(Table)
	table.Schema = src.Schema
	table.Path = append(table.Path, src.Path...)
	table.MaxDefinitionLevel = 0
	table.MaxRepetitionLevel = 0
	table.Info = src.Info
	return table
}

func NewEmptyTable() *Table {
	table := new(Table)
	table.Info = &common.Tag{}
	return table
}

// Table is the core data structure used to store the values
type Table struct {
	// Repetition type of the values: REQUIRED/OPTIONAL/REPEATED
	RepetitionType parquet.FieldRepetitionType
	// Schema
	Schema *parquet.SchemaElement
	// Path of this column
	Path []string
	// Maximum of definition levels
	MaxDefinitionLevel int32
	// Maximum of repetition levels
	MaxRepetitionLevel int32

	// Parquet values
	Values []any
	// Definition Levels slice
	DefinitionLevels []int32
	// Repetition Levels slice
	RepetitionLevels []int32

	// Tag info
	Info *common.Tag
}

// Merge several tables to one table(the first table)
func (t *Table) Merge(tables ...*Table) {
	for i := range len(tables) {
		if tables[i] == nil {
			continue
		}
		t.Values = append(t.Values, tables[i].Values...)
		t.RepetitionLevels = append(t.RepetitionLevels, tables[i].RepetitionLevels...)
		t.DefinitionLevels = append(t.DefinitionLevels, tables[i].DefinitionLevels...)
		t.MaxDefinitionLevel = max(t.MaxDefinitionLevel, tables[i].MaxDefinitionLevel)
		t.MaxRepetitionLevel = max(t.MaxRepetitionLevel, tables[i].MaxRepetitionLevel)
	}
}

func (t *Table) Pop(numRows int64) *Table {
	res := NewTableFromTable(t)
	endIndex := int64(0)
	ln := int64(len(t.Values))

	// Ensure all arrays have consistent lengths
	if ln != int64(len(t.RepetitionLevels)) || ln != int64(len(t.DefinitionLevels)) {
		return res // Return empty table if arrays are inconsistent
	}

	i, num := int64(0), int64(-1)
	for i = 0; i < ln; i++ {
		if t.RepetitionLevels[i] == 0 {
			num++
			if num >= numRows {
				break
			}
		}
		res.MaxRepetitionLevel = max(res.MaxRepetitionLevel, t.RepetitionLevels[i])
		res.MaxDefinitionLevel = max(res.MaxDefinitionLevel, t.DefinitionLevels[i])
	}
	endIndex = i

	// Validate endIndex is within bounds
	if endIndex > ln {
		endIndex = ln
	}

	res.RepetitionLevels = t.RepetitionLevels[:endIndex]
	res.DefinitionLevels = t.DefinitionLevels[:endIndex]
	res.Values = t.Values[:endIndex]

	t.RepetitionLevels = t.RepetitionLevels[endIndex:]
	t.DefinitionLevels = t.DefinitionLevels[endIndex:]
	t.Values = t.Values[endIndex:]

	return res
}
