package layout

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// RowGroup stores the RowGroup in parquet file
type RowGroup struct {
	Chunks         []*Chunk
	RowGroupHeader *parquet.RowGroup
}

// Create a RowGroup
func NewRowGroup() *RowGroup {
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = parquet.NewRowGroup()
	return rowGroup
}

// RowGroupToTableMap converts a RowGroup to a map of path to Table.
// Returns an error if any page has a nil DataTable.
func (rowGroup *RowGroup) RowGroupToTableMap() (*map[string]*Table, error) {
	tableMap := make(map[string]*Table, 0)
	for _, chunk := range rowGroup.Chunks {
		pathStr := ""
		for _, page := range chunk.Pages {
			if page.DataTable == nil {
				return nil, fmt.Errorf("page DataTable is nil, data may not have been decoded")
			}
			if pathStr == "" {
				pathStr = common.PathToStr(page.DataTable.Path)
			}
			if _, ok := tableMap[pathStr]; !ok {
				tableMap[pathStr] = NewTableFromTable(page.DataTable)
			}
			tableMap[pathStr].Merge(page.DataTable)
		}
	}
	return &tableMap, nil
}
