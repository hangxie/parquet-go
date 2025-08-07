package layout

import (
	"fmt"
	"sync"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
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

// Convert a RowGroup to table map
func (rowGroup *RowGroup) RowGroupToTableMap() *map[string]*Table {
	tableMap := make(map[string]*Table, 0)
	for _, chunk := range rowGroup.Chunks {
		pathStr := ""
		for _, page := range chunk.Pages {
			if pathStr == "" {
				pathStr = common.PathToStr(page.DataTable.Path)
			}
			if _, ok := tableMap[pathStr]; !ok {
				tableMap[pathStr] = NewTableFromTable(page.DataTable)
			}
			tableMap[pathStr].Merge(page.DataTable)
		}
	}
	return &tableMap
}

// Read one RowGroup from parquet file (Deprecated)
func ReadRowGroup(rowGroupHeader *parquet.RowGroup, PFile source.ParquetFileReader, schemaHandler *schema.SchemaHandler, NP int64) (*RowGroup, error) {
	if rowGroupHeader == nil {
		return nil, fmt.Errorf("rowGroupHeader cannot be nil")
	}
	if NP <= 0 {
		return nil, fmt.Errorf("NP must be greater than 0, got %d", NP)
	}

	var err error
	rowGroup := new(RowGroup)
	rowGroup.RowGroupHeader = rowGroupHeader

	columnChunks := rowGroupHeader.GetColumns()
	ln := int64(len(columnChunks))
	chunksList := make([][]*Chunk, NP)
	for i := range NP {
		chunksList[i] = make([]*Chunk, 0)
	}

	delta := (ln + NP - 1) / NP
	var wg sync.WaitGroup
	for c := range NP {
		bgn := c * delta
		end := bgn + delta
		if end > ln {
			end = ln
		}
		if bgn >= ln {
			bgn, end = ln, ln
		}

		wg.Add(1)
		go func(index, bgn, end int64) {
			defer wg.Done()
			for i := bgn; i < end; i++ {
				offset := columnChunks[i].FileOffset
				PFile := PFile
				if columnChunks[i].FilePath != nil {
					PFile, _ = PFile.Open(*columnChunks[i].FilePath)
				} else {
					PFile, _ = PFile.Clone()
				}
				thriftReader := source.ConvertToThriftReader(PFile, offset)
				chunk, _ := ReadChunk(thriftReader, schemaHandler, columnChunks[i])
				chunksList[index] = append(chunksList[index], chunk)
				_ = PFile.Close()
			}
		}(c, bgn, end)
	}

	wg.Wait()

	for c := range NP {
		if len(chunksList[c]) <= 0 {
			continue
		}
		rowGroup.Chunks = append(rowGroup.Chunks, chunksList[c]...)
	}

	return rowGroup, err
}
