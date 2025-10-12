package reader

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
)

type ColumnBufferType struct {
	PFile        source.ParquetFileReader
	ThriftReader *thrift.TBufferedTransport

	Footer        *parquet.FileMetaData
	SchemaHandler *schema.SchemaHandler

	PathStr       string
	RowGroupIndex int64
	ChunkHeader   *parquet.ColumnChunk

	ChunkReadValues int64

	DictPage *layout.Page

	DataTable        *layout.Table
	DataTableNumRows int64
}

func NewColumnBuffer(pFile source.ParquetFileReader, footer *parquet.FileMetaData, schemaHandler *schema.SchemaHandler, pathStr string) (*ColumnBufferType, error) {
	if pFile == nil {
		return nil, fmt.Errorf("pFile cannot be nil")
	}
	if footer == nil {
		return nil, fmt.Errorf("footer cannot be nil")
	}
	if schemaHandler == nil {
		return nil, fmt.Errorf("schema handler cannot be nil")
	}
	// If the path exists in the schema handler map, validate its type to catch
	// corrupt or unsupported schemas early. Otherwise, skip validation because
	// some callers rely on locating columns by footer path only.
	if schemaHandler.MapIndex != nil {
		if _, exists := schemaHandler.MapIndex[pathStr]; exists {
			if _, err := schemaHandler.GetType(pathStr); err != nil {
				return nil, err
			}
		}
	}
	newPFile, err := pFile.Clone()
	if err != nil {
		return nil, err
	}
	res := &ColumnBufferType{
		PFile:            newPFile,
		Footer:           footer,
		SchemaHandler:    schemaHandler,
		PathStr:          pathStr,
		DataTableNumRows: -1,
	}

	if err = res.NextRowGroup(); err == io.EOF {
		err = nil
	} else if err != nil {
		return nil, err
	}
	return res, err
}

func (cbt *ColumnBufferType) NextRowGroup() error {
	if cbt.Footer == nil {
		return io.EOF
	}

	var err error
	rowGroups := cbt.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if cbt.RowGroupIndex >= ln {
		if ln > 0 { // Only increment if there were actually row groups to process
			cbt.DataTableNumRows++ // very important, because DataTableNumRows is one smaller than real rows number
		}
		return io.EOF
	}

	cbt.RowGroupIndex++

	columnChunks := rowGroups[cbt.RowGroupIndex-1].GetColumns()
	i := int64(0)
	ln = int64(len(columnChunks))
	for i = 0; i < ln; i++ {
		path := make([]string, 0)
		path = append(path, cbt.SchemaHandler.GetRootInName())
		path = append(path, columnChunks[i].MetaData.GetPathInSchema()...)

		if common.ReformPathStr(cbt.PathStr) == common.PathToStr(path) {
			break
		}
	}

	if i >= ln {
		return fmt.Errorf("[NextRowGroup] Column not found: %v", cbt.PathStr)
	}

	cbt.ChunkHeader = columnChunks[i]
	if columnChunks[i].FilePath != nil {
		_ = cbt.PFile.Close()
		if cbt.PFile, err = cbt.PFile.Open(*columnChunks[i].FilePath); err != nil {
			return fmt.Errorf("failed to open file %s: %w", *columnChunks[i].FilePath, err)
		}
	}

	// offset := columnChunks[i].FileOffset
	offset := columnChunks[i].MetaData.DataPageOffset
	if columnChunks[i].MetaData.DictionaryPageOffset != nil {
		offset = *columnChunks[i].MetaData.DictionaryPageOffset
	}

	if cbt.ThriftReader != nil {
		_ = cbt.ThriftReader.Close()
	}

	cbt.ThriftReader = source.ConvertToThriftReader(cbt.PFile, offset)
	cbt.ChunkReadValues = 0
	cbt.DictPage = nil
	return nil
}

func (cbt *ColumnBufferType) ReadPage() error {
	if cbt.ChunkHeader != nil && cbt.ChunkHeader.MetaData != nil && cbt.ChunkReadValues < cbt.ChunkHeader.MetaData.NumValues {
		page, numValues, numRows, err := layout.ReadPage(cbt.ThriftReader, cbt.SchemaHandler, cbt.ChunkHeader.MetaData)
		if err != nil {
			// data is nil and rl/dl=0, no pages in file
			if err == io.EOF && cbt.DataTable == nil && cbt.SchemaHandler != nil &&
				cbt.SchemaHandler.MapIndex != nil && cbt.SchemaHandler.SchemaElements != nil {
				if index, exists := cbt.SchemaHandler.MapIndex[cbt.PathStr]; exists &&
					index >= 0 && int(index) < len(cbt.SchemaHandler.SchemaElements) {
					cbt.DataTable = layout.NewEmptyTable()
					cbt.DataTable.Schema = cbt.SchemaHandler.SchemaElements[index]
					cbt.DataTable.Path = common.StrToPath(cbt.PathStr)
				}

				cbt.DataTableNumRows = cbt.ChunkHeader.MetaData.NumValues

				for cbt.ChunkReadValues < cbt.ChunkHeader.MetaData.NumValues {
					cbt.DataTable.Values = append(cbt.DataTable.Values, nil)
					cbt.DataTable.RepetitionLevels = append(cbt.DataTable.RepetitionLevels, int32(0))
					cbt.DataTable.DefinitionLevels = append(cbt.DataTable.DefinitionLevels, int32(0))
					cbt.ChunkReadValues++
				}
			}

			return fmt.Errorf("failed to read page: %w", err)
		}

		if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
			cbt.DictPage = page
			return nil
		}

		page.Decode(cbt.DictPage)

		if cbt.DataTable == nil {
			cbt.DataTable = layout.NewTableFromTable(page.DataTable)
		}

		cbt.DataTable.Merge(page.DataTable)
		cbt.ChunkReadValues += numValues

		cbt.DataTableNumRows += numRows
	} else {
		if err := cbt.NextRowGroup(); err != nil {
			return fmt.Errorf("failed to move to next row group: %w", err)
		}

		return cbt.ReadPage()
	}

	return nil
}

func (cbt *ColumnBufferType) ReadPageForSkip() (*layout.Page, error) {
	if cbt.ChunkHeader != nil && cbt.ChunkHeader.MetaData != nil && cbt.ChunkReadValues < cbt.ChunkHeader.MetaData.NumValues {
		page, err := layout.ReadPageRawData(cbt.ThriftReader, cbt.SchemaHandler, cbt.ChunkHeader.MetaData)
		if err != nil {
			return nil, err
		}

		numValues, numRows, err := page.GetRLDLFromRawData(cbt.SchemaHandler)
		if err != nil {
			return nil, err
		}

		if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
			if err := page.GetValueFromRawData(cbt.SchemaHandler); err != nil {
				return nil, err
			}
			cbt.DictPage = page
			return page, nil
		}

		if cbt.DataTable == nil {
			cbt.DataTable = layout.NewTableFromTable(page.DataTable)
		}

		cbt.DataTable.Merge(page.DataTable)
		cbt.ChunkReadValues += numValues
		cbt.DataTableNumRows += numRows
		return page, nil

	} else {
		if err := cbt.NextRowGroup(); err != nil {
			return nil, err
		}

		return cbt.ReadPageForSkip()
	}
}

// SkipRowsWithError skips up to num rows and returns how many were skipped.
// It propagates underlying read/decoding errors rather than hiding them.
// This function is optimized to skip entire row groups when possible, making it
// efficient for large skip distances.
func (cbt *ColumnBufferType) SkipRowsWithError(num int64) (int64, error) {
	if num <= 0 {
		return 0, nil
	}

	originalNum := num

	// First, consume any remaining rows in the current data table
	if cbt.DataTableNumRows >= 0 {
		if num <= cbt.DataTableNumRows {
			// We have enough rows in the current buffer
			cbt.DataTable.Pop(num)
			cbt.DataTableNumRows -= num
			if cbt.DataTableNumRows <= 0 {
				tmp := cbt.DataTable
				cbt.DataTable = layout.NewTableFromTable(tmp)
				cbt.DataTable.Merge(tmp)
			}
			return num, nil
		}
		// Skip all remaining rows in current buffer
		num -= cbt.DataTableNumRows + 1
		if cbt.SchemaHandler != nil && cbt.SchemaHandler.MapIndex != nil &&
			cbt.SchemaHandler.SchemaElements != nil {
			if index, exists := cbt.SchemaHandler.MapIndex[cbt.PathStr]; exists &&
				index >= 0 && int(index) < len(cbt.SchemaHandler.SchemaElements) {
				cbt.DataTable = layout.NewEmptyTable()
				cbt.DataTable.Schema = cbt.SchemaHandler.SchemaElements[index]
				cbt.DataTable.Path = common.StrToPath(cbt.PathStr)
			}
		}
		cbt.DataTableNumRows = -1
	}

	// Now skip entire row groups if possible
	if cbt.Footer != nil && cbt.Footer.RowGroups != nil {
		for num > 0 && cbt.RowGroupIndex < int64(len(cbt.Footer.RowGroups)) {
			// Get the number of rows in the current row group (not yet processed)
			currentRG := cbt.Footer.RowGroups[cbt.RowGroupIndex]
			if currentRG == nil {
				break
			}
			rgNumRows := currentRG.GetNumRows()

			// Calculate remaining rows in current row group
			// ChunkReadValues tracks how many values we've read from the current chunk
			var rowsReadInCurrentRG int64
			if cbt.ChunkHeader != nil && cbt.ChunkHeader.MetaData != nil {
				// We need to estimate rows already read in this row group
				// For simplicity, we'll use the data table state
				if cbt.DataTableNumRows >= 0 {
					rowsReadInCurrentRG = 0 // Already accounted above
				}
			}
			remainingInRG := rgNumRows - rowsReadInCurrentRG

			if num < remainingInRG {
				// We need to skip partial rows in this row group
				break
			}

			// Skip entire row group
			num -= remainingInRG
			if err := cbt.NextRowGroup(); err != nil {
				if errors.Is(err, io.EOF) {
					// We've skipped all available rows
					return originalNum - num, nil
				}
				return 0, err
			}
		}
	}

	// Finally, skip remaining rows by reading pages
	var (
		err  error
		page *layout.Page
	)

	for cbt.DataTableNumRows < num && err == nil {
		if cbt.DataTableNumRows >= 0 {
			num -= cbt.DataTableNumRows + 1
			if cbt.SchemaHandler != nil && cbt.SchemaHandler.MapIndex != nil &&
				cbt.SchemaHandler.SchemaElements != nil {
				if index, exists := cbt.SchemaHandler.MapIndex[cbt.PathStr]; exists &&
					index >= 0 && int(index) < len(cbt.SchemaHandler.SchemaElements) {
					cbt.DataTable = layout.NewEmptyTable()
					cbt.DataTable.Schema = cbt.SchemaHandler.SchemaElements[index]
					cbt.DataTable.Path = common.StrToPath(cbt.PathStr)
				}
			}
			cbt.DataTableNumRows = -1
		}
		page, err = cbt.ReadPageForSkip()
		if err != nil {
			return 0, err
		}
	}

	if num > cbt.DataTableNumRows {
		num = cbt.DataTableNumRows
	}

	if page != nil {
		if err = page.GetValueFromRawData(cbt.SchemaHandler); err != nil {
			return 0, err
		}

		page.Decode(cbt.DictPage)
		i, j := len(cbt.DataTable.Values)-1, len(page.DataTable.Values)-1
		for i >= 0 && j >= 0 {
			cbt.DataTable.Values[i] = page.DataTable.Values[j]
			i, j = i-1, j-1
		}
	}

	cbt.DataTable.Pop(num)
	cbt.DataTableNumRows -= num
	if cbt.DataTableNumRows <= 0 {
		tmp := cbt.DataTable
		cbt.DataTable = layout.NewTableFromTable(tmp)
		cbt.DataTable.Merge(tmp)
	}

	return originalNum - num, nil
}

// read/decoding errors and will be removed in a future major release.
// Deprecated: Use SkipRowsWithError instead. This method ignores underlying
func (cbt *ColumnBufferType) SkipRows(num int64) int64 {
	n, _ := cbt.SkipRowsWithError(num)
	return n
}

// ReadRowsWithError reads up to num rows into a table and returns any non-EOF error.
func (cbt *ColumnBufferType) ReadRowsWithError(num int64) (*layout.Table, int64, error) {
	if cbt.Footer.NumRows == 0 {
		return &layout.Table{}, 0, nil
	}

	var err error

	for cbt.DataTableNumRows < num && err == nil {
		err = cbt.ReadPage()
	}

	if cbt.DataTableNumRows < 0 {
		cbt.DataTableNumRows = 0
		cbt.DataTable = layout.NewEmptyTable()
	}

	if num > cbt.DataTableNumRows {
		num = cbt.DataTableNumRows
	}

	res := cbt.DataTable.Pop(num)
	cbt.DataTableNumRows -= num

	if cbt.DataTableNumRows <= 0 { // release previous slice memory
		tmp := cbt.DataTable
		cbt.DataTable = layout.NewTableFromTable(tmp)
		cbt.DataTable.Merge(tmp)
	}
	// Propagate non-EOF errors; treat io.EOF as normal completion
	if err != nil && !errors.Is(err, io.EOF) {
		return res, num, err
	}
	return res, num, nil
}

// read/decoding errors and will be removed in a future major release.
// Deprecated: Use ReadRowsWithError instead. This method ignores underlying
func (cbt *ColumnBufferType) ReadRows(num int64) (*layout.Table, int64) {
	tbl, n, _ := cbt.ReadRowsWithError(num)
	return tbl, n
}
