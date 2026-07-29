package reader

import (
	"errors"
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func (cbt *ColumnBufferType) ReadPageForSkip() (*layout.Page, error) {
	for cbt.ChunkHeader == nil || cbt.ChunkHeader.MetaData == nil || cbt.ChunkReadValues >= cbt.ChunkHeader.MetaData.NumValues {
		// Current chunk is exhausted; advance to the next row group and retry.
		if err := cbt.NextRowGroup(); err != nil {
			return nil, fmt.Errorf("move to next row group: %w", err)
		}
	}

	if cbt.Reader != nil {
		if err := cbt.Reader.requirePageDecryptor(cbt); err != nil {
			return nil, fmt.Errorf("require page decryptor: %w", err)
		}
	}
	page, err := layout.ReadPageRawData(cbt.ThriftReader, cbt.SchemaHandler, cbt.readMetaData(), &cbt.PageReadOptions)
	if err != nil {
		return nil, fmt.Errorf("read page raw data: %w", err)
	}

	numValues, numRows, err := page.GetRLDLFromRawData(cbt.SchemaHandler)
	if err != nil {
		return nil, fmt.Errorf("read repetition/definition levels: %w", err)
	}

	if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		if err := page.GetValueFromRawData(cbt.SchemaHandler); err != nil {
			return nil, fmt.Errorf("decode dictionary page: %w", err)
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
}

func (cbt *ColumnBufferType) resetDataTable() {
	if cbt.SchemaHandler != nil && cbt.SchemaHandler.MapIndex != nil && cbt.SchemaHandler.SchemaElements != nil {
		if index, exists := cbt.SchemaHandler.MapIndex[cbt.PathStr]; exists && index >= 0 && int(index) < len(cbt.SchemaHandler.SchemaElements) {
			cbt.DataTable = layout.NewEmptyTable()
			cbt.DataTable.Schema = cbt.SchemaHandler.SchemaElements[index]
			cbt.DataTable.Path = common.StrToPath(cbt.PathStr)
		}
	}
	cbt.DataTableNumRows = -1
}

func (cbt *ColumnBufferType) consumeExistingRows(num int64) (int64, bool) {
	if cbt.DataTableNumRows < 0 {
		return num, false
	}
	if num <= cbt.DataTableNumRows {
		// We have enough rows in the current buffer
		cbt.DataTable.Pop(num)
		cbt.DataTableNumRows -= num
		if cbt.DataTableNumRows <= 0 {
			tmp := cbt.DataTable
			cbt.DataTable = layout.NewTableFromTable(tmp)
			cbt.DataTable.Merge(tmp)
		}
		return num, true
	}

	// Skip all remaining rows in current buffer
	num -= cbt.DataTableNumRows + 1
	cbt.resetDataTable()
	return num, false
}

func (cbt *ColumnBufferType) skipEntireRowGroups(num int64) (int64, error) {
	if cbt.Footer == nil || cbt.Footer.RowGroups == nil {
		return num, nil
	}
	for num > 0 && cbt.RowGroupIndex < int64(len(cbt.Footer.RowGroups)) {
		// Get the number of rows in the current row group (not yet processed)
		currentRG := cbt.Footer.RowGroups[cbt.RowGroupIndex]
		if currentRG == nil {
			break
		}
		rgNumRows := currentRG.GetNumRows()

		// Calculate remaining rows in current row group
		remainingInRG := rgNumRows

		if num < remainingInRG {
			// We need to skip partial rows in this row group
			break
		}

		// Skip entire row group
		num -= remainingInRG
		if err := cbt.NextRowGroup(); err != nil {
			if errors.Is(err, io.EOF) {
				// We've skipped all available rows
				return num, io.EOF
			}
			return num, fmt.Errorf("skip row group: %w", err)
		}
	}
	return num, nil
}

func (cbt *ColumnBufferType) skipByReadingPages(num int64) (int64, error) {
	var (
		err  error
		page *layout.Page
	)

	for cbt.DataTableNumRows < num && err == nil {
		if cbt.DataTableNumRows >= 0 {
			num -= cbt.DataTableNumRows + 1
			cbt.resetDataTable()
		}
		page, err = cbt.ReadPageForSkip()
		if err != nil {
			return 0, fmt.Errorf("read page for skip: %w", err)
		}
	}

	if num > cbt.DataTableNumRows {
		num = cbt.DataTableNumRows
	}

	if page != nil {
		if err = page.GetValueFromRawData(cbt.SchemaHandler); err != nil {
			return 0, fmt.Errorf("decode page values during skip: %w", err)
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
	return num, nil
}

// SkipRows skips up to num rows and returns how many were skipped.
// It propagates underlying read/decoding errors rather than hiding them.
// This function is optimized to skip entire row groups when possible, making it
// efficient for large skip distances.
func (cbt *ColumnBufferType) SkipRows(num int64) (int64, error) {
	if num <= 0 {
		return 0, nil
	}

	originalNum := num

	// First, consume any remaining rows in the current data table
	var done bool
	if num, done = cbt.consumeExistingRows(num); done {
		return num, nil
	}

	// Now skip entire row groups if possible
	var err error
	if num, err = cbt.skipEntireRowGroups(num); err != nil {
		if errors.Is(err, io.EOF) {
			return originalNum - num, nil
		}
		return 0, fmt.Errorf("skip row groups: %w", err)
	}

	// Finally, skip remaining rows by reading pages.
	// Save remaining before the call: skipByReadingPages returns the count popped,
	// not the remaining count. Total skipped = (consumed in phases 1+2) + popped here
	// = (originalNum - remaining) + num = originalNum - remaining + num.
	remaining := num
	if num, err = cbt.skipByReadingPages(remaining); err != nil {
		return 0, fmt.Errorf("skip by reading pages: %w", err)
	}

	return originalNum - remaining + num, nil
}
