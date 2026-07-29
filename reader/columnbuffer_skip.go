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
	// Loop so that an empty chunk (no page bytes) is synthesized and then skipped over
	// to the next row group, rather than ending the whole column: only a truly terminal
	// empty chunk (no further row groups) reports completion. This keeps a skip that
	// spans an empty row group in sync with columns that do have data there.
	for {
		for cbt.ChunkHeader == nil || cbt.ChunkHeader.MetaData == nil || cbt.ChunkReadValues >= cbt.ChunkHeader.MetaData.NumValues {
			// Current chunk is exhausted; advance to the next row group and retry.
			if err := cbt.NextRowGroup(); err != nil {
				if errors.Is(err, io.EOF) {
					return nil, errColumnExhausted
				}
				return nil, fmt.Errorf("move to next row group: %w", err)
			}
		}

		if cbt.Reader != nil {
			if err := cbt.Reader.requirePageDecryptor(cbt); err != nil {
				return nil, fmt.Errorf("require page decryptor: %w", err)
			}
		}

		// Mirror ReadPage: no page bytes at the cursor. Only an untouched chunk
		// (ChunkReadValues == 0) is a recoverable empty chunk; if pages were already read
		// but the declared value count is unmet, the chunk is truncated and must surface
		// as an error. The synthesized rows stay in the "one less than actual" convention
		// (DataTableNumRows is not normalized here), so consumeExistingRows and
		// skipByReadingPages account for them consistently.
		if cbt.emptyChunkAtCursor() {
			if cbt.chunkReadStarted() {
				return nil, fmt.Errorf("truncated column chunk: %w", io.EOF)
			}
			if err := cbt.appendNullChunk(); err != nil {
				return nil, err
			}
			// When another row group remains, continue into it so a skip spanning this
			// empty row group stays in sync with columns that have data there. Only a
			// terminal empty chunk reports completion, and directly — going through
			// NextRowGroup would normalize the count out of the one-less convention.
			if cbt.RowGroupIndex < int64(len(cbt.Footer.GetRowGroups())) {
				continue
			}
			return nil, errColumnExhausted
		}

		return cbt.readRawPageForSkip()
	}
}

// readRawPageForSkip reads and merges the current chunk's next raw page during a skip,
// returning the page so its values can be decoded on demand. A dictionary page is
// cached and returned without advancing the row count.
func (cbt *ColumnBufferType) readRawPageForSkip() (*layout.Page, error) {
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

// bufferedRowCount converts DataTableNumRows into an actual row count. The value is
// normally stored one below the real count, but terminal reads normalize it exactly.
func (cbt *ColumnBufferType) bufferedRowCount() int64 {
	if cbt.DataTableNumRows < 0 {
		return 0
	}
	if cbt.dataTableNumRowsNormalized {
		return cbt.DataTableNumRows
	}
	return cbt.DataTableNumRows + 1
}

func (cbt *ColumnBufferType) consumeExistingRows(num int64) (int64, bool) {
	buffered := cbt.bufferedRowCount()
	if buffered == 0 {
		return num, false
	}
	if num <= buffered {
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
	num -= buffered
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
		err     error
		page    *layout.Page
		skipped int64
	)

	for cbt.bufferedRowCount() < num && err == nil {
		if buffered := cbt.bufferedRowCount(); buffered > 0 {
			// The current buffer is fully consumed by this skip; discard it and count
			// its rows so the total skipped is reported even across multiple pages.
			skipped += buffered
			num -= buffered
			cbt.resetDataTable()
		}
		page, err = cbt.ReadPageForSkip()
	}
	// ReadPageForSkip signals a fully-consumed column (row groups exhausted, or a
	// no-page-bytes chunk whose null rows are already synthesized) with
	// errColumnExhausted. Treat that as normal completion and skip what is buffered;
	// propagate anything else, including a truncated chunk, but report the rows already
	// skipped and discarded so the caller sees the cursor's true advance.
	if err != nil && !errors.Is(err, errColumnExhausted) {
		return skipped, fmt.Errorf("read page for skip: %w", err)
	}
	buffered := cbt.bufferedRowCount()
	if buffered == 0 {
		// EOF reached with nothing more buffered; report the rows already skipped.
		return skipped, nil
	}

	// Discarding the whole buffer does not require decoding its values. This handles
	// both an exact-page skip and a request that runs past terminal exhaustion.
	if num >= buffered {
		skipped += buffered
		cbt.resetDataTable()
		return skipped, nil
	}

	if page != nil {
		if err = page.GetValueFromRawData(cbt.SchemaHandler); err != nil {
			return skipped, fmt.Errorf("decode page values during skip: %w", err)
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
	return skipped + num, nil
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

	// If the earlier phases already accounted for every requested row, stop here.
	// Calling skipByReadingPages(0) would read the next page needlessly and could
	// surface an error from data beyond the requested skip boundary.
	if num == 0 {
		return originalNum, nil
	}

	// Finally, skip remaining rows by reading pages.
	// Save remaining before the call: skipByReadingPages returns the count popped,
	// not the remaining count. Total skipped = (consumed in phases 1+2) + popped here
	// = (originalNum - remaining) + num = originalNum - remaining + num.
	remaining := num
	if num, err = cbt.skipByReadingPages(remaining); err != nil {
		// skipByReadingPages returns the count it popped/discarded even on error, so the
		// reported total reflects the cursor's real advance rather than dropping to zero.
		return originalNum - remaining + num, fmt.Errorf("skip by reading pages: %w", err)
	}

	return originalNum - remaining + num, nil
}
