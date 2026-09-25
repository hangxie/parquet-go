package reader

import (
	"errors"
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source"
)

type columnBufferReader struct {
	buffer    *ColumnBufferType
	file      source.ParquetFileReader
	remaining int64
}

func (r *columnBufferReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	file := r.file
	if file == nil {
		file = r.buffer.PFile
	}
	n, err := source.ReadWithContext(r.buffer.context(), file, p)
	r.remaining -= int64(n)
	return n, err
}

// appendNullChunk synthesizes the current chunk's remaining declared values as all-null
// top-level rows, appending to any rows already buffered from earlier row groups (so an
// empty chunk after a populated one still contributes its declared nulls). The counts
// are advanced exactly as a real page merge would (DataTableNumRows keeps the "one less
// than actual" convention that NextRowGroup later normalizes), so callers must not also
// adjust them. Synthetic allocation is bounded independently of file size, and a
// column absent from the schema map (footer-only access) is rejected rather than
// dereferencing a nil DataTable.
func (cbt *ColumnBufferType) appendNullChunk() error {
	if cbt.SchemaHandler == nil {
		return fmt.Errorf("read page: schema handler is nil")
	}
	index, exists := cbt.SchemaHandler.MapIndex[cbt.PathStr]
	if !exists || index < 0 || int(index) >= len(cbt.SchemaHandler.SchemaElements) {
		return fmt.Errorf("read page: no schema element for column %q", cbt.PathStr)
	}

	// An empty chunk stands in for the current row group's top-level rows, all null.
	// ColumnMetaData.NumValues counts leaf values, which for a repeated column can
	// exceed the row count; synthesize one null per row (using the row group's NumRows)
	// so no phantom rows are created, while still accounting the declared value count so
	// the chunk reads as consumed.
	numRows := cbt.currentRowGroupNumRows()
	if numRows < 0 {
		return fmt.Errorf("read page: column chunk row count is negative: %d", numRows)
	}
	maxAllocation := cbt.PageReadOptions.MaxPageSize
	if maxAllocation <= 0 {
		maxAllocation = layout.DefaultMaxPageSize
	}
	// Each synthesized row grows one interface slice and two int32 slices. Bound the
	// estimated allocation directly rather than assuming any relationship between
	// compressed file bytes and row count.
	const syntheticNullRowBytes int64 = 24
	if numRows > maxAllocation/syntheticNullRowBytes {
		return fmt.Errorf("read page: synthetic null row count %d exceeds allocation limit %d",
			numRows, maxAllocation)
	}
	if cbt.DataTable == nil {
		cbt.DataTable = layout.NewEmptyTable()
		cbt.DataTable.Schema = cbt.SchemaHandler.SchemaElements[index]
		cbt.DataTable.Path = common.StrToPath(cbt.PathStr)
	}

	for range numRows {
		cbt.DataTable.Values = append(cbt.DataTable.Values, nil)
		cbt.DataTable.RepetitionLevels = append(cbt.DataTable.RepetitionLevels, int32(0))
		cbt.DataTable.DefinitionLevels = append(cbt.DataTable.DefinitionLevels, int32(0))
	}
	cbt.DataTableNumRows += numRows
	cbt.ChunkReadValues += cbt.ChunkHeader.MetaData.GetNumValues()
	return nil
}

// currentRowGroupNumRows returns the number of top-level rows in the row group whose
// chunk is currently loaded. It falls back to the chunk's declared value count (correct
// for flat columns) when the row group cannot be resolved.
func (cbt *ColumnBufferType) currentRowGroupNumRows() int64 {
	if cbt.Footer != nil {
		rowGroups := cbt.Footer.GetRowGroups()
		if idx := cbt.RowGroupIndex - 1; idx >= 0 && idx < int64(len(rowGroups)) && rowGroups[idx] != nil {
			return rowGroups[idx].GetNumRows()
		}
	}
	return cbt.ChunkHeader.MetaData.GetNumValues()
}

// errColumnExhausted signals normal completion of a column read or skip: the row
// groups are exhausted, or the page cursor holds no more page bytes (an empty or
// prematurely-ended chunk). It wraps io.EOF so external callers of the exported
// ReadPage/ReadPageForSkip can keep detecting completion with errors.Is(err,
// io.EOF), while internal code matches the specific sentinel. A truncated page
// yields an error that wraps io.EOF but not this sentinel, so it is treated as
// completion by neither and propagates as a real error.
var errColumnExhausted = fmt.Errorf("column exhausted: %w", io.EOF)

// emptyChunkAtCursor reports whether the page cursor holds no more bytes, i.e. it
// sits at or past EOF. It peeks a single byte without consuming it, so a following
// page read is unaffected. This distinguishes a chunk with no (further) page bytes,
// which is normal completion, from an io.EOF that only surfaces after a page header
// has been parsed (a truncated or corrupt page), which must not be silently turned
// into fabricated null data or swallowed.
func (cbt *ColumnBufferType) emptyChunkAtCursor() bool {
	if cbt.ThriftReader == nil {
		return false
	}
	_, err := cbt.ThriftReader.Peek(1)
	return errors.Is(err, io.EOF)
}

// chunkReadStarted reports whether any page from the current chunk has been read.
// Dictionary pages do not advance ChunkReadValues, so DictPage must be checked too.
func (cbt *ColumnBufferType) chunkReadStarted() bool {
	return cbt.ChunkReadValues > 0 || cbt.DictPage != nil
}

func (cbt *ColumnBufferType) ReadPage() error {
	for cbt.ChunkHeader == nil || cbt.ChunkHeader.MetaData == nil || cbt.ChunkReadValues >= cbt.ChunkHeader.MetaData.NumValues {
		// Current chunk is exhausted; advance to the next row group and retry.
		if err := cbt.NextRowGroup(); err != nil {
			if errors.Is(err, io.EOF) {
				return errColumnExhausted
			}
			return fmt.Errorf("move to next row group: %w", err)
		}
	}

	if cbt.Reader != nil {
		if err := cbt.Reader.requirePageDecryptor(cbt); err != nil {
			return fmt.Errorf("require page decryptor: %w", err)
		}
	}

	// No page bytes remain at the cursor. Only an untouched chunk (ChunkReadValues == 0,
	// tracked per chunk since DataTable persists across row groups) is a recoverable
	// empty chunk: synthesize its declared nulls and return without error so the next
	// iteration advances via NextRowGroup, which normalizes the "one less than actual"
	// count once. If pages were already read but the declared value count is not yet
	// met, the chunk ran out of page data mid-stream — a truncation — and must surface
	// as an error rather than be completed silently.
	if cbt.emptyChunkAtCursor() {
		if cbt.chunkReadStarted() {
			return fmt.Errorf("read page: truncated column chunk: %w", io.EOF)
		}
		if err := cbt.appendNullChunk(); err != nil {
			return err
		}
		return nil
	}

	page, numValues, numRows, err := layout.ReadPage(cbt.ThriftReader, cbt.SchemaHandler, cbt.readMetaData(), &cbt.PageReadOptions)
	if err != nil {
		return fmt.Errorf("read page: %w", err)
	}

	if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		cbt.DictPage = page
		return nil
	}

	if err := cbt.ensureIndexedDictionary(); err != nil {
		return fmt.Errorf("load indexed dictionary: %w", err)
	}
	page.Decode(cbt.DictPage)
	if cbt.DataTable == nil {
		cbt.DataTable = layout.NewTableFromTable(page.DataTable)
	}

	cbt.DataTable.Merge(page.DataTable)
	cbt.ChunkReadValues += numValues
	cbt.DataTableNumRows += numRows
	cbt.finishIndexedDataPage()
	return nil
}

// ReadRows reads up to num rows into a table. Reaching the end of the column's data
// is normal completion and returns a nil error; any other failure (including an
// io.EOF from a truncated page) is returned.
func (cbt *ColumnBufferType) ReadRows(num int64) (*layout.Table, int64, error) {
	if cbt.Footer.NumRows == 0 {
		return &layout.Table{}, 0, nil
	}

	var err error

	for cbt.DataTableNumRows < num && err == nil {
		err = cbt.ReadPage()
	}

	// errColumnExhausted is normal completion; everything else (e.g. a truncated chunk)
	// is a real error that must be reported.
	realErr := err != nil && !errors.Is(err, errColumnExhausted)

	// A real error stops reading with rows still buffered under the one-less convention;
	// normalize once, as terminal completion would, so every buffered row is exposed
	// alongside the error instead of one being hidden.
	if realErr && !cbt.dataTableNumRowsNormalized && cbt.DataTableNumRows >= 0 {
		cbt.DataTableNumRows++
		cbt.dataTableNumRowsNormalized = true
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
	if realErr {
		return res, num, fmt.Errorf("read rows: %w", err)
	}
	return res, num, nil
}
