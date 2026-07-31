package reader

import (
	"fmt"
	"io"
	"sort"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source"
)

// Offset-index skipping trusts the writer-provided FirstRowIndex values to position the
// cursor: a page's absolute start cannot be confirmed without reading (and counting) the
// very pages the optimization skips, so there is no cheap way to detect an index whose
// offsets are self-consistent but shifted. The structural checks below only establish
// that the index is well formed enough to seek and read safely; a chunk whose data is
// genuinely unreadable that way falls back to a sequential skip. This matches how the
// non-indexed reader and other Parquet implementations treat the offset index.

type indexedSkipTarget struct {
	pageIndex int
	offset    int64
	chunkEnd  int64
	firstRow  int64
	// pageCount is how many data pages remain from the target page to the end of the
	// chunk; the cursor reads them in order until this many have been consumed.
	pageCount int
}

func (cbt *ColumnBufferType) hasIndexedPageCursor() bool {
	return cbt.indexedPagesRemaining > 0
}

func (cbt *ColumnBufferType) skipWithOffsetIndex(num int64) (int64, bool, error) {
	if cbt.Reader == nil || cbt.ChunkHeader == nil || cbt.ChunkHeader.MetaData == nil ||
		cbt.ChunkReadValues != 0 || cbt.hasIndexedPageCursor() {
		return 0, false, nil
	}
	rowGroupIndex := cbt.RowGroupIndex - 1
	if rowGroupIndex < 0 || int(rowGroupIndex) >= len(cbt.Footer.GetRowGroups()) {
		return 0, false, nil
	}
	rowGroup := cbt.Footer.RowGroups[rowGroupIndex]
	if rowGroup == nil || num >= rowGroup.GetNumRows() {
		return 0, false, nil
	}

	index, err := cbt.Reader.readOffsetIndexWithContext(cbt.context(), int(rowGroupIndex), int(cbt.ColumnOrdinal))
	if err != nil {
		if ctxErr := cbt.context().Err(); ctxErr != nil {
			// The caller cancelled or timed out; surface that rather than hiding it.
			return 0, true, fmt.Errorf("read row group %d column %d: %w", rowGroupIndex, cbt.ColumnOrdinal, err)
		}
		// The offset index could not be read or decoded. It is only an optimization
		// hint, and a failure confined to the index region leaves the data pages
		// readable, so fall back to sequential page skipping. A genuine I/O fault that
		// also affects the data pages will resurface there.
		return 0, false, nil
	}
	if index == nil {
		return 0, false, nil
	}
	target, err := validateIndexedSkipTarget(cbt.ChunkHeader.MetaData, index, rowGroup.GetNumRows(), num)
	if err != nil {
		// A structurally invalid offset index is an unusable hint; fall back rather
		// than failing the skip.
		return 0, false, nil
	}
	if target.pageIndex > int(^uint16(0)>>1) && cbt.ChunkHeader.GetCryptoMetadata() != nil {
		// The encrypted page ordinal is a signed 16-bit value; a chunk with more pages
		// than that cannot be seeked by ordinal, so skip the optimization.
		return 0, false, nil
	}

	withinPage := num - target.firstRow
	if err := cbt.seekToIndexedPage(target, withinPage > 0); err != nil {
		return 0, true, fmt.Errorf("seek row group %d column %d to data page %d: %w", rowGroupIndex, cbt.ColumnOrdinal, target.pageIndex, err)
	}
	if withinPage == 0 {
		return num, true, nil
	}
	skipped, err := cbt.skipByReadingPages(withinPage)
	return target.firstRow + skipped, true, err
}

func validateIndexedSkipTarget(meta *parquet.ColumnMetaData, index *parquet.OffsetIndex, rowCount, targetRow int64) (indexedSkipTarget, error) {
	if meta == nil {
		return indexedSkipTarget{}, fmt.Errorf("column metadata is nil")
	}
	locations := index.GetPageLocations()
	if len(locations) == 0 {
		return indexedSkipTarget{}, fmt.Errorf("offset index has no page locations")
	}
	if rowCount <= 0 || targetRow < 0 || targetRow >= rowCount {
		return indexedSkipTarget{}, fmt.Errorf("target row %d is outside row group with %d rows", targetRow, rowCount)
	}

	chunkStart, chunkEnd, err := indexedChunkRange(meta)
	if err != nil {
		return indexedSkipTarget{}, err
	}
	if err := validateIndexedPageLocations(meta, locations, rowCount, chunkStart, chunkEnd); err != nil {
		return indexedSkipTarget{}, err
	}

	pageIndex := sort.Search(len(locations), func(i int) bool {
		return locations[i].GetFirstRowIndex() > targetRow
	}) - 1
	location := locations[pageIndex]
	return indexedSkipTarget{
		pageIndex: pageIndex,
		offset:    location.GetOffset(),
		chunkEnd:  chunkEnd,
		firstRow:  location.GetFirstRowIndex(),
		pageCount: len(locations) - pageIndex,
	}, nil
}

func indexedChunkRange(meta *parquet.ColumnMetaData) (int64, int64, error) {
	chunkStart := meta.GetDataPageOffset()
	if meta.IsSetDictionaryPageOffset() && meta.GetDictionaryPageOffset() < chunkStart {
		chunkStart = meta.GetDictionaryPageOffset()
	}
	chunkSize := meta.GetTotalCompressedSize()
	if chunkStart < 0 || chunkSize <= 0 || chunkStart > int64(^uint64(0)>>1)-chunkSize {
		return 0, 0, fmt.Errorf("invalid column chunk range: offset=%d size=%d", chunkStart, chunkSize)
	}
	return chunkStart, chunkStart + chunkSize, nil
}

func validateIndexedPageLocations(meta *parquet.ColumnMetaData, locations []*parquet.PageLocation, rowCount, chunkStart, chunkEnd int64) error {
	for i, location := range locations {
		if location == nil {
			return fmt.Errorf("page location %d is nil", i)
		}
	}
	for i := range locations {
		if err := validateIndexedPageLocation(meta, locations, i, rowCount, chunkStart, chunkEnd); err != nil {
			return err
		}
	}
	return nil
}

func validateIndexedPageLocation(meta *parquet.ColumnMetaData, locations []*parquet.PageLocation, pageIndex int, rowCount, chunkStart, chunkEnd int64) error {
	location := locations[pageIndex]
	if pageIndex == 0 {
		if location.GetFirstRowIndex() != 0 {
			return fmt.Errorf("first page starts at row %d, want 0", location.GetFirstRowIndex())
		}
		if location.GetOffset() != meta.GetDataPageOffset() {
			return fmt.Errorf("first page offset %d does not match data page offset %d", location.GetOffset(), meta.GetDataPageOffset())
		}
	} else if location.GetFirstRowIndex() <= locations[pageIndex-1].GetFirstRowIndex() {
		return fmt.Errorf("page %d first row %d is not greater than page %d first row %d", pageIndex, location.GetFirstRowIndex(), pageIndex-1, locations[pageIndex-1].GetFirstRowIndex())
	}
	if location.GetFirstRowIndex() < 0 || location.GetFirstRowIndex() >= rowCount {
		return fmt.Errorf("page %d first row %d is outside row group with %d rows", pageIndex, location.GetFirstRowIndex(), rowCount)
	}
	size := int64(location.GetCompressedPageSize())
	if size <= 0 || location.GetOffset() < meta.GetDataPageOffset() || location.GetOffset() > chunkEnd-size {
		return fmt.Errorf("page %d has invalid range: offset=%d size=%d chunk=[%d,%d)", pageIndex, location.GetOffset(), size, chunkStart, chunkEnd)
	}
	// Listed data pages are read back-to-back from a single stream, so each must abut the
	// next, exactly as the non-indexed reader consumes a chunk. The final page only has
	// to fit inside the chunk: like a sequential read, the cursor stops once the chunk's
	// declared value count is reached, so any trailing bytes (padding accounted for in
	// TotalCompressedSize) are never touched.
	pageEndOffset := location.GetOffset() + size
	if pageIndex+1 < len(locations) && pageEndOffset != locations[pageIndex+1].GetOffset() {
		return fmt.Errorf("page %d ends at %d, next page starts at %d", pageIndex, pageEndOffset, locations[pageIndex+1].GetOffset())
	}
	return nil
}

func (cbt *ColumnBufferType) seekToIndexedPage(target indexedSkipTarget, loadDictionary bool) error {
	pageFile, err := source.CloneWithContext(cbt.context(), cbt.PFile)
	if err != nil {
		return fmt.Errorf("clone column file: %w", err)
	}
	keepFile := false
	defer func() {
		if !keepFile {
			_ = source.CloseWithContext(cbt.context(), pageFile)
		}
	}()

	var dictionary *layout.Page
	if loadDictionary && cbt.ChunkHeader.MetaData.IsSetDictionaryPageOffset() {
		dictionary, err = cbt.readIndexedDictionary(pageFile)
		if err != nil {
			return err
		}
	}
	transport, err := cbt.newIndexedTransport(pageFile, target.offset, target.chunkEnd-target.offset)
	if err != nil {
		return err
	}
	if cbt.PageReadOptions.Decryptor != nil {
		cbt.PageReadOptions.Decryptor.PageOrdinal = int16(target.pageIndex)
	}

	oldFile := cbt.PFile
	if cbt.ThriftReader != nil {
		_ = cbt.ThriftReader.Close()
	}
	cbt.PFile = pageFile
	cbt.ThriftReader = transport
	cbt.DictPage = dictionary
	cbt.indexedPagesRemaining = target.pageCount
	cbt.indexedDictionaryPending = dictionary == nil && cbt.ChunkHeader.MetaData.IsSetDictionaryPageOffset()
	keepFile = true
	_ = source.CloseWithContext(cbt.context(), oldFile)
	return nil
}

func (cbt *ColumnBufferType) readIndexedDictionary(pageFile source.ParquetFileReader) (*layout.Page, error) {
	dictionaryOffset := cbt.ChunkHeader.MetaData.GetDictionaryPageOffset()
	dataOffset := cbt.ChunkHeader.MetaData.GetDataPageOffset()
	if dictionaryOffset < 0 || dictionaryOffset >= dataOffset {
		return nil, fmt.Errorf("invalid dictionary page offset %d for data page offset %d", dictionaryOffset, dataOffset)
	}
	transport, err := cbt.newIndexedTransport(pageFile, dictionaryOffset, dataOffset-dictionaryOffset)
	if err != nil {
		return nil, fmt.Errorf("prepare dictionary page: %w", err)
	}
	defer func() { _ = transport.Close() }()
	options := cbt.PageReadOptions
	if options.Decryptor != nil {
		decryptor := *options.Decryptor
		decryptor.PageOrdinal = 0
		options.Decryptor = &decryptor
	}
	page, err := layout.ReadPageRawData(transport, cbt.SchemaHandler, cbt.readMetaData(), &options)
	if err != nil {
		return nil, fmt.Errorf("read dictionary page: %w", err)
	}
	if page.Header.GetType() != parquet.PageType_DICTIONARY_PAGE {
		return nil, fmt.Errorf("page at dictionary offset %d has type %v", dictionaryOffset, page.Header.GetType())
	}
	if _, _, err := page.GetRLDLFromRawData(cbt.SchemaHandler); err != nil {
		return nil, fmt.Errorf("read dictionary levels: %w", err)
	}
	if err := page.GetValueFromRawData(cbt.SchemaHandler); err != nil {
		return nil, fmt.Errorf("decode dictionary page: %w", err)
	}
	return page, nil
}

func (cbt *ColumnBufferType) newIndexedTransport(pageFile source.ParquetFileReader, offset, remaining int64) (*thrift.TBufferedTransport, error) {
	position, err := source.SeekWithContext(cbt.context(), pageFile, offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek to offset %d: %w", offset, err)
	}
	if position != offset {
		return nil, fmt.Errorf("seek to offset %d stopped at %d", offset, position)
	}
	stream := thrift.NewStreamTransportR(&columnBufferReader{buffer: cbt, file: pageFile, remaining: remaining})
	return thrift.NewTBufferedTransport(stream, 4096), nil
}

func (cbt *ColumnBufferType) ensureIndexedDictionary() error {
	if !cbt.indexedDictionaryPending {
		return nil
	}
	pageFile, err := source.CloneWithContext(cbt.context(), cbt.PFile)
	if err != nil {
		return fmt.Errorf("clone column file: %w", err)
	}
	defer func() { _ = source.CloseWithContext(cbt.context(), pageFile) }()
	dictionary, err := cbt.readIndexedDictionary(pageFile)
	if err != nil {
		return err
	}
	cbt.DictPage = dictionary
	cbt.indexedDictionaryPending = false
	return nil
}

func (cbt *ColumnBufferType) finishIndexedDataPage() {
	if !cbt.hasIndexedPageCursor() {
		return
	}
	cbt.indexedPagesRemaining--
	if cbt.indexedPagesRemaining == 0 {
		// The skipped leading pages never advanced ChunkReadValues, so force it to the
		// chunk's full value count now that the last indexed page is consumed; otherwise
		// the chunk would look unfinished and read past its end.
		cbt.ChunkReadValues = cbt.ChunkHeader.MetaData.GetNumValues()
	}
}
