package reader

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
)

type ColumnBufferType struct {
	PFile        source.ParquetFileReader
	ThriftReader *thrift.TBufferedTransport

	Footer        *parquet.FileMetaData
	SchemaHandler *schema.SchemaHandler

	PathStr       string
	RowGroupIndex int64
	ColumnOrdinal int16
	ChunkHeader   *parquet.ColumnChunk

	ChunkReadValues int64

	// fileSize is the total byte size of the backing file, used as a hard ceiling
	// on metadata-declared value counts to prevent a corrupt chunk from driving
	// an unbounded null back-fill.
	fileSize int64

	DictPage *layout.Page

	DataTable        *layout.Table
	DataTableNumRows int64

	PageReadOptions layout.PageReadOptions
	Reader          *ParquetReader

	caseInsensitive   bool
	chunkReadMetaData *parquet.ColumnMetaData
}

// NewColumnBuffer creates a column buffer for the column identified by pathStr.
// pathStr components must be separated by common.ParGoPathDelimiter (build it with
// common.PathToStr); "." is an ordinary character in a name, not a separator.
func NewColumnBuffer(pFile source.ParquetFileReader, footer *parquet.FileMetaData, schemaHandler *schema.SchemaHandler, pathStr string, opts *layout.PageReadOptions) (*ColumnBufferType, error) {
	return newColumnBuffer(pFile, footer, schemaHandler, pathStr, opts, false)
}

func newColumnBuffer(pFile source.ParquetFileReader, footer *parquet.FileMetaData, schemaHandler *schema.SchemaHandler, pathStr string, opts *layout.PageReadOptions, caseInsensitive bool) (*ColumnBufferType, error) {
	if pFile == nil {
		return nil, fmt.Errorf("pFile is nil")
	}
	if footer == nil {
		return nil, fmt.Errorf("footer is nil")
	}
	if schemaHandler == nil {
		return nil, fmt.Errorf("schema handler is nil")
	}
	// If the path exists in the schema handler map, validate its type to catch
	// corrupt or unsupported schemas early. Otherwise, skip validation because
	// some callers rely on locating columns by footer path only.
	if schemaHandler.MapIndex != nil {
		if _, exists := schemaHandler.MapIndex[pathStr]; exists {
			if _, err := schemaHandler.GetType(pathStr); err != nil {
				return nil, fmt.Errorf("get type for %s: %w", pathStr, err)
			}
		}
	}
	ctx := context.Background()
	if opts != nil && opts.Context != nil {
		ctx = opts.Context
	}
	newPFile, err := source.CloneWithContext(ctx, pFile)
	if err != nil {
		return nil, fmt.Errorf("clone file reader: %w", err)
	}
	// Capture the file size as a ceiling for metadata-declared counts. NextRowGroup
	// re-seeks to the column offset before reading, so seeking to the end here is safe.
	fileSize, err := source.SeekWithContext(ctx, newPFile, 0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("determine file size: %w", err)
	}
	var opt layout.PageReadOptions
	if opts != nil {
		opt = *opts
	}
	res := &ColumnBufferType{
		PFile:            newPFile,
		Footer:           footer,
		SchemaHandler:    schemaHandler,
		PathStr:          pathStr,
		DataTableNumRows: -1,
		PageReadOptions:  opt,
		fileSize:         fileSize,
		caseInsensitive:  caseInsensitive,
	}

	if err := res.NextRowGroup(); err != nil && err != io.EOF {
		// res is discarded, so close its file handle to avoid leaking the clone
		// (or an external reader NextRowGroup opened before failing).
		if res.PFile != nil {
			_ = res.PFile.Close()
		}
		return nil, fmt.Errorf("advance to first row group: %w", err)
	}
	return res, nil
}

func (cbt *ColumnBufferType) NextRowGroup() error {
	if cbt.Footer == nil {
		return io.EOF
	}

	rowGroups := cbt.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if cbt.RowGroupIndex >= ln {
		if ln > 0 { // Only increment if there were actually row groups to process
			cbt.DataTableNumRows++ // very important, because DataTableNumRows is one smaller than real rows number
		}
		return io.EOF
	}

	cbt.RowGroupIndex++
	cbt.chunkReadMetaData = nil

	columnChunks := rowGroups[cbt.RowGroupIndex-1].GetColumns()
	i := int64(0)
	ln = int64(len(columnChunks))
	for i = 0; i < ln; i++ {
		// A column chunk with no metadata cannot be matched and must be skipped
		// rather than dereferenced (corrupt footers may omit it).
		if columnChunks[i] == nil || columnChunks[i].MetaData == nil {
			continue
		}
		path := columnPathToInPath(cbt.SchemaHandler, columnChunks[i].MetaData.GetPathInSchema(), cbt.caseInsensitive)

		if cbt.PathStr == path {
			break
		}
	}

	if i >= ln {
		return fmt.Errorf("[NextRowGroup] Column not found: %v", cbt.PathStr)
	}

	cbt.ChunkHeader = columnChunks[i]
	cbt.chunkReadMetaData = columnMetaDataForRead(cbt.SchemaHandler, cbt.ChunkHeader.MetaData, cbt.caseInsensitive)
	cbt.ColumnOrdinal = int16(i)
	if cbt.Reader != nil {
		if err := cbt.Reader.configureOptionalPageDecryptor(cbt, rowGroups[cbt.RowGroupIndex-1], int16(i)); err != nil {
			return fmt.Errorf("configure page decryptor: %w", err)
		}
	}
	if columnChunks[i].FilePath != nil {
		// Open into a local variable and assign only on success; a failed Open
		// returns a nil interface, which would otherwise clobber cbt.PFile and
		// panic when ReadStop later calls Close on it. The previous handle is
		// released only after the new one is opened.
		pFile, err := source.OpenWithContext(cbt.context(), cbt.PFile, *columnChunks[i].FilePath)
		if err != nil {
			return fmt.Errorf("open file %s: %w", *columnChunks[i].FilePath, err)
		}
		_ = source.CloseWithContext(cbt.context(), cbt.PFile)
		cbt.PFile = pFile
	}

	// offset := columnChunks[i].FileOffset
	offset := columnChunks[i].MetaData.DataPageOffset
	if columnChunks[i].MetaData.DictionaryPageOffset != nil {
		offset = *columnChunks[i].MetaData.DictionaryPageOffset
	}

	if cbt.ThriftReader != nil {
		_ = cbt.ThriftReader.Close()
	}

	if _, err := source.SeekWithContext(cbt.context(), cbt.PFile, offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek to thrift reader offset %d: %w", offset, err)
	}
	thriftTransport := thrift.NewStreamTransportR(columnBufferReader{buffer: cbt})
	thriftReader := thrift.NewTBufferedTransport(thriftTransport, 4096)
	cbt.ThriftReader = thriftReader
	cbt.ChunkReadValues = 0
	cbt.DictPage = nil
	return nil
}

func (cbt *ColumnBufferType) context() context.Context {
	if cbt.PageReadOptions.Context == nil {
		return context.Background()
	}
	return cbt.PageReadOptions.Context
}

type columnBufferReader struct {
	buffer *ColumnBufferType
}

func (r columnBufferReader) Read(p []byte) (int, error) {
	return source.ReadWithContext(r.buffer.context(), r.buffer.PFile, p)
}

func (cbt *ColumnBufferType) readMetaData() *parquet.ColumnMetaData {
	if cbt.chunkReadMetaData != nil {
		return cbt.chunkReadMetaData
	}
	if cbt.ChunkHeader == nil {
		return nil
	}
	return columnMetaDataForRead(cbt.SchemaHandler, cbt.ChunkHeader.MetaData, cbt.caseInsensitive)
}

// backfillEmptyChunk synthesizes all-null top-level rows for a column chunk that
// declares values but yields no readable page data (an EOF on the first read).
// It rejects a declared count larger than the file size, since a chunk with no
// page bytes cannot legitimately hold more values than the file contains, which
// guards against an unbounded allocation from a corrupt count.
func (cbt *ColumnBufferType) backfillEmptyChunk() error {
	if cbt.ChunkHeader.MetaData.GetNumValues() > cbt.fileSize {
		return fmt.Errorf("read page: column chunk value count %d exceeds file size %d",
			cbt.ChunkHeader.MetaData.GetNumValues(), cbt.fileSize)
	}

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
	return nil
}

func (cbt *ColumnBufferType) ReadPage() error {
	for cbt.ChunkHeader == nil || cbt.ChunkHeader.MetaData == nil || cbt.ChunkReadValues >= cbt.ChunkHeader.MetaData.NumValues {
		// Current chunk is exhausted; advance to the next row group and retry.
		if err := cbt.NextRowGroup(); err != nil {
			return fmt.Errorf("move to next row group: %w", err)
		}
	}

	if cbt.Reader != nil {
		if err := cbt.Reader.requirePageDecryptor(cbt); err != nil {
			return fmt.Errorf("require page decryptor: %w", err)
		}
	}
	page, numValues, numRows, err := layout.ReadPage(cbt.ThriftReader, cbt.SchemaHandler, cbt.readMetaData(), &cbt.PageReadOptions)
	if err != nil {
		// data is nil and rl/dl=0, no pages in file
		if err == io.EOF && cbt.DataTable == nil && cbt.SchemaHandler != nil &&
			cbt.SchemaHandler.MapIndex != nil && cbt.SchemaHandler.SchemaElements != nil {
			if ferr := cbt.backfillEmptyChunk(); ferr != nil {
				return ferr
			}
		}

		return fmt.Errorf("read page: %w", err)
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
	return nil
}

// ReadRows reads up to num rows into a table and returns any non-EOF error.
func (cbt *ColumnBufferType) ReadRows(num int64) (*layout.Table, int64, error) {
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
		return res, num, fmt.Errorf("read rows: %w", err)
	}
	return res, num, nil
}
