package reader

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

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

	DictPage *layout.Page

	DataTable        *layout.Table
	DataTableNumRows int64

	PageReadOptions layout.PageReadOptions
	Reader          *ParquetReader

	caseInsensitive   bool
	chunkReadMetaData *parquet.ColumnMetaData

	// dataTableNumRowsNormalized records that the increment converting DataTableNumRows
	// from "one less than actual" to the real count has already been applied at
	// completion, so it is never applied twice — neither by repeated NextRowGroup calls
	// past the end (which would report phantom rows) nor across the shared skip-then-read
	// use of a column buffer.
	dataTableNumRowsNormalized bool
	// indexedPagesRemaining counts data pages left to read on a cursor positioned by
	// the offset index; zero means no such cursor is active. Reaching zero marks the
	// column chunk as fully consumed even though earlier pages were skipped unread.
	indexedPagesRemaining    int
	indexedDictionaryPending bool
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
		// Normalize DataTableNumRows (kept one smaller than the real row count while
		// reading) to the real count. Apply the increment only once and only while rows
		// are actually buffered, but record that terminal exhaustion has been observed
		// unconditionally — otherwise a drained buffer (DataTableNumRows < 0) would leave
		// the flag unset, letting a later read increment a count ReadRows has since reset
		// to 0 and report a phantom row (which can panic callers slicing values by it).
		if ln > 0 {
			if !cbt.dataTableNumRowsNormalized && cbt.DataTableNumRows >= 0 {
				cbt.DataTableNumRows++
			}
			cbt.dataTableNumRowsNormalized = true
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
	chunkSize := cbt.readMetaData().GetTotalCompressedSize()
	if chunkSize < 0 {
		return fmt.Errorf("column chunk compressed size is negative: %d", chunkSize)
	}
	thriftTransport := thrift.NewStreamTransportR(&columnBufferReader{buffer: cbt, remaining: chunkSize})
	thriftReader := thrift.NewTBufferedTransport(thriftTransport, 4096)
	cbt.ThriftReader = thriftReader
	cbt.ChunkReadValues = 0
	cbt.DictPage = nil
	cbt.indexedPagesRemaining = 0
	cbt.indexedDictionaryPending = false
	return nil
}

func (cbt *ColumnBufferType) context() context.Context {
	if cbt.PageReadOptions.Context == nil {
		return context.Background()
	}
	return cbt.PageReadOptions.Context
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
