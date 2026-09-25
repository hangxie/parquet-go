package reader

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
)

// InternalFooter returns a copy of Footer with schema names and column metadata
// paths translated from external Parquet names to internal Go names.
//
// The reader's Footer remains unchanged and continues to expose the metadata as
// stored in the Parquet file.
func (pr *ParquetReader) InternalFooter() (*parquet.FileMetaData, error) {
	if pr == nil || pr.Footer == nil {
		return nil, nil
	}
	footer, err := cloneFileMetaData(pr.Footer)
	if err != nil {
		return nil, fmt.Errorf("clone footer: %w", err)
	}
	renameFooterSchema(pr.SchemaHandler, footer, pr.caseInsensitive)
	return footer, nil
}

// RowGroupSortingColumns returns a copy of the sorting-column metadata for a
// row group. ColumnIdx is the zero-based leaf-column ordinal in that row group.
func (pr *ParquetReader) RowGroupSortingColumns(rowGroupIndex int) ([]*parquet.SortingColumn, error) {
	if pr == nil || pr.Footer == nil {
		return nil, fmt.Errorf("reader footer is unavailable")
	}
	if rowGroupIndex < 0 || rowGroupIndex >= len(pr.Footer.RowGroups) {
		return nil, fmt.Errorf("row group index %d out of range [0, %d)", rowGroupIndex, len(pr.Footer.RowGroups))
	}
	rowGroup := pr.Footer.RowGroups[rowGroupIndex]
	if rowGroup == nil {
		return nil, fmt.Errorf("row group %d is nil", rowGroupIndex)
	}
	if rowGroup.SortingColumns == nil {
		return nil, nil
	}
	columns := make([]*parquet.SortingColumn, len(rowGroup.SortingColumns))
	for i, column := range rowGroup.SortingColumns {
		if column == nil {
			continue
		}
		clonedColumn := *column
		columns[i] = &clonedColumn
	}
	return columns, nil
}

// RenameSchema rewrites Footer schema names and column metadata paths from
// external Parquet names to internal Go names. Prefer InternalFooter when a
// converted footer is needed without mutating the reader's file metadata.
//
// Deprecated: use InternalFooter to get converted metadata without mutating
// Footer.
func (pr *ParquetReader) RenameSchema() {
	if pr == nil {
		return
	}
	renameFooterSchema(pr.SchemaHandler, pr.Footer, pr.caseInsensitive)
}

func cloneFileMetaData(footer *parquet.FileMetaData) (*parquet.FileMetaData, error) {
	if footer == nil {
		return nil, nil
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	buf, err := ts.Write(context.Background(), footer)
	if err != nil {
		return nil, fmt.Errorf("encode file metadata: %w", err)
	}
	return readFileMetaDataFromBytes(context.Background(), buf)
}

func renameFooterSchema(sh *schema.SchemaHandler, footer *parquet.FileMetaData, caseInsensitive bool) {
	if sh == nil || sh.Infos == nil || footer == nil || footer.Schema == nil {
		return
	}

	for i := range len(sh.Infos) {
		if i < len(footer.Schema) && footer.Schema[i] != nil && sh.Infos[i] != nil {
			footer.Schema[i].Name = sh.Infos[i].InName
		}
	}

	exPathToInPath := make(map[string]string)
	if caseInsensitive {
		for exPath, inPath := range sh.ExPathToInPath {
			exPathToInPath[strings.ToLower(exPath)] = inPath
		}
	} else {
		exPathToInPath = sh.ExPathToInPath
	}

	if footer.RowGroups == nil {
		return
	}

	for _, rowGroup := range footer.RowGroups {
		if rowGroup == nil || rowGroup.Columns == nil {
			continue
		}
		for _, chunk := range rowGroup.Columns {
			if chunk == nil || chunk.MetaData == nil {
				continue
			}
			exPath := append([]string{sh.GetRootExName()}, chunk.MetaData.GetPathInSchema()...)
			exPathStr := common.PathToStr(exPath)

			if caseInsensitive {
				exPathStr = strings.ToLower(exPathStr)
			}

			if inPathStr, exists := exPathToInPath[exPathStr]; exists {
				inPath := common.StrToPath(inPathStr)[1:]
				chunk.MetaData.PathInSchema = inPath
			}
		}
	}
}

// Get the footer size
//
// Deprecated: use GetFooterSizeWithContext.
func (pr *ParquetReader) GetFooterSize() (uint32, error) {
	return pr.GetFooterSizeWithContext(pr.defaultContext())
}

// GetFooterSizeWithContext returns the footer size using ctx.
func (pr *ParquetReader) GetFooterSizeWithContext(ctx context.Context) (uint32, error) {
	if err := pr.setContext(ctx); err != nil {
		return 0, err
	}
	if pr.PFile == nil {
		return 0, fmt.Errorf("PFile is nil")
	}

	buf := make([]byte, 4)
	if _, err := source.SeekWithContext(pr.context(), pr.PFile, -8, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("seek to footer size: %w", err)
	}
	if _, err := source.ReadFullWithContext(pr.context(), pr.PFile, buf); err != nil {
		return 0, fmt.Errorf("read footer size: %w", err)
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (pr *ParquetReader) getFooterTail() (uint32, string, error) {
	if pr.PFile == nil {
		return 0, "", fmt.Errorf("PFile is nil")
	}

	buf := make([]byte, 8)
	if _, err := source.SeekWithContext(pr.context(), pr.PFile, -8, io.SeekEnd); err != nil {
		return 0, "", fmt.Errorf("seek to footer tail: %w", err)
	}
	if _, err := source.ReadFullWithContext(pr.context(), pr.PFile, buf); err != nil {
		return 0, "", fmt.Errorf("read footer tail: %w", err)
	}
	return binary.LittleEndian.Uint32(buf[:4]), string(buf[4:]), nil
}

// ReadFooter reads and publishes the file footer once.
//
// After a successful read, the reader treats Footer as immutable. Repeated calls
// return without reloading so internal caches remain tied to the same footer.
//
// Deprecated: use ReadFooterWithContext.
func (pr *ParquetReader) ReadFooter() error {
	return pr.ReadFooterWithContext(pr.defaultContext())
}

// ReadFooterWithContext reads and publishes the file footer once using ctx.
func (pr *ParquetReader) ReadFooterWithContext(ctx context.Context) error {
	if err := pr.setContext(ctx); err != nil {
		return err
	}
	pr.footerMu.Lock()
	defer pr.footerMu.Unlock()

	if pr.footerLoaded {
		return nil
	}

	size, magic, err := pr.getFooterTail()
	if err != nil {
		return fmt.Errorf("get footer tail: %w", err)
	}
	switch magic {
	case common.MagicBytesEncrypted:
		if err := pr.readEncryptedFooter(size); err != nil {
			return fmt.Errorf("read encrypted footer: %w", err)
		}
	default:
		if err := pr.readPlainFooter(size); err != nil {
			return fmt.Errorf("read plain footer: %w", err)
		}
	}
	if err := pr.decryptEncryptedColumnMetadata(); err != nil {
		return fmt.Errorf("decrypt encrypted column metadata: %w", err)
	}
	pr.footerLoaded = true
	return nil
}

func (pr *ParquetReader) readPlainFooter(size uint32) error {
	if _, err := source.SeekWithContext(pr.context(), pr.PFile, -int64(8+size), io.SeekEnd); err != nil {
		return fmt.Errorf("seek to footer: %w", err)
	}
	pr.Footer = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{})
	thriftReader := thrift.NewStreamTransportR(source.ReaderWithContext{Ctx: pr.context(), Reader: pr.PFile})
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
	protocol := pf.GetProtocol(bufferReader)
	if err := pr.Footer.Read(pr.context(), protocol); err != nil {
		return fmt.Errorf("read footer: %w", err)
	}

	if pr.Footer.IsSetEncryptionAlgorithm() {
		if _, err := source.SeekWithContext(pr.context(), pr.PFile, -int64(8+size), io.SeekEnd); err != nil {
			return fmt.Errorf("seek to plaintext footer section: %w", err)
		}
		section := make([]byte, size)
		if _, err := source.ReadFullWithContext(pr.context(), pr.PFile, section); err != nil {
			return fmt.Errorf("read plaintext footer section: %w", err)
		}
		if err := pr.verifyPlaintextFooter(section); err != nil {
			return fmt.Errorf("verify plaintext footer: %w", err)
		}
	}
	return nil
}
