package reader

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/bloomfilter"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source"
)

// BloomFilterCheck checks if a value might exist in the given column of the given row group.
// It returns true if the value might exist (or if there is no bloom filter), false if the value
// is definitely not present. columnPath is rootless and matched against the parquet tag names;
// its components must be separated by common.ParGoPathDelimiter (build it with common.PathToStr,
// e.g. common.PathToStr([]string{"address", "city"})). "." is treated as an ordinary character,
// so a column whose name itself contains a dot is a single path component.
//
// Deprecated: use BloomFilterCheckWithContext.
func (pr *ParquetReader) BloomFilterCheck(columnPath string, rowGroupIndex int, value any) (bool, error) {
	return pr.BloomFilterCheckWithContext(pr.defaultContext(), columnPath, rowGroupIndex, value)
}

// BloomFilterCheckWithContext checks a bloom filter using ctx.
func (pr *ParquetReader) BloomFilterCheckWithContext(ctx context.Context, columnPath string, rowGroupIndex int, value any) (bool, error) {
	if err := pr.setContext(ctx); err != nil {
		return false, err
	}
	rg, columnChunk, columnOrdinal, err := pr.locateColumnChunk(columnPath, rowGroupIndex)
	if err != nil {
		return false, err
	}
	// checked before the filter is looked for, so the answer does not depend on whether one was written
	if err := pr.checkFixedWidth(columnPath, columnOrdinal, columnChunk.MetaData.GetType(), value); err != nil {
		return false, err
	}
	if !columnChunk.MetaData.IsSetBloomFilterOffset() {
		// No bloom filter for this column; conservatively return true
		return true, nil
	}

	pf, err := source.CloneWithContext(ctx, pr.PFile)
	if err != nil {
		return false, fmt.Errorf("clone file reader: %w", err)
	}
	defer func() { _ = source.CloseWithContext(ctx, pf) }()

	filter, err := pr.readBloomFilterForColumn(pf, rowGroupIndex, columnOrdinal, rg, columnChunk)
	if err != nil {
		return false, fmt.Errorf("read bloom filter: %w", err)
	}

	hash, err := bloomfilter.HashValue(value, columnChunk.MetaData.GetType())
	if err != nil {
		return false, fmt.Errorf("hash value: %w", err)
	}

	return filter.Check(hash), nil
}

// BloomFilterSize returns one row group's bloom filter bitset size in bytes, 0 when it has none.
func (pr *ParquetReader) BloomFilterSize(ctx context.Context, columnPath string, rowGroupIndex int) (int32, error) {
	// columnPath follows the same rules as BloomFilterCheckWithContext
	if err := pr.setContext(ctx); err != nil {
		return 0, err
	}
	rg, columnChunk, columnOrdinal, err := pr.locateColumnChunk(columnPath, rowGroupIndex)
	if err != nil {
		return 0, err
	}
	if !columnChunk.MetaData.IsSetBloomFilterOffset() {
		return 0, nil
	}

	pf, err := source.CloneWithContext(ctx, pr.PFile)
	if err != nil {
		return 0, fmt.Errorf("clone file reader: %w", err)
	}
	defer func() { _ = source.CloseWithContext(ctx, pf) }()

	// Only the header is fetched: a bitset reaches 128MB, and an inventory walks every row group.
	size, err := pr.readBloomFilterSizeForColumn(pf, rowGroupIndex, columnOrdinal, rg, columnChunk)
	if err != nil {
		return 0, fmt.Errorf("read bloom filter: %w", err)
	}
	return size, nil
}

// locateColumnChunk resolves a rootless external column path to its chunk in one row group.
func (pr *ParquetReader) locateColumnChunk(columnPath string, rowGroupIndex int) (*parquet.RowGroup, *parquet.ColumnChunk, int, error) {
	if rowGroupIndex < 0 || rowGroupIndex >= len(pr.Footer.RowGroups) {
		return nil, nil, 0, fmt.Errorf("row group index %d out of range [0, %d)", rowGroupIndex, len(pr.Footer.RowGroups))
	}
	rg := pr.Footer.RowGroups[rowGroupIndex]

	// Prepend the schema root to the rootless external column path and resolve it
	// to the internal path used in the footer.
	rootPath := common.PathToStr([]string{pr.SchemaHandler.GetRootExName(), columnPath})
	inPath, err := pr.SchemaHandler.ConvertToInPathStr(rootPath)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("column %q not found in row group %d", columnPath, rowGroupIndex)
	}

	for i, cc := range rg.Columns {
		if cc == nil || cc.MetaData == nil {
			continue
		}
		if columnPathToInPath(pr.SchemaHandler, pr.fileSchemaHandler, cc.MetaData.GetPathInSchema(), pr.caseInsensitive) == inPath {
			return rg, cc, i, nil
		}
	}
	return nil, nil, 0, fmt.Errorf("column %q not found in row group %d", columnPath, rowGroupIndex)
}

// checkFixedWidth rejects a byte value that is not as wide as its FIXED_LEN_BYTE_ARRAY or INT96 column.
func (pr *ParquetReader) checkFixedWidth(columnPath string, columnOrdinal int, pT parquet.Type, value any) error {
	var width int
	switch pT {
	case parquet.Type_INT96:
		width = 12
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		// the filter holds the file's values, so the width is the file's, not the destination struct's
		leaf := fileLeaf(pr.Footer, columnOrdinal)
		if leaf == nil || leaf.GetType() != pT {
			return fmt.Errorf("column [%s]: chunk %d has no FIXED_LEN_BYTE_ARRAY leaf in the file schema", columnPath, columnOrdinal)
		}
		width = int(leaf.GetTypeLength())
	default:
		return nil
	}
	var n int
	switch v := value.(type) {
	case string:
		n = len(v)
	case []byte:
		n = len(v)
	default:
		// left to the hash, which reports a value that is not bytes at all
		return nil
	}
	if width > 0 && n != width {
		return fmt.Errorf("column [%s]: %s value of length %d does not match column length %d", columnPath, pT, n, width)
	}
	return nil
}

// fileLeaf returns the footer schema's leaf for a column chunk ordinal, nil when there is none.
func fileLeaf(footer *parquet.FileMetaData, columnOrdinal int) *parquet.SchemaElement {
	// chunks follow the file schema's leaves in order, whatever names the reader renamed them to
	leaf := 0
	for i, element := range footer.GetSchema() {
		if i == 0 || element == nil || element.GetNumChildren() > 0 {
			continue
		}
		if leaf == columnOrdinal {
			return element
		}
		leaf++
	}
	return nil
}

func (pr *ParquetReader) readBloomFilterForColumn(pf source.ParquetFileReader, rowGroupIndex, columnOrdinal int, rowGroup *parquet.RowGroup, columnChunk *parquet.ColumnChunk) (*bloomfilter.Filter, error) {
	offset, opt, err := pr.bloomFilterLocation(rowGroupIndex, columnOrdinal, rowGroup, columnChunk)
	if err != nil {
		return nil, err
	}
	rs := source.ReadSeekerWithContext{Ctx: pr.context(), ReadSeeker: pf}
	if opt == nil {
		return bloomfilter.ReadBloomFilterWithContext(pr.context(), rs, offset)
	}
	return bloomfilter.ReadEncryptedBloomFilter(rs, offset, *opt)
}

func (pr *ParquetReader) readBloomFilterSizeForColumn(pf source.ParquetFileReader, rowGroupIndex, columnOrdinal int, rowGroup *parquet.RowGroup, columnChunk *parquet.ColumnChunk) (int32, error) {
	offset, opt, err := pr.bloomFilterLocation(rowGroupIndex, columnOrdinal, rowGroup, columnChunk)
	if err != nil {
		return 0, err
	}
	rs := source.ReadSeekerWithContext{Ctx: pr.context(), ReadSeeker: pf}
	if opt == nil {
		return bloomfilter.ReadBloomFilterSize(pr.context(), rs, offset)
	}
	return bloomfilter.ReadEncryptedBloomFilterSize(rs, offset, *opt)
}

// bloomFilterLocation returns a column's bloom filter offset, and its decryption options when encrypted.
func (pr *ParquetReader) bloomFilterLocation(rowGroupIndex, columnOrdinal int, rowGroup *parquet.RowGroup, columnChunk *parquet.ColumnChunk) (int64, *bloomfilter.ReadOptions, error) {
	if columnChunk == nil || columnChunk.MetaData == nil {
		return 0, nil, fmt.Errorf("column metadata is nil")
	}
	offset := columnChunk.MetaData.GetBloomFilterOffset()
	if columnChunk.GetCryptoMetadata() == nil {
		return offset, nil, nil
	}

	algorithm := pr.encryptionAlgorithm()
	if algorithm == nil {
		return 0, nil, fmt.Errorf("encrypted bloom filter missing file encryption algorithm")
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(algorithm)
	if err != nil {
		return 0, nil, fmt.Errorf("footer AAD: %w", err)
	}
	key, err := pr.resolveColumnKey(columnChunk)
	if err != nil {
		return 0, nil, fmt.Errorf("resolve column key: %w", err)
	}
	rowGroupOrdinal := int16(rowGroupIndex)
	if rowGroup != nil && rowGroup.IsSetOrdinal() {
		rowGroupOrdinal = rowGroup.GetOrdinal()
	}
	// The stored length covers both modules, so it bounds the header module exactly;
	// writers may omit it, in which case the decoder falls back to its own limit.
	return offset, &bloomfilter.ReadOptions{
		Context:             pr.context(),
		Key:                 key,
		AADPrefix:           aadPrefix,
		AADFileUnique:       aadFileUnique,
		RowGroupOrdinal:     rowGroupOrdinal,
		ColumnOrdinal:       int16(columnOrdinal), // the AAD format holds the ordinal in 16 bits
		MaxHeaderModuleSize: int64(columnChunk.MetaData.GetBloomFilterLength()),
	}, nil
}
