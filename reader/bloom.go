package reader

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/bloomfilter"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source"
)

// detectBloomFilters scans the first row group's metadata to detect which columns have bloom
// filters and populates the BloomFilter and BloomFilterSize fields in SchemaHandler.Infos.
func (pr *ParquetReader) detectBloomFilters() {
	if pr.Footer == nil || len(pr.Footer.RowGroups) == 0 || pr.SchemaHandler == nil {
		return
	}

	rg := pr.Footer.RowGroups[0]
	if rg == nil {
		return
	}

	for columnOrdinal, cc := range rg.Columns {
		if cc == nil || cc.MetaData == nil || !cc.MetaData.IsSetBloomFilterOffset() {
			continue
		}

		pathStr := columnPathToInPath(pr.SchemaHandler, cc.MetaData.GetPathInSchema(), pr.caseInsensitive)
		if index, ok := pr.SchemaHandler.MapIndex[pathStr]; ok {
			pr.SchemaHandler.Infos[index].BloomFilter = true
			// Read the bloom filter header from the file to get the actual bitset size.
			// BloomFilterLength in metadata includes the Thrift header overhead, so we
			// read the header's NumBytes field which contains only the bitset size.
			if pr.PFile == nil {
				continue
			}
			pf, err := source.CloneWithContext(pr.context(), pr.PFile)
			if err != nil {
				continue
			}

			filter, err := pr.readBloomFilterForColumn(pf, 0, int16(columnOrdinal), rg, cc)
			if err == nil {
				pr.SchemaHandler.Infos[index].BloomFilterSize = filter.NumBytes()
			}
			_ = source.CloseWithContext(pr.context(), pf)
		}
	}
}

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

// BloomFilterSizeWithContext returns the bitset size in bytes of a column's bloom filter in one row group.
// Unlike SchemaHandler.Infos[i].BloomFilterSize, which only ever describes row group 0, this reports the
// filter stored in the requested row group. columnPath follows the same rules as BloomFilterCheckWithContext.
func (pr *ParquetReader) BloomFilterSizeWithContext(ctx context.Context, columnPath string, rowGroupIndex int) (int32, error) {
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
func (pr *ParquetReader) locateColumnChunk(columnPath string, rowGroupIndex int) (*parquet.RowGroup, *parquet.ColumnChunk, int16, error) {
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
		if columnPathToInPath(pr.SchemaHandler, cc.MetaData.GetPathInSchema(), pr.caseInsensitive) == inPath {
			return rg, cc, int16(i), nil
		}
	}
	return nil, nil, 0, fmt.Errorf("column %q not found in row group %d", columnPath, rowGroupIndex)
}

func (pr *ParquetReader) readBloomFilterForColumn(pf source.ParquetFileReader, rowGroupIndex int, columnOrdinal int16, rowGroup *parquet.RowGroup, columnChunk *parquet.ColumnChunk) (*bloomfilter.Filter, error) {
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

func (pr *ParquetReader) readBloomFilterSizeForColumn(pf source.ParquetFileReader, rowGroupIndex int, columnOrdinal int16, rowGroup *parquet.RowGroup, columnChunk *parquet.ColumnChunk) (int32, error) {
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
func (pr *ParquetReader) bloomFilterLocation(rowGroupIndex int, columnOrdinal int16, rowGroup *parquet.RowGroup, columnChunk *parquet.ColumnChunk) (int64, *bloomfilter.ReadOptions, error) {
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
		ColumnOrdinal:       columnOrdinal,
		MaxHeaderModuleSize: int64(columnChunk.MetaData.GetBloomFilterLength()),
	}, nil
}
