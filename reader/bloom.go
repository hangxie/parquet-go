package reader

import (
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
		if cc.MetaData == nil || !cc.MetaData.IsSetBloomFilterOffset() {
			continue
		}

		pathStr := common.PathToStr(append([]string{pr.SchemaHandler.GetRootInName()}, cc.MetaData.GetPathInSchema()...))
		if index, ok := pr.SchemaHandler.MapIndex[pathStr]; ok {
			pr.SchemaHandler.Infos[index].BloomFilter = true
			// Read the bloom filter header from the file to get the actual bitset size.
			// BloomFilterLength in metadata includes the Thrift header overhead, so we
			// read the header's NumBytes field which contains only the bitset size.
			if pr.PFile == nil {
				continue
			}
			pf, err := pr.PFile.Clone()
			if err != nil {
				continue
			}

			filter, err := pr.readBloomFilterForColumn(pf, 0, int16(columnOrdinal), rg, cc)
			if err == nil {
				pr.SchemaHandler.Infos[index].BloomFilterSize = filter.NumBytes()
			}
			_ = pf.Close()
		}
	}
}

// BloomFilterCheck checks if a value might exist in the given column of the given row group.
// It returns true if the value might exist (or if there is no bloom filter), false if the value
// is definitely not present. The columnPath uses dot-separated notation matching the parquet tag
// name (e.g. "id" or "address.city").
func (pr *ParquetReader) BloomFilterCheck(columnPath string, rowGroupIndex int, value any) (bool, error) {
	if rowGroupIndex < 0 || rowGroupIndex >= len(pr.Footer.RowGroups) {
		return false, fmt.Errorf("row group index %d out of range [0, %d)", rowGroupIndex, len(pr.Footer.RowGroups))
	}

	rg := pr.Footer.RowGroups[rowGroupIndex]

	// Convert the external column path (tag name) to the internal path used in the footer.
	exPath := common.ReformPathStr(pr.SchemaHandler.GetRootExName() + "." + columnPath)
	inPath, ok := pr.SchemaHandler.ExPathToInPath[exPath]
	if !ok {
		return false, fmt.Errorf("column %q not found in row group %d", columnPath, rowGroupIndex)
	}

	// Find the column chunk by matching the internal path
	var columnChunk *parquet.ColumnChunk
	var columnType parquet.Type
	columnOrdinal := int16(0)
	for i, cc := range rg.Columns {
		ccPath := common.PathToStr(append([]string{pr.SchemaHandler.GetRootInName()}, cc.MetaData.GetPathInSchema()...))
		if ccPath == inPath {
			columnChunk = cc
			columnOrdinal = int16(i)
			columnType = cc.MetaData.GetType()
			break
		}
	}
	if columnChunk == nil {
		return false, fmt.Errorf("column %q not found in row group %d", columnPath, rowGroupIndex)
	}

	if !columnChunk.MetaData.IsSetBloomFilterOffset() {
		// No bloom filter for this column; conservatively return true
		return true, nil
	}

	pf, err := pr.PFile.Clone()
	if err != nil {
		return false, fmt.Errorf("clone file reader: %w", err)
	}
	defer func() { _ = pf.Close() }()

	filter, err := pr.readBloomFilterForColumn(pf, rowGroupIndex, columnOrdinal, rg, columnChunk)
	if err != nil {
		return false, fmt.Errorf("read bloom filter: %w", err)
	}

	hash, err := bloomfilter.HashValue(value, columnType)
	if err != nil {
		return false, fmt.Errorf("hash value: %w", err)
	}

	return filter.Check(hash), nil
}

func (pr *ParquetReader) readBloomFilterForColumn(pf source.ParquetFileReader, rowGroupIndex int, columnOrdinal int16, rowGroup *parquet.RowGroup, columnChunk *parquet.ColumnChunk) (*bloomfilter.Filter, error) {
	if columnChunk == nil || columnChunk.MetaData == nil {
		return nil, fmt.Errorf("column metadata is nil")
	}
	offset := columnChunk.MetaData.GetBloomFilterOffset()
	if columnChunk.GetCryptoMetadata() == nil {
		return bloomfilter.ReadBloomFilter(pf, offset)
	}

	algorithm := pr.encryptionAlgorithm()
	if algorithm == nil {
		return nil, fmt.Errorf("encrypted bloom filter missing file encryption algorithm")
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(algorithm)
	if err != nil {
		return nil, fmt.Errorf("footer AAD: %w", err)
	}
	key, err := pr.resolveColumnKey(columnChunk)
	if err != nil {
		return nil, fmt.Errorf("resolve column key: %w", err)
	}
	rowGroupOrdinal := int16(rowGroupIndex)
	if rowGroup != nil && rowGroup.IsSetOrdinal() {
		rowGroupOrdinal = rowGroup.GetOrdinal()
	}
	return bloomfilter.ReadEncryptedBloomFilter(pf, offset, bloomfilter.ReadOptions{
		Key:             key,
		AADPrefix:       aadPrefix,
		AADFileUnique:   aadFileUnique,
		RowGroupOrdinal: rowGroupOrdinal,
		ColumnOrdinal:   columnOrdinal,
	})
}
