package reader

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/bloomfilter"
	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
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

	for _, cc := range rg.Columns {
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

			filter, err := bloomfilter.ReadBloomFilter(pf, cc.MetaData.GetBloomFilterOffset())
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
	for _, cc := range rg.Columns {
		ccPath := common.PathToStr(append([]string{pr.SchemaHandler.GetRootInName()}, cc.MetaData.GetPathInSchema()...))
		if ccPath == inPath {
			columnChunk = cc
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

	filter, err := bloomfilter.ReadBloomFilter(pf, columnChunk.MetaData.GetBloomFilterOffset())
	if err != nil {
		return false, fmt.Errorf("read bloom filter: %w", err)
	}

	hash, err := bloomfilter.HashValue(value, columnType)
	if err != nil {
		return false, fmt.Errorf("hash value: %w", err)
	}

	return filter.Check(hash), nil
}
