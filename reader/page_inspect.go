package reader

import (
	"context"
	"fmt"

	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source"
)

// GetAllPageHeaders returns metadata for all pages in a column chunk. When the
// column is encrypted the reader's configured keys are used to decrypt page
// headers transparently. Missing keys surface the standard "decryption key
// required for column" error.
//
// Deprecated: use GetAllPageHeadersWithContext.
func (pr *ParquetReader) GetAllPageHeaders(rgIndex, colIndex int) ([]PageHeaderInfo, error) {
	return pr.GetAllPageHeadersWithContext(pr.defaultContext(), rgIndex, colIndex)
}

// GetAllPageHeadersWithContext returns metadata for all pages using ctx.
func (pr *ParquetReader) GetAllPageHeadersWithContext(ctx context.Context, rgIndex, colIndex int) ([]PageHeaderInfo, error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, err
	}
	if rgIndex < 0 || rgIndex >= len(pr.Footer.RowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d (valid range: 0-%d)", rgIndex, len(pr.Footer.RowGroups)-1)
	}

	rg := pr.Footer.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return nil, fmt.Errorf("invalid column index: %d (valid range: 0-%d)", colIndex, len(rg.Columns)-1)
	}

	column := rg.Columns[colIndex]
	decryptor, err := pr.pageInspectionDecryptor(rg, column, int16(rgIndex), int16(colIndex))
	if err != nil {
		return nil, err
	}
	return readAllPageHeaders(ctx, source.ReadSeekerWithContext{Ctx: ctx, ReadSeeker: pr.PFile}, column, decryptor)
}

// GetFirstDataPageHeader returns metadata for the first data page in a column
// chunk. When the column is encrypted the reader's configured keys are used to
// decrypt page headers transparently.
//
// Deprecated: use GetFirstDataPageHeaderWithContext.
func (pr *ParquetReader) GetFirstDataPageHeader(rgIndex, colIndex int) (*PageHeaderInfo, error) {
	return pr.GetFirstDataPageHeaderWithContext(pr.defaultContext(), rgIndex, colIndex)
}

// GetFirstDataPageHeaderWithContext returns first-page metadata using ctx.
func (pr *ParquetReader) GetFirstDataPageHeaderWithContext(ctx context.Context, rgIndex, colIndex int) (*PageHeaderInfo, error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, err
	}
	if rgIndex < 0 || rgIndex >= len(pr.Footer.RowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d (valid range: 0-%d)", rgIndex, len(pr.Footer.RowGroups)-1)
	}

	rg := pr.Footer.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return nil, fmt.Errorf("invalid column index: %d (valid range: 0-%d)", colIndex, len(rg.Columns)-1)
	}

	column := rg.Columns[colIndex]
	decryptor, err := pr.pageInspectionDecryptor(rg, column, int16(rgIndex), int16(colIndex))
	if err != nil {
		return nil, err
	}
	return readFirstDataPageHeader(ctx, source.ReadSeekerWithContext{Ctx: ctx, ReadSeeker: pr.PFile}, column, decryptor)
}

// ReadDictionaryPageValues reads and decodes dictionary page values at the
// given offset. This offset-based form is plaintext-only because the offset
// cannot identify the column whose key would be required to decrypt the page;
// use ReadDictionaryPageValuesInColumn for encrypted columns.
//
// Deprecated: use ReadDictionaryPageValuesWithContext.
func (pr *ParquetReader) ReadDictionaryPageValues(offset int64, codec parquet.CompressionCodec, physicalType parquet.Type) ([]interface{}, error) {
	return pr.ReadDictionaryPageValuesWithContext(pr.defaultContext(), offset, codec, physicalType)
}

// ReadDictionaryPageValuesWithContext reads dictionary-page values using ctx.
func (pr *ParquetReader) ReadDictionaryPageValuesWithContext(ctx context.Context, offset int64, codec parquet.CompressionCodec, physicalType parquet.Type) ([]interface{}, error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, err
	}
	encrypted, err := pr.pageOffsetEncrypted(offset)
	if err != nil {
		return nil, err
	}
	if encrypted {
		return nil, fmt.Errorf("dictionary page inspection is not supported for encrypted columns at offset %d; use ReadDictionaryPageValuesInColumn", offset)
	}

	// Read page header at the offset
	contextFile := source.ReadSeekerWithContext{Ctx: ctx, ReadSeeker: pr.PFile}
	pageHeader, _, err := readPageHeader(ctx, contextFile, offset, nil)
	if err != nil {
		return nil, fmt.Errorf("read page header: %w", err)
	}

	// Verify it's a dictionary page
	if pageHeader.Type != parquet.PageType_DICTIONARY_PAGE {
		return nil, fmt.Errorf("expected dictionary page but got %v", pageHeader.Type)
	}

	// Read and decode the page data
	data, err := ReadPageData(contextFile, offset, pageHeader, codec, &layout.PageReadOptions{Context: ctx, CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize})
	if err != nil {
		return nil, fmt.Errorf("read page data: %w", err)
	}

	values, err := DecodeDictionaryPage(data, pageHeader, physicalType)
	if err != nil {
		return nil, fmt.Errorf("decode dictionary page: %w", err)
	}

	return values, nil
}

// ReadDictionaryPageValuesInColumn reads and decodes dictionary page values
// for the dictionary page of the given column chunk. Offset, codec, and
// physical type are derived from the column metadata, and encrypted columns
// are decrypted transparently when the reader has the right keys.
//
// Deprecated: use ReadDictionaryPageValuesInColumnWithContext.
func (pr *ParquetReader) ReadDictionaryPageValuesInColumn(rgIndex, colIndex int) ([]interface{}, error) {
	return pr.ReadDictionaryPageValuesInColumnWithContext(pr.defaultContext(), rgIndex, colIndex)
}

// ReadDictionaryPageValuesInColumnWithContext reads column dictionary values using ctx.
func (pr *ParquetReader) ReadDictionaryPageValuesInColumnWithContext(ctx context.Context, rgIndex, colIndex int) ([]interface{}, error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, err
	}
	if rgIndex < 0 || rgIndex >= len(pr.Footer.RowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d (valid range: 0-%d)", rgIndex, len(pr.Footer.RowGroups)-1)
	}
	rg := pr.Footer.RowGroups[rgIndex]
	if colIndex < 0 || colIndex >= len(rg.Columns) {
		return nil, fmt.Errorf("invalid column index: %d (valid range: 0-%d)", colIndex, len(rg.Columns)-1)
	}

	column := rg.Columns[colIndex]
	if column == nil || column.MetaData == nil {
		return nil, fmt.Errorf("column %d metadata is nil", colIndex)
	}
	if !column.MetaData.IsSetDictionaryPageOffset() {
		return nil, fmt.Errorf("column %d does not have a dictionary page", colIndex)
	}

	decryptor, err := pr.pageInspectionDecryptor(rg, column, int16(rgIndex), int16(colIndex))
	if err != nil {
		return nil, err
	}

	offset := column.MetaData.GetDictionaryPageOffset()
	contextFile := source.ReadSeekerWithContext{Ctx: ctx, ReadSeeker: pr.PFile}
	pageHeader, headerSize, err := readPageHeader(ctx, contextFile, offset, decryptor)
	if err != nil {
		return nil, fmt.Errorf("read page header: %w", err)
	}
	if pageHeader.Type != parquet.PageType_DICTIONARY_PAGE {
		return nil, fmt.Errorf("expected dictionary page but got %v", pageHeader.Type)
	}

	data, err := readPageBody(contextFile, offset+headerSize, pageHeader, column.MetaData.GetCodec(), pr.crcMode, decryptor)
	if err != nil {
		return nil, fmt.Errorf("read page data: %w", err)
	}

	values, err := DecodeDictionaryPage(data, pageHeader, column.MetaData.GetType())
	if err != nil {
		return nil, fmt.Errorf("decode dictionary page: %w", err)
	}
	return values, nil
}

// pageInspectionDecryptor builds a PageDecryptor when the column is encrypted
// and returns nil for plaintext columns. Missing keys surface the standard
// "decryption key required for column" error. rowGroupOrdinal is the index
// fallback used when the row group does not carry an explicit ordinal.
func (pr *ParquetReader) pageInspectionDecryptor(rg *parquet.RowGroup, column *parquet.ColumnChunk, rowGroupOrdinal, columnOrdinal int16) (*layout.PageDecryptor, error) {
	if column == nil || column.GetCryptoMetadata() == nil {
		return nil, nil
	}
	algorithm := pr.encryptionAlgorithm()
	if algorithm == nil {
		return nil, fmt.Errorf("encrypted column missing file encryption algorithm")
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(algorithm)
	if err != nil {
		return nil, fmt.Errorf("footer AAD: %w", err)
	}
	key, err := pr.resolveColumnKey(column)
	if err != nil {
		return nil, fmt.Errorf("resolve column key: %w", err)
	}
	pageAlgorithm := layout.PageEncryptionAESGCM
	if algorithm.IsSetAES_GCM_CTR_V1() {
		pageAlgorithm = layout.PageEncryptionAESGCMCTR
	}
	if rg != nil && rg.IsSetOrdinal() {
		rowGroupOrdinal = rg.GetOrdinal()
	}
	return &layout.PageDecryptor{
		Algorithm:       pageAlgorithm,
		Key:             key,
		AADPrefix:       aadPrefix,
		AADFileUnique:   aadFileUnique,
		RowGroupOrdinal: rowGroupOrdinal,
		ColumnOrdinal:   columnOrdinal,
	}, nil
}

// pageOffsetEncrypted reports whether offset lies in an encrypted column's
// payload region (data, dictionary, index, or bloom-filter page). It returns an
// error if the footer carries an encrypted column whose metadata is missing
// entirely, which indicates a corrupted file.
func (pr *ParquetReader) pageOffsetEncrypted(offset int64) (bool, error) {
	if pr == nil || pr.Footer == nil {
		return false, nil
	}
	pr.encryptedPageOffsetOnce.Do(pr.buildEncryptedPageOffsets)
	if pr.encryptedPageOffsetsErr != nil {
		return false, pr.encryptedPageOffsetsErr
	}
	_, ok := pr.encryptedPageOffsets[offset]
	return ok, nil
}

// buildEncryptedPageOffsets populates encryptedPageOffsets with every per-column
// offset that belongs to an encrypted column. CryptoMetadata is the only signal
// used to decide that a column is encrypted, because ReadFooter has already
// decrypted any EncryptedColumnMetadata for which a key was available. Columns
// whose metadata was deferred (EncryptedColumnMetadata set but MetaData not yet
// populated) are skipped: their offsets are unknown, and the caller cannot read
// those pages without a key anyway. A column with CryptoMetadata but no
// MetaData and no EncryptedColumnMetadata is treated as a corrupted file and
// recorded as encryptedPageOffsetsErr.
func (pr *ParquetReader) buildEncryptedPageOffsets() {
	offsets := make(map[int64]struct{})
	for rowGroupIndex, rowGroup := range pr.Footer.GetRowGroups() {
		if rowGroup == nil {
			continue
		}
		for columnIndex, column := range rowGroup.GetColumns() {
			if column == nil || column.GetCryptoMetadata() == nil {
				continue
			}
			if column.MetaData == nil {
				if column.IsSetEncryptedColumnMetadata() {
					continue
				}
				pr.encryptedPageOffsetsErr = fmt.Errorf("row group %d column %d: encrypted column is missing metadata", rowGroupIndex, columnIndex)
				pr.encryptedPageOffsets = nil
				return
			}
			meta := column.MetaData
			offsets[meta.GetDataPageOffset()] = struct{}{}
			if meta.IsSetDictionaryPageOffset() {
				offsets[meta.GetDictionaryPageOffset()] = struct{}{}
			}
			if meta.IsSetIndexPageOffset() {
				offsets[meta.GetIndexPageOffset()] = struct{}{}
			}
			if meta.IsSetBloomFilterOffset() {
				offsets[meta.GetBloomFilterOffset()] = struct{}{}
			}
		}
	}
	pr.encryptedPageOffsets = offsets
}
