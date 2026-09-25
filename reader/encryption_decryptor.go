package reader

import (
	"fmt"
	"sync"

	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

type footerAADResolver func() ([]byte, []byte, error)

func (pr *ParquetReader) cachedFooterAADParts(algorithm *parquet.EncryptionAlgorithm) footerAADResolver {
	var once sync.Once
	var aadPrefix []byte
	var aadFileUnique []byte
	var err error
	return func() ([]byte, []byte, error) {
		once.Do(func() {
			aadPrefix, aadFileUnique, err = pr.footerAADParts(algorithm)
		})
		return aadPrefix, aadFileUnique, err
	}
}

func (pr *ParquetReader) configurePageDecryptor(cbt *ColumnBufferType, rowGroup *parquet.RowGroup, columnOrdinal int16) error {
	return pr.configurePageDecryptorWithKeyRequirement(cbt, rowGroup, columnOrdinal, true)
}

func (pr *ParquetReader) configureOptionalPageDecryptor(cbt *ColumnBufferType, rowGroup *parquet.RowGroup, columnOrdinal int16) error {
	return pr.configurePageDecryptorWithKeyRequirement(cbt, rowGroup, columnOrdinal, false)
}

func (pr *ParquetReader) configurePageDecryptorWithKeyRequirement(cbt *ColumnBufferType, rowGroup *parquet.RowGroup, columnOrdinal int16, requireKey bool) error {
	if cbt == nil {
		return nil
	}
	cbt.PageReadOptions.Decryptor = nil
	if cbt.ChunkHeader == nil || cbt.ChunkHeader.GetCryptoMetadata() == nil {
		return nil
	}
	algorithm := pr.encryptionAlgorithm()
	if algorithm == nil {
		return fmt.Errorf("encrypted column missing file encryption algorithm")
	}
	key, err := pr.resolveOptionalColumnKey(cbt.ChunkHeader)
	if err != nil {
		return fmt.Errorf("resolve column key: %w", err)
	}
	if len(key) == 0 {
		if requireKey {
			_, err := pr.resolveColumnKey(cbt.ChunkHeader)
			return fmt.Errorf("require column key: %w", err)
		}
		return nil
	}
	// AAD is needed only when a page decryptor is actually installed.
	aadPrefix, aadFileUnique, err := pr.footerAADParts(algorithm)
	if err != nil {
		return fmt.Errorf("footer AAD: %w", err)
	}
	pageAlgorithm := layout.PageEncryptionAESGCM
	if algorithm.IsSetAES_GCM_CTR_V1() {
		pageAlgorithm = layout.PageEncryptionAESGCMCTR
	}

	rowGroupOrdinal := int16(cbt.RowGroupIndex - 1)
	if rowGroup != nil && rowGroup.IsSetOrdinal() {
		rowGroupOrdinal = rowGroup.GetOrdinal()
	}
	cbt.PageReadOptions.Decryptor = &layout.PageDecryptor{
		Algorithm:       pageAlgorithm,
		Key:             key,
		AADPrefix:       aadPrefix,
		AADFileUnique:   aadFileUnique,
		RowGroupOrdinal: rowGroupOrdinal,
		ColumnOrdinal:   columnOrdinal,
	}
	return nil
}

func (pr *ParquetReader) requirePageDecryptor(cbt *ColumnBufferType) error {
	if cbt == nil || cbt.ChunkHeader == nil || cbt.ChunkHeader.GetCryptoMetadata() == nil || cbt.PageReadOptions.Decryptor != nil {
		return nil
	}
	if _, err := pr.resolveColumnKey(cbt.ChunkHeader); err != nil {
		return fmt.Errorf("require column key: %w", err)
	}
	return nil
}

func (pr *ParquetReader) reconfigureDecryptorForBuffer(cbt *ColumnBufferType) error {
	if cbt == nil || cbt.RowGroupIndex <= 0 {
		return nil
	}
	rgIdx := cbt.RowGroupIndex - 1
	if int(rgIdx) >= len(pr.Footer.RowGroups) {
		return nil
	}
	return pr.configurePageDecryptor(cbt, pr.Footer.RowGroups[rgIdx], cbt.ColumnOrdinal)
}

func (pr *ParquetReader) reconfigureOptionalDecryptorForBuffer(cbt *ColumnBufferType) error {
	if cbt == nil || cbt.RowGroupIndex <= 0 {
		return nil
	}
	rgIdx := cbt.RowGroupIndex - 1
	if int(rgIdx) >= len(pr.Footer.RowGroups) {
		return nil
	}
	return pr.configureOptionalPageDecryptor(cbt, pr.Footer.RowGroups[rgIdx], cbt.ColumnOrdinal)
}

func (pr *ParquetReader) footerAADParts(algorithm *parquet.EncryptionAlgorithm) ([]byte, []byte, error) {
	if algorithm == nil {
		return nil, nil, fmt.Errorf("missing encryption algorithm")
	}
	if algorithm.IsSetAES_GCM_V1() {
		return pr.aadParts(algorithm.GetAES_GCM_V1().GetAadPrefix(), algorithm.GetAES_GCM_V1().GetAadFileUnique(), algorithm.GetAES_GCM_V1().GetSupplyAadPrefix())
	}
	if algorithm.IsSetAES_GCM_CTR_V1() {
		return pr.aadParts(algorithm.GetAES_GCM_CTR_V1().GetAadPrefix(), algorithm.GetAES_GCM_CTR_V1().GetAadFileUnique(), algorithm.GetAES_GCM_CTR_V1().GetSupplyAadPrefix())
	}
	return nil, nil, fmt.Errorf("unsupported encryption algorithm")
}

func (pr *ParquetReader) aadParts(storedPrefix, fileUnique []byte, supplyPrefix bool) ([]byte, []byte, error) {
	if supplyPrefix {
		if len(pr.aadPrefix) == 0 {
			return nil, nil, fmt.Errorf("AAD prefix is required")
		}
		return pr.aadPrefix, fileUnique, nil
	}
	return storedPrefix, fileUnique, nil
}
