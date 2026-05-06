package reader

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func (pr *ParquetReader) readEncryptedFooter(size uint32) error {
	section := make([]byte, size)
	if _, err := pr.PFile.Seek(-int64(8+size), io.SeekEnd); err != nil {
		return fmt.Errorf("seek to encrypted footer section: %w", err)
	}
	if _, err := io.ReadFull(pr.PFile, section); err != nil {
		return fmt.Errorf("read encrypted footer section: %w", err)
	}

	fileCrypto, consumed, err := readFileCryptoMetaData(section)
	if err != nil {
		return fmt.Errorf("read file crypto metadata: %w", err)
	}
	if consumed >= len(section) {
		return fmt.Errorf("encrypted footer section missing encrypted footer")
	}

	key, err := pr.resolveFooterKeyFromMetadata(fileCrypto.GetKeyMetadata())
	if err != nil {
		return err
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(fileCrypto.GetEncryptionAlgorithm())
	if err != nil {
		return err
	}

	module, err := encryption.DecodeModule(section[consumed:])
	if err != nil {
		return fmt.Errorf("decode encrypted footer module: %w", err)
	}
	footerBytes, err := encryption.DecryptGCM(key, encryption.AAD(aadPrefix, aadFileUnique, encryption.ModuleFooter, 0, 0, 0), module)
	if err != nil {
		return fmt.Errorf("decrypt footer: %w", err)
	}

	footer, err := readFileMetaDataFromBytes(footerBytes)
	if err != nil {
		return fmt.Errorf("read decrypted footer: %w", err)
	}
	pr.FileCrypto = fileCrypto
	pr.Footer = footer
	return nil
}

func readFileCryptoMetaData(buf []byte) (*parquet.FileCryptoMetaData, int, error) {
	mem := thrift.NewTMemoryBufferLen(len(buf))
	if _, err := mem.Write(buf); err != nil {
		return nil, 0, err
	}
	protocol := thrift.NewTCompactProtocolConf(mem, &thrift.TConfiguration{})
	meta := parquet.NewFileCryptoMetaData()
	if err := meta.Read(context.TODO(), protocol); err != nil {
		return nil, 0, err
	}
	remaining := int(mem.RemainingBytes())
	return meta, len(buf) - remaining, nil
}

func readFileMetaDataFromBytes(buf []byte) (*parquet.FileMetaData, error) {
	footer := parquet.NewFileMetaData()
	protocol := thrift.NewTCompactProtocolConf(thrift.NewTBufferedTransport(thrift.NewStreamTransportR(bytes.NewReader(buf)), len(buf)), &thrift.TConfiguration{})
	if err := footer.Read(context.TODO(), protocol); err != nil {
		return nil, err
	}
	return footer, nil
}

func (pr *ParquetReader) verifyPlaintextFooter(section []byte) error {
	if len(section) < 28 {
		return fmt.Errorf("plaintext footer signature is missing")
	}
	footerBytes := section[:len(section)-28]
	signature := section[len(section)-28:]
	footer, err := readFileMetaDataFromBytes(footerBytes)
	if err != nil {
		return fmt.Errorf("read signed plaintext footer: %w", err)
	}
	key, err := pr.resolveFooterKeyFromMetadata(footer.GetFooterSigningKeyMetadata())
	if err != nil {
		return err
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(footer.GetEncryptionAlgorithm())
	if err != nil {
		return err
	}
	if err := encryption.VerifyGCMTag(
		key,
		encryption.AAD(aadPrefix, aadFileUnique, encryption.ModuleFooter, 0, 0, 0),
		signature[:12],
		footerBytes,
		signature[12:],
	); err != nil {
		return fmt.Errorf("verify plaintext footer signature: %w", err)
	}
	pr.Footer = footer
	return nil
}

func readColumnMetaDataFromBytes(buf []byte) (*parquet.ColumnMetaData, error) {
	meta := parquet.NewColumnMetaData()
	protocol := thrift.NewTCompactProtocolConf(thrift.NewStreamTransportR(bytes.NewReader(buf)), &thrift.TConfiguration{})
	if err := meta.Read(context.TODO(), protocol); err != nil {
		return nil, err
	}
	return meta, nil
}

func (pr *ParquetReader) resolveFooterKeyFromMetadata(keyMetadata []byte) ([]byte, error) {
	if len(pr.footerKey) > 0 {
		pr.resolvedFooterKey = append(pr.resolvedFooterKey[:0], pr.footerKey...)
		return pr.footerKey, nil
	}
	if pr.keyRetriever != nil {
		key, err := pr.keyRetriever(keyMetadata)
		if err != nil {
			return nil, fmt.Errorf("retrieve footer key: %w", err)
		}
		if len(key) > 0 {
			pr.resolvedFooterKey = append(pr.resolvedFooterKey[:0], key...)
			return key, nil
		}
	}
	return nil, fmt.Errorf("footer decryption key is required")
}

func (pr *ParquetReader) resolveFooterKey() ([]byte, error) {
	if len(pr.resolvedFooterKey) > 0 {
		return pr.resolvedFooterKey, nil
	}
	return pr.resolveFooterKeyFromMetadata(nil)
}

func (pr *ParquetReader) decryptEncryptedColumnMetadata() error {
	if pr.Footer == nil || pr.Footer.RowGroups == nil {
		return nil
	}
	algorithm := pr.encryptionAlgorithm()
	if algorithm == nil {
		return nil
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(algorithm)
	if err != nil {
		return err
	}

	for rowGroupIndex, rowGroup := range pr.Footer.RowGroups {
		if rowGroup == nil {
			continue
		}
		rowGroupOrdinal := int16(rowGroupIndex)
		if rowGroup.IsSetOrdinal() {
			rowGroupOrdinal = rowGroup.GetOrdinal()
		}
		for columnOrdinal, chunk := range rowGroup.GetColumns() {
			if chunk == nil || !chunk.IsSetEncryptedColumnMetadata() {
				continue
			}
			key, err := pr.resolveColumnKey(chunk)
			if err != nil {
				return fmt.Errorf("row group %d column %d: %w", rowGroupIndex, columnOrdinal, err)
			}
			module, err := encryption.DecodeModule(chunk.GetEncryptedColumnMetadata())
			if err != nil {
				return fmt.Errorf("row group %d column %d: decode module: %w", rowGroupIndex, columnOrdinal, err)
			}
			aad := encryption.AAD(aadPrefix, aadFileUnique, encryption.ModuleColumnMetaData, rowGroupOrdinal, int16(columnOrdinal), 0)
			plain, err := encryption.DecryptGCM(key, aad, module)
			if err != nil {
				return fmt.Errorf("row group %d column %d: decrypt: %w", rowGroupIndex, columnOrdinal, err)
			}
			meta, err := readColumnMetaDataFromBytes(plain)
			if err != nil {
				return fmt.Errorf("row group %d column %d: read metadata: %w", rowGroupIndex, columnOrdinal, err)
			}
			chunk.MetaData = meta
		}
	}
	return nil
}

func (pr *ParquetReader) encryptionAlgorithm() *parquet.EncryptionAlgorithm {
	if pr.FileCrypto != nil && pr.FileCrypto.IsSetEncryptionAlgorithm() {
		return pr.FileCrypto.GetEncryptionAlgorithm()
	}
	if pr.Footer != nil && pr.Footer.IsSetEncryptionAlgorithm() {
		return pr.Footer.GetEncryptionAlgorithm()
	}
	return nil
}

func (pr *ParquetReader) resolveColumnKey(chunk *parquet.ColumnChunk) ([]byte, error) {
	cryptoMeta := chunk.GetCryptoMetadata()
	if cryptoMeta == nil {
		return nil, fmt.Errorf("column crypto metadata is required")
	}
	if cryptoMeta.IsSetENCRYPTION_WITH_FOOTER_KEY() {
		return pr.resolveFooterKey()
	}
	columnKeyMeta := cryptoMeta.GetENCRYPTION_WITH_COLUMN_KEY()
	if columnKeyMeta == nil {
		return nil, fmt.Errorf("unsupported column crypto metadata")
	}
	path := common.PathToStr(columnKeyMeta.GetPathInSchema())
	if key, ok := pr.columnKeys[path]; ok {
		return key, nil
	}
	if pr.keyRetriever != nil {
		key, err := pr.keyRetriever(columnKeyMeta.GetKeyMetadata())
		if err != nil {
			return nil, fmt.Errorf("retrieve column key for %s: %w", path, err)
		}
		if len(key) > 0 {
			return key, nil
		}
	}
	return nil, fmt.Errorf("column decryption key is required for %s", path)
}

func (pr *ParquetReader) configurePageDecryptor(cbt *ColumnBufferType, rowGroup *parquet.RowGroup, columnOrdinal int16) error {
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
	aadPrefix, aadFileUnique, err := pr.footerAADParts(algorithm)
	if err != nil {
		return err
	}
	key, err := pr.resolveColumnKey(cbt.ChunkHeader)
	if err != nil {
		return err
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
