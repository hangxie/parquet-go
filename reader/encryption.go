package reader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

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
		return fmt.Errorf("resolve footer key: %w", err)
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(fileCrypto.GetEncryptionAlgorithm())
	if err != nil {
		return fmt.Errorf("footer AAD: %w", err)
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
		return nil, 0, fmt.Errorf("buffer file crypto metadata: %w", err)
	}
	protocol := thrift.NewTCompactProtocolConf(mem, &thrift.TConfiguration{})
	meta := parquet.NewFileCryptoMetaData()
	if err := meta.Read(context.TODO(), protocol); err != nil {
		return nil, 0, fmt.Errorf("decode file crypto metadata: %w", err)
	}
	remaining := int(mem.RemainingBytes())
	return meta, len(buf) - remaining, nil
}

func readFileMetaDataFromBytes(buf []byte) (*parquet.FileMetaData, error) {
	footer := parquet.NewFileMetaData()
	protocol := thrift.NewTCompactProtocolConf(thrift.NewTBufferedTransport(thrift.NewStreamTransportR(bytes.NewReader(buf)), len(buf)), &thrift.TConfiguration{})
	if err := footer.Read(context.TODO(), protocol); err != nil {
		return nil, fmt.Errorf("decode file metadata: %w", err)
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
	key, err := pr.resolveOptionalFooterKeyFromMetadata(footer.GetFooterSigningKeyMetadata())
	if err != nil {
		return fmt.Errorf("resolve footer signing key: %w", err)
	}
	if len(key) == 0 {
		pr.Footer = footer
		return nil
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(footer.GetEncryptionAlgorithm())
	if err != nil {
		return fmt.Errorf("footer AAD: %w", err)
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
		return nil, fmt.Errorf("decode column metadata: %w", err)
	}
	return meta, nil
}

func (pr *ParquetReader) resolveFooterKeyFromMetadata(keyMetadata []byte) ([]byte, error) {
	if len(pr.footerKey) > 0 {
		pr.resolvedFooterKey = append(pr.resolvedFooterKey[:0], pr.footerKey...)
		return pr.footerKey, nil
	}
	if pr.keyRetriever != nil {
		key, err := pr.retrieveKeyFromMetadata(keyMetadata)
		if err != nil {
			return nil, fmt.Errorf("retrieve footer key: %w", err)
		}
		if len(key) > 0 {
			pr.resolvedFooterKey = append(pr.resolvedFooterKey[:0], key...)
			return key, nil
		}
	}
	return nil, fmt.Errorf("decryption key required for footer")
}

// resolveOptionalFooterKeyFromMetadata returns the footer key if one is
// configured or retrievable, or a nil key (with nil error) when no key is
// available. Callers check len(key) == 0 to distinguish "no key" from "got key".
func (pr *ParquetReader) resolveOptionalFooterKeyFromMetadata(keyMetadata []byte) ([]byte, error) {
	if len(pr.resolvedFooterKey) > 0 {
		return pr.resolvedFooterKey, nil
	}
	if len(pr.footerKey) > 0 {
		pr.resolvedFooterKey = append(pr.resolvedFooterKey[:0], pr.footerKey...)
		return pr.footerKey, nil
	}
	if pr.keyRetriever == nil {
		return nil, nil
	}
	key, err := pr.retrieveKeyFromMetadata(keyMetadata)
	if err != nil {
		return nil, fmt.Errorf("retrieve footer key: %w", err)
	}
	if len(key) == 0 {
		return nil, nil
	}
	pr.resolvedFooterKey = append(pr.resolvedFooterKey[:0], key...)
	return key, nil
}

func (pr *ParquetReader) resolveFooterKey() ([]byte, error) {
	if len(pr.resolvedFooterKey) > 0 {
		return pr.resolvedFooterKey, nil
	}
	return pr.resolveFooterKeyFromMetadata(nil)
}

type keyCacheEntry struct {
	once sync.Once
	key  []byte
	err  error
}

func (pr *ParquetReader) retrieveKeyFromMetadata(keyMetadata []byte) ([]byte, error) {
	if pr.keyRetriever == nil {
		return nil, nil
	}
	// key_metadata is an opaque byte sequence. The string conversion is only
	// for stable map lookup and is not intended to produce printable text.
	cacheKey := string(keyMetadata)

	entryAny, _ := pr.keyCache.LoadOrStore(cacheKey, new(keyCacheEntry))
	entry := entryAny.(*keyCacheEntry)

	// sync.Once does not tolerate transient retriever failures: if the first
	// call returns an error, that error is cached and this reader will not retry.
	entry.once.Do(func() {
		key, err := pr.keyRetriever(keyMetadata)
		if err != nil {
			entry.err = err
			return
		}
		entry.key = append([]byte(nil), key...)
	})
	if entry.err != nil {
		return nil, fmt.Errorf("retrieve key: %w", entry.err)
	}
	return append([]byte(nil), entry.key...), nil
}

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

func (pr *ParquetReader) decryptEncryptedColumnMetadata() error {
	if pr.Footer == nil || pr.Footer.RowGroups == nil {
		return nil
	}
	algorithm := pr.encryptionAlgorithm()
	if algorithm == nil {
		return nil
	}
	aadParts := pr.cachedFooterAADParts(algorithm)

	for rowGroupIndex, rowGroup := range pr.Footer.RowGroups {
		if rowGroup == nil {
			continue
		}
		rowGroupOrdinal := int16(rowGroupIndex)
		if rowGroup.IsSetOrdinal() {
			rowGroupOrdinal = rowGroup.GetOrdinal()
		}
		for columnOrdinal, chunk := range rowGroup.GetColumns() {
			if err := pr.decryptEncryptedColumnMetadataChunk(chunk, rowGroupIndex, int16(columnOrdinal), rowGroupOrdinal, aadParts); err != nil {
				return fmt.Errorf("decrypt column metadata: %w", err)
			}
		}
	}
	return nil
}

func (pr *ParquetReader) decryptEncryptedColumnMetadataChunk(chunk *parquet.ColumnChunk, rowGroupIndex int, columnOrdinal, rowGroupOrdinal int16, aadParts footerAADResolver) error {
	if chunk == nil || !chunk.IsSetEncryptedColumnMetadata() {
		return nil
	}
	key, err := pr.resolveOptionalColumnKey(chunk)
	if err != nil {
		return fmt.Errorf("row group %d column %d: %w", rowGroupIndex, columnOrdinal, err)
	}
	if len(key) == 0 {
		if chunk.MetaData != nil {
			return nil
		}
		_, err := pr.resolveColumnKey(chunk)
		return fmt.Errorf("row group %d column %d: %w", rowGroupIndex, columnOrdinal, err)
	}
	// Resolve footer AAD only after a usable key is present. This lets
	// no-key projections skip encrypted column metadata without requiring an
	// externally supplied AAD prefix, while still caching the value for
	// readers that decrypt multiple column metadata modules.
	aadPrefix, aadFileUnique, err := aadParts()
	if err != nil {
		return fmt.Errorf("row group %d column %d: footer AAD: %w", rowGroupIndex, columnOrdinal, err)
	}
	module, err := encryption.DecodeModule(chunk.GetEncryptedColumnMetadata())
	if err != nil {
		return fmt.Errorf("row group %d column %d: decode module: %w", rowGroupIndex, columnOrdinal, err)
	}
	aad := encryption.AAD(aadPrefix, aadFileUnique, encryption.ModuleColumnMetaData, rowGroupOrdinal, columnOrdinal, 0)
	plain, err := encryption.DecryptGCM(key, aad, module)
	if err != nil {
		return fmt.Errorf("row group %d column %d: decrypt: %w", rowGroupIndex, columnOrdinal, err)
	}
	meta, err := readColumnMetaDataFromBytes(plain)
	if err != nil {
		return fmt.Errorf("row group %d column %d: read metadata: %w", rowGroupIndex, columnOrdinal, err)
	}
	chunk.MetaData = meta
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
		key, err := pr.retrieveKeyFromMetadata(columnKeyMeta.GetKeyMetadata())
		if err != nil {
			return nil, fmt.Errorf("retrieve column key for %s: %w", path, err)
		}
		if len(key) > 0 {
			return key, nil
		}
	}
	return nil, fmt.Errorf("decryption key required for column %s", path)
}

func (pr *ParquetReader) resolveOptionalColumnKey(chunk *parquet.ColumnChunk) ([]byte, error) {
	cryptoMeta := chunk.GetCryptoMetadata()
	if cryptoMeta == nil {
		return nil, fmt.Errorf("column crypto metadata is required")
	}
	if cryptoMeta.IsSetENCRYPTION_WITH_FOOTER_KEY() {
		return pr.resolveOptionalFooterKeyFromMetadata(nil)
	}
	columnKeyMeta := cryptoMeta.GetENCRYPTION_WITH_COLUMN_KEY()
	if columnKeyMeta == nil {
		return nil, fmt.Errorf("unsupported column crypto metadata")
	}
	path := common.PathToStr(columnKeyMeta.GetPathInSchema())
	if key, ok := pr.columnKeys[path]; ok {
		return key, nil
	}
	if pr.keyRetriever == nil {
		return nil, nil
	}
	key, err := pr.retrieveKeyFromMetadata(columnKeyMeta.GetKeyMetadata())
	if err != nil {
		// Optional paths only probe for keys. Strict page access will surface
		// the cached retrieval error if the encrypted column is actually read.
		return nil, nil
	}
	return key, nil
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
