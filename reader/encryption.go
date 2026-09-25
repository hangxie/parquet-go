package reader

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source"
)

func (pr *ParquetReader) readEncryptedFooter(size uint32) error {
	section := make([]byte, size)
	if _, err := source.SeekWithContext(pr.context(), pr.PFile, -int64(8+size), io.SeekEnd); err != nil {
		return fmt.Errorf("seek to encrypted footer section: %w", err)
	}
	if _, err := source.ReadFullWithContext(pr.context(), pr.PFile, section); err != nil {
		return fmt.Errorf("read encrypted footer section: %w", err)
	}

	fileCrypto, consumed, err := readFileCryptoMetaData(pr.context(), section)
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

	footer, err := readFileMetaDataFromBytes(pr.context(), footerBytes)
	if err != nil {
		return fmt.Errorf("read decrypted footer: %w", err)
	}
	pr.FileCrypto = fileCrypto
	pr.Footer = footer
	return nil
}

func readFileCryptoMetaData(ctx context.Context, buf []byte) (*parquet.FileCryptoMetaData, int, error) {
	mem := thrift.NewTMemoryBufferLen(len(buf))
	if _, err := mem.Write(buf); err != nil {
		return nil, 0, fmt.Errorf("buffer file crypto metadata: %w", err)
	}
	protocol := thrift.NewTCompactProtocolConf(mem, &thrift.TConfiguration{})
	meta := parquet.NewFileCryptoMetaData()
	if err := meta.Read(ctx, protocol); err != nil {
		return nil, 0, fmt.Errorf("decode file crypto metadata: %w", err)
	}
	remaining := int(mem.RemainingBytes())
	return meta, len(buf) - remaining, nil
}

func readFileMetaDataFromBytes(ctx context.Context, buf []byte) (*parquet.FileMetaData, error) {
	footer := parquet.NewFileMetaData()
	protocol := thrift.NewTCompactProtocolConf(thrift.NewTBufferedTransport(thrift.NewStreamTransportR(bytes.NewReader(buf)), len(buf)), &thrift.TConfiguration{})
	if err := footer.Read(ctx, protocol); err != nil {
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
	footer, err := readFileMetaDataFromBytes(pr.context(), footerBytes)
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

func readColumnMetaDataFromBytes(ctx context.Context, buf []byte) (*parquet.ColumnMetaData, error) {
	meta := parquet.NewColumnMetaData()
	protocol := thrift.NewTCompactProtocolConf(thrift.NewStreamTransportR(bytes.NewReader(buf)), &thrift.TConfiguration{})
	if err := meta.Read(ctx, protocol); err != nil {
		return nil, fmt.Errorf("decode column metadata: %w", err)
	}
	return meta, nil
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
	meta, err := readColumnMetaDataFromBytes(pr.context(), plain)
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
