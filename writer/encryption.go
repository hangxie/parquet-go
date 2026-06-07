package writer

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

type encryptionState struct {
	algorithm          EncryptionAlgorithm
	footerKey          []byte
	footerKeyMetadata  []byte
	columnKeys         map[string]EncryptionColumnKey
	columnKeysFullPath bool
	aadPrefix          []byte
	aadFileUnique      []byte
	supplyAADPrefix    bool
	plaintextFooter    bool
}

func newEncryptionState(config EncryptionConfig) (*encryptionState, error) {
	if config.KeyRetriever != nil && len(config.FooterKey) == 0 {
		key, err := config.KeyRetriever(config.FooterKeyMetadata)
		if err != nil {
			return nil, fmt.Errorf("WithKeyRetriever: retrieve footer key: %w", err)
		}
		if len(key) > 0 {
			config.FooterKey = key
		}
	}
	if !isValidAESKey(config.FooterKey) {
		return nil, fmt.Errorf("WithEncryption: invalid AES footer key size %d", len(config.FooterKey))
	}
	switch config.Algorithm {
	case EncryptionAESGCMV1, EncryptionAESGCMCTRV1:
	default:
		return nil, fmt.Errorf("WithEncryption: unsupported encryption algorithm %d", config.Algorithm)
	}
	if config.SupplyAADPrefix && len(config.AADPrefix) == 0 {
		return nil, fmt.Errorf("WithSupplyAADPrefix: AADPrefix must be non-empty when supply_aad_prefix is true")
	}

	fileUnique := append([]byte(nil), config.AADFileUnique...)
	if len(fileUnique) == 0 {
		fileUnique = make([]byte, 12)
		if _, err := rand.Read(fileUnique); err != nil {
			return nil, fmt.Errorf("WithEncryption: generate AAD file unique: %w", err)
		}
	}

	state := &encryptionState{
		algorithm:         config.Algorithm,
		footerKey:         append([]byte(nil), config.FooterKey...),
		footerKeyMetadata: append([]byte(nil), config.FooterKeyMetadata...),
		columnKeys:        make(map[string]EncryptionColumnKey, len(config.ColumnKeys)),
		aadPrefix:         append([]byte(nil), config.AADPrefix...),
		aadFileUnique:     fileUnique,
		supplyAADPrefix:   config.SupplyAADPrefix,
		plaintextFooter:   config.PlaintextFooter,
	}
	for path, entry := range config.ColumnKeys {
		resolved, err := resolveColumnEntry(path, entry, config.KeyRetriever)
		if err != nil {
			return nil, err
		}
		state.columnKeys[common.ReformPathStr(path)] = resolved
	}
	return state, nil
}

// resolveColumnEntry validates and resolves one ColumnKeys entry. The
// supported states are:
//
//   - {Key: nil, KeyMetadata: nil}                       — footer-key sentinel
//   - {Key: valid, KeyMetadata: nil}                     — literal column key
//   - {Key: valid, KeyMetadata: md}                      — literal column key + stored metadata
//   - {Key: nil, KeyMetadata: md} + KeyRetriever         — retrieve via metadata
//
// Other shapes are configuration errors.
func resolveColumnEntry(path string, entry EncryptionColumnKey, retriever KeyRetriever) (EncryptionColumnKey, error) {
	switch {
	case len(entry.Key) == 0 && len(entry.KeyMetadata) == 0:
		// Footer-key sentinel.
		return EncryptionColumnKey{}, nil

	case len(entry.Key) == 0:
		// Retrieve via KeyRetriever.
		if retriever == nil {
			return EncryptionColumnKey{}, fmt.Errorf("WithColumnEncrypted: column %q has KeyMetadata but no KeyRetriever is configured", path)
		}
		resolvedKey, err := retriever(entry.KeyMetadata)
		if err != nil {
			return EncryptionColumnKey{}, fmt.Errorf("WithColumnEncrypted: retrieve column key %q: %w", path, err)
		}
		if len(resolvedKey) == 0 {
			return EncryptionColumnKey{}, fmt.Errorf("WithColumnEncrypted: column %q resolved to empty key; KeyRetriever must return a valid AES key", path)
		}
		if !isValidAESKey(resolvedKey) {
			return EncryptionColumnKey{}, fmt.Errorf("WithColumnEncrypted: column %q resolved key has invalid AES size %d", path, len(resolvedKey))
		}
		return EncryptionColumnKey{
			Key:         append([]byte(nil), resolvedKey...),
			KeyMetadata: append([]byte(nil), entry.KeyMetadata...),
		}, nil

	default:
		// Literal column key (with or without stored metadata).
		if !isValidAESKey(entry.Key) {
			return EncryptionColumnKey{}, fmt.Errorf("WithColumnEncrypted: column %q has invalid AES key size %d", path, len(entry.Key))
		}
		return EncryptionColumnKey{
			Key:         append([]byte(nil), entry.Key...),
			KeyMetadata: append([]byte(nil), entry.KeyMetadata...),
		}, nil
	}
}

func isValidAESKey(key []byte) bool {
	switch len(key) {
	case 16, 24, 32:
		return true
	default:
		return false
	}
}

func (s *encryptionState) parquetAlgorithm() *parquet.EncryptionAlgorithm {
	if s.algorithm == EncryptionAESGCMCTRV1 {
		return &parquet.EncryptionAlgorithm{
			AES_GCM_CTR_V1: &parquet.AesGcmCtrV1{
				AadPrefix:       storedAADPrefix(s),
				AadFileUnique:   append([]byte(nil), s.aadFileUnique...),
				SupplyAadPrefix: &s.supplyAADPrefix,
			},
		}
	}
	return &parquet.EncryptionAlgorithm{
		AES_GCM_V1: &parquet.AesGcmV1{
			AadPrefix:       storedAADPrefix(s),
			AadFileUnique:   append([]byte(nil), s.aadFileUnique...),
			SupplyAadPrefix: &s.supplyAADPrefix,
		},
	}
}

func storedAADPrefix(s *encryptionState) []byte {
	if s.supplyAADPrefix {
		return nil
	}
	return append([]byte(nil), s.aadPrefix...)
}

func (pw *ParquetWriter) encryptPage(page *layout.Page, key []byte, rowGroupOrdinal, columnOrdinal, pageOrdinal int16) error {
	if pw.encryptionState == nil {
		return nil
	}
	// Serialize the original header to locate the body within RawData.
	originalHeaderBuf, err := serializeCompact(page.Header)
	if err != nil {
		return fmt.Errorf("serialize page header: %w", err)
	}
	if len(page.RawData) < len(originalHeaderBuf) {
		return fmt.Errorf("page raw data shorter than serialized header")
	}
	if !bytes.Equal(originalHeaderBuf, page.RawData[:len(originalHeaderBuf)]) {
		return fmt.Errorf("page header re-serialization mismatch: cannot determine body offset")
	}
	body := page.RawData[len(originalHeaderBuf):]

	headerModuleType := encryption.ModuleDataPageHeader
	bodyModuleType := encryption.ModuleDataPage
	if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		headerModuleType = encryption.ModuleDictionaryPageHeader
		bodyModuleType = encryption.ModuleDictionaryPage
	}

	// Encrypt body first so we know its encoded size.
	var bodyModule []byte
	if pw.encryptionState.algorithm == EncryptionAESGCMCTRV1 {
		bodyModule, err = encryption.EncryptCTR(key, body)
	} else {
		bodyModule, err = encryption.EncryptGCM(key, pw.moduleAAD(bodyModuleType, rowGroupOrdinal, columnOrdinal, pageOrdinal), body)
	}
	if err != nil {
		return fmt.Errorf("encrypt page body: %w", err)
	}
	encodedBody, err := encryption.EncodeModule(bodyModule)
	if err != nil {
		return fmt.Errorf("encode body module: %w", err)
	}

	// Update CompressedPageSize to the encrypted body module size so that
	// readers (e.g. Java) can advance their stream position past the body
	// to reach the next page header.
	page.Header.CompressedPageSize = int32(len(encodedBody))

	// Re-serialize the header with the updated CompressedPageSize.
	headerBuf, err := serializeCompact(page.Header)
	if err != nil {
		return fmt.Errorf("re-serialize page header: %w", err)
	}

	headerModule, err := encryption.EncryptGCM(key, pw.moduleAAD(headerModuleType, rowGroupOrdinal, columnOrdinal, pageOrdinal), headerBuf)
	if err != nil {
		return fmt.Errorf("encrypt page header: %w", err)
	}
	encodedHeader, err := encryption.EncodeModule(headerModule)
	if err != nil {
		return fmt.Errorf("encode header module: %w", err)
	}

	page.RawData = encodedHeader
	page.RawData = append(page.RawData, encodedBody...)
	return nil
}

func (pw *ParquetWriter) moduleAAD(moduleType encryption.ModuleType, rowGroupOrdinal, columnOrdinal, pageOrdinal int16) []byte {
	return encryption.AAD(
		pw.encryptionState.aadPrefix,
		pw.encryptionState.aadFileUnique,
		moduleType,
		rowGroupOrdinal,
		columnOrdinal,
		pageOrdinal,
	)
}

func serializeCompact(v thrift.TStruct) ([]byte, error) {
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	return ts.Write(context.TODO(), v)
}

func (pw *ParquetWriter) encryptThriftModule(key []byte, moduleType encryption.ModuleType, rowGroupOrdinal, columnOrdinal int16, plain []byte) ([]byte, error) {
	module, err := encryption.EncryptGCM(key, pw.moduleAAD(moduleType, rowGroupOrdinal, columnOrdinal, 0), plain)
	if err != nil {
		return nil, fmt.Errorf("encrypt module %d: %w", moduleType, err)
	}
	encoded, err := encryption.EncodeModule(module)
	if err != nil {
		return nil, fmt.Errorf("encode module %d: %w", moduleType, err)
	}
	return encoded, nil
}

func (pw *ParquetWriter) encryptFooter(footerBuf []byte) ([]byte, string, error) {
	if pw.encryptionState == nil {
		return footerBuf, common.MagicBytes, nil
	}
	aad := pw.moduleAAD(encryption.ModuleFooter, 0, 0, 0)
	if pw.encryptionState.plaintextFooter {
		signature, err := encryption.SignGCM(pw.encryptionState.footerKey, aad, footerBuf)
		if err != nil {
			return nil, "", fmt.Errorf("sign plaintext footer: %w", err)
		}
		return append(footerBuf, signature...), common.MagicBytes, nil
	}

	fileCrypto := parquet.NewFileCryptoMetaData()
	fileCrypto.EncryptionAlgorithm = pw.encryptionState.parquetAlgorithm()
	fileCrypto.KeyMetadata = append([]byte(nil), pw.encryptionState.footerKeyMetadata...)
	fileCryptoBuf, err := serializeCompact(fileCrypto)
	if err != nil {
		return nil, "", fmt.Errorf("serialize file crypto metadata: %w", err)
	}
	module, err := encryption.EncryptGCM(pw.encryptionState.footerKey, aad, footerBuf)
	if err != nil {
		return nil, "", fmt.Errorf("encrypt footer: %w", err)
	}
	encodedModule, err := encryption.EncodeModule(module)
	if err != nil {
		return nil, "", fmt.Errorf("encode footer module: %w", err)
	}
	section := append(fileCryptoBuf, encodedModule...)
	return section, common.MagicBytesEncrypted, nil
}
