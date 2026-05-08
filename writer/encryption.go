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
	algorithm         EncryptionAlgorithm
	footerKey         []byte
	footerKeyMetadata []byte
	columnKeys        map[string]EncryptionColumnKey
	aadPrefix         []byte
	aadFileUnique     []byte
	supplyAADPrefix   bool
	plaintextFooter   bool
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
	for path, key := range config.ColumnKeys {
		if len(key.Key) == 0 && config.KeyRetriever != nil {
			resolvedKey, err := config.KeyRetriever(key.KeyMetadata)
			if err != nil {
				return nil, fmt.Errorf("WithKeyRetriever: retrieve column key %q: %w", path, err)
			}
			key.Key = resolvedKey
		}
		if !isValidAESKey(key.Key) {
			return nil, fmt.Errorf("WithEncryption: invalid AES column key %q size %d", path, len(key.Key))
		}
		state.columnKeys[common.ReformPathStr(path)] = EncryptionColumnKey{
			Key:         append([]byte(nil), key.Key...),
			KeyMetadata: append([]byte(nil), key.KeyMetadata...),
		}
	}
	return state, nil
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

func (pw *ParquetWriter) columnKey(path []string) ([]byte, []byte, bool) {
	if pw.encryptionState == nil {
		return nil, nil, false
	}
	for _, candidate := range pw.columnPathCandidates(path) {
		if key, ok := pw.encryptionState.columnKeys[candidate]; ok {
			return key.Key, key.KeyMetadata, false
		}
	}
	return pw.encryptionState.footerKey, nil, true
}

func (pw *ParquetWriter) keyForEncryptedColumn(column *parquet.ColumnChunk) ([]byte, error) {
	if column.GetCryptoMetadata().IsSetENCRYPTION_WITH_FOOTER_KEY() {
		return pw.encryptionState.footerKey, nil
	}
	columnKey := column.GetCryptoMetadata().GetENCRYPTION_WITH_COLUMN_KEY()
	if columnKey == nil {
		return nil, fmt.Errorf("unsupported column crypto metadata")
	}
	for _, candidate := range pw.columnPathCandidates(pw.fullColumnPath(columnKey.GetPathInSchema())) {
		if key, ok := pw.encryptionState.columnKeys[candidate]; ok {
			return key.Key, nil
		}
	}
	return nil, fmt.Errorf("missing encryption key for column %s", common.PathToStr(columnKey.GetPathInSchema()))
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

func (pw *ParquetWriter) columnPathCandidates(path []string) []string {
	candidates := []string{common.PathToStr(path)}
	stripped := stripRoot(path)
	candidates = append(candidates, common.PathToStr(stripped))
	if pw.SchemaHandler != nil {
		if exPath, ok := pw.SchemaHandler.InPathToExPath[common.PathToStr(path)]; ok {
			candidates = append(candidates, exPath, common.PathToStr(stripRoot(common.StrToPath(exPath))))
		}
		if exPath, ok := pw.SchemaHandler.InPathToExPath[common.PathToStr(stripped)]; ok {
			candidates = append(candidates, exPath, common.PathToStr(stripRoot(common.StrToPath(exPath))))
		}
	}
	return candidates
}

// validateEncryptionColumnKeys checks every configured column-key path
// against the leaf columns of the schema. A typo silently falling back to
// the footer key would be a security-sensitive misconfiguration, so this
// fails fast as soon as the schema becomes available.
func (pw *ParquetWriter) validateEncryptionColumnKeys() error {
	if pw.encryptionState == nil || len(pw.encryptionState.columnKeys) == 0 {
		return nil
	}
	if pw.SchemaHandler == nil {
		return nil
	}
	valid := pw.leafColumnPathSet()
	for path := range pw.encryptionState.columnKeys {
		if _, ok := valid[path]; !ok {
			return fmt.Errorf("WithColumnKey: path %q does not match any schema column", path)
		}
	}
	return nil
}

// leafColumnPathSet returns the set of leaf column paths in every form
// WithColumnKey accepts: internal and external names, with and without the
// schema root prefix.
func (pw *ParquetWriter) leafColumnPathSet() map[string]struct{} {
	paths := make(map[string]struct{})
	sh := pw.SchemaHandler
	if sh == nil {
		return paths
	}
	schemas := sh.SchemaElements
	type frame struct {
		pos      int32
		children int32
	}
	var stack []frame
	emit := func() {
		inPath := make([]string, 0, len(stack))
		exPath := make([]string, 0, len(stack))
		for _, f := range stack {
			inPath = append(inPath, sh.Infos[f.pos].InName)
			exPath = append(exPath, sh.Infos[f.pos].ExName)
		}
		paths[common.PathToStr(inPath)] = struct{}{}
		paths[common.PathToStr(stripRoot(inPath))] = struct{}{}
		paths[common.PathToStr(exPath)] = struct{}{}
		paths[common.PathToStr(stripRoot(exPath))] = struct{}{}
	}
	for pos := int32(0); pos < int32(len(schemas)); pos++ {
		for len(stack) > 0 && stack[len(stack)-1].children == 0 {
			stack = stack[:len(stack)-1]
		}
		if len(stack) > 0 {
			stack[len(stack)-1].children--
		}
		nc := schemas[pos].GetNumChildren()
		stack = append(stack, frame{pos: pos, children: nc})
		if nc == 0 {
			emit()
		}
	}
	return paths
}

func stripRoot(path []string) []string {
	if len(path) > 0 && (path[0] == common.ParGoRootInName || path[0] == common.ParGoRootExName) {
		return path[1:]
	}
	return path
}

func (pw *ParquetWriter) fullColumnPath(path []string) []string {
	if len(path) > 0 && (path[0] == common.ParGoRootInName || path[0] == common.ParGoRootExName) {
		return path
	}
	root := common.ParGoRootInName
	if pw.SchemaHandler != nil {
		root = pw.SchemaHandler.GetRootInName()
	}
	return append([]string{root}, path...)
}

func (pw *ParquetWriter) columnCryptoMetadata(path []string, footerKey bool, keyMetadata []byte) *parquet.ColumnCryptoMetaData {
	if footerKey {
		return &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
		}
	}
	return &parquet.ColumnCryptoMetaData{
		ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{
			PathInSchema: pw.columnExternalPath(path),
			KeyMetadata:  append([]byte(nil), keyMetadata...),
		},
	}
}

func (pw *ParquetWriter) columnExternalPath(path []string) []string {
	pathStr := common.PathToStr(path)
	if pw.SchemaHandler != nil {
		if exPath, ok := pw.SchemaHandler.InPathToExPath[pathStr]; ok {
			return stripRoot(common.StrToPath(exPath))
		}
	}
	return stripRoot(path)
}

func (pw *ParquetWriter) encryptColumnMetadata(rowGroupOrdinal, columnOrdinal int16, column *parquet.ColumnChunk) error {
	if pw.encryptionState == nil || column == nil || column.MetaData == nil {
		return nil
	}
	path := pw.fullColumnPath(column.MetaData.GetPathInSchema())
	key, keyMetadata, footerKey := pw.columnKey(path)
	column.CryptoMetadata = pw.columnCryptoMetadata(path, footerKey, keyMetadata)
	plaintextFooter := pw.encryptionState.plaintextFooter
	// Encrypted footer + footer-key column: ColumnMetaData stays in place;
	// the encrypted footer blob protects it. encrypted_column_metadata is
	// disallowed by the spec for this case.
	if !plaintextFooter && footerKey {
		return nil
	}
	plain, err := serializeCompact(column.MetaData)
	if err != nil {
		return fmt.Errorf("serialize column metadata: %w", err)
	}
	module, err := encryption.EncryptGCM(key, pw.moduleAAD(encryption.ModuleColumnMetaData, rowGroupOrdinal, columnOrdinal, 0), plain)
	if err != nil {
		return fmt.Errorf("encrypt column metadata: %w", err)
	}
	encoded, err := encryption.EncodeModule(module)
	if err != nil {
		return fmt.Errorf("encode column metadata module: %w", err)
	}
	column.EncryptedColumnMetadata = encoded
	if plaintextFooter {
		// Plaintext footer must keep ColumnMetaData populated for legacy
		// readers to find data, but sensitive statistics are stripped so
		// they only appear in the authenticated encrypted_column_metadata.
		column.MetaData.Statistics = nil
		column.MetaData.SizeStatistics = nil
		column.MetaData.GeospatialStatistics = nil
	}
	// Encrypted footer: MetaData stays in place (it is protected by the
	// encrypted footer). Readers that hold only the column key use
	// EncryptedColumnMetadata; readers with the footer key use MetaData.
	return nil
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
