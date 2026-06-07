package writer

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// columnEncryptionKind identifies how a column is encrypted by the writer.
// Plaintext columns omit CryptoMetadata entirely and skip all module
// encryption; footer-key and column-key columns set CryptoMetadata to the
// matching ColumnCryptoMetaData variant.
type columnEncryptionKind int

const (
	columnEncryptionPlaintext columnEncryptionKind = iota
	columnEncryptionFooterKey
	columnEncryptionColumnKey
)

// columnEncryptionClassification names a single column's authoring intent
// resolved against the writer's encryption state. Key is empty for
// plaintext columns; KeyMetadata is non-empty only for column-key columns
// that carry stored metadata for downstream readers.
type columnEncryptionClassification struct {
	Kind        columnEncryptionKind
	Key         []byte
	KeyMetadata []byte
}

// classifyColumn decides how the column at path should be encrypted. The
// configured ColumnKeys map stores validated full internal schema paths.
//
// When encryption is disabled, every column is plaintext. When encryption
// is enabled, classification follows the configured column entry:
//
//   - path listed in ColumnKeys with empty Key and empty KeyMetadata ->
//     footer-key (this is the explicit footer-key sentinel, equivalent to
//     ColumnFooterKey() or WithColumnEncrypted(path)).
//   - path listed with non-empty Key -> column-key, using that key. Stored
//     KeyMetadata travels into the resulting classification so callers
//     populating CryptoMetadata can emit it.
//   - path not listed -> plaintext.
func (pw *ParquetWriter) classifyColumn(path []string) columnEncryptionClassification {
	if pw.encryptionState == nil {
		return columnEncryptionClassification{Kind: columnEncryptionPlaintext}
	}
	entry, ok := pw.lookupConfiguredColumn(path)
	if !ok {
		return columnEncryptionClassification{Kind: columnEncryptionPlaintext}
	}
	if len(entry.Key) == 0 {
		return columnEncryptionClassification{
			Kind: columnEncryptionFooterKey,
			Key:  pw.encryptionState.footerKey,
		}
	}
	return columnEncryptionClassification{
		Kind:        columnEncryptionColumnKey,
		Key:         entry.Key,
		KeyMetadata: entry.KeyMetadata,
	}
}

func (pw *ParquetWriter) lookupConfiguredColumn(path []string) (EncryptionColumnKey, bool) {
	if pw.encryptionState == nil {
		return EncryptionColumnKey{}, false
	}
	entry, ok := pw.encryptionState.columnKeys[pw.fullInternalColumnPath(path)]
	if ok && len(entry.Key) > 0 {
		return entry, true
	}
	return EncryptionColumnKey{}, ok
}

func (pw *ParquetWriter) keyForEncryptedColumn(column *parquet.ColumnChunk) ([]byte, error) {
	if column.GetCryptoMetadata().IsSetENCRYPTION_WITH_FOOTER_KEY() {
		return pw.encryptionState.footerKey, nil
	}
	columnKey := column.GetCryptoMetadata().GetENCRYPTION_WITH_COLUMN_KEY()
	if columnKey == nil {
		return nil, fmt.Errorf("unsupported column crypto metadata")
	}
	entry, ok := pw.lookupConfiguredColumn(columnKey.GetPathInSchema())
	if ok && len(entry.Key) > 0 {
		return entry.Key, nil
	}
	return nil, fmt.Errorf(
		"missing encryption key for column %s",
		strings.ReplaceAll(pw.fullExternalColumnPath(columnKey.GetPathInSchema()), common.ParGoPathDelimiter, "."),
	)
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
	if pw.encryptionState.columnKeysFullPath {
		return nil
	}
	valid := pw.leafColumnFullInternalPaths()
	normalized := make(map[string]EncryptionColumnKey, len(pw.encryptionState.columnKeys))
	for path, entry := range pw.encryptionState.columnKeys {
		fullPath, ok := pw.fullInternalOptionPath(path, valid)
		if !ok {
			fullExternalPath := common.PathToStr(append([]string{pw.SchemaHandler.GetRootExName()}, common.StrToPath(path)...))
			return fmt.Errorf(
				"WithColumnEncrypted: path %q resolves to %q, which does not match any schema column",
				strings.ReplaceAll(path, common.ParGoPathDelimiter, "."),
				strings.ReplaceAll(fullExternalPath, common.ParGoPathDelimiter, "."),
			)
		}
		normalized[fullPath] = entry
	}
	pw.encryptionState.columnKeys = normalized
	pw.encryptionState.columnKeysFullPath = true
	return nil
}

// leafColumnFullInternalPaths returns full internal paths for leaf columns.
func (pw *ParquetWriter) leafColumnFullInternalPaths() map[string]struct{} {
	paths := make(map[string]struct{})
	if pw.SchemaHandler == nil {
		return paths
	}
	for _, path := range pw.SchemaHandler.ValueColumns {
		paths[path] = struct{}{}
	}
	return paths
}

func (pw *ParquetWriter) fullInternalOptionPath(path string, valid map[string]struct{}) (string, bool) {
	fullEx := common.PathToStr(append([]string{pw.SchemaHandler.GetRootExName()}, common.StrToPath(path)...))
	inPath, ok := pw.SchemaHandler.ExPathToInPath[fullEx]
	if !ok {
		return "", false
	}
	if _, ok := valid[inPath]; !ok {
		return "", false
	}
	return inPath, true
}

func (pw *ParquetWriter) fullInternalColumnPath(path []string) string {
	if pw.SchemaHandler == nil {
		return common.PathToStr(path)
	}
	pathStr := common.PathToStr(path)

	if _, ok := pw.SchemaHandler.InPathToExPath[pathStr]; ok {
		return pathStr
	}
	fullIn := common.PathToStr(append([]string{pw.SchemaHandler.GetRootInName()}, path...))
	if _, ok := pw.SchemaHandler.InPathToExPath[fullIn]; ok {
		return fullIn
	}
	fullEx := common.PathToStr(append([]string{pw.SchemaHandler.GetRootExName()}, path...))
	if inPath, ok := pw.SchemaHandler.ExPathToInPath[fullEx]; ok {
		return inPath
	}
	if inPath, ok := pw.SchemaHandler.ExPathToInPath[pathStr]; ok {
		return inPath
	}
	return pathStr
}

func (pw *ParquetWriter) fullExternalColumnPath(path []string) string {
	if pw.SchemaHandler == nil {
		return common.PathToStr(path)
	}
	fullIn := pw.fullInternalColumnPath(path)
	if exPath, ok := pw.SchemaHandler.InPathToExPath[fullIn]; ok {
		return exPath
	}
	return common.PathToStr(append([]string{pw.SchemaHandler.GetRootExName()}, path...))
}

func (pw *ParquetWriter) columnCryptoMetadata(path []string, classification columnEncryptionClassification) *parquet.ColumnCryptoMetaData {
	switch classification.Kind {
	case columnEncryptionFooterKey:
		return &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
		}
	case columnEncryptionColumnKey:
		return &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{
				PathInSchema: pw.columnExternalPath(path),
				KeyMetadata:  append([]byte(nil), classification.KeyMetadata...),
			},
		}
	default:
		return nil
	}
}

func (pw *ParquetWriter) columnExternalPath(path []string) []string {
	if pw.SchemaHandler == nil {
		return path
	}
	fullIn := pw.fullInternalColumnPath(path)
	if exPath, ok := pw.SchemaHandler.InPathToExPath[fullIn]; ok {
		exPathParts := common.StrToPath(exPath)
		if len(exPathParts) > 1 {
			return exPathParts[1:]
		}
	}
	return path
}

func (pw *ParquetWriter) encryptColumnMetadata(rowGroupOrdinal, columnOrdinal int16, column *parquet.ColumnChunk) error {
	if pw.encryptionState == nil || column == nil || column.MetaData == nil {
		return nil
	}
	path := column.MetaData.GetPathInSchema()
	classification := pw.classifyColumn(path)
	if classification.Kind == columnEncryptionPlaintext {
		// Plaintext columns are spec-compliant when CryptoMetadata is
		// absent. Skip both the CryptoMetadata assignment and any
		// statistics stripping - the column's MetaData is intentionally
		// in the clear.
		return nil
	}
	footerKey := classification.Kind == columnEncryptionFooterKey
	column.CryptoMetadata = pw.columnCryptoMetadata(path, classification)
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
	module, err := encryption.EncryptGCM(classification.Key, pw.moduleAAD(encryption.ModuleColumnMetaData, rowGroupOrdinal, columnOrdinal, 0), plain)
	if err != nil {
		return fmt.Errorf("encrypt column metadata: %w", err)
	}
	encoded, err := encryption.EncodeModule(module)
	if err != nil {
		return fmt.Errorf("encode column metadata module: %w", err)
	}
	column.EncryptedColumnMetadata = encoded
	if plaintextFooter {
		// Plaintext footer keeps ColumnMetaData populated for legacy
		// readers to find data, but sensitive statistics are stripped so
		// they only appear in the authenticated encrypted_column_metadata.
		// This applies to every encrypted column (footer-key and
		// column-key alike); plaintext columns under mixed mode are
		// handled by the early return above and retain their statistics.
		column.MetaData.Statistics = nil
		column.MetaData.SizeStatistics = nil
		column.MetaData.GeospatialStatistics = nil
	}
	// Encrypted footer: MetaData stays in place (it is protected by the
	// encrypted footer). Readers that hold only the column key use
	// EncryptedColumnMetadata; readers with the footer key use MetaData.
	return nil
}
