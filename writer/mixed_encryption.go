package writer

import (
	"fmt"

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
// caller may pass the path either with or without the schema root prefix;
// the helper normalizes internally by trying all known path candidates
// against the configured ColumnKeys map.
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
//   - path not listed: PlaintextUnkeyedColumns=true -> plaintext;
//     PlaintextUnkeyedColumns=false -> footer-key (the legacy default).
func (pw *ParquetWriter) classifyColumn(path []string) columnEncryptionClassification {
	if pw.encryptionState == nil {
		return columnEncryptionClassification{Kind: columnEncryptionPlaintext}
	}
	if entry, ok := pw.lookupConfiguredColumn(path); ok {
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
	if pw.encryptionState.plaintextUnkeyedColumns {
		return columnEncryptionClassification{Kind: columnEncryptionPlaintext}
	}
	return columnEncryptionClassification{
		Kind: columnEncryptionFooterKey,
		Key:  pw.encryptionState.footerKey,
	}
}

func (pw *ParquetWriter) lookupConfiguredColumn(path []string) (EncryptionColumnKey, bool) {
	if pw.encryptionState == nil {
		return EncryptionColumnKey{}, false
	}
	for _, candidate := range pw.columnPathCandidates(path) {
		entry, ok := pw.encryptionState.columnKeys[candidate]
		if ok {
			return entry, true
		}
	}
	return EncryptionColumnKey{}, false
}

func (pw *ParquetWriter) keyForEncryptedColumn(column *parquet.ColumnChunk) ([]byte, error) {
	if column.GetCryptoMetadata().IsSetENCRYPTION_WITH_FOOTER_KEY() {
		return pw.encryptionState.footerKey, nil
	}
	columnKey := column.GetCryptoMetadata().GetENCRYPTION_WITH_COLUMN_KEY()
	if columnKey == nil {
		return nil, fmt.Errorf("unsupported column crypto metadata")
	}
	entry, ok := pw.lookupConfiguredColumn(pw.fullColumnPath(columnKey.GetPathInSchema()))
	if ok && len(entry.Key) > 0 {
		return entry.Key, nil
	}
	return nil, fmt.Errorf("missing encryption key for column %s", common.PathToStr(columnKey.GetPathInSchema()))
}

func (pw *ParquetWriter) columnPathCandidates(path []string) []string {
	candidates := make([]string, 0, 8)
	candidates = appendPathCandidate(candidates, common.PathToStr(path))
	stripped := stripRoot(path)
	candidates = appendPathCandidate(candidates, common.PathToStr(stripped))

	if pw.SchemaHandler != nil {
		if len(stripped) > 0 {
			if root := pw.SchemaHandler.GetRootInName(); root != "" {
				candidates = appendPathCandidate(candidates, common.PathToStr(append([]string{root}, stripped...)))
			}
			if root := pw.SchemaHandler.GetRootExName(); root != "" {
				candidates = appendPathCandidate(candidates, common.PathToStr(append([]string{root}, stripped...)))
			}
		}
		for _, candidate := range append([]string(nil), candidates...) {
			if exPath, ok := pw.SchemaHandler.InPathToExPath[candidate]; ok {
				candidates = appendPathCandidate(candidates, exPath)
			}
			if inPath, ok := pw.SchemaHandler.ExPathToInPath[candidate]; ok {
				candidates = appendPathCandidate(candidates, inPath)
			}
		}
	}
	return dedupePathCandidates(candidates)
}

func appendPathCandidate(candidates []string, path string) []string {
	candidates = append(candidates, path)
	stripped := common.PathToStr(stripRoot(common.StrToPath(path)))
	if stripped != path {
		candidates = append(candidates, stripped)
	}
	return candidates
}

func dedupePathCandidates(candidates []string) []string {
	seen := make(map[string]struct{}, len(candidates))
	out := candidates[:0]
	for _, candidate := range candidates {
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	return out
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
			return fmt.Errorf("WithColumnEncrypted: path %q does not match any schema column", path)
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
