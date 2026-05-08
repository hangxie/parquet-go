package writer

import (
	"github.com/hangxie/parquet-go/v3/common"
)

// EncryptionAlgorithm selects the Parquet modular encryption algorithm.
type EncryptionAlgorithm int

const (
	// EncryptionAESGCMV1 uses AES-GCM for every encrypted module.
	EncryptionAESGCMV1 EncryptionAlgorithm = iota
	// EncryptionAESGCMCTRV1 uses AES-GCM for metadata and AES-CTR for data pages.
	EncryptionAESGCMCTRV1
)

// EncryptionColumnKey configures a column encryption key and optional key
// retrieval metadata.
type EncryptionColumnKey struct {
	// Key is the AES column encryption key (16, 24, or 32 bytes). When empty,
	// KeyRetriever is called with KeyMetadata to resolve the key at write time.
	Key []byte
	// KeyMetadata is opaque bytes stored in the file and passed to KeyRetriever
	// when Key is empty.
	KeyMetadata []byte
}

// KeyRetriever resolves a Parquet encryption key from key metadata.
type KeyRetriever func(keyMetadata []byte) ([]byte, error)

// EncryptionConfig configures Parquet modular encryption for writer output.
// FooterKey is required unless KeyRetriever resolves a non-empty footer key.
// Columns use FooterKey unless a path-specific key is present in ColumnKeys.
// Column key paths are dot-separated schema paths without the root element.
type EncryptionConfig struct {
	// Algorithm selects the encryption scheme. EncryptionAESGCMV1 (default)
	// authenticates every module. EncryptionAESGCMCTRV1 uses AES-CTR for page
	// bodies — lower overhead, but page body tampering is not detected.
	Algorithm EncryptionAlgorithm
	// FooterKey is the AES footer encryption key (16, 24, or 32 bytes).
	// Required unless KeyRetriever returns a non-empty key for FooterKeyMetadata.
	FooterKey []byte
	// FooterKeyMetadata is opaque bytes stored in the file and passed to
	// KeyRetriever when FooterKey is empty.
	FooterKeyMetadata []byte
	// ColumnKeys maps dot-separated column paths (without the root element) to
	// per-column keys. Columns not listed use FooterKey.
	ColumnKeys map[string]EncryptionColumnKey
	// AADPrefix is the file-identity prefix mixed into every module's AAD.
	// Must be non-empty when SupplyAADPrefix is true.
	AADPrefix []byte
	// AADFileUnique is the per-file portion of the AAD. When empty, 12 random
	// bytes are generated automatically. Supply explicitly for deterministic
	// file identity (e.g. content-addressed storage).
	AADFileUnique []byte
	// SupplyAADPrefix omits AADPrefix from the file so readers must supply it
	// via reader.WithAADPrefix. Requires a non-empty AADPrefix.
	SupplyAADPrefix bool
	// PlaintextFooter writes a PAR1 file with a plaintext footer and an
	// AES-GCM signature instead of an encrypted PARE footer.
	PlaintextFooter bool
	// KeyRetriever resolves encryption keys from key metadata at write time.
	// Called for footer and column keys that have no direct key set.
	KeyRetriever KeyRetriever
}

// WithEncryption enables Parquet modular encryption for writer output.
func WithEncryption(config EncryptionConfig) WriterOption {
	return func(pw *ParquetWriter) {
		pw.encryptionConfig = &config
	}
}

// WithFooterKey sets the footer encryption key and optional key metadata.
func WithFooterKey(key []byte, keyMetadata ...[]byte) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.FooterKey = append(config.FooterKey[:0], key...)
		if len(keyMetadata) > 0 {
			config.FooterKeyMetadata = append(config.FooterKeyMetadata[:0], keyMetadata[0]...)
		}
	}
}

// WithFooterKeyMetadata sets footer key metadata. WithKeyRetriever resolves the
// footer key from this metadata and takes priority when it returns a non-empty
// key.
func WithFooterKeyMetadata(keyMetadata []byte) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.FooterKeyMetadata = append(config.FooterKeyMetadata[:0], keyMetadata...)
	}
}

// WithColumnKey sets an encryption key and optional key metadata for a column.
// The path is the dot-separated schema path without the root element.
func WithColumnKey(path string, key []byte, keyMetadata ...[]byte) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		if config.ColumnKeys == nil {
			config.ColumnKeys = make(map[string]EncryptionColumnKey)
		}
		columnKey := config.ColumnKeys[common.ReformPathStr(path)]
		columnKey.Key = append(columnKey.Key[:0], key...)
		if len(keyMetadata) > 0 {
			columnKey.KeyMetadata = append(columnKey.KeyMetadata[:0], keyMetadata[0]...)
		}
		config.ColumnKeys[common.ReformPathStr(path)] = columnKey
	}
}

// WithColumnKeyMetadata sets column key metadata. When the column key is not
// set directly, WithKeyRetriever resolves the key from this metadata.
func WithColumnKeyMetadata(path string, keyMetadata []byte) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		if config.ColumnKeys == nil {
			config.ColumnKeys = make(map[string]EncryptionColumnKey)
		}
		columnKey := config.ColumnKeys[common.ReformPathStr(path)]
		columnKey.KeyMetadata = append(columnKey.KeyMetadata[:0], keyMetadata...)
		config.ColumnKeys[common.ReformPathStr(path)] = columnKey
	}
}

// WithKeyRetriever sets a callback for resolving encryption keys from Parquet
// key_metadata fields while writing. Explicit keys set via WithFooterKey or
// WithColumnKey always take priority; the retriever is only called when no
// direct key is provided.
func WithKeyRetriever(retriever KeyRetriever) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.KeyRetriever = retriever
	}
}

// WithAADPrefix sets the AAD prefix used for encrypted modules.
func WithAADPrefix(prefix []byte) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.AADPrefix = append(config.AADPrefix[:0], prefix...)
	}
}

// WithAADFileUnique sets the unique file identifier portion of AAD.
func WithAADFileUnique(fileUnique []byte) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.AADFileUnique = append(config.AADFileUnique[:0], fileUnique...)
	}
}

// WithSupplyAADPrefix controls whether the AAD prefix is stored in file
// metadata. When true, readers must supply the prefix with reader.WithAADPrefix.
func WithSupplyAADPrefix(enabled bool) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.SupplyAADPrefix = enabled
	}
}

// WithPlaintextFooter writes an encrypted file with plaintext footer metadata
// and an AES-GCM footer signature.
func WithPlaintextFooter(enabled bool) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.PlaintextFooter = enabled
	}
}

// WithEncryptionAlgorithm sets the modular encryption algorithm.
func WithEncryptionAlgorithm(algorithm EncryptionAlgorithm) WriterOption {
	return func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.Algorithm = algorithm
	}
}

func (pw *ParquetWriter) ensureEncryptionConfig() *EncryptionConfig {
	if pw.encryptionConfig == nil {
		pw.encryptionConfig = &EncryptionConfig{}
	}
	return pw.encryptionConfig
}
