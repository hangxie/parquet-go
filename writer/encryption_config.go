package writer

// EncryptionAlgorithm selects the Parquet modular encryption algorithm.
type EncryptionAlgorithm int

const (
	// EncryptionAESGCMV1 uses AES-GCM for every encrypted module.
	EncryptionAESGCMV1 EncryptionAlgorithm = iota
	// EncryptionAESGCMCTRV1 uses AES-GCM for metadata and AES-CTR for data pages.
	EncryptionAESGCMCTRV1
)

// EncryptionColumnKey configures a column encryption key and optional key
// retrieval metadata. The zero value (both fields empty) signals
// ENCRYPTION_WITH_FOOTER_KEY for the column.
type EncryptionColumnKey struct {
	// Key is the AES column encryption key (16, 24, or 32 bytes). When empty
	// and KeyMetadata is non-empty, KeyRetriever is called with KeyMetadata
	// to resolve the key at write time. When both fields are empty the
	// column is encrypted with the footer key.
	Key []byte
	// KeyMetadata is opaque bytes stored in the file and passed to
	// KeyRetriever when Key is empty.
	KeyMetadata []byte
}

// KeyRetriever resolves a Parquet encryption key from key metadata.
type KeyRetriever func(keyMetadata []byte) ([]byte, error)

// EncryptionConfig configures Parquet modular encryption for writer output.
// FooterKey is required unless KeyRetriever resolves a non-empty footer key.
// Column treatment is selected by ColumnKeys entries; columns with no entry
// follow the PlaintextUnkeyedColumns flag (default: footer-key encryption).
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
	// per-column encryption settings. See EncryptionColumnKey for the
	// per-entry state table. Columns absent from this map follow
	// PlaintextUnkeyedColumns: false (default) → footer-key encryption,
	// true → plaintext.
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
	// PlaintextUnkeyedColumns controls how columns absent from ColumnKeys are
	// treated. Default false preserves the legacy uniform footer-key
	// encryption. Setting true makes absent columns plaintext (mixed mode).
	//
	// This field is transitional: it exists to keep the default behavior
	// stable while WithColumnKey and WithColumnKeyMetadata remain in their
	// deprecation window. When those deprecated wrappers are removed, this
	// field and WithPlaintextUnkeyedColumns will be removed in the same
	// release; the natural semantic after removal will be "absence from
	// ColumnKeys means plaintext column."
	PlaintextUnkeyedColumns bool
	// KeyRetriever resolves encryption keys from key metadata at write time.
	// Called for footer and column keys that have no direct key set.
	KeyRetriever KeyRetriever
}

// WithEncryption enables Parquet modular encryption for writer output.
func WithEncryption(config EncryptionConfig) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		pw.encryptionConfig = &config
	})
}

// WithFooterKey sets the footer encryption key and optional key metadata.
func WithFooterKey(key []byte, keyMetadata ...[]byte) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.FooterKey = append(config.FooterKey[:0], key...)
		if len(keyMetadata) > 0 {
			config.FooterKeyMetadata = append(config.FooterKeyMetadata[:0], keyMetadata[0]...)
		}
	})
}

// WithFooterKeyMetadata sets footer key metadata. WithKeyRetriever resolves the
// footer key from this metadata and takes priority when it returns a non-empty
// key.
func WithFooterKeyMetadata(keyMetadata []byte) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.FooterKeyMetadata = append(config.FooterKeyMetadata[:0], keyMetadata...)
	})
}

// WithKeyRetriever sets a callback for resolving encryption keys from Parquet
// key_metadata fields while writing. Explicit keys set via WithFooterKey or
// WithColumnEncrypted(ColumnKey(...)) always take priority; the retriever is
// only called when no direct key is provided.
func WithKeyRetriever(retriever KeyRetriever) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.KeyRetriever = retriever
	})
}

// WithAADPrefix sets the AAD prefix used for encrypted modules.
func WithAADPrefix(prefix []byte) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.AADPrefix = append(config.AADPrefix[:0], prefix...)
	})
}

// WithAADFileUnique sets the unique file identifier portion of AAD.
func WithAADFileUnique(fileUnique []byte) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.AADFileUnique = append(config.AADFileUnique[:0], fileUnique...)
	})
}

// WithSupplyAADPrefix controls whether the AAD prefix is stored in file
// metadata. When true, readers must supply the prefix with reader.WithAADPrefix.
func WithSupplyAADPrefix(enabled bool) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.SupplyAADPrefix = enabled
	})
}

// WithPlaintextFooter writes an encrypted file with plaintext footer metadata
// and an AES-GCM footer signature.
func WithPlaintextFooter(enabled bool) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.PlaintextFooter = enabled
	})
}

// WithPlaintextUnkeyedColumns controls whether columns absent from ColumnKeys
// are written as plaintext. Default false preserves the legacy uniform
// footer-key encryption for any column not listed in ColumnKeys. Setting true
// enables mixed mode where absent columns are plaintext.
//
// This option is transitional: it will be removed in the same release that
// removes the deprecated WithColumnKey and WithColumnKeyMetadata. After
// removal, the natural semantic becomes "absence from ColumnKeys means
// plaintext column."
func WithPlaintextUnkeyedColumns(enabled bool) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.PlaintextUnkeyedColumns = enabled
	})
}

// WithEncryptionAlgorithm sets the modular encryption algorithm.
func WithEncryptionAlgorithm(algorithm EncryptionAlgorithm) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		config := pw.ensureEncryptionConfig()
		config.Algorithm = algorithm
	})
}

func (pw *ParquetWriter) ensureEncryptionConfig() *EncryptionConfig {
	if pw.encryptionConfig == nil {
		pw.encryptionConfig = &EncryptionConfig{}
	}
	return pw.encryptionConfig
}

func (pw *ParquetWriter) recordOptionError(err error) {
	pw.optionErrors = append(pw.optionErrors, err)
}
