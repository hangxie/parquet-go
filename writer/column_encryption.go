package writer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
)

// ColumnEncryptionOption selects how a column is encrypted by
// WithColumnEncrypted. Construct values with ColumnFooterKey, ColumnKey, or
// ColumnKeyByMetadata; third-party packages cannot supply their own
// implementations.
type ColumnEncryptionOption interface {
	columnEncryption() columnEncryptionSpec
}

type columnSubOptionKind int

const (
	columnKindFooterKey columnSubOptionKind = iota
	columnKindLiteralKey
	columnKindByMetadata
)

// columnEncryptionSpec is the internal representation of a single
// ColumnEncryptionOption call. err captures eager validation errors so
// WithColumnEncrypted can surface them with the path attached.
type columnEncryptionSpec struct {
	kind        columnSubOptionKind
	key         []byte
	keyMetadata []byte
	err         error
}

func (c columnEncryptionSpec) columnEncryption() columnEncryptionSpec { return c }

// ColumnFooterKey selects ENCRYPTION_WITH_FOOTER_KEY for the column.
// Equivalent to passing no sub-options to WithColumnEncrypted; provided for
// callers that prefer to spell the intent out explicitly.
func ColumnFooterKey() ColumnEncryptionOption {
	return columnEncryptionSpec{kind: columnKindFooterKey}
}

// ColumnKey selects ENCRYPTION_WITH_COLUMN_KEY with the literal key. The
// optional keyMetadata argument is stored in the file's
// EncryptionWithColumnKey.KeyMetadata so downstream readers' KeyRetriever
// can resolve the same key from a KMS. The writer does not invoke its own
// retriever in this form.
//
// Passing nil for key is rejected: use ColumnFooterKey or omit options for
// footer-key encryption. Passing more than one keyMetadata element is also
// rejected.
func ColumnKey(key []byte, keyMetadata ...[]byte) ColumnEncryptionOption {
	spec := columnEncryptionSpec{kind: columnKindLiteralKey}
	if key == nil {
		spec.err = errors.New("ColumnKey: key must not be nil; use ColumnFooterKey or omit options for footer-key encryption")
		return spec
	}
	if len(keyMetadata) > 1 {
		spec.err = fmt.Errorf("ColumnKey: at most one keyMetadata argument accepted, got %d", len(keyMetadata))
		return spec
	}
	spec.key = append([]byte(nil), key...)
	if len(keyMetadata) == 1 {
		spec.keyMetadata = append([]byte(nil), keyMetadata[0]...)
	}
	return spec
}

// ColumnKeyByMetadata selects ENCRYPTION_WITH_COLUMN_KEY where the writer
// resolves the column key by calling its configured KeyRetriever with
// keyMetadata at write time. An empty retriever result is an error, never
// a silent fallback to footer-key encryption.
//
// keyMetadata must be non-empty; pass it via WithKeyRetriever to authorize
// the resolver. ColumnKeyByMetadata without a configured KeyRetriever is
// rejected at writer construction time.
func ColumnKeyByMetadata(keyMetadata []byte) ColumnEncryptionOption {
	spec := columnEncryptionSpec{kind: columnKindByMetadata}
	if len(keyMetadata) == 0 {
		spec.err = errors.New("ColumnKeyByMetadata: keyMetadata must be non-empty")
		return spec
	}
	spec.keyMetadata = append([]byte(nil), keyMetadata...)
	return spec
}

// WithColumnEncrypted enables per-column encryption for path. The path is
// rootless, dot-separated, and matched against external Parquet names. At most
// one sub-option is accepted. With no sub-options, the column uses the footer key.
//
// Repeated calls for the same path follow standard Go map semantics: the
// last call wins. No conflict detection runs.
func WithColumnEncrypted(path string, opts ...ColumnEncryptionOption) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		if strings.TrimSpace(path) == "" {
			pw.recordOptionError(errors.New("WithColumnEncrypted: path must be non-empty"))
			return
		}
		config := pw.ensureEncryptionConfig()
		if config.ColumnKeys == nil {
			config.ColumnKeys = make(map[string]EncryptionColumnKey)
		}
		normPath := common.ReformPathStr(path)

		var spec columnEncryptionSpec
		switch len(opts) {
		case 0:
			spec = columnEncryptionSpec{kind: columnKindFooterKey}
		case 1:
			spec = opts[0].columnEncryption()
		default:
			pw.recordOptionError(fmt.Errorf("WithColumnEncrypted %q: at most one sub-option allowed, got %d", path, len(opts)))
			return
		}
		if spec.err != nil {
			pw.recordOptionError(fmt.Errorf("WithColumnEncrypted %q: %w", path, spec.err))
			return
		}

		var entry EncryptionColumnKey
		switch spec.kind {
		case columnKindFooterKey:
			// zero entry - footer-key sentinel
		case columnKindLiteralKey:
			entry.Key = spec.key
			entry.KeyMetadata = spec.keyMetadata
		case columnKindByMetadata:
			entry.KeyMetadata = spec.keyMetadata
			// Key intentionally nil - signals retrieve via KeyRetriever.
		}
		config.ColumnKeys[normPath] = entry
	})
}
