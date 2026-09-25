package reader

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// ErrColumnKeyRequired reports encrypted data, either a column or the file
// footer, with no configured or retrievable key. Match with errors.Is.
var ErrColumnKeyRequired = errors.New("decryption key required")

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
	return nil, fmt.Errorf("%w for footer", ErrColumnKeyRequired)
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
	pathInSchema := columnKeyMeta.GetPathInSchema()
	if key, ok := pr.lookupColumnKey(pathInSchema); ok {
		return key, nil
	}
	displayPath := strings.ReplaceAll(pr.fullExternalColumnPath(pathInSchema), common.ParGoPathDelimiter, ".")
	if pr.keyRetriever != nil {
		key, err := pr.retrieveKeyFromMetadata(columnKeyMeta.GetKeyMetadata())
		if err != nil {
			return nil, fmt.Errorf("retrieve column key for %s: %w", displayPath, err)
		}
		if len(key) > 0 {
			return key, nil
		}
	}
	return nil, fmt.Errorf("%w for column %s", ErrColumnKeyRequired, displayPath)
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
	if key, ok := pr.lookupColumnKey(columnKeyMeta.GetPathInSchema()); ok {
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
