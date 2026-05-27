package reader

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
)

// ReaderOption configures a ParquetReader when passed to constructors such as
// NewParquetReader or NewParquetColumnReader. ReaderOption values are opaque;
// callers should not use them to mutate an already-created reader.
type ReaderOption interface {
	apply(*ParquetReader)
}

type readerOptionFunc func(*ParquetReader)

func (fn readerOptionFunc) apply(pr *ParquetReader) {
	fn(pr)
}

// KeyRetriever resolves a Parquet encryption key from file key metadata.
type KeyRetriever func(keyMetadata []byte) ([]byte, error)

// WithNP sets the number of goroutines for parallel processing. Default is 4.
func WithNP(np int64) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) { pr.np = np })
}

// WithCaseInsensitive enables case-insensitive schema matching.
func WithCaseInsensitive(enabled bool) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) { pr.caseInsensitive = enabled })
}

// WithCRCMode sets the CRC validation mode when reading pages.
func WithCRCMode(mode common.CRCMode) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) { pr.crcMode = mode })
}

// WithFooterKey sets the key used to decrypt encrypted footers or verify
// plaintext-footer signatures when no key retriever is configured.
func WithFooterKey(key []byte) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) {
		pr.footerKey = append(pr.footerKey[:0], key...)
	})
}

// WithKeyRetriever sets a callback for resolving encryption keys from Parquet
// key_metadata fields.
func WithKeyRetriever(retriever KeyRetriever) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) {
		pr.keyRetriever = retriever
	})
}

// WithAADPrefix supplies an external AAD prefix for files whose encryption
// algorithm requires readers to supply it.
func WithAADPrefix(prefix []byte) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) {
		pr.aadPrefix = append(pr.aadPrefix[:0], prefix...)
	})
}

// WithColumnKey sets the key used to decrypt a column. The path is the
// dot-separated schema path without the root element.
func WithColumnKey(path string, key []byte) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) {
		if pr.columnKeys == nil {
			pr.columnKeys = make(map[string][]byte)
		}
		pr.columnKeys[common.ReformPathStr(path)] = append([]byte(nil), key...)
	})
}

// applyReaderDefaults sets defaults, applies constructor options, and validates them.
func applyReaderDefaults(pr *ParquetReader, opts []ReaderOption) error {
	pr.np = 4 // default parallel number

	for _, opt := range opts {
		opt.apply(pr)
	}

	if pr.np <= 0 {
		return fmt.Errorf("WithNP: value must be positive, got %d", pr.np)
	}
	switch pr.crcMode {
	case common.CRCIgnore, common.CRCAuto, common.CRCStrict:
		// valid
	default:
		return fmt.Errorf("WithCRCMode: unsupported mode %d", pr.crcMode)
	}
	return nil
}
