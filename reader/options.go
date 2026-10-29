package reader

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/layout"
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

// WithColumnKey sets the key used to decrypt a column. The path is rootless and
// matched against external Parquet names in PathInSchema. Path components must be
// separated by common.ParGoPathDelimiter (build it with common.PathToStr); "." is
// treated as an ordinary character in a name. WithCaseInsensitive accepts
// case-only differences.
func WithColumnKey(path string, key []byte) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) {
		if pr.columnKeys == nil {
			pr.columnKeys = make(map[string][]byte)
		}
		pr.columnKeys[path] = append([]byte(nil), key...)
	})
}

// WithMaxPageSize limits each page's stored size; default 256 MB, -1 for none (unsafe for untrusted files).
func WithMaxPageSize(size int64) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) { pr.maxPageSize = size })
}

// WithMaxDecompressedSize limits each page's decoded size; default 256 MB, -1 for none (unsafe for untrusted files).
func WithMaxDecompressedSize(size int64) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) { pr.maxDecompressedSize = size })
}

// WithMaxDecodedBatchBytes limits DELTA byte-array bytes decoded per 1,024 values; default 256 MB, -1 for none.
func WithMaxDecodedBatchBytes(size int64) ReaderOption {
	return readerOptionFunc(func(pr *ParquetReader) { pr.maxDecodedBatchBytes = size })
}

// applyReaderDefaults sets defaults, applies constructor options, and validates them.
func applyReaderDefaults(pr *ParquetReader, opts []ReaderOption) error {
	pr.np = 4 // default parallel number
	pr.maxPageSize = layout.DefaultMaxPageSize
	pr.maxDecompressedSize = compress.DefaultMaxDecompressedSize
	pr.maxDecodedBatchBytes = layout.DefaultMaxDecodedBatchBytes

	for _, opt := range opts {
		opt.apply(pr)
	}

	if pr.np <= 0 {
		return fmt.Errorf("WithNP: value must be positive, got %d", pr.np)
	}
	for _, limit := range []struct {
		name string
		size int64
	}{
		{"WithMaxPageSize", pr.maxPageSize},
		{"WithMaxDecompressedSize", pr.maxDecompressedSize},
		{"WithMaxDecodedBatchBytes", pr.maxDecodedBatchBytes},
	} {
		if limit.size <= 0 && limit.size != layout.NoSizeLimit {
			return fmt.Errorf("%s: value must be positive or -1, got %d", limit.name, limit.size)
		}
	}
	if pr.maxDecompressedSize != compress.DefaultMaxDecompressedSize {
		pr.compressor = compress.DefaultCompressor().WithMaxDecompressedSize(pr.maxDecompressedSize)
	}
	switch pr.crcMode {
	case common.CRCIgnore, common.CRCAuto, common.CRCStrict:
		// valid
	default:
		return fmt.Errorf("WithCRCMode: unsupported mode %d", pr.crcMode)
	}
	return nil
}
