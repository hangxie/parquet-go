package writer

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/types"
)

// DefaultMaxDictionarySize is the default encoded dictionary value-byte limit.
const DefaultMaxDictionarySize = 1024 * 1024

// WriterOption configures a ParquetWriter when passed to constructors such as
// NewParquetWriter. WriterOption values are opaque; callers should not use them
// to mutate an already-created writer.
type WriterOption interface {
	apply(*ParquetWriter)
}

type writerOptionFunc func(*ParquetWriter)

func (fn writerOptionFunc) apply(pw *ParquetWriter) {
	fn(pw)
}

// WithNP sets the number of goroutines for parallel processing. Default is 4.
func WithNP(np int64) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) { pw.np = np })
}

// WithPageSize sets the page size in bytes. Default is 8KB.
func WithPageSize(size int64) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) { pw.pageSize = size })
}

// WithRowGroupSize sets the row group size in bytes. Default is 128MB.
func WithRowGroupSize(size int64) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) { pw.rowGroupSize = size })
}

// WithMaxDictionarySize limits encoded dictionary value bytes per column and row group.
func WithMaxDictionarySize(size int64) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) { pw.maxDictionarySize = size })
}

// WithBinaryMinMaxTruncateLength sets the target byte limit for unannotated
// BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY and STRING/UTF8 statistics and column-index
// bounds. By default, bounds are not truncated. STRING/UTF8 bounds may exceed
// the target when they cannot be shortened to a valid UTF-8 bound. Other
// annotated types remain untruncated.
func WithBinaryMinMaxTruncateLength(length int) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		pw.binaryMinMaxTruncateLength = length
		pw.binaryMinMaxTruncateLengthSet = true
	})
}

// WithCompressionCodec sets the compression codec. Default is SNAPPY.
func WithCompressionCodec(ct parquet.CompressionCodec) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) { pw.compressionType = ct })
}

// WithCompressionLevel sets the compression level for a specific codec.
// Not all codecs support compression levels; invalid codecs or levels are
// reported when constructing the writer.
func WithCompressionLevel(codec parquet.CompressionCodec, level int) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		if pw.compressionLevels == nil {
			pw.compressionLevels = make(map[parquet.CompressionCodec]int)
		}
		pw.compressionLevels[codec] = level
	})
}

// WithDataPageVersion sets the data page version (1 or 2). Default is 1.
func WithDataPageVersion(v int32) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) { pw.dataPageVersion = v })
}

// WithWriteCRC enables or disables CRC32 page checksums. Default is false.
func WithWriteCRC(enabled bool) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) { pw.writeCRC = enabled })
}

// WithValueMode selects how JSONWriter and CSVWriter read logical values: interpreted
// (the default) expects canonical text, raw the physical value as base64 where byte-backed.
// ParquetWriter is unaffected, its values already being typed.
func WithValueMode(m types.ValueMode) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		if !m.IsValid() {
			pw.optionErrors = append(pw.optionErrors, fmt.Errorf("WithValueMode: %w %d", types.ErrUnsupportedValueMode, int(m)))
			return
		}
		pw.valueOptions = append(pw.valueOptions, types.WithValueMode(m))
	})
}

// WithEnforceUTF8 enables UTF-8 validation in JSONWriter and CSVWriter value conversion.
// JSONWriter validates the complete JSON document, including unmapped fields and object
// names. Typed ParquetWriter input is unaffected and must be validated by its caller.
func WithEnforceUTF8(enabled bool) WriterOption {
	return writerOptionFunc(func(pw *ParquetWriter) {
		pw.valueOptions = append(pw.valueOptions, types.WithEnforceUTF8(enabled))
	})
}

func formatOptionErrors(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	return strings.Join(parts, "; ")
}

// optionErrorList reports the option errors as one message, each still matchable.
type optionErrorList []error

func (e optionErrorList) Error() string { return formatOptionErrors(e) }

func (e optionErrorList) Unwrap() []error { return e }
