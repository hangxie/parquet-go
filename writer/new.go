package writer

import (
	"context"
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/marshal"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

// columnCompressorKey uniquely identifies a (codec, level) combination for sharing compressor instances.
type columnCompressorKey struct {
	codec parquet.CompressionCodec
	level int
}

// NewParquetWriterFromWriter creates a ParquetWriter from an io.Writer.
//
// Deprecated: use NewParquetWriterFromWriterWithContext.
func NewParquetWriterFromWriter(w io.Writer, obj any, opts ...WriterOption) (*ParquetWriter, error) {
	return NewParquetWriterFromWriterWithContext(context.Background(), w, obj, opts...)
}

// NewParquetWriterFromWriterWithContext creates a ParquetWriter from an
// io.Writer and uses ctx for initialization.
func NewParquetWriterFromWriterWithContext(ctx context.Context, w io.Writer, obj any, opts ...WriterOption) (*ParquetWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewParquetWriterWithContext(ctx, wf, obj, opts...)
}

// initBase sets up the ParquetWriter with defaults and applies and validates
// constructor options. It performs no IO so constructors can finish schema and
// schema-dependent validation before writing the magic header.
//
// Callers must still set SchemaHandler and marshalFunc, finish constructor
// validation and initialization, call writeMagicHeader, and set stopped to
// false.
func (pw *ParquetWriter) initBase(ctx context.Context, pFile source.ParquetFileWriter, opts ...WriterOption) error {
	pw.defaultCtx = ctx
	if err := pw.setContext(ctx); err != nil {
		return err
	}
	pw.np = 4                                    // default parallel number
	pw.pageSize = common.DefaultPageSize         // 8K
	pw.rowGroupSize = common.DefaultRowGroupSize // 128M
	pw.maxDictionarySize = DefaultMaxDictionarySize
	pw.binaryMinMaxTruncateLength = 0
	pw.binaryMinMaxTruncateLengthSet = false
	pw.compressionType = parquet.CompressionCodec_SNAPPY
	pw.compressionLevels = nil
	pw.compressor = nil
	pw.dataPageVersion = 1 // default to DATA_PAGE (V1)
	pw.offset = 4
	pw.valueOptions = nil
	pw.encryptionConfig = nil
	pw.encryptionState = nil
	pw.optionErrors = nil
	pw.PFile = pFile
	pw.pagesMapBuf = make(map[string][]*layout.Page)
	// DictRecs sync.Map zero value is ready to use
	pw.Footer = parquet.NewFileMetaData()
	pw.Footer.Version = 1
	pw.columnIndexes = make([]*parquet.ColumnIndex, 0)
	pw.offsetIndexes = make([]*parquet.OffsetIndex, 0)
	// include the createdBy to avoid
	// WARN  CorruptStatistics:118 - Ignoring statistics because created_by is null or empty! See PARQUET-251 and PARQUET-297
	pw.Footer.CreatedBy = common.ToPtr("github.com/hangxie/parquet-go/v3")
	// marshalFunc must be set by the caller (NewParquetWriter, NewCSVWriter, etc.)
	// after initBase returns. Each writer type uses a different marshal function.
	// stopped starts true so that a partially-constructed writer (e.g. after
	// schema failure) rejects Write calls rather than panicking.
	pw.stopped = true

	// Apply constructor options.
	for _, opt := range opts {
		opt.apply(pw)
	}

	// Surface any errors recorded by options before IO so partial output is
	// impossible. Option-time errors carry full path/argument context.
	if len(pw.optionErrors) > 0 {
		return fmt.Errorf("invalid writer options: %w", optionErrorList(pw.optionErrors))
	}

	// Validate options before any IO to avoid partial writes on invalid input.
	if pw.np <= 0 {
		return fmt.Errorf("WithNP: value must be positive, got %d", pw.np)
	}
	if pw.pageSize <= 0 {
		return fmt.Errorf("WithPageSize: value must be positive, got %d", pw.pageSize)
	}
	if pw.rowGroupSize <= 0 {
		return fmt.Errorf("WithRowGroupSize: value must be positive, got %d", pw.rowGroupSize)
	}
	if pw.maxDictionarySize <= 0 {
		return fmt.Errorf("WithMaxDictionarySize: value must be positive, got %d", pw.maxDictionarySize)
	}
	if pw.binaryMinMaxTruncateLengthSet && pw.binaryMinMaxTruncateLength <= 0 {
		return fmt.Errorf("WithBinaryMinMaxTruncateLength: value must be positive, got %d", pw.binaryMinMaxTruncateLength)
	}
	if pw.dataPageVersion != 1 && pw.dataPageVersion != 2 {
		return fmt.Errorf("WithDataPageVersion: value must be 1 or 2, got %d", pw.dataPageVersion)
	}
	if len(pw.compressionLevels) > 0 {
		opts := make([]compress.CompressorOption, 0, len(pw.compressionLevels))
		for codec, level := range pw.compressionLevels {
			opts = append(opts, compress.WithCompressionLevel(codec, level))
		}
		c, err := compress.NewCompressor(opts...)
		if err != nil {
			return fmt.Errorf("WithCompressionLevel: %w", err)
		}
		pw.compressor = c
	}
	if pw.encryptionConfig != nil {
		state, err := newEncryptionState(*pw.encryptionConfig)
		if err != nil {
			return fmt.Errorf("init encryption state: %w", err)
		}
		pw.encryptionState = state
	}

	return nil
}

func (pw *ParquetWriter) writeMagicHeader() error {
	magic := common.MagicBytes
	if pw.encryptionState != nil && !pw.encryptionState.plaintextFooter {
		magic = common.MagicBytesEncrypted
	}
	_, err := pw.write([]byte(magic))
	return err
}

// buildColumnCompressors creates per-column compressors for columns that specify
// an explicit compression level in their tag (e.g. compression=ZSTD:3).
// Columns sharing the same (codec, level) pair reuse the same compressor instance.
func (pw *ParquetWriter) buildColumnCompressors() error {
	pw.columnCompressors = nil
	if pw.SchemaHandler == nil {
		return nil
	}

	shared := make(map[columnCompressorKey]*compress.Compressor)
	for name, idx := range pw.SchemaHandler.MapIndex {
		info := pw.SchemaHandler.Infos[idx]
		if info == nil || info.CompressionCodec == nil || info.CompressionLevel == nil {
			continue
		}
		k := columnCompressorKey{*info.CompressionCodec, *info.CompressionLevel}
		if _, exists := shared[k]; !exists {
			c, err := compress.NewCompressor(compress.WithCompressionLevel(k.codec, k.level))
			if err != nil {
				return fmt.Errorf("build column compressor for %s level %d: %w", k.codec, k.level, err)
			}
			shared[k] = c
		}
		if pw.columnCompressors == nil {
			pw.columnCompressors = make(map[string]*compress.Compressor)
		}
		k2 := columnCompressorKey{*info.CompressionCodec, *info.CompressionLevel}
		pw.columnCompressors[name] = shared[k2]
	}
	return nil
}

func (pw *ParquetWriter) compressorForColumn(name string) *compress.Compressor {
	if pw.columnCompressors != nil {
		if c, ok := pw.columnCompressors[name]; ok {
			return c
		}
	}
	return pw.compressor
}

// NewParquetWriter creates a parquet writer. Obj is an object with tags or a JSON schema string.
//
// Deprecated: use NewParquetWriterWithContext.
func NewParquetWriter(pFile source.ParquetFileWriter, obj any, opts ...WriterOption) (*ParquetWriter, error) {
	return NewParquetWriterWithContext(context.Background(), pFile, obj, opts...)
}

// NewParquetWriterWithContext creates a parquet writer and uses ctx for
// initialization, including writing the magic header.
func NewParquetWriterWithContext(ctx context.Context, pFile source.ParquetFileWriter, obj any, opts ...WriterOption) (*ParquetWriter, error) {
	res := new(ParquetWriter)
	if err := res.initBase(ctx, pFile, opts...); err != nil {
		return nil, fmt.Errorf("init writer base: %w", err)
	}
	res.marshalFunc = marshal.Marshal
	sortingColumnsValidated := false

	if obj != nil {
		if sa, ok := obj.(string); ok {
			// SetSchemaHandlerFromJSON handles Footer.Schema internally
			if err := res.SetSchemaHandlerFromJSON(sa); err != nil {
				return nil, fmt.Errorf("set schema from JSON: %w", err)
			}
			sortingColumnsValidated = true
		} else {
			var err error
			if sa, ok := obj.(*schema.SchemaHandler); ok {
				res.SchemaHandler = schema.NewSchemaHandlerFromSchemaHandler(sa)
			} else if sa, ok := obj.([]*parquet.SchemaElement); ok {
				res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(sa)
			} else {
				if res.SchemaHandler, err = schema.NewSchemaHandlerFromStruct(obj); err != nil {
					return nil, fmt.Errorf("build schema handler: %w", err)
				}
			}
			res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
		}
	}

	if err := res.validateSchemaForWrite(); err != nil {
		return nil, fmt.Errorf("validate schema: %w", err)
	}
	if err := res.buildColumnCompressors(); err != nil {
		return nil, fmt.Errorf("build column compressors: %w", err)
	}
	if !sortingColumnsValidated {
		if err := res.validateSortingColumns(); err != nil {
			return nil, fmt.Errorf("validate sorting columns: %w", err)
		}
	}
	if err := res.initBloomFilters(); err != nil {
		return nil, fmt.Errorf("init bloom filters: %w", err)
	}
	if err := res.validateEncryptionColumnKeys(); err != nil {
		return nil, fmt.Errorf("validate encryption column keys: %w", err)
	}
	if err := res.writeMagicHeader(); err != nil {
		return nil, fmt.Errorf("write magic header: %w", err)
	}

	// Enable writing after init completed successfully
	res.stopped = false

	return res, nil
}
