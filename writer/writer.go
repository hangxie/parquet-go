package writer

import (
	"context"
	"fmt"
	"sync"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/bloomfilter"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/types"
)

// ParquetWriter writes parquet files.
//
// A ParquetWriter must not be used by multiple goroutines concurrently.
// Callers must serialize all operations on an instance, including Write,
// Flush, WriteStop, and their context-aware variants. WithNP controls internal
// parallelism and does not make concurrent method calls safe.
type ParquetWriter struct {
	SchemaHandler *schema.SchemaHandler
	Footer        *parquet.FileMetaData
	PFile         source.ParquetFileWriter

	np                            int64 // parallel number
	pageSize                      int64
	rowGroupSize                  int64
	maxDictionarySize             int64
	binaryMinMaxTruncateLength    int
	binaryMinMaxTruncateLengthSet bool
	compressionType               parquet.CompressionCodec
	compressionLevels             map[parquet.CompressionCodec]int
	compressor                    *compress.Compressor
	columnCompressors             map[string]*compress.Compressor
	dataPageVersion               int32 // 1 for DATA_PAGE (default), 2 for DATA_PAGE_V2
	writeCRC                      bool  // compute and write CRC32 checksums on pages (default false)
	valueOptions                  []types.ValueOption
	sortingColumns                []*parquet.SortingColumn
	encryptionConfig              *EncryptionConfig
	encryptionState               *encryptionState
	optionErrors                  []error
	offset                        int64

	objs              []any
	objsSize          int64
	objSize           int64
	checkSizeCritical int64

	pagesMapBuf map[string][]*layout.Page
	size        int64
	numRows     int64

	// DictRecs stores dictionary recorders per column path
	// key: path string, value: *layout.DictRecType
	DictRecs sync.Map

	columnIndexes []*parquet.ColumnIndex
	offsetIndexes []*parquet.OffsetIndex

	// bloomFilters holds the active bloom filters being built for the current row group.
	// Key is the column path string (e.g. common.ParGoRootInName + "\x01Name").
	bloomFilters map[string]*bloomfilter.Filter
	// bloomFilterData holds serialized (header+bitset) per column chunk, parallel to ColumnIndexes/OffsetIndexes.
	bloomFilterData [][]byte

	marshalFunc func(src []any, sh *schema.SchemaHandler) (*map[string]*layout.Table, error)

	stopped            bool
	stopErr            error
	encodingsValidated bool // tracks if encoding/version validation has been done
	defaultCtx         context.Context
	ctx                context.Context
}

func (pw *ParquetWriter) context() context.Context {
	if pw.ctx == nil {
		return context.Background()
	}
	return pw.ctx
}

func (pw *ParquetWriter) defaultContext() context.Context {
	if pw.defaultCtx == nil {
		return context.Background()
	}
	return pw.defaultCtx
}

func (pw *ParquetWriter) setContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pw.ctx = ctx
	return nil
}

func (pw *ParquetWriter) write(p []byte) (int, error) {
	return source.WriteWithContext(pw.context(), pw.PFile, p)
}

func (pw *ParquetWriter) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error
	if pw.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return fmt.Errorf("parse JSON schema: %w", err)
	}
	pw.Footer.Schema = pw.Footer.Schema[:0]
	pw.Footer.Schema = append(pw.Footer.Schema, pw.SchemaHandler.SchemaElements...)
	if err := pw.buildColumnCompressors(); err != nil {
		return fmt.Errorf("build column compressors: %w", err)
	}
	if err := pw.validateSortingColumns(); err != nil {
		return fmt.Errorf("validate sorting columns: %w", err)
	}
	if err := pw.initBloomFilters(); err != nil {
		return fmt.Errorf("init bloom filters: %w", err)
	}
	if err := pw.validateEncryptionColumnKeys(); err != nil {
		return fmt.Errorf("validate encryption column keys: %w", err)
	}
	pw.encodingsValidated = false
	return nil
}

// Rename schema name to exname in tags
func (pw *ParquetWriter) RenameSchema() {
	for i := range len(pw.Footer.Schema) {
		pw.Footer.Schema[i].Name = pw.SchemaHandler.Infos[i].ExName
	}
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, chunk := range rowGroup.Columns {
			inPathStr := common.PathToStr(chunk.MetaData.PathInSchema)
			exPathStr := pw.SchemaHandler.InPathToExPath[inPathStr]
			exPath := common.StrToPath(exPathStr)[1:]
			chunk.MetaData.PathInSchema = exPath
		}
	}
}
