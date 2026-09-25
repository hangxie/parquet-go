package reader

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
)

// ParquetReader reads parquet files.
//
// A ParquetReader must not be used by multiple goroutines concurrently.
// Callers must serialize all operations on an instance, including row reads,
// column reads, skips, inspection methods, Reset, ReadStop, and their
// context-aware variants. WithNP controls internal parallelism and does not
// make concurrent method calls safe.
type ParquetReader struct {
	SchemaHandler *schema.SchemaHandler
	// Footer is loaded once by ReadFooter and then treated as immutable for the
	// reader lifetime. Direct mutation or reassignment by callers is unsupported.
	Footer     *parquet.FileMetaData
	PFile      source.ParquetFileReader
	FileCrypto *parquet.FileCryptoMetaData

	ColumnBuffers map[string]*ColumnBufferType

	// One reader can only read one type objects
	ObjType        reflect.Type
	ObjPartialType reflect.Type

	// Reader options.
	np              int64          // parallel number
	caseInsensitive bool           // case-insensitive schema matching
	crcMode         common.CRCMode // CRC validation when reading pages

	// Encryption options.
	footerKey         []byte
	resolvedFooterKey []byte
	aadPrefix         []byte
	keyRetriever      KeyRetriever
	columnKeys        map[string][]byte

	// Lazy runtime state.
	columnKeysFullPath bool
	keyCache           sync.Map
	footerMu           sync.Mutex
	footerLoaded       bool
	defaultCtx         context.Context
	ctx                context.Context

	// encryptedPageOffsets is populated once from the immutable footer and then
	// treated as read-only. It tracks every per-column offset that belongs to an
	// encrypted column: data, dictionary, index, and bloom-filter pages. The set
	// is keyed on CryptoMetadata, which is the authoritative signal that a
	// column's payloads are encrypted. If the footer carries an encrypted column
	// whose MetaData cannot be recovered (no plaintext and no
	// EncryptedColumnMetadata), the build records encryptedPageOffsetsErr and
	// every lookup surfaces that error.
	encryptedPageOffsetOnce sync.Once
	encryptedPageOffsets    map[int64]struct{}
	encryptedPageOffsetsErr error
}

// NewParquetReader creates a parquet reader. obj is an object with schema tags or a JSON schema string.
//
// Deprecated: use NewParquetReaderWithContext.
func NewParquetReader(pFile source.ParquetFileReader, obj any, opts ...ReaderOption) (*ParquetReader, error) {
	return NewParquetReaderWithContext(context.Background(), pFile, obj, opts...)
}

// NewParquetReaderWithContext creates a parquet reader using ctx for footer,
// schema, and column-buffer initialization.
func NewParquetReaderWithContext(ctx context.Context, pFile source.ParquetFileReader, obj any, opts ...ReaderOption) (*ParquetReader, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	var err error
	res := new(ParquetReader)
	res.PFile = pFile
	res.defaultCtx = ctx
	res.ctx = ctx

	if err = applyReaderDefaults(res, opts); err != nil {
		return nil, fmt.Errorf("apply reader options: %w", err)
	}
	if err = res.ReadFooterWithContext(ctx); err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}
	res.ColumnBuffers = make(map[string]*ColumnBufferType)

	if obj != nil {
		if sa, ok := obj.(string); ok {
			err = res.SetSchemaHandlerFromJSON(sa)
			if err != nil {
				return res, fmt.Errorf("set schema from JSON: %w", err)
			}
			return res, nil

		} else if sa, ok := obj.([]*parquet.SchemaElement); ok {
			res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(sa)
		} else {
			if res.SchemaHandler, err = schema.NewSchemaHandlerFromStruct(obj); err != nil {
				return res, fmt.Errorf("build schema handler: %w", err)
			}

			res.ObjType = reflect.TypeOf(obj).Elem()
		}
	} else {
		res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(res.Footer.Schema)
	}

	if err = res.validateColumnKeyPaths(); err != nil {
		return res, err
	}
	for i := range len(res.SchemaHandler.SchemaElements) {
		schema := res.SchemaHandler.SchemaElements[i]
		if schema == nil {
			continue
		}
		if schema.GetNumChildren() == 0 {
			if pathStr, exists := res.SchemaHandler.IndexMap[int32(i)]; exists {
				if res.ColumnBuffers[pathStr], err = res.newColumnBuffer(pathStr); err != nil {
					return res, fmt.Errorf("init column buffer for %s: %w", pathStr, err)
				}
			}
		}
	}

	return res, nil
}

func (pr *ParquetReader) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error

	if pr.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return fmt.Errorf("parse JSON schema: %w", err)
	}

	if err = pr.validateColumnKeyPaths(); err != nil {
		return err
	}

	for i := range len(pr.SchemaHandler.SchemaElements) {
		schemaElement := pr.SchemaHandler.SchemaElements[i]
		if schemaElement.GetNumChildren() == 0 {
			pathStr := pr.SchemaHandler.IndexMap[int32(i)]
			if pr.ColumnBuffers[pathStr], err = pr.newColumnBuffer(pathStr); err != nil {
				return fmt.Errorf("init column buffer for %s: %w", pathStr, err)
			}
		}
	}
	return nil
}

func (pr *ParquetReader) newColumnBuffer(pathStr string) (*ColumnBufferType, error) {
	cb, err := newColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr, &layout.PageReadOptions{Context: pr.context(), CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize}, pr.caseInsensitive)
	if err != nil {
		return nil, fmt.Errorf("new column buffer for %s: %w", pathStr, err)
	}
	cb.Reader = pr
	if err := pr.reconfigureOptionalDecryptorForBuffer(cb); err != nil {
		_ = cb.PFile.Close()
		return nil, fmt.Errorf("configure decryptor for %s: %w", pathStr, err)
	}
	return cb, nil
}
