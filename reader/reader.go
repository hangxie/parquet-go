package reader

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
)

// ParquetReader is a reader for parquet files.
type ParquetReader struct {
	SchemaHandler *schema.SchemaHandler
	np            int64 // parallel number
	Footer        *parquet.FileMetaData
	PFile         source.ParquetFileReader
	FileCrypto    *parquet.FileCryptoMetaData

	ColumnBuffers map[string]*ColumnBufferType

	// One reader can only read one type objects
	ObjType        reflect.Type
	ObjPartialType reflect.Type

	caseInsensitive   bool           // case-insensitive schema matching
	crcMode           common.CRCMode // CRC validation when reading pages
	footerKey         []byte
	resolvedFooterKey []byte
	aadPrefix         []byte
	keyRetriever      KeyRetriever
	columnKeys        map[string][]byte
}

// NewParquetReader creates a parquet reader. obj is an object with schema tags or a JSON schema string.
func NewParquetReader(pFile source.ParquetFileReader, obj any, opts ...ReaderOption) (*ParquetReader, error) {
	var err error
	res := new(ParquetReader)
	res.PFile = pFile

	if err = applyReaderDefaults(res, opts); err != nil {
		return nil, err
	}
	if err = res.ReadFooter(); err != nil {
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

	res.RenameSchema()
	for i := range len(res.SchemaHandler.SchemaElements) {
		schema := res.SchemaHandler.SchemaElements[i]
		if schema == nil {
			continue
		}
		if schema.GetNumChildren() == 0 {
			if pathStr, exists := res.SchemaHandler.IndexMap[int32(i)]; exists {
				if res.ColumnBuffers[pathStr], err = NewColumnBuffer(pFile, res.Footer, res.SchemaHandler, pathStr, &layout.PageReadOptions{CRCMode: res.crcMode, MaxPageSize: layout.DefaultMaxPageSize}); err != nil {
					return res, fmt.Errorf("init column buffer for %s: %w", pathStr, err)
				}
				res.ColumnBuffers[pathStr].Reader = res
				if err := res.reconfigureDecryptorForBuffer(res.ColumnBuffers[pathStr]); err != nil {
					return res, fmt.Errorf("configure page decryptor for %s: %w", pathStr, err)
				}
			}
		}
	}

	res.detectBloomFilters()
	return res, nil
}

func (pr *ParquetReader) SetSchemaHandlerFromJSON(jsonSchema string) error {
	var err error

	if pr.SchemaHandler, err = schema.NewSchemaHandlerFromJSON(jsonSchema); err != nil {
		return fmt.Errorf("parse JSON schema: %w", err)
	}

	pr.RenameSchema()
	for i := range len(pr.SchemaHandler.SchemaElements) {
		schemaElement := pr.SchemaHandler.SchemaElements[i]
		if schemaElement.GetNumChildren() == 0 {
			pathStr := pr.SchemaHandler.IndexMap[int32(i)]
			if pr.ColumnBuffers[pathStr], err = NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr, &layout.PageReadOptions{CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize}); err != nil {
				return fmt.Errorf("init column buffer for %s: %w", pathStr, err)
			}
			pr.ColumnBuffers[pathStr].Reader = pr
			if err := pr.reconfigureDecryptorForBuffer(pr.ColumnBuffers[pathStr]); err != nil {
				return fmt.Errorf("configure page decryptor for %s: %w", pathStr, err)
			}
		}
	}
	pr.detectBloomFilters()
	return nil
}

func (pr *ParquetReader) newColumnBuffer(pathStr string) (*ColumnBufferType, error) {
	cb, err := NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr, &layout.PageReadOptions{CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize})
	if err != nil {
		return nil, err
	}
	cb.Reader = pr
	if err := pr.reconfigureDecryptorForBuffer(cb); err != nil {
		_ = cb.PFile.Close()
		return nil, err
	}
	return cb, nil
}

// Rename schema name to inname
func (pr *ParquetReader) RenameSchema() {
	if pr.SchemaHandler == nil || pr.SchemaHandler.Infos == nil || pr.Footer == nil || pr.Footer.Schema == nil {
		return
	}

	for i := range len(pr.SchemaHandler.Infos) {
		if i < len(pr.Footer.Schema) && pr.Footer.Schema[i] != nil && pr.SchemaHandler.Infos[i] != nil {
			pr.Footer.Schema[i].Name = pr.SchemaHandler.Infos[i].InName
		}
	}

	exPathToInPath := make(map[string]string)
	if pr.caseInsensitive {
		for exPath, inPath := range pr.SchemaHandler.ExPathToInPath {
			lowerCaseKey := strings.ToLower(exPath)
			exPathToInPath[lowerCaseKey] = inPath
		}
	} else {
		exPathToInPath = pr.SchemaHandler.ExPathToInPath
	}

	if pr.Footer.RowGroups == nil {
		return
	}

	for _, rowGroup := range pr.Footer.RowGroups {
		if rowGroup == nil || rowGroup.Columns == nil {
			continue
		}
		for _, chunk := range rowGroup.Columns {
			if chunk == nil || chunk.MetaData == nil {
				continue
			}
			exPath := append([]string{pr.SchemaHandler.GetRootExName()}, chunk.MetaData.GetPathInSchema()...)
			exPathStr := common.PathToStr(exPath)

			if pr.caseInsensitive {
				exPathStr = strings.ToLower(exPathStr)
			}

			if inPathStr, exists := exPathToInPath[exPathStr]; exists {
				inPath := common.StrToPath(inPathStr)[1:]
				chunk.MetaData.PathInSchema = inPath
			}
		}
	}
}

func (pr *ParquetReader) GetNumRows() int64 {
	return pr.Footer.GetNumRows()
}

// Get the footer size
func (pr *ParquetReader) GetFooterSize() (uint32, error) {
	if pr.PFile == nil {
		return 0, fmt.Errorf("PFile is nil")
	}

	var err error
	buf := make([]byte, 4)
	if _, err = pr.PFile.Seek(-8, io.SeekEnd); err != nil {
		return 0, err
	}
	if _, err = io.ReadFull(pr.PFile, buf); err != nil {
		return 0, err
	}
	size := binary.LittleEndian.Uint32(buf)
	return size, err
}

func (pr *ParquetReader) getFooterTail() (uint32, string, error) {
	if pr.PFile == nil {
		return 0, "", fmt.Errorf("PFile is nil")
	}

	buf := make([]byte, 8)
	if _, err := pr.PFile.Seek(-8, io.SeekEnd); err != nil {
		return 0, "", err
	}
	if _, err := io.ReadFull(pr.PFile, buf); err != nil {
		return 0, "", err
	}
	return binary.LittleEndian.Uint32(buf[:4]), string(buf[4:]), nil
}

// Read footer from parquet file
func (pr *ParquetReader) ReadFooter() error {
	size, magic, err := pr.getFooterTail()
	if err != nil {
		return fmt.Errorf("get footer tail: %w", err)
	}
	switch magic {
	case "PARE":
		if err := pr.readEncryptedFooter(size); err != nil {
			return err
		}
	default:
		if err := pr.readPlainFooter(size); err != nil {
			return err
		}
	}
	if err := pr.decryptEncryptedColumnMetadata(); err != nil {
		return fmt.Errorf("decrypt encrypted column metadata: %w", err)
	}
	return nil
}

func (pr *ParquetReader) readPlainFooter(size uint32) error {
	if _, err := pr.PFile.Seek(-int64(8+size), io.SeekEnd); err != nil {
		return fmt.Errorf("seek to footer: %w", err)
	}
	pr.Footer = parquet.NewFileMetaData()
	pf := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{})
	thriftReader := thrift.NewStreamTransportR(pr.PFile)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(size))
	protocol := pf.GetProtocol(bufferReader)
	if err := pr.Footer.Read(context.TODO(), protocol); err != nil {
		return fmt.Errorf("read footer: %w", err)
	}

	if pr.Footer.IsSetEncryptionAlgorithm() {
		if _, err := pr.PFile.Seek(-int64(8+size), io.SeekEnd); err != nil {
			return fmt.Errorf("seek to plaintext footer section: %w", err)
		}
		section := make([]byte, size)
		if _, err := io.ReadFull(pr.PFile, section); err != nil {
			return fmt.Errorf("read plaintext footer section: %w", err)
		}
		if err := pr.verifyPlaintextFooter(section); err != nil {
			return err
		}
	}
	return nil
}
