package reader

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// InternalFooter returns a copy of Footer with schema names and column metadata
// paths translated from external Parquet names to internal Go names.
//
// The reader's Footer remains unchanged and continues to expose the metadata as
// stored in the Parquet file.
func (pr *ParquetReader) InternalFooter() (*parquet.FileMetaData, error) {
	if pr == nil || pr.Footer == nil {
		return nil, nil
	}
	footer, err := cloneFileMetaData(pr.Footer)
	if err != nil {
		return nil, fmt.Errorf("clone footer: %w", err)
	}
	renameFooterSchema(pr.SchemaHandler, footer, pr.caseInsensitive)
	return footer, nil
}

// RenameSchema rewrites Footer schema names and column metadata paths from
// external Parquet names to internal Go names. Prefer InternalFooter when a
// converted footer is needed without mutating the reader's file metadata.
//
// Deprecated: use InternalFooter to get converted metadata without mutating
// Footer.
func (pr *ParquetReader) RenameSchema() {
	if pr == nil {
		return
	}
	renameFooterSchema(pr.SchemaHandler, pr.Footer, pr.caseInsensitive)
}

func cloneFileMetaData(footer *parquet.FileMetaData) (*parquet.FileMetaData, error) {
	if footer == nil {
		return nil, nil
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	buf, err := ts.Write(context.TODO(), footer)
	if err != nil {
		return nil, fmt.Errorf("encode file metadata: %w", err)
	}
	return readFileMetaDataFromBytes(buf)
}

func renameFooterSchema(sh *schema.SchemaHandler, footer *parquet.FileMetaData, caseInsensitive bool) {
	if sh == nil || sh.Infos == nil || footer == nil || footer.Schema == nil {
		return
	}

	for i := range len(sh.Infos) {
		if i < len(footer.Schema) && footer.Schema[i] != nil && sh.Infos[i] != nil {
			footer.Schema[i].Name = sh.Infos[i].InName
		}
	}

	exPathToInPath := make(map[string]string)
	if caseInsensitive {
		for exPath, inPath := range sh.ExPathToInPath {
			exPathToInPath[strings.ToLower(exPath)] = inPath
		}
	} else {
		exPathToInPath = sh.ExPathToInPath
	}

	if footer.RowGroups == nil {
		return
	}

	for _, rowGroup := range footer.RowGroups {
		if rowGroup == nil || rowGroup.Columns == nil {
			continue
		}
		for _, chunk := range rowGroup.Columns {
			if chunk == nil || chunk.MetaData == nil {
				continue
			}
			exPath := append([]string{sh.GetRootExName()}, chunk.MetaData.GetPathInSchema()...)
			exPathStr := common.PathToStr(exPath)

			if caseInsensitive {
				exPathStr = strings.ToLower(exPathStr)
			}

			if inPathStr, exists := exPathToInPath[exPathStr]; exists {
				inPath := common.StrToPath(inPathStr)[1:]
				chunk.MetaData.PathInSchema = inPath
			}
		}
	}
}
