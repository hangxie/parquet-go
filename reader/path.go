package reader

import (
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

func schemaRootInName(sh *schema.SchemaHandler) string {
	if sh == nil || len(sh.Infos) == 0 || sh.Infos[0] == nil {
		return ""
	}
	return sh.Infos[0].InName
}

func schemaRootExName(sh *schema.SchemaHandler) string {
	if sh == nil || len(sh.Infos) == 0 || sh.Infos[0] == nil {
		return ""
	}
	return sh.Infos[0].ExName
}

func lookupExternalPath(sh *schema.SchemaHandler, path string, caseInsensitive bool) (string, bool) {
	if sh == nil || sh.ExPathToInPath == nil {
		return "", false
	}
	if inPath, ok := sh.ExPathToInPath[path]; ok {
		return inPath, true
	}
	if !caseInsensitive {
		return "", false
	}

	foldedPath := strings.ToLower(path)
	for exPath, inPath := range sh.ExPathToInPath {
		if strings.ToLower(exPath) == foldedPath {
			return inPath, true
		}
	}
	return "", false
}

func columnPathToInPath(sh *schema.SchemaHandler, path []string, caseInsensitive bool) string {
	rootInName := schemaRootInName(sh)
	if rootInName == "" {
		return common.PathToStr(path)
	}

	inPath := common.PathToStr(append([]string{rootInName}, path...))
	if sh != nil && sh.MapIndex != nil {
		if _, ok := sh.MapIndex[inPath]; ok {
			return inPath
		}
	}

	rootExName := schemaRootExName(sh)
	if rootExName == "" {
		return inPath
	}

	exPath := common.PathToStr(append([]string{rootExName}, path...))
	if mappedPath, ok := lookupExternalPath(sh, exPath, caseInsensitive); ok {
		return mappedPath
	}
	return inPath
}

func columnMetaDataForRead(sh *schema.SchemaHandler, meta *parquet.ColumnMetaData, caseInsensitive bool) *parquet.ColumnMetaData {
	if meta == nil {
		return nil
	}

	inPath := common.StrToPath(columnPathToInPath(sh, meta.GetPathInSchema(), caseInsensitive))
	if len(inPath) > 0 {
		inPath = inPath[1:]
	}

	metaCopy := *meta
	metaCopy.PathInSchema = append([]string(nil), inPath...)
	return &metaCopy
}
