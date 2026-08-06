package reader

import (
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// schemaRootInName reports the root element's internal name and whether a root exists.
func schemaRootInName(sh *schema.SchemaHandler) (string, bool) {
	// parquet-mr names the root "", so presence is reported separately: treating
	// "" as "no root" drops the root prefix and no column ever matches.
	if sh == nil || len(sh.Infos) == 0 || sh.Infos[0] == nil {
		return "", false
	}
	return sh.Infos[0].InName, true
}

// schemaRootExName reports the root element's external name and whether a root exists.
func schemaRootExName(sh *schema.SchemaHandler) (string, bool) {
	if sh == nil || len(sh.Infos) == 0 || sh.Infos[0] == nil {
		return "", false
	}
	return sh.Infos[0].ExName, true
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
	rootInName, ok := schemaRootInName(sh)
	if !ok {
		return common.PathToStr(path)
	}

	inPath := common.PathToStr(append([]string{rootInName}, path...))
	if sh != nil && sh.MapIndex != nil {
		if _, ok := sh.MapIndex[inPath]; ok {
			return inPath
		}
	}

	// The root is known to exist by now, and "" is still a name to build a path from.
	rootExName, _ := schemaRootExName(sh)

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
