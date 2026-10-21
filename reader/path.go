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

// columnPathToInPath maps a footer column path to sh; fileSchema, when set, supplies the
// file's own schema so LIST wrappers can match under other names.
func columnPathToInPath(sh *schema.SchemaHandler, fileSchema func() *schema.SchemaHandler, path []string, caseInsensitive bool) string {
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
	if fileSchema != nil {
		if mappedPath, ok := listAliasInPath(sh, fileSchema(), rootInName, rootExName, path, caseInsensitive); ok {
			return mappedPath
		}
	}
	return inPath
}

// listAliasInPath resolves path while letting a LIST in sh match a file LIST with other wrapper names.
func listAliasInPath(sh, fileSH *schema.SchemaHandler, rootInName, rootExName string, path []string, caseInsensitive bool) (string, bool) {
	if sh.MapIndex == nil || sh.InPathToExPath == nil || fileSH == nil || fileSH.ExPathToInPath == nil {
		return "", false
	}
	inPath, exPath := rootInName, rootExName
	fileExPath := fileSH.GetRootExName()
	for i := 0; i < len(path); i++ {
		fileExPath += common.ParGoPathDelimiter + path[i]
		// Both sides must be a three-level LIST: a legacy file group that is the
		// element itself must not have its struct layer skipped.
		if listInPath, ok := listElementInPath(sh, inPath); ok && i+1 < len(path) && isFileListWrapper(fileSH, fileExPath, caseInsensitive) {
			inPath, exPath = listInPath, sh.InPathToExPath[listInPath]
			i++
			fileExPath += common.ParGoPathDelimiter + path[i]
			continue
		}
		next, ok := lookupExternalPath(sh, exPath+common.ParGoPathDelimiter+path[i], caseInsensitive)
		if !ok {
			return "", false
		}
		inPath, exPath = next, sh.InPathToExPath[next]
	}
	return inPath, true
}

// isFileListWrapper reports whether the file element at exPath is a LIST's repeated wrapper.
func isFileListWrapper(fileSH *schema.SchemaHandler, exPath string, caseInsensitive bool) bool {
	inPath, ok := lookupExternalPath(fileSH, exPath, caseInsensitive)
	return ok && fileSH.IsListWrapper(fileSH.MapIndex[inPath])
}

// listElementInPath returns the element path of the LIST group at inPath, if it is one.
func listElementInPath(sh *schema.SchemaHandler, inPath string) (string, bool) {
	idx, ok := sh.MapIndex[inPath]
	if !ok || int(idx) >= len(sh.SchemaElements) || sh.SchemaElements[idx] == nil {
		return "", false
	}
	// converted_type is optional, so a LIST may carry only the logical annotation.
	if elem := sh.SchemaElements[idx]; elem.GetConvertedType() != parquet.ConvertedType_LIST && (elem.LogicalType == nil || !elem.LogicalType.IsSetLIST()) {
		return "", false
	}
	elementPath := common.PathToStr([]string{inPath, "List", "Element"})
	if _, ok := sh.MapIndex[elementPath]; !ok {
		return "", false
	}
	return elementPath, true
}

func columnMetaDataForRead(sh *schema.SchemaHandler, fileSchema func() *schema.SchemaHandler, meta *parquet.ColumnMetaData, caseInsensitive bool) *parquet.ColumnMetaData {
	if meta == nil {
		return nil
	}

	inPath := common.StrToPath(columnPathToInPath(sh, fileSchema, meta.GetPathInSchema(), caseInsensitive))
	if len(inPath) > 0 {
		inPath = inPath[1:]
	}

	metaCopy := *meta
	metaCopy.PathInSchema = append([]string(nil), inPath...)
	return &metaCopy
}
