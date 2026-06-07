package reader

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
)

// lookupColumnKey returns the configured key for a metadata PathInSchema.
func (pr *ParquetReader) lookupColumnKey(path []string) ([]byte, bool) {
	fullPath := pr.fullExternalColumnPath(path)
	if key, ok := pr.lookupColumnKeyPath(fullPath); ok {
		return key, true
	}
	if !pr.columnKeysFullPath {
		rootlessPath := common.PathToStr(path)
		if rootlessPath == fullPath {
			return nil, false
		}
		if key, ok := pr.lookupColumnKeyPath(rootlessPath); ok {
			return key, true
		}
	}
	return nil, false
}

// validateColumnKeyPaths rejects paths that are not schema leaves, catching typos
// before decrypting.
func (pr *ParquetReader) validateColumnKeyPaths() error {
	if len(pr.columnKeys) == 0 || pr.SchemaHandler == nil {
		return nil
	}
	if pr.columnKeysFullPath {
		return nil
	}
	rootExName := pr.SchemaHandler.GetRootExName()
	valid := pr.leafColumnFullExternalPaths()
	normalized := make(map[string][]byte, len(pr.columnKeys))
	for path, key := range pr.columnKeys {
		fullPath := common.PathToStr(append([]string{rootExName}, common.StrToPath(path)...))
		matchedPath, ok, ambiguous := pr.matchLeafColumnFullExternalPath(valid, fullPath)
		if ambiguous {
			return fmt.Errorf(
				"WithColumnKey: path %q resolves to %q, which ambiguously matches multiple schema columns when WithCaseInsensitive is enabled",
				strings.ReplaceAll(path, common.ParGoPathDelimiter, "."),
				strings.ReplaceAll(fullPath, common.ParGoPathDelimiter, "."),
			)
		}
		if !ok {
			return fmt.Errorf(
				"WithColumnKey: path %q resolves to %q, which does not match any schema column",
				strings.ReplaceAll(path, common.ParGoPathDelimiter, "."),
				strings.ReplaceAll(fullPath, common.ParGoPathDelimiter, "."),
			)
		}
		if _, exists := normalized[matchedPath]; exists {
			return fmt.Errorf(
				"WithColumnKey: multiple configured paths resolve to schema column %q",
				strings.ReplaceAll(matchedPath, common.ParGoPathDelimiter, "."),
			)
		}
		normalized[matchedPath] = key
	}
	pr.columnKeys = normalized
	pr.columnKeysFullPath = true
	return nil
}

func (pr *ParquetReader) lookupColumnKeyPath(path string) ([]byte, bool) {
	if key, ok := pr.columnKeys[path]; ok {
		return key, true
	}
	if !pr.caseInsensitive {
		return nil, false
	}
	foldedPath := strings.ToLower(path)
	for configuredPath, key := range pr.columnKeys {
		if strings.ToLower(configuredPath) == foldedPath {
			return key, true
		}
	}
	return nil, false
}

func (pr *ParquetReader) matchLeafColumnFullExternalPath(valid map[string]struct{}, path string) (string, bool, bool) {
	if _, ok := valid[path]; ok {
		return path, true, false
	}
	if !pr.caseInsensitive {
		return "", false, false
	}
	foldedPath := strings.ToLower(path)
	var match string
	for validPath := range valid {
		if strings.ToLower(validPath) != foldedPath {
			continue
		}
		if match != "" {
			return "", false, true
		}
		match = validPath
	}
	if match == "" {
		return "", false, false
	}
	return match, true, false
}

func (pr *ParquetReader) fullExternalColumnPath(path []string) string {
	if pr.SchemaHandler == nil || pr.SchemaHandler.GetRootExName() == "" {
		return common.PathToStr(path)
	}
	return common.PathToStr(append([]string{pr.SchemaHandler.GetRootExName()}, path...))
}

// leafColumnFullExternalPaths returns full external paths for leaf columns.
func (pr *ParquetReader) leafColumnFullExternalPaths() map[string]struct{} {
	paths := make(map[string]struct{})
	sh := pr.SchemaHandler
	if sh == nil {
		return paths
	}
	for _, inPath := range sh.ValueColumns {
		if exPath, ok := sh.InPathToExPath[inPath]; ok {
			paths[exPath] = struct{}{}
		}
	}
	return paths
}
