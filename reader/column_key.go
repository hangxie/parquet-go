package reader

import (
	"fmt"
	"strings"

	"github.com/hangxie/parquet-go/v3/common"
)

// lookupColumnKey returns the configured key for a metadata PathInSchema.
func (pr *ParquetReader) lookupColumnKey(path []string) ([]byte, bool) {
	key, ok := pr.columnKeys[pr.fullExternalColumnPath(path)]
	if ok {
		return key, true
	}
	if !pr.columnKeysFullPath {
		key, ok = pr.columnKeys[common.PathToStr(path)]
		if ok {
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
		if _, ok := valid[fullPath]; !ok {
			return fmt.Errorf(
				"WithColumnKey: path %q resolves to %q, which does not match any schema column",
				strings.ReplaceAll(path, common.ParGoPathDelimiter, "."),
				strings.ReplaceAll(fullPath, common.ParGoPathDelimiter, "."),
			)
		}
		normalized[fullPath] = key
	}
	pr.columnKeys = normalized
	pr.columnKeysFullPath = true
	return nil
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
