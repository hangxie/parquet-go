package common

import (
	"strings"
)

const (
	ParGoPathDelimiter  = "\x01"
	ParGoRootInName     = "Parquet_go_root"
	ParGoRootExName     = "parquet_go_root"
	DefaultPageSize     = 8 * 1024
	DefaultRowGroupSize = 128 * 1024 * 1024
)

// . -> \x01
func ReformPathStr(pathStr string) string {
	return strings.ReplaceAll(pathStr, ".", ParGoPathDelimiter)
}

// Convert path slice to string
func PathToStr(path []string) string {
	return strings.Join(path, ParGoPathDelimiter)
}

// Convert string to path slice
func StrToPath(str string) []string {
	return strings.Split(str, ParGoPathDelimiter)
}

// Get the pathStr index in a path
func PathStrIndex(str string) int {
	return len(strings.Split(str, ParGoPathDelimiter))
}

func IsChildPath(parent, child string) bool {
	ln := len(parent)
	return strings.HasPrefix(child, parent) && (len(child) == ln || child[ln] == ParGoPathDelimiter[0])
}

func ToPtr[T any](value T) *T {
	return &value
}
