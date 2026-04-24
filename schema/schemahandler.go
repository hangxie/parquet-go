package schema

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

/*
PathMap Example
            root(a dummy root)  (Path: "root", Children: A)
             |
             A  (Path:"root/A", Childend: B,C)
        /           \
B(Path:"root/A/B")   C(Path:"root/A/C")
*/

// PathMapType records the path and its children; This is used in Marshal for improve performance.
type PathMapType struct {
	Path     string
	Children map[string]*PathMapType
}

func NewPathMap(path string) *PathMapType {
	pathMap := new(PathMapType)
	pathMap.Path = path
	pathMap.Children = make(map[string]*PathMapType)
	return pathMap
}

func (pmt *PathMapType) Add(path []string) {
	ln := len(path)
	if ln <= 1 {
		return
	}
	c := path[1]
	if _, ok := pmt.Children[c]; !ok {
		pmt.Children[c] = NewPathMap(pmt.Path + common.ParGoPathDelimiter + c)
	}
	pmt.Children[c].Add(path[1:])
}

// SchemaHandler stores the schema data
type SchemaHandler struct {
	SchemaElements []*parquet.SchemaElement
	MapIndex       map[string]int32
	IndexMap       map[int32]string
	PathMap        *PathMapType
	Infos          []*common.Tag

	InPathToExPath map[string]string
	ExPathToInPath map[string]string

	ValueColumns []string

	// Cached data for performance
	childrenMap  [][]int32      // childrenMap[i] contains child indices of element i
	elementTypes []reflect.Type // cached types from GetTypes()

	// VariantSchemas maps variant group paths to their schema info.
	// Populated during schema initialization for efficient lookup during unmarshaling.
	VariantSchemas map[string]*VariantSchemaInfo
}

// setValueColumns collects leaf nodes' full path in SchemaHandler.ValueColumns
func (sh *SchemaHandler) setValueColumns() {
	for i := range len(sh.SchemaElements) {
		schema := sh.SchemaElements[i]
		if schema == nil {
			continue
		}
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			if pathStr, exists := sh.IndexMap[int32(i)]; exists {
				sh.ValueColumns = append(sh.ValueColumns, pathStr)
			}
		}
	}
}

func (sh *SchemaHandler) GetColumnNum() int64 {
	return int64(len(sh.ValueColumns))
}

// ValidateEncodingsForDataPageVersion checks that all field encodings are compatible
// with the specified data page version. Returns an error if any encoding is incompatible.
func (sh *SchemaHandler) ValidateEncodingsForDataPageVersion(version int32) error {
	for i, info := range sh.Infos {
		if info == nil {
			continue
		}
		// Skip non-leaf elements (groups don't have encodings)
		if sh.SchemaElements[i].GetNumChildren() > 0 {
			continue
		}
		if err := common.ValidateEncodingForDataPageVersion(info.InName, info.Encoding, version); err != nil {
			return err
		}
	}
	return nil
}

// setPathMap builds the PathMap from leaf SchemaElement
func (sh *SchemaHandler) setPathMap() {
	sh.PathMap = NewPathMap(sh.GetRootInName())
	for i := range len(sh.SchemaElements) {
		schema := sh.SchemaElements[i]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			pathStr := sh.IndexMap[int32(i)]
			sh.PathMap.Add(common.StrToPath(pathStr))
		}
	}
}

// setVariantSchemas identifies and indexes all VARIANT schema groups.
// This enables efficient lookup during unmarshaling for shredded variant reconstruction.
func (sh *SchemaHandler) setVariantSchemas() {
	sh.VariantSchemas = make(map[string]*VariantSchemaInfo)

	// Build children map if not already done
	if sh.childrenMap == nil {
		sh.childrenMap = sh.buildChildrenMap()
	}

	// Scan for variant groups
	for i, elem := range sh.SchemaElements {
		if elem.LogicalType != nil && elem.LogicalType.VARIANT != nil {
			children := sh.childrenMap[i]
			if info := sh.getVariantSchemaInfo(int32(i), children); info != nil {
				path := sh.IndexMap[int32(i)]
				sh.VariantSchemas[path] = info
			}
		}
	}
}

// GetVariantSchemaInfo returns the VariantSchemaInfo for a given path, or nil if not a variant.
func (sh *SchemaHandler) GetVariantSchemaInfo(path string) *VariantSchemaInfo {
	if sh.VariantSchemas == nil {
		return nil
	}
	return sh.VariantSchemas[path]
}

// GetRepetitionType returns the repetition type of a column by it's schema path
func (sh *SchemaHandler) GetRepetitionType(path []string) (parquet.FieldRepetitionType, error) {
	pathStr := common.PathToStr(path)
	if index, ok := sh.MapIndex[pathStr]; ok {
		if sh.SchemaElements[index] == nil {
			return 0, fmt.Errorf("schema element at index %d is nil", index)
		}
		return sh.SchemaElements[index].GetRepetitionType(), nil
	}
	return 0, fmt.Errorf("name not in schema")
}

// MaxDefinitionLevel returns the max definition level type of a column by it's schema path
func (sh *SchemaHandler) MaxDefinitionLevel(path []string) (int32, error) {
	var res int32 = 0
	ln := len(path)
	for i := 2; i <= ln; i++ {
		rt, err := sh.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}
		if rt != parquet.FieldRepetitionType_REQUIRED {
			res++
		}
	}
	return res, nil
}

// MaxRepetitionLevel returns the max repetition level type of a column by it's schema path
func (sh *SchemaHandler) GetRepetitionLevelIndex(path []string, rl int32) (int32, error) {
	var res int32 = 0
	ln := len(path)
	for i := 2; i <= ln; i++ {
		rt, err := sh.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}
		if rt == parquet.FieldRepetitionType_REPEATED {
			res++
		}

		if res == rl {
			return int32(i - 1), nil
		}
	}
	return res, fmt.Errorf("rl = %d not found in path = %v", rl, path)
}

// MaxRepetitionLevel returns the max repetition level type of a column by it's schema path
func (sh *SchemaHandler) MaxRepetitionLevel(path []string) (int32, error) {
	var res int32 = 0
	ln := len(path)
	for i := 2; i <= ln; i++ {
		rt, err := sh.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}
		if rt == parquet.FieldRepetitionType_REPEATED {
			res++
		}
	}
	return res, nil
}

func (sh *SchemaHandler) GetInName(index int) string {
	return sh.Infos[index].InName
}

func (sh *SchemaHandler) GetExName(index int) string {
	return sh.Infos[index].ExName
}

func (sh *SchemaHandler) CreateInExMap() {
	// use DFS get path of schema
	sh.ExPathToInPath, sh.InPathToExPath = map[string]string{}, map[string]string{}
	schemas := sh.SchemaElements
	ln := int32(len(schemas))
	var pos int32 = 0
	stack := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}
			item := [2]int32{pos, schemas[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else { // leaf node
			inPath, exPath := make([]string, 0), make([]string, 0)
			for i := range len(stack) {
				inPath = append(inPath, sh.Infos[stack[i][0]].InName)
				exPath = append(exPath, sh.Infos[stack[i][0]].ExName)

				inPathStr, exPathStr := common.PathToStr(inPath), common.PathToStr(exPath)
				sh.ExPathToInPath[exPathStr] = inPathStr
				sh.InPathToExPath[inPathStr] = exPathStr
			}
			stack = stack[:len(stack)-1]
		}
	}
}

// Convert a path to internal path
func (sh *SchemaHandler) ConvertToInPathStr(pathStr string) (string, error) {
	if _, ok := sh.InPathToExPath[pathStr]; ok {
		return pathStr, nil
	}

	if res, ok := sh.ExPathToInPath[pathStr]; ok {
		return res, nil
	}

	return "", fmt.Errorf("path not found: %v", pathStr)
}

// Get root name from the schema handler
func (sh *SchemaHandler) GetRootInName() string {
	if len(sh.SchemaElements) <= 0 {
		return ""
	}
	return sh.Infos[0].InName
}

func (sh *SchemaHandler) GetRootExName() string {
	if len(sh.SchemaElements) <= 0 {
		return ""
	}
	return sh.Infos[0].ExName
}
