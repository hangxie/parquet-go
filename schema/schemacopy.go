package schema

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
)

// NewSchemaHandlerFromSchemaHandler rebuilds a handler without validating its schema tree.
//
// Deprecated: use NewSchemaHandlerFromSchemaHandlerWithError.
func NewSchemaHandlerFromSchemaHandler(sh *SchemaHandler) *SchemaHandler {
	return newSchemaHandlerFromSchemaHandler(sh)
}

// NewSchemaHandlerFromSchemaHandlerWithError checks tree completeness and nonempty, unique sibling names before rebuilding.
func NewSchemaHandlerFromSchemaHandlerWithError(sh *SchemaHandler) (*SchemaHandler, error) {
	if sh == nil {
		return nil, fmt.Errorf("schema handler is nil")
	}
	if err := validateSchemaList(sh.SchemaElements); err != nil {
		return nil, err
	}
	if len(sh.Infos) != len(sh.SchemaElements) {
		return nil, fmt.Errorf("schema info count %d does not match element count %d", len(sh.Infos), len(sh.SchemaElements))
	}
	for i, info := range sh.Infos {
		if info == nil {
			return nil, fmt.Errorf("schema info %d is nil", i)
		}
		if info.InName == "" {
			return nil, fmt.Errorf("schema info %d has empty InName", i)
		}
		if info.ExName == "" {
			return nil, fmt.Errorf("schema info %d has empty ExName", i)
		}
	}
	if err := sh.ValidateUniqueNames(); err != nil {
		return nil, fmt.Errorf("validate column names: %w", err)
	}
	return newSchemaHandlerFromSchemaHandler(sh), nil
}

func newSchemaHandlerFromSchemaHandler(sh *SchemaHandler) *SchemaHandler {
	schemaHandler := new(SchemaHandler)
	schemaHandler.MapIndex = make(map[string]int32)
	schemaHandler.IndexMap = make(map[int32]string)
	schemaHandler.InPathToExPath = make(map[string]string)
	schemaHandler.ExPathToInPath = make(map[string]string)
	schemaHandler.SchemaElements = sh.SchemaElements

	schemaHandler.Infos = make([]*common.Tag, len(sh.SchemaElements))
	for i := range len(sh.SchemaElements) {
		InName, ExName := sh.GetInName(i), sh.GetExName(i)
		schemaHandler.Infos[i] = &common.Tag{
			InName: InName,
			ExName: ExName,
		}
	}
	schemaHandler.CreateInExMap()

	// use DFS get path of schema
	ln := int32(len(sh.SchemaElements))
	var pos int32 = 0
	stack := make([][2]int32, 0) // stack item[0]: index of schemas; item[1]: numChildren
	for pos < ln || len(stack) > 0 {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}
			item := [2]int32{pos, sh.SchemaElements[pos].GetNumChildren()}
			stack = append(stack, item)
			pos++
		} else {
			path := make([]string, 0)
			for i := range len(stack) {
				inname := schemaHandler.Infos[stack[i][0]].InName
				path = append(path, inname)
			}
			topPos := stack[len(stack)-1][0]
			schemaHandler.MapIndex[common.PathToStr(path)] = topPos
			schemaHandler.IndexMap[topPos] = common.PathToStr(path)
			stack = stack[:len(stack)-1]
		}
	}
	schemaHandler.setPathMap()
	schemaHandler.setValueColumns()
	schemaHandler.setVariantSchemas()

	return schemaHandler
}
