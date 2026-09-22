package marshal

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/types"
)

func appendNilToChildTables(basePath string, node *Node, res map[string]*layout.Table) {
	for key, table := range res {
		if common.IsChildPath(basePath, key) {
			table.Values = append(table.Values, nil)
			table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
			table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
		}
	}
}

func marshalJSONRealMap(node *Node, pathStr string, res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, nodeBuf *NodeBufType, stack []*Node) []*Node {
	keys := node.Val.MapKeys()
	pathStr = pathStr + common.ParGoPathDelimiter + "Key_value"
	if len(keys) <= 0 {
		appendNilToChildTables(node.PathMap.Path, node, res)
	}

	rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))
	for j := len(keys) - 1; j >= 0; j-- {
		key := keys[j]
		value := node.Val.MapIndex(key).Elem()

		newNode := nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["Key_value"].Children["Key"]
		newNode.Val = key
		newNode.DL = node.DL + 1
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		stack = append(stack, newNode)

		newNode = nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["Key_value"].Children["Value"]
		newNode.Val = value
		newNode.DL = node.DL + 1
		newSchemaIndex := schemaHandler.MapIndex[newNode.PathMap.Path]
		newSchema := schemaHandler.SchemaElements[newSchemaIndex]
		if newSchema.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
			newNode.DL++
		}
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		stack = append(stack, newNode)
	}
	return stack
}

func marshalJSONStruct(node *Node, res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, nodeBuf *NodeBufType, stack []*Node) []*Node {
	keys := node.Val.MapKeys()
	keysMap := make(map[string]int)
	for j := range keys {
		keysMap[common.StringToVariableName(keys[j].String())] = j
	}
	for key := range node.PathMap.Children {
		ki, ok := keysMap[key]
		if ok && node.Val.MapIndex(keys[ki]).Elem().IsValid() {
			newNode := nodeBuf.GetNode()
			newNode.PathMap = node.PathMap.Children[key]
			newNode.Val = node.Val.MapIndex(keys[ki]).Elem()
			newNode.RL = node.RL
			newNode.DL = node.DL
			newSchemaIndex := schemaHandler.MapIndex[newNode.PathMap.Path]
			newSchema := schemaHandler.SchemaElements[newSchemaIndex]
			if newSchema.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
				newNode.DL++
			}
			stack = append(stack, newNode)
		} else {
			appendNilToChildTables(node.PathMap.Children[key].Path, node, res)
		}
	}
	return stack
}

func marshalJSONList(node *Node, pathStr string, res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, nodeBuf *NodeBufType, stack []*Node) []*Node {
	ln := node.Val.Len()
	pathStr = pathStr + common.ParGoPathDelimiter + "List" + common.ParGoPathDelimiter + "Element"
	if ln <= 0 {
		appendNilToChildTables(node.PathMap.Path, node, res)
		return stack
	}
	rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))
	for j := ln - 1; j >= 0; j-- {
		newNode := nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["List"].Children["Element"]
		newNode.Val = node.Val.Index(j).Elem()
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		newNode.DL = node.DL + 1
		newSchemaIndex := schemaHandler.MapIndex[newNode.PathMap.Path]
		newSchema := schemaHandler.SchemaElements[newSchemaIndex]
		if newSchema.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
			newNode.DL++
		}
		stack = append(stack, newNode)
	}
	return stack
}

func marshalJSONRepeated(node *Node, pathStr string, res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, nodeBuf *NodeBufType, stack []*Node) []*Node {
	ln := node.Val.Len()
	if ln <= 0 {
		appendNilToChildTables(node.PathMap.Path, node, res)
		return stack
	}
	rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))
	for j := ln - 1; j >= 0; j-- {
		newNode := nodeBuf.GetNode()
		newNode.PathMap = node.PathMap
		newNode.Val = node.Val.Index(j).Elem()
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		newNode.DL = node.DL + 1
		stack = append(stack, newNode)
	}
	return stack
}

func marshalJSONPrimitive(node *Node, se *parquet.SchemaElement, res map[string]*layout.Table, opts []types.ValueOption) error {
	table := res[node.PathMap.Path]
	val, err := types.JSONTypeToParquetTypeWithLogical(node.Val, se.Type, se.ConvertedType, se.LogicalType, int(se.GetTypeLength()), int(se.GetScale()), opts...)
	if err != nil {
		return fmt.Errorf("convert JSON value for %s: %w", node.PathMap.Path, err)
	}
	table.Values = append(table.Values, val)
	table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
	table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
	return nil
}

// isListColumn and isMapColumn recognise both spellings: a schema built from raw
// SchemaElement values may carry only the logical type a tag would have backfilled.
func isListColumn(se *parquet.SchemaElement) bool {
	if lT := se.LogicalType; lT != nil && lT.IsSetLIST() {
		return true
	}
	return se.GetConvertedType() == parquet.ConvertedType_LIST
}

func isMapColumn(se *parquet.SchemaElement) bool {
	if lT := se.LogicalType; lT != nil && lT.IsSetMAP() {
		return true
	}
	return se.GetConvertedType() == parquet.ConvertedType_MAP
}

func processJSONNode(node *Node, res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, nodeBuf *NodeBufType, stack []*Node, opts []types.ValueOption) ([]*Node, error) {
	pathStr := node.PathMap.Path
	schemaIndex, ok := schemaHandler.MapIndex[pathStr]
	if !ok {
		return stack, nil
	}

	se := schemaHandler.SchemaElements[schemaIndex]

	if newStack, handled, err := HandleVariant(node, se, res, schemaHandler, nodeBuf, stack); err != nil {
		return nil, fmt.Errorf("handle variant for %s: %w", pathStr, err)
	} else if handled {
		return newStack, nil
	}

	if newStack, handled, err := HandleUnknown(node, se, res, stack); err != nil {
		return nil, fmt.Errorf("handle UNKNOWN for %s: %w", pathStr, err)
	} else if handled {
		return newStack, nil
	}

	// The value's shape picks the handler, but only among the shapes the column can hold.
	// Dispatching on the Go kind alone dropped an object into a primitive column without
	// a word, and spread an array across a column that takes one value.
	switch node.Val.Type().Kind() {
	case reflect.Map:
		// An object fills a group's fields or a MAP's entries. A LIST's children are its
		// repeated element, never the object's keys, so it filled none and left a null.
		switch {
		case isMapColumn(se):
			stack = marshalJSONRealMap(node, pathStr, res, schemaHandler, nodeBuf, stack)
		case isListColumn(se):
			return nil, fmt.Errorf("column %s is a LIST and cannot take a JSON object", pathStr)
		case se.GetNumChildren() > 0:
			stack = marshalJSONStruct(node, res, schemaHandler, nodeBuf, stack)
		case se.LogicalType != nil && (se.LogicalType.IsSetGEOMETRY() || se.LogicalType.IsSetGEOGRAPHY()):
			// The one primitive column whose value is an object: every geospatial
			// rendering is one, so the converter takes it whole.
			if err := marshalJSONPrimitive(node, se, res, opts); err != nil {
				return nil, fmt.Errorf("marshal JSON primitive for %s: %w", pathStr, err)
			}
		default:
			return nil, fmt.Errorf("column %s is primitive and cannot take a JSON object", pathStr)
		}
	case reflect.Slice:
		switch {
		case isListColumn(se):
			stack = marshalJSONList(node, pathStr, res, schemaHandler, nodeBuf, stack)
		case se.GetRepetitionType() == parquet.FieldRepetitionType_REPEATED:
			stack = marshalJSONRepeated(node, pathStr, res, schemaHandler, nodeBuf, stack)
		default:
			return nil, fmt.Errorf("column %s is not repeated and cannot take a JSON array", pathStr)
		}
	default:
		if err := marshalJSONPrimitive(node, se, res, opts); err != nil {
			return nil, fmt.Errorf("marshal JSON primitive for %s: %w", pathStr, err)
		}
	}
	return stack, nil
}

// MarshalJSON converts JSON rows to column tables, reading values under opts.
func MarshalJSON(ss []any, schemaHandler *schema.SchemaHandler, opts ...types.ValueOption) (tb *map[string]*layout.Table, err error) {
	res, err := setupTableMap(schemaHandler, len(ss))
	if err != nil {
		return nil, fmt.Errorf("setup table map: %w", err)
	}
	decoder := jsonRowDecoder{enforceUTF8: types.NewValueConfig(opts...).EnforceUTF8}
	pathMap := schemaHandler.PathMap
	nodeBuf := NewNodeBuf(1)

	stack := make([]*Node, 0, 100)
	for i := range ss {
		stack = stack[:0]
		nodeBuf.Reset()

		ui, err := decoder.decode(ss[i])
		if err != nil {
			return nil, fmt.Errorf("decode JSON row %d: %w", i, err)
		}

		node := nodeBuf.GetNode()
		node.Val = reflect.ValueOf(ui)
		node.PathMap = pathMap
		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node = stack[ln-1]
			stack = stack[:ln-1]

			if stack, err = processJSONNode(node, res, schemaHandler, nodeBuf, stack, opts); err != nil {
				return nil, fmt.Errorf("process JSON node row %d: %w", i, err)
			}
		}
	}

	return &res, nil
}
