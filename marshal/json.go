package marshal

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"

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

func marshalJSONPrimitive(node *Node, se *parquet.SchemaElement, res map[string]*layout.Table) error {
	table := res[node.PathMap.Path]
	val, err := types.JSONTypeToParquetTypeWithLogical(node.Val, se.Type, se.ConvertedType, se.LogicalType, int(se.GetTypeLength()), int(se.GetScale()))
	if err != nil {
		return err
	}
	table.Values = append(table.Values, val)
	table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
	table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
	return nil
}

func processJSONNode(node *Node, res map[string]*layout.Table, schemaHandler *schema.SchemaHandler, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	pathStr := node.PathMap.Path
	schemaIndex, ok := schemaHandler.MapIndex[pathStr]
	if !ok {
		return stack, nil
	}

	se := schemaHandler.SchemaElements[schemaIndex]

	if newStack, handled, err := HandleVariant(node, se, res, schemaHandler, nodeBuf, stack); err != nil {
		return nil, err
	} else if handled {
		return newStack, nil
	}

	switch node.Val.Type().Kind() {
	case reflect.Map:
		if se.GetConvertedType() == parquet.ConvertedType_MAP {
			stack = marshalJSONRealMap(node, pathStr, res, schemaHandler, nodeBuf, stack)
		} else {
			stack = marshalJSONStruct(node, res, schemaHandler, nodeBuf, stack)
		}
	case reflect.Slice:
		if se.GetConvertedType() == parquet.ConvertedType_LIST {
			stack = marshalJSONList(node, pathStr, res, schemaHandler, nodeBuf, stack)
		} else {
			stack = marshalJSONRepeated(node, pathStr, res, schemaHandler, nodeBuf, stack)
		}
	default:
		if err := marshalJSONPrimitive(node, se, res); err != nil {
			return nil, err
		}
	}
	return stack, nil
}

// ss is []string
func MarshalJSON(ss []any, schemaHandler *schema.SchemaHandler) (tb *map[string]*layout.Table, err error) {
	res, err := setupTableMap(schemaHandler, len(ss))
	if err != nil {
		return nil, err
	}
	pathMap := schemaHandler.PathMap
	nodeBuf := NewNodeBuf(1)

	stack := make([]*Node, 0, 100)
	for i := range ss {
		stack = stack[:0]
		nodeBuf.Reset()

		var d *json.Decoder
		switch t := ss[i].(type) {
		case string:
			d = json.NewDecoder(strings.NewReader(t))
		case []byte:
			d = json.NewDecoder(bytes.NewReader(t))
		}
		d.UseNumber()
		var ui any
		if err := d.Decode(&ui); err != nil {
			return nil, err
		}

		node := nodeBuf.GetNode()
		node.Val = reflect.ValueOf(ui)
		node.PathMap = pathMap
		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node = stack[ln-1]
			stack = stack[:ln-1]

			if stack, err = processJSONNode(node, res, schemaHandler, nodeBuf, stack); err != nil {
				return nil, err
			}
		}
	}

	return &res, nil
}
