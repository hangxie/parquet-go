package marshal

import (
	"fmt"
	"reflect"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/layout"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/types"
)

type Node struct {
	Val     reflect.Value
	PathMap *schema.PathMapType
	RL      int32
	DL      int32
}

// NodeBuf
type NodeBufType struct {
	Index int
	Buf   []*Node
}

func NewNodeBuf(ln int) *NodeBufType {
	nodeBuf := new(NodeBufType)
	nodeBuf.Index = 0
	nodeBuf.Buf = make([]*Node, ln)
	for i := range ln {
		nodeBuf.Buf[i] = new(Node)
	}
	return nodeBuf
}

func (nbt *NodeBufType) GetNode() *Node {
	if nbt.Index >= len(nbt.Buf) {
		nbt.Buf = append(nbt.Buf, new(Node))
	}
	nbt.Index++
	return nbt.Buf[nbt.Index-1]
}

func (nbt *NodeBufType) Reset() {
	nbt.Index = 0
}

type Marshaler interface {
	Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) (newStack []*Node, err error)
}

type ParquetPtr struct{}

func (p *ParquetPtr) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	if node.Val.IsNil() {
		return stack, nil
	}
	node.Val = node.Val.Elem()
	node.DL++
	stack = append(stack, node)
	return stack, nil
}

type ParquetStruct struct{}

func (p *ParquetStruct) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	var ok bool

	numField := node.Val.Type().NumField()
	for j := range numField {
		tf := node.Val.Type().Field(j)
		name := tf.Name
		newNode := nodeBuf.GetNode()

		// some ignored item
		if newNode.PathMap, ok = node.PathMap.Children[name]; !ok {
			continue
		}

		newNode.Val = node.Val.Field(j)
		newNode.RL = node.RL
		newNode.DL = node.DL
		stack = append(stack, newNode)
	}
	return stack, nil
}

type ParquetMapStruct struct {
	schemaHandler *schema.SchemaHandler
}

func (p *ParquetMapStruct) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	var ok bool

	keys := node.Val.MapKeys()
	if len(keys) <= 0 {
		return stack, nil
	}

	missingKeys := make(map[string]bool)
	for k, typ := range node.PathMap.Children {
		if len(typ.Children) == 0 {
			missingKeys[k] = true
		}
	}
	for j := len(keys) - 1; j >= 0; j-- {
		key := keys[j]
		newNode := nodeBuf.GetNode()

		// some ignored item
		k := key.String()
		if newNode.PathMap, ok = node.PathMap.Children[k]; !ok {
			continue
		}
		missingKeys[k] = false
		v := node.Val.MapIndex(key)
		newNode.RL = node.RL
		newNode.DL = node.DL
		if v.Type().Kind() == reflect.Interface {
			newNode.Val = v.Elem()
			if newNode.Val.IsValid() && p.schemaHandler != nil &&
				p.schemaHandler.SchemaElements != nil && p.schemaHandler.MapIndex != nil {
				if index, exists := p.schemaHandler.MapIndex[newNode.PathMap.Path]; exists &&
					int(index) < len(p.schemaHandler.SchemaElements) {
					if elem := p.schemaHandler.SchemaElements[index]; elem != nil &&
						elem.RepetitionType != nil && *elem.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
						newNode.DL++
					}
				}
			}
		} else {
			newNode.Val = v
		}
		stack = append(stack, newNode)
	}

	var null any
	for k, isMissing := range missingKeys {
		if isMissing {
			newNode := nodeBuf.GetNode()
			newNode.PathMap = node.PathMap.Children[k]
			newNode.Val = reflect.ValueOf(null)
			newNode.RL = node.RL
			newNode.DL = node.DL
			stack = append(stack, newNode)
		}
	}
	return stack, nil
}

type ParquetSlice struct {
	schemaHandler *schema.SchemaHandler
}

func (p *ParquetSlice) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	ln := node.Val.Len()
	pathMap := node.PathMap
	path := node.PathMap.Path
	if p.schemaHandler != nil && p.schemaHandler.SchemaElements != nil && p.schemaHandler.MapIndex != nil {
		if index, exists := p.schemaHandler.MapIndex[node.PathMap.Path]; exists && int(index) < len(p.schemaHandler.SchemaElements) {
			if elem := p.schemaHandler.SchemaElements[index]; elem != nil && elem.RepetitionType != nil && *elem.RepetitionType != parquet.FieldRepetitionType_REPEATED {
				if pmList, ok := pathMap.Children["List"]; ok {
					if pmElement, ok := pmList.Children["Element"]; ok {
						pathMap = pmElement
						path = path + common.PAR_GO_PATH_DELIMITER + "List" + common.PAR_GO_PATH_DELIMITER + "Element"
					} else {
						return stack, nil
					}
				} else {
					return stack, nil
				}
			}
		}
	}
	if ln <= 0 {
		return stack, nil
	}

	rlNow, err := p.schemaHandler.MaxRepetitionLevel(common.StrToPath(path))
	if err != nil {
		return nil, err
	}
	for j := ln - 1; j >= 0; j-- {
		newNode := nodeBuf.GetNode()
		newNode.PathMap = pathMap
		v := node.Val.Index(j)
		if v.Type().Kind() == reflect.Interface {
			newNode.Val = v.Elem()
		} else {
			newNode.Val = v
		}
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		newNode.DL = node.DL + 1
		stack = append(stack, newNode)
	}
	return stack, nil
}

type ParquetMap struct {
	schemaHandler *schema.SchemaHandler
}

func (p *ParquetMap) Marshal(node *Node, nodeBuf *NodeBufType, stack []*Node) ([]*Node, error) {
	path := node.PathMap.Path + common.PAR_GO_PATH_DELIMITER + "Key_value"
	keys := node.Val.MapKeys()
	if len(keys) <= 0 {
		return stack, nil
	}

	rlNow, err := p.schemaHandler.MaxRepetitionLevel(common.StrToPath(path))
	if err != nil {
		return nil, err
	}
	for j := len(keys) - 1; j >= 0; j-- {
		key := keys[j]
		value := node.Val.MapIndex(key)
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
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		stack = append(stack, newNode)
	}
	return stack, nil
}

// Convert the objects to table map. srcInterface is a slice of objects
func Marshal(srcInterface []any, schemaHandler *schema.SchemaHandler) (tb *map[string]*layout.Table, err error) {
	src := reflect.ValueOf(srcInterface)
	res, err := setupTableMap(schemaHandler, len(srcInterface))
	if err != nil {
		return nil, err
	}
	pathMap := schemaHandler.PathMap
	nodeBuf := NewNodeBuf(1)

	for i := range len(schemaHandler.SchemaElements) {
		schema := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			table := layout.NewEmptyTable()
			table.Path = common.StrToPath(pathStr)
			if table.MaxDefinitionLevel, err = schemaHandler.MaxDefinitionLevel(table.Path); err != nil {
				return nil, err
			}
			if table.MaxRepetitionLevel, err = schemaHandler.MaxRepetitionLevel(table.Path); err != nil {
				return nil, err
			}
			table.RepetitionType = schema.GetRepetitionType()
			table.Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
			table.Info = schemaHandler.Infos[i]
			// Pre-size tables under the assumption that they'll be filled.
			table.Values = make([]any, 0, len(srcInterface))
			table.DefinitionLevels = make([]int32, 0, len(srcInterface))
			table.RepetitionLevels = make([]int32, 0, len(srcInterface))
			res[pathStr] = table
		}
	}

	stack := make([]*Node, 0, 100)
	for i := range srcInterface {
		stack = stack[:0]
		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		node.Val = src.Index(i)
		if src.Index(i).Type().Kind() == reflect.Interface {
			node.Val = src.Index(i).Elem()
		}
		node.PathMap = pathMap
		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node = stack[ln-1]
			stack = stack[:ln-1]

			if node.PathMap == nil {
				return nil, fmt.Errorf("internal error: node has nil PathMap")
			}

			schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
			schema := schemaHandler.SchemaElements[schemaIndex]

			if newStack, handled, err := HandleVariant(node, schema, res, schemaHandler, nodeBuf, stack); err != nil {
				return nil, err
			} else if handled {
				stack = newStack
				continue
			}

			tk := reflect.Interface
			if node.Val.IsValid() {
				tk = node.Val.Type().Kind()
			}
			var m Marshaler

			switch tk {
			case reflect.Ptr:
				m = &ParquetPtr{}
			case reflect.Struct:
				m = &ParquetStruct{}
			case reflect.Slice:
				// []byte should be treated as primitive BYTE_ARRAY, not as a LIST
				if node.Val.Type().Elem().Kind() == reflect.Uint8 {
					table := res[node.PathMap.Path]
					schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
					schema := schemaHandler.SchemaElements[schemaIndex]
					var v any
					if node.Val.IsValid() {
						v = node.Val.Interface()
					}
					val, err := types.InterfaceToParquetType(v, schema.Type)
					if err != nil {
						return nil, err
					}
					table.Values = append(table.Values, val)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
					continue
				}
				m = &ParquetSlice{schemaHandler: schemaHandler}
			case reflect.Map:
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				sele := schemaHandler.SchemaElements[schemaIndex]
				if !sele.IsSetConvertedType() {
					m = &ParquetMapStruct{schemaHandler: schemaHandler}
				} else {
					m = &ParquetMap{schemaHandler: schemaHandler}
				}
			default:
				table := res[node.PathMap.Path]
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				schema := schemaHandler.SchemaElements[schemaIndex]
				var v any
				if node.Val.IsValid() {
					v = node.Val.Interface()
				}
				val, err := types.InterfaceToParquetType(v, schema.Type)
				if err != nil {
					return nil, err
				}
				table.Values = append(table.Values, val)
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				continue
			}

			oldLen := len(stack)
			if stack, err = m.Marshal(node, nodeBuf, stack); err != nil {
				return nil, err
			}
			if len(stack) == oldLen {
				path := node.PathMap.Path
				index := schemaHandler.MapIndex[path]
				numChildren := schemaHandler.SchemaElements[index].GetNumChildren()
				if numChildren > int32(0) {
					for key, table := range res {
						if common.IsChildPath(path, key) {
							table.Values = append(table.Values, nil)
							table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
							table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
						}
					}
				} else {
					table := res[path]
					table.Values = append(table.Values, nil)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				}
			}
		}
	}

	return &res, nil
}

func setupTableMap(schemaHandler *schema.SchemaHandler, numElements int) (map[string]*layout.Table, error) {
	tableMap := make(map[string]*layout.Table)
	for i := range len(schemaHandler.SchemaElements) {
		schema := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			table := layout.NewEmptyTable()
			table.Path = common.StrToPath(pathStr)
			var err error
			if table.MaxDefinitionLevel, err = schemaHandler.MaxDefinitionLevel(table.Path); err != nil {
				return nil, err
			}
			if table.MaxRepetitionLevel, err = schemaHandler.MaxRepetitionLevel(table.Path); err != nil {
				return nil, err
			}
			table.RepetitionType = schema.GetRepetitionType()
			table.Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
			table.Info = schemaHandler.Infos[i]
			// Pre-size tables under the assumption that they'll be filled.
			table.Values = make([]any, 0, numElements)
			table.DefinitionLevels = make([]int32, 0, numElements)
			table.RepetitionLevels = make([]int32, 0, numElements)
			tableMap[pathStr] = table
		}
	}
	return tableMap, nil
}

func HandleVariant(
	node *Node,
	schema *parquet.SchemaElement,
	res map[string]*layout.Table,
	schemaHandler *schema.SchemaHandler,
	nodeBuf *NodeBufType,
	stack []*Node,
) ([]*Node, bool, error) {
	if schema.LogicalType == nil || schema.LogicalType.VARIANT == nil {
		return stack, false, nil
	}

	isNil := !node.Val.IsValid()
	if !isNil {
		switch node.Val.Kind() {
		case reflect.Interface, reflect.Ptr, reflect.Slice, reflect.Map:
			isNil = node.Val.IsNil()
		}
	}

	if isNil {
		for key := range node.PathMap.Children {
			newPathStr := node.PathMap.Children[key].Path
			for path, table := range res {
				if common.IsChildPath(newPathStr, path) {
					table.Values = append(table.Values, nil)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				}
			}
		}
		return stack, true, nil
	}

	v, err := types.AnyToVariant(node.Val.Interface())
	if err != nil {
		return nil, true, fmt.Errorf("convert to variant: %w", err)
	}

	// Validate that PathMap has the expected VARIANT children
	valuePathMap := node.PathMap.Children["Value"]
	metadataPathMap := node.PathMap.Children["Metadata"]
	if valuePathMap == nil || metadataPathMap == nil {
		return nil, true, fmt.Errorf("VARIANT schema missing required children (Value and/or Metadata) in PathMap")
	}

	// If the variant group is present, its definition level should be at its max
	childDL, err := schemaHandler.MaxDefinitionLevel(common.StrToPath(node.PathMap.Path))
	if err != nil {
		return nil, true, err
	}

	// Push Metadata
	metaNode := nodeBuf.GetNode()
	metaNode.PathMap = metadataPathMap
	metaNode.Val = reflect.ValueOf(string(v.Metadata))
	metaNode.DL = childDL
	metaNode.RL = node.RL
	stack = append(stack, metaNode)

	// Push Value
	valueNode := nodeBuf.GetNode()
	valueNode.PathMap = valuePathMap
	valueNode.Val = reflect.ValueOf(string(v.Value))
	valueNode.DL = childDL
	valueNode.RL = node.RL
	stack = append(stack, valueNode)

	return stack, true, nil
}
